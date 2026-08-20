//! Call video wiring: watches call lifecycle events and runs the real
//! screen-share pipelines of `mikall-media` — capture → encode once →
//! seal once → lanes when *we* share, tap → pump → decode → frame feed
//! when a *remote* shares, and the sealed-bytes relay when the sharer
//! elects us forwarder.
//!
//! M17 roles per call (all live-switchable, the session never restarts):
//! - **sharer**: sends each sealed frame once — to the elected forwarder
//!   when there is one, to every viewer otherwise (the M16 fallback);
//!   runs the election through the aggregate, assigns the forwarder
//!   in-band, and adapts the single encode from viewer feedback.
//! - **forwarder**: repeats the sharer's sealed bytes to the viewer set
//!   through its own fan-out — never decrypting; a keyframe-run cache
//!   beside its lanes serves late joiners without touching the sharer.
//! - **viewer**: decodes and presents, and reports received/lost to the
//!   sharer every couple of seconds.
//!
//! Same shape as [`crate::voice`]: the engine lives behind the
//! `hardware-video` feature; [`RemoteVideoFrame`] and the feed are always
//! available so frontends subscribe unconditionally. Capture trouble is
//! honest, never fatal.

use std::sync::Arc;

use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

/// 90 kHz wall-clock helpers re-exported for frontends: the frame feed's
/// `ts90k` stamps come from this clock, and the renderer estimates
/// glass-to-glass latency with them.
pub use mikall_media::video::{ts90k_diff_ms, ts90k_now};

/// One decoded remote picture for frontends, RGBA. Shared pixels: the
/// broadcast clones the handle, never the frame.
#[derive(Debug, Clone)]
pub struct RemoteVideoFrame {
    pub call: CallId,
    /// The *origin* of the picture — the sharer, even when the bytes
    /// were carried by a forwarder.
    pub from: IdentityId,
    /// The peer that relayed the frame to us, when it wasn't the sharer
    /// itself ("via <forwarder>" in the viewer pane).
    pub via: Option<IdentityId>,
    pub width: u32,
    pub height: u32,
    pub rgba: Arc<Vec<u8>>,
    /// The sharer's wall-clock capture stamp (90 kHz video clock,
    /// wrapping) — lets the renderer estimate glass-to-glass latency
    /// when both ends share a wall clock (same machine).
    pub ts90k: u32,
    /// When the local receiver handed this picture to the feed — the
    /// decode→render stage is measured from here.
    pub decoded_at: std::time::Instant,
}

#[cfg(feature = "hardware-video")]
pub(crate) use engine::spawn_video_engine;

#[cfg(feature = "hardware-video")]
mod engine {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::{broadcast, mpsc};

    use mikall_app::events::{AppEvent, EventBus};
    use mikall_app::ports::MediaStreamTransport;
    use mikall_app::services::{CallService, IdentityService};
    use mikall_domain::calls::{CallEvent, CallId, CallPhase};
    use mikall_domain::shared::IdentityId;
    use mikall_domain::DomainEvent;
    use mikall_media::capture::{ScapCapture, CAPTURE_FPS, CAPTURE_FPS_HW};
    use mikall_media::codec_h264::{H264Decoder, H264Encoder, MIN_BITRATE_BPS, TARGET_BITRATE_BPS};
    #[cfg(target_os = "macos")]
    use mikall_media::codec_vt::VtH264Encoder;
    use mikall_media::control::{BitrateController, ControlMessage, ControlPlane};
    use mikall_media::frame::CallKey;
    use mikall_media::relay::{run_video_pump, PumpEvent, PumpShared, RelayTarget};
    use mikall_media::video::VideoEncoder;
    use mikall_media::video::{
        run_video_receiver, run_video_sender, video_ssrc_of, DecodedPicture, VideoDecoder,
        VideoFanout, VideoSenderControl, VideoSink,
    };

    use super::RemoteVideoFrame;
    use crate::voice::MediaAlert;

    /// Feedback cadence (spec: coarse, ~every 2 s).
    const REPORT_INTERVAL: Duration = Duration::from_secs(2);
    /// Consecutive lane failures before a peer counts as unreachable for
    /// election purposes. Lane sends are deadline-bounded, but a silently
    /// killed peer's socket keeps *absorbing* writes until kernel and
    /// yamux buffers fill — tens of seconds at screen-share rates — so
    /// this is the slow path.
    const UNREACHABLE_AFTER_FAILURES: u32 = 3;
    /// The fast liveness signal: an assigned forwarder proves liveness
    /// every ~2 s (ack, then feedback like any viewer). Three missed
    /// windows and it counts as unreachable — re-election fires long
    /// before the transport notices.
    const FORWARDER_SILENCE_MS: u64 = 6_500;
    /// A forwarder declared dead stays out of candidacy this long —
    /// enough for its (restarted) lane's failure counters to take over as
    /// the standing exclusion. Without it, a dead peer that is still the
    /// lowest seated identity would be re-elected the moment its fresh
    /// lane counters read zero, stalling viewers in a flap.
    const SUSPECT_QUARANTINE_MS: u64 = 30_000;

    /// Our outbound share: dropping it aborts the encode loop, tears down
    /// the lanes and stops capture — the macOS indicator goes away
    /// because the ScreenCaptureKit stream is stopped, not leaked.
    struct SendPipeline {
        _capture: ScapCapture,
        fanout: Arc<VideoFanout>,
        sender_control: Arc<VideoSenderControl>,
        bitrate: BitrateController,
        /// The forwarder in force (mirrors the aggregate; cached here so
        /// target sync is a comparison, not a lock).
        forwarder: Option<IdentityId>,
        /// The viewer set last assigned to the forwarder.
        assigned_viewers: Vec<IdentityId>,
        /// The forwarder acknowledged its current assignment. Until it
        /// does, the watchdog re-sends the assign — the in-band control
        /// frame can race the forwarder's tap registration and be lost.
        assign_acked: bool,
        /// When the forwarder last proved liveness (ack or feedback),
        /// engine-epoch ms. Reset on (re-)assignment.
        forwarder_alive_ms: u64,
        /// Peers recently declared dead as forwarders, quarantined from
        /// candidacy until the stored engine-epoch ms.
        suspects: BTreeMap<IdentityId, u64>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for SendPipeline {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// The forwarder role: our relay fan-out for one sharer. The pump
    /// does the actual repeating; this owns the lanes' lifetime.
    struct RelayPipeline {
        sharer: IdentityId,
        fanout: Arc<VideoFanout>,
    }

    /// One call's live video session: the pump owns the tap for the
    /// whole session and routes to whichever roles are active.
    struct Session {
        shared: Arc<PumpShared>,
        control: Arc<ControlPlane>,
        pump_task: tokio::task::JoinHandle<()>,
        events_task: tokio::task::JoinHandle<()>,
        recv_task: tokio::task::JoinHandle<()>,
        send: Option<SendPipeline>,
        relay: Option<RelayPipeline>,
        /// Capture failed for this share attempt: alerted once, no retry
        /// until the user toggles the share off and on again.
        send_failed: bool,
    }

    impl Drop for Session {
        fn drop(&mut self) {
            self.pump_task.abort();
            self.events_task.abort();
            self.recv_task.abort();
        }
    }

    /// Decoder of last resort when OpenH264 refuses to initialize: skips
    /// every frame, so the failure stays visible as "no picture", never
    /// garbage pixels.
    #[derive(Debug, Default)]
    struct NullDecoder;

    impl VideoDecoder for NullDecoder {
        fn decode(&mut self, _packet: &[u8]) -> Option<DecodedPicture> {
            None
        }
    }

    fn h264_decoder() -> Box<dyn VideoDecoder> {
        match H264Decoder::new() {
            Ok(decoder) => Box::new(decoder),
            Err(error) => {
                tracing::error!(%error, "video: decoder init failed, sharer will show no picture");
                Box::new(NullDecoder)
            }
        }
    }

    /// Presents decoded pictures onto the node's frame feed, attributed
    /// to their origin, with the relaying peer (if any) alongside.
    struct FeedSink {
        call: CallId,
        feed: broadcast::Sender<RemoteVideoFrame>,
        shared: Arc<PumpShared>,
    }

    impl VideoSink for FeedSink {
        fn present(&mut self, from: IdentityId, picture: DecodedPicture, ts90k: u32) {
            let _ = self.feed.send(RemoteVideoFrame {
                call: self.call,
                from,
                via: self.shared.via_of(&from),
                width: picture.width,
                height: picture.height,
                rgba: Arc::new(picture.rgba),
                ts90k,
                decoded_at: std::time::Instant::now(),
            });
        }
    }

    /// Watch the event bus and run video for every call we are in — the
    /// reconcile pattern of the voice engine, one task per node, plus the
    /// pump-event lane and a 1 s watchdog for forwarder liveness.
    pub(crate) fn spawn_video_engine(
        identity: Arc<IdentityService>,
        calls: Arc<CallService>,
        streams: Arc<dyn MediaStreamTransport>,
        bus: EventBus,
        alerts: broadcast::Sender<MediaAlert>,
        feed: broadcast::Sender<RemoteVideoFrame>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut sessions: BTreeMap<CallId, Session> = BTreeMap::new();
            let mut rx = bus.subscribe();
            let (engine_tx, mut engine_rx) = mpsc::channel::<(CallId, PumpEvent)>(256);
            let mut watchdog = tokio::time::interval(Duration::from_secs(1));
            watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let epoch = Instant::now();
            loop {
                tokio::select! {
                    event = rx.recv() => {
                        let event = match event {
                            Ok(event) => event,
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        };
                        let AppEvent::Domain(DomainEvent::Calls(call_event)) = event else {
                            continue;
                        };
                        let call = call_id_of(&call_event);
                        reconcile(
                            call,
                            &mut sessions,
                            &identity,
                            &calls,
                            &streams,
                            &alerts,
                            &feed,
                            &engine_tx,
                            epoch,
                        )
                        .await;
                    }
                    pump_event = engine_rx.recv() => {
                        let Some((call, event)) = pump_event else { break };
                        handle_pump_event(
                            call,
                            event,
                            &mut sessions,
                            &identity,
                            &calls,
                            &streams,
                            epoch,
                        )
                        .await;
                    }
                    _ = watchdog.tick() => {
                        let live: Vec<CallId> = sessions
                            .iter()
                            .filter(|(_, session)| session.send.is_some())
                            .map(|(call, _)| *call)
                            .collect();
                        for call in live {
                            if let Some(session) = sessions.get_mut(&call) {
                                sync_share_targets(call, session, &identity, &calls, epoch).await;
                            }
                        }
                    }
                }
            }
        })
    }

    fn call_id_of(event: &CallEvent) -> CallId {
        match event {
            CallEvent::CallOffered { call, .. }
            | CallEvent::CallOpened { call, .. }
            | CallEvent::CallInvited { call, .. }
            | CallEvent::CallAccepted { call, .. }
            | CallEvent::CallDeclined { call, .. }
            | CallEvent::ParticipantJoined { call, .. }
            | CallEvent::ParticipantLeft { call, .. }
            | CallEvent::ScreenShareStarted { call, .. }
            | CallEvent::ScreenShareStopped { call, .. }
            | CallEvent::MicMuted { call, .. }
            | CallEvent::MicUnmuted { call, .. }
            | CallEvent::ForwarderElected { call, .. }
            | CallEvent::ForwarderCleared { call, .. }
            | CallEvent::CallEnded { call, .. } => *call,
        }
    }

    /// What the aggregate says about one call, reduced to what video
    /// needs.
    struct View {
        i_share: bool,
        remote_shares: bool,
        peers: Vec<IdentityId>,
        /// (identity, sharing) for everyone but us.
        sharers: Vec<IdentityId>,
    }

    async fn view_of(call: CallId, me: IdentityId, calls: &Arc<CallService>) -> Option<View> {
        let snapshot = calls.snapshot(call).await.ok()?;
        let live = matches!(snapshot.phase, CallPhase::Active)
            && snapshot.participants.iter().any(|(who, _)| *who == me);
        if !live {
            return None;
        }
        let peers: Vec<IdentityId> = snapshot
            .participants
            .iter()
            .map(|(who, _)| *who)
            .filter(|who| *who != me)
            .collect();
        let i_share = !peers.is_empty()
            && snapshot
                .participants
                .iter()
                .any(|(who, media)| *who == me && media.sharing_screen);
        let sharers: Vec<IdentityId> = snapshot
            .participants
            .iter()
            .filter(|(who, media)| *who != me && media.sharing_screen)
            .map(|(who, _)| *who)
            .collect();
        Some(View {
            i_share,
            remote_shares: !sharers.is_empty(),
            peers,
            sharers,
        })
    }

    /// Drive the session set toward what the aggregate says. Roster and
    /// share changes retarget the running pipelines live.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile(
        call: CallId,
        sessions: &mut BTreeMap<CallId, Session>,
        identity: &Arc<IdentityService>,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
        alerts: &broadcast::Sender<MediaAlert>,
        feed: &broadcast::Sender<RemoteVideoFrame>,
        engine_tx: &mpsc::Sender<(CallId, PumpEvent)>,
        epoch: Instant,
    ) {
        let me = identity.local_id();
        let view = view_of(call, me, calls).await;
        let Some(view) = view.filter(|view| view.i_share || view.remote_shares) else {
            if sessions.remove(&call).is_some() {
                tracing::info!(%call, "video: stopping video session");
            }
            return;
        };

        if let std::collections::btree_map::Entry::Vacant(slot) = sessions.entry(call) {
            match start_session(call, me, calls, streams, feed, engine_tx).await {
                Some(session) => {
                    slot.insert(session);
                    tracing::info!(%call, "video: session started (pump owns the tap)");
                }
                None => return,
            }
        }
        let Some(session) = sessions.get_mut(&call) else {
            return;
        };

        // Origin attribution: remote sharers' video ssrcs, so relayed
        // frames land under the sharer, not the forwarder.
        session.shared.set_origins(
            view.sharers
                .iter()
                .map(|sharer| (video_ssrc_of(sharer), *sharer))
                .collect(),
        );

        // A relay assignment for a sharer that stopped or left dies here;
        // an active sharer's assignment survives roster churn (the sharer
        // re-assigns the viewer set itself).
        if let Some(relay) = &session.relay {
            if !view.sharers.contains(&relay.sharer) {
                tracing::info!(%call, sharer = %relay.sharer, "video: relay stood down (share ended)");
                session.relay = None;
                session.shared.set_relay(None);
            }
        }

        // --- outbound: our share ---
        if view.i_share {
            if session.send.is_none() && !session.send_failed {
                match start_send(call, me, calls, streams).await {
                    Ok(send) => {
                        session.send = Some(send);
                    }
                    Err(detail) => {
                        tracing::warn!(%call, detail, "video: screen capture unavailable");
                        session.send_failed = true;
                        let _ = alerts.send(MediaAlert::ScreenCaptureUnavailable {
                            call,
                            detail: detail.to_string(),
                        });
                    }
                }
            }
            sync_share_targets(call, session, identity, calls, epoch).await;
        } else {
            if let Some(send) = session.send.take() {
                // Best effort: tell the forwarder to stand down.
                if let Some(forwarder) = send.forwarder {
                    let _ = session
                        .control
                        .send(forwarder, &ControlMessage::ForwarderRevoke)
                        .await;
                }
                tracing::info!(%call, "video: share stopped, capture released");
            }
            session.send_failed = false;
        }
    }

    /// Session bootstrap: the pump takes the call's video tap for good,
    /// the receiver decodes whatever the pump routes to it, and control
    /// lanes stand ready. Roles attach to this skeleton.
    async fn start_session(
        call: CallId,
        me: IdentityId,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
        feed: &broadcast::Sender<RemoteVideoFrame>,
        engine_tx: &mpsc::Sender<(CallId, PumpEvent)>,
    ) -> Option<Session> {
        let key_bytes = calls.media_key(call).await?;
        let tap = calls.video_tap(call).await;
        let shared = PumpShared::new();
        let control = ControlPlane::new(Arc::clone(streams), call, CallKey::new(key_bytes), &me);
        let (decode_tx, decode_rx) = mpsc::channel(64);
        let (events_tx, mut events_rx) = mpsc::channel::<PumpEvent>(64);

        let pump_task = tokio::spawn({
            let shared = Arc::clone(&shared);
            let key = CallKey::new(key_bytes);
            async move {
                let stats =
                    run_video_pump(tap, &key, shared, decode_tx, events_tx, REPORT_INTERVAL).await;
                tracing::info!(%call, ?stats, "video: pump finished");
            }
        });
        let events_task = tokio::spawn({
            let engine_tx = engine_tx.clone();
            async move {
                while let Some(event) = events_rx.recv().await {
                    if engine_tx.send((call, event)).await.is_err() {
                        break;
                    }
                }
            }
        });
        let recv_task = tokio::spawn({
            let key = CallKey::new(key_bytes);
            let mut sink = FeedSink {
                call,
                feed: feed.clone(),
                shared: Arc::clone(&shared),
            };
            async move {
                let stats =
                    run_video_receiver(decode_rx, &key, h264_decoder, &mut sink, None).await;
                tracing::info!(%call, ?stats, "video: receiver finished");
            }
        });

        Some(Session {
            shared,
            control,
            pump_task,
            events_task,
            recv_task,
            send: None,
            relay: None,
            send_failed: false,
        })
    }

    /// Pick the best available encoder and the capture cadence it earns:
    /// VideoToolbox hardware (macOS) at 15 fps — the spec's upper band,
    /// affordable because the media engine does the per-frame work — with
    /// software OpenH264 at 10 fps as the automatic fallback (non-macOS,
    /// or a Mac whose hardware encoder refuses to initialize).
    fn pick_encoder() -> Result<(Box<dyn VideoEncoder>, u32, &'static str), String> {
        #[cfg(target_os = "macos")]
        match VtH264Encoder::new(CAPTURE_FPS_HW, TARGET_BITRATE_BPS) {
            Ok(encoder) => return Ok((Box::new(encoder), CAPTURE_FPS_HW, "videotoolbox")),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "video: VideoToolbox unavailable — falling back to software OpenH264"
                );
            }
        }
        let encoder = H264Encoder::new(CAPTURE_FPS).map_err(|e| e.to_string())?;
        Ok((Box::new(encoder), CAPTURE_FPS, "openh264"))
    }

    async fn start_send(
        call: CallId,
        me: IdentityId,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
    ) -> Result<SendPipeline, String> {
        let key_bytes = calls
            .media_key(call)
            .await
            .ok_or_else(|| "no media key for active call".to_owned())?;
        let (encoder, fps, codec) = pick_encoder()?;
        let (capture, frames) = ScapCapture::start(fps).map_err(|e| e.to_string())?;
        // The sharer's own fan-out: no cache — forcing the shared encoder
        // is the cheaper primer when a lane of ours needs a keyframe.
        let fanout = VideoFanout::new(Arc::clone(streams), call);
        let sender_control = VideoSenderControl::new(TARGET_BITRATE_BPS);
        let key = CallKey::new(key_bytes);
        let ssrc = video_ssrc_of(&me);
        tracing::info!(%call, ssrc, codec, fps, "video: starting screen share");
        let task = tokio::spawn({
            let fanout = Arc::clone(&fanout);
            let sender_control = Arc::clone(&sender_control);
            async move {
                let totals =
                    run_video_sender(fanout, &key, ssrc, fps, sender_control, frames, encoder)
                        .await;
                tracing::info!(%call, ?totals, "video: sender finished");
            }
        });
        Ok(SendPipeline {
            _capture: capture,
            fanout,
            sender_control,
            bitrate: BitrateController::new(
                TARGET_BITRATE_BPS,
                MIN_BITRATE_BPS,
                TARGET_BITRATE_BPS,
            ),
            forwarder: None,
            assigned_viewers: Vec::new(),
            assign_acked: false,
            forwarder_alive_ms: 0,
            suspects: BTreeMap::new(),
            task,
        })
    }

    /// Reconcile who we actually send to with the aggregate's election:
    /// keep a live forwarder, drop a dead one (transport failures feed
    /// the election as unreachability), fall back to direct fan-out when
    /// nobody is worth electing — and keep the forwarder's viewer set
    /// current. Idempotent; runs on every call event and every watchdog
    /// tick while we share.
    async fn sync_share_targets(
        call: CallId,
        session: &mut Session,
        identity: &Arc<IdentityService>,
        calls: &Arc<CallService>,
        epoch: Instant,
    ) {
        let me = identity.local_id();
        let Some(view) = view_of(call, me, calls).await else {
            return;
        };
        let Some(send) = session.send.as_mut() else {
            return;
        };
        let now_ms = epoch.elapsed().as_millis() as u64;
        let mut failing = send.fanout.failing_peers(UNREACHABLE_AFTER_FAILURES);
        // The fast liveness check: an assigned forwarder that has proved
        // nothing (no ack, no feedback) for several report windows is
        // dead even while the transport still swallows writes into
        // socket buffers.
        if let Some(current) = send.forwarder {
            if now_ms.saturating_sub(send.forwarder_alive_ms) >= FORWARDER_SILENCE_MS
                && !failing.contains(&current)
            {
                tracing::info!(
                    %call,
                    forwarder = %current,
                    silent_ms = now_ms.saturating_sub(send.forwarder_alive_ms),
                    "video tx: forwarder went silent — treating as unreachable"
                );
                failing.push(current);
            }
        }
        // Quarantine the forwarder we are about to drop, and keep every
        // still-quarantined peer out of candidacy.
        if let Some(current) = send.forwarder {
            if failing.contains(&current) {
                send.suspects
                    .insert(current, now_ms + SUSPECT_QUARANTINE_MS);
            }
        }
        send.suspects.retain(|_, until| *until > now_ms);
        for suspect in send.suspects.keys() {
            if !failing.contains(suspect) {
                failing.push(*suspect);
            }
        }
        if !failing.is_empty() {
            tracing::debug!(%call, ?failing, "video tx: lanes failing");
        }
        let forwarder = calls
            .reconcile_forwarder(call, &failing)
            .await
            .ok()
            .flatten();
        let viewers: Vec<IdentityId> = view
            .peers
            .iter()
            .copied()
            .filter(|peer| Some(*peer) != forwarder)
            .collect();
        if forwarder != send.forwarder {
            if let Some(old) = send.forwarder.take() {
                let _ = session
                    .control
                    .send(old, &ControlMessage::ForwarderRevoke)
                    .await;
            }
            match forwarder {
                Some(chosen) => {
                    tracing::info!(
                        %call,
                        forwarder = %chosen,
                        viewers = viewers.len(),
                        "video tx: forwarding via peer — one outbound lane"
                    );
                    send.fanout.set_peers(vec![chosen]);
                    send.assign_acked = false;
                    send.forwarder_alive_ms = now_ms;
                    let _ = session
                        .control
                        .send(
                            chosen,
                            &ControlMessage::ForwarderAssign {
                                viewers: viewers.clone(),
                            },
                        )
                        .await;
                    send.assigned_viewers = viewers;
                }
                None => {
                    tracing::info!(
                        %call,
                        peers = view.peers.len(),
                        "video tx: direct fan-out (no eligible forwarder)"
                    );
                    send.fanout.set_peers(view.peers.clone());
                    send.assigned_viewers.clear();
                    send.assign_acked = false;
                }
            }
            send.forwarder = forwarder;
        } else if let Some(chosen) = forwarder {
            // Re-send the assignment until it is acknowledged (the
            // in-band frame can race the forwarder's tap registration) or
            // whenever the viewer set changed.
            if viewers != send.assigned_viewers || !send.assign_acked {
                tracing::info!(
                    %call,
                    forwarder = %chosen,
                    viewers = viewers.len(),
                    acked = send.assign_acked,
                    "video tx: (re-)assigning forwarder"
                );
                let _ = session
                    .control
                    .send(
                        chosen,
                        &ControlMessage::ForwarderAssign {
                            viewers: viewers.clone(),
                        },
                    )
                    .await;
                send.assigned_viewers = viewers;
            }
        } else {
            // Direct mode follows the roster.
            send.fanout.set_peers(view.peers.clone());
        }
    }

    /// Control and feedback surfaced by a session's pump.
    #[allow(clippy::too_many_arguments)]
    async fn handle_pump_event(
        call: CallId,
        event: PumpEvent,
        sessions: &mut BTreeMap<CallId, Session>,
        identity: &Arc<IdentityService>,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
        epoch: Instant,
    ) {
        let me = identity.local_id();
        let Some(session) = sessions.get_mut(&call) else {
            return;
        };
        match event {
            PumpEvent::Control { from, message } => match message {
                ControlMessage::ForwarderAssign { viewers } => {
                    // Only a participant the aggregate says is sharing may
                    // conscript us; the viewer set is sanitized — never
                    // ourselves, never the sharer.
                    let Some(view) = view_of(call, me, calls).await else {
                        return;
                    };
                    if !view.sharers.contains(&from) {
                        tracing::debug!(%call, %from, "video: assign from a non-sharer ignored");
                        return;
                    }
                    let viewers: Vec<IdentityId> = viewers
                        .into_iter()
                        .filter(|viewer| *viewer != me && *viewer != from)
                        .collect();
                    let fanout = match &session.relay {
                        Some(relay) if relay.sharer == from => Arc::clone(&relay.fanout),
                        _ => {
                            let fanout = VideoFanout::new_relay(Arc::clone(streams), call);
                            session.relay = Some(RelayPipeline {
                                sharer: from,
                                fanout: Arc::clone(&fanout),
                            });
                            session.shared.set_relay(Some(RelayTarget {
                                sharer: from,
                                fanout: Arc::clone(&fanout),
                            }));
                            fanout
                        }
                    };
                    tracing::info!(
                        %call,
                        sharer = %from,
                        viewers = viewers.len(),
                        "video: forwarding role accepted — relaying sealed bytes"
                    );
                    fanout.set_peers(viewers);
                    let _ = session
                        .control
                        .send(from, &ControlMessage::ForwarderAck)
                        .await;
                }
                ControlMessage::ForwarderRevoke => {
                    if session
                        .relay
                        .as_ref()
                        .is_some_and(|relay| relay.sharer == from)
                    {
                        tracing::info!(%call, sharer = %from, "video: forwarding role revoked");
                        session.relay = None;
                        session.shared.set_relay(None);
                    }
                }
                ControlMessage::ForwarderAck => {
                    if let Some(send) = session
                        .send
                        .as_mut()
                        .filter(|send| send.forwarder == Some(from))
                    {
                        if !send.assign_acked {
                            tracing::info!(%call, forwarder = %from, "video tx: forwarder acknowledged");
                        }
                        send.assign_acked = true;
                        send.forwarder_alive_ms = epoch.elapsed().as_millis() as u64;
                    }
                }
                ControlMessage::Feedback(report) => {
                    let Some(send) = session.send.as_mut() else {
                        return;
                    };
                    let now_ms = epoch.elapsed().as_millis() as u64;
                    if send.forwarder == Some(from) {
                        // The forwarder's own report doubles as liveness.
                        send.forwarder_alive_ms = now_ms;
                    }
                    tracing::info!(
                        %call,
                        viewer = %from,
                        received = report.received,
                        lost = report.lost,
                        "video feedback"
                    );
                    if let Some(bps) = send.bitrate.on_report(now_ms, &report) {
                        tracing::info!(
                            %call,
                            viewer = %from,
                            loss = report.loss_ratio(),
                            bitrate_bps = bps,
                            "video tx: feedback stepped the bitrate"
                        );
                        send.sender_control.set_bitrate(bps);
                    }
                }
                ControlMessage::KeyframeRequest => {
                    if let Some(send) = &session.send {
                        tracing::info!(%call, %from, "video tx: keyframe requested by forwarder");
                        send.fanout.demand_keyframe();
                    }
                }
            },
            PumpEvent::FeedbackDue { origin, report } => {
                if origin != me {
                    let _ = session
                        .control
                        .send(origin, &ControlMessage::Feedback(report))
                        .await;
                }
            }
            PumpEvent::RelayNeedsKeyframe { sharer } => {
                let _ = session
                    .control
                    .send(sharer, &ControlMessage::KeyframeRequest)
                    .await;
            }
        }
    }
}
