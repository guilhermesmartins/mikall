//! Call video wiring: watches call lifecycle events and runs the real
//! screen-share pipelines of `mikall-media` — capture → encode once →
//! seal once → per-viewer lanes when *we* share, and tap → decode →
//! frame feed when a *remote* peer shares.
//!
//! Same shape as [`crate::voice`]: the engine lives behind the
//! `hardware-video` feature (it opens the screen and compiles OpenH264);
//! [`RemoteVideoFrame`] and the feed are always available so frontends
//! subscribe unconditionally. Capture trouble (macOS Screen Recording
//! permission above all) is honest, never fatal: an alert is raised and
//! the share stays signaling-only.

use std::sync::Arc;

use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

/// One decoded remote picture for frontends, RGBA. Shared pixels: the
/// broadcast clones the handle, never the frame.
#[derive(Debug, Clone)]
pub struct RemoteVideoFrame {
    pub call: CallId,
    pub from: IdentityId,
    pub width: u32,
    pub height: u32,
    pub rgba: Arc<Vec<u8>>,
}

#[cfg(feature = "hardware-video")]
pub(crate) use engine::spawn_video_engine;

#[cfg(feature = "hardware-video")]
mod engine {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use mikall_app::events::{AppEvent, EventBus};
    use mikall_app::ports::MediaStreamTransport;
    use mikall_app::services::{CallService, IdentityService};
    use mikall_domain::calls::{CallEvent, CallId, CallPhase};
    use mikall_domain::shared::IdentityId;
    use mikall_domain::DomainEvent;
    use mikall_media::capture::{ScapCapture, CAPTURE_FPS};
    use mikall_media::codec_h264::{H264Decoder, H264Encoder};
    use mikall_media::frame::CallKey;
    use mikall_media::video::{
        run_video_receiver, run_video_sender, video_ssrc_of, DecodedPicture, VideoDecoder,
        VideoFanout, VideoSink,
    };

    use super::RemoteVideoFrame;
    use crate::voice::MediaAlert;

    /// Our outbound share: dropping it aborts the encode loop, tears down
    /// the per-viewer lanes and stops capture — the macOS indicator goes
    /// away because the ScreenCaptureKit stream is stopped, not leaked.
    struct SendPipeline {
        _capture: ScapCapture,
        fanout: Arc<VideoFanout>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for SendPipeline {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// A remote share we are decoding.
    struct RecvPipeline {
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for RecvPipeline {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[derive(Default)]
    struct Session {
        send: Option<SendPipeline>,
        /// Capture failed for this share attempt: alerted once, no retry
        /// until the user toggles the share off and on again.
        send_failed: bool,
        recv: Option<RecvPipeline>,
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

    /// Presents decoded pictures onto the node's frame feed.
    struct FeedSink {
        call: CallId,
        feed: broadcast::Sender<RemoteVideoFrame>,
    }

    impl VideoSink for FeedSink {
        fn present(&mut self, from: IdentityId, picture: DecodedPicture) {
            let _ = self.feed.send(RemoteVideoFrame {
                call: self.call,
                from,
                width: picture.width,
                height: picture.height,
                rgba: Arc::new(picture.rgba),
            });
        }
    }

    /// Watch the event bus and run video for every call we are in — the
    /// reconcile pattern of the voice engine, one task per node.
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
            loop {
                let event = match rx.recv().await {
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
                )
                .await;
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
            | CallEvent::CallEnded { call, .. } => *call,
        }
    }

    /// Drive the session set toward what the aggregate says: we send
    /// while we are sharing to somebody, we receive while a remote is
    /// sharing at us. Roster changes retarget the fan-out live — a fresh
    /// lane starts by demanding a keyframe, which is the whole
    /// late-joiner story.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile(
        call: CallId,
        sessions: &mut BTreeMap<CallId, Session>,
        identity: &Arc<IdentityService>,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
        alerts: &broadcast::Sender<MediaAlert>,
        feed: &broadcast::Sender<RemoteVideoFrame>,
    ) {
        let me = identity.local_id();
        let snapshot = calls.snapshot(call).await.ok();
        let (i_share, remote_shares, peers) = match &snapshot {
            None => (false, false, Vec::new()),
            Some(snapshot) => {
                let live = matches!(snapshot.phase, CallPhase::Active)
                    && snapshot.participants.iter().any(|(who, _)| *who == me);
                let peers: Vec<IdentityId> = snapshot
                    .participants
                    .iter()
                    .map(|(who, _)| *who)
                    .filter(|who| *who != me)
                    .collect();
                let i_share = live
                    && !peers.is_empty()
                    && snapshot
                        .participants
                        .iter()
                        .any(|(who, media)| *who == me && media.sharing_screen);
                let remote_shares = live
                    && snapshot
                        .participants
                        .iter()
                        .any(|(who, media)| *who != me && media.sharing_screen);
                (i_share, remote_shares, peers)
            }
        };

        if !i_share && !remote_shares {
            if sessions.remove(&call).is_some() {
                tracing::info!(%call, "video: stopping video session");
            }
            return;
        }
        let session = sessions.entry(call).or_default();

        // --- outbound: our share ---
        if i_share {
            if let Some(send) = &session.send {
                send.fanout.set_peers(peers.clone());
            } else if !session.send_failed {
                match start_send(call, &peers, me, calls, streams).await {
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
        } else {
            if session.send.take().is_some() {
                tracing::info!(%call, "video: share stopped, capture released");
            }
            session.send_failed = false;
        }

        // --- inbound: a remote share ---
        if remote_shares {
            if session.recv.is_none() {
                if let Some(key_bytes) = calls.media_key(call).await {
                    let tap = calls.video_tap(call).await;
                    let key = CallKey::new(key_bytes);
                    let mut sink = FeedSink {
                        call,
                        feed: feed.clone(),
                    };
                    let task = tokio::spawn(async move {
                        let stats =
                            run_video_receiver(tap, &key, h264_decoder, &mut sink, None).await;
                        tracing::info!(%call, ?stats, "video: receiver finished");
                    });
                    session.recv = Some(RecvPipeline { task });
                    tracing::info!(%call, "video: watching remote share");
                }
            }
        } else if session.recv.take().is_some() {
            tracing::info!(%call, "video: remote share ended, decoder released");
        }
    }

    async fn start_send(
        call: CallId,
        peers: &[IdentityId],
        me: IdentityId,
        calls: &Arc<CallService>,
        streams: &Arc<dyn MediaStreamTransport>,
    ) -> Result<SendPipeline, String> {
        let key_bytes = calls
            .media_key(call)
            .await
            .ok_or_else(|| "no media key for active call".to_owned())?;
        let (capture, frames) = ScapCapture::start(CAPTURE_FPS).map_err(|e| e.to_string())?;
        let encoder = H264Encoder::new(CAPTURE_FPS).map_err(|e| e.to_string())?;
        let fanout = VideoFanout::new(Arc::clone(streams), call);
        fanout.set_peers(peers.to_vec());
        let key = CallKey::new(key_bytes);
        let ssrc = video_ssrc_of(&me);
        tracing::info!(%call, viewers = peers.len(), ssrc, "video: starting screen share");
        let task = tokio::spawn({
            let fanout = Arc::clone(&fanout);
            async move {
                let totals =
                    run_video_sender(fanout, &key, ssrc, CAPTURE_FPS, frames, Box::new(encoder))
                        .await;
                tracing::info!(%call, ?totals, "video: sender finished");
            }
        });
        Ok(SendPipeline {
            _capture: capture,
            fanout,
            task,
        })
    }
}
