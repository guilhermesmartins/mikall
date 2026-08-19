//! The video pump: one task per call that owns the inbound video tap and
//! routes every sealed frame by its *plaintext* header — to the local
//! decoder, to the relay fan-out (forwarder role), or to the control
//! handler.
//!
//! The relay path never decrypts. A forwarder repeats the sealed bytes
//! it received, verbatim, into its own [`VideoFanout`]; the only thing it
//! reads is the authenticated plaintext header ([`peek_header`]) — the
//! keyframe flag it needs for run bookkeeping travels there as AAD. The
//! call key is used exclusively to (a) open [`MediaKind::Control`] frames
//! and (b) let this participant *watch* the share it relays; neither is
//! on the relay data path, and the tests prove it by relaying frames
//! sealed under a key the pump does not hold.
//!
//! The pump also runs the viewer half of the feedback loop: it counts
//! received frames and header-counter gaps per origin and emits a coarse
//! [`FeedbackReport`] every report interval (~2 s) for the engine to send
//! to the sharer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use mikall_domain::shared::IdentityId;

use crate::control::{ControlMessage, FeedbackReport};
use crate::frame::{open, peek_header, CallKey, MediaKind, FLAG_KEYFRAME};
use crate::video::{SealedVideoFrame, VideoFanout};

/// The forwarder role, while assigned: relay `sharer`'s sealed video
/// frames through `fanout` to the viewer set.
#[derive(Debug, Clone)]
pub struct RelayTarget {
    pub sharer: IdentityId,
    pub fanout: Arc<VideoFanout>,
}

/// State shared between the pump task and the engine that reconfigures it
/// live (roles change mid-call; the pump never restarts).
#[derive(Debug, Default)]
pub struct PumpShared {
    /// ssrc → origin identity, maintained by the engine from roster
    /// snapshots (each sharing participant's video ssrc). Lets a viewer
    /// attribute relayed frames to the *sharer*, not the forwarder that
    /// carried them.
    origins: Mutex<BTreeMap<u32, IdentityId>>,
    /// The relay assignment in force, if any.
    relay: Mutex<Option<RelayTarget>>,
    /// origin → transport sender, for origins whose frames arrive
    /// relayed ("via <forwarder>" in the viewer pane).
    via: Mutex<BTreeMap<IdentityId, IdentityId>>,
}

impl PumpShared {
    pub fn new() -> Arc<Self> {
        Arc::new(PumpShared::default())
    }

    pub fn set_origins(&self, map: BTreeMap<u32, IdentityId>) {
        if let Ok(mut origins) = self.origins.lock() {
            *origins = map;
        }
    }

    pub fn set_relay(&self, target: Option<RelayTarget>) {
        if let Ok(mut relay) = self.relay.lock() {
            *relay = target;
        }
    }

    pub fn relay(&self) -> Option<RelayTarget> {
        self.relay.lock().ok().and_then(|relay| relay.clone())
    }

    /// Who carried `origin`'s frames to us, when it wasn't `origin`.
    pub fn via_of(&self, origin: &IdentityId) -> Option<IdentityId> {
        self.via
            .lock()
            .ok()
            .and_then(|via| via.get(origin).copied())
    }

    fn origin_for(&self, ssrc: u32, from: IdentityId) -> IdentityId {
        self.origins
            .lock()
            .ok()
            .and_then(|origins| origins.get(&ssrc).copied())
            .unwrap_or(from)
    }

    fn note_via(&self, origin: IdentityId, from: IdentityId) {
        if let Ok(mut via) = self.via.lock() {
            if origin == from {
                via.remove(&origin);
            } else {
                via.insert(origin, from);
            }
        }
    }
}

/// What the pump surfaces to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PumpEvent {
    /// An authenticated control message arrived from `from`.
    Control {
        from: IdentityId,
        message: ControlMessage,
    },
    /// A report window closed for `origin`'s stream — send it to the
    /// sharer (the engine knows the lanes; the pump only counts).
    FeedbackDue {
        origin: IdentityId,
        report: FeedbackReport,
    },
    /// The relay fan-out has a lane the cache could not prime — ask the
    /// sharer for one IDR.
    RelayNeedsKeyframe { sharer: IdentityId },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PumpStats {
    /// Sealed video frames repeated into the relay fan-out (no decrypt).
    pub relayed: u64,
    /// Video frames handed to the local decode path.
    pub to_decode: u64,
    /// Video frames dropped because the local decoder lagged.
    pub decode_dropped: u64,
    /// Authenticated control messages surfaced.
    pub control_in: u64,
    /// Frames that failed header parse, control authentication, or
    /// arrived with a kind the video tap never carries.
    pub rejected: u64,
}

/// Per-origin feedback accounting: ordered streams mean a counter gap is
/// a frame lost upstream (a dropped run at some sender lane), which is
/// exactly what the sharer's rate controller wants to know about.
#[derive(Debug, Default)]
struct OriginWindow {
    next_counter: Option<u64>,
    received: u32,
    lost: u32,
}

/// Run the pump until the tap closes. `report_interval` is the feedback
/// cadence (~2 s in production, driven faster in tests).
pub async fn run_video_pump(
    mut tap: mpsc::Receiver<(IdentityId, Vec<u8>)>,
    key: &CallKey,
    shared: Arc<PumpShared>,
    decode_tx: mpsc::Sender<(IdentityId, Vec<u8>)>,
    events: mpsc::Sender<PumpEvent>,
    report_interval: Duration,
) -> PumpStats {
    let mut stats = PumpStats::default();
    let mut windows: BTreeMap<IdentityId, OriginWindow> = BTreeMap::new();
    // First report one interval from now — a tokio interval's immediate
    // first tick would race the first frames and emit a torn window.
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + report_interval,
        report_interval,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut relay_window: (u64, u64) = (0, 0); // frames, bytes
    loop {
        tokio::select! {
            inbound = tap.recv() => {
                let Some((from, sealed)) = inbound else { break };
                route_frame(
                    from,
                    sealed,
                    key,
                    &shared,
                    &decode_tx,
                    &events,
                    &mut windows,
                    &mut relay_window,
                    &mut stats,
                )
                .await;
            }
            _ = ticker.tick() => {
                flush_reports(&events, &mut windows).await;
                if let Some(target) = shared.relay() {
                    if relay_window.0 > 0 {
                        let secs = report_interval.as_secs().max(1);
                        tracing::info!(
                            sharer = %target.sharer,
                            fps = relay_window.0 / secs,
                            bytes_per_s = relay_window.1 / secs,
                            lanes = target.fanout.peer_count(),
                            "video relay"
                        );
                        relay_window = (0, 0);
                    }
                    if target.fanout.keyframe_wanted() {
                        let _ = events
                            .send(PumpEvent::RelayNeedsKeyframe {
                                sharer: target.sharer,
                            })
                            .await;
                    }
                }
            }
        }
    }
    stats
}

#[allow(clippy::too_many_arguments)]
async fn route_frame(
    from: IdentityId,
    sealed: Vec<u8>,
    key: &CallKey,
    shared: &Arc<PumpShared>,
    decode_tx: &mpsc::Sender<(IdentityId, Vec<u8>)>,
    events: &mpsc::Sender<PumpEvent>,
    windows: &mut BTreeMap<IdentityId, OriginWindow>,
    relay_window: &mut (u64, u64),
    stats: &mut PumpStats,
) {
    let Ok(header) = peek_header(&sealed) else {
        stats.rejected += 1;
        return;
    };
    match header.kind {
        MediaKind::Control => {
            // Control *is* opened — authenticity gates every role change.
            let message = match open(key, &sealed) {
                Ok((_, payload)) => ControlMessage::decode(&payload),
                Err(_) => None,
            };
            match message {
                Some(message) => {
                    stats.control_in += 1;
                    let _ = events.send(PumpEvent::Control { from, message }).await;
                }
                None => stats.rejected += 1,
            }
        }
        MediaKind::Video => {
            // Forwarder role: repeat the sealed bytes, verbatim, into our
            // own lanes. Nothing here can decrypt — only the plaintext
            // header is read.
            if let Some(target) = shared.relay() {
                if target.sharer == from {
                    target.fanout.broadcast(&SealedVideoFrame {
                        sealed: Arc::new(sealed.clone()),
                        keyframe: header.flags & FLAG_KEYFRAME != 0,
                    });
                    stats.relayed += 1;
                    relay_window.0 += 1;
                    relay_window.1 += sealed.len() as u64;
                }
            }
            // Viewer role: attribute to the origin (the sharer), account
            // the window, and hand the frame to the decoder.
            let origin = shared.origin_for(header.ssrc, from);
            shared.note_via(origin, from);
            let window = windows.entry(origin).or_default();
            match window.next_counter {
                Some(next) if header.counter < next => {
                    // Replay/stale after a path change: not loss.
                }
                Some(next) => {
                    window.lost += u32::try_from(header.counter - next).unwrap_or(u32::MAX);
                    window.received += 1;
                    window.next_counter = Some(header.counter + 1);
                }
                None => {
                    window.received += 1;
                    window.next_counter = Some(header.counter + 1);
                }
            }
            match decode_tx.try_send((origin, sealed)) {
                Ok(()) => stats.to_decode += 1,
                Err(_) => stats.decode_dropped += 1,
            }
        }
        MediaKind::Audio => {
            stats.rejected += 1;
        }
    }
}

async fn flush_reports(
    events: &mpsc::Sender<PumpEvent>,
    windows: &mut BTreeMap<IdentityId, OriginWindow>,
) {
    for (origin, window) in windows.iter_mut() {
        if window.received == 0 && window.lost == 0 {
            continue; // an empty window is silence, not a report
        }
        let report = FeedbackReport {
            received: window.received,
            lost: window.lost,
        };
        window.received = 0;
        window.lost = 0;
        let _ = events
            .send(PumpEvent::FeedbackDue {
                origin: *origin,
                report,
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use async_trait::async_trait;
    use tokio::sync::Mutex as AsyncMutex;

    use mikall_app::ports::{
        MediaSendStream, MediaStreamKind, MediaStreamTransport, TransportError,
    };
    use mikall_domain::calls::CallId;

    use crate::frame::{seal, FrameHeader, FLAG_END_OF_PICTURE};
    use crate::video::video_ssrc_of;

    fn identity(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn call() -> CallId {
        CallId::from_bytes([7; 16])
    }

    type SentLog = Arc<AsyncMutex<Vec<(IdentityId, Vec<u8>)>>>;

    #[derive(Default)]
    struct RecordingStreamTransport {
        sent: SentLog,
    }

    struct RecordingStream {
        to: IdentityId,
        sent: SentLog,
    }

    #[async_trait]
    impl MediaSendStream for RecordingStream {
        async fn send(&mut self, sealed_frame: &[u8]) -> Result<(), TransportError> {
            self.sent
                .lock()
                .await
                .push((self.to, sealed_frame.to_vec()));
            Ok(())
        }
    }

    #[async_trait]
    impl MediaStreamTransport for RecordingStreamTransport {
        async fn open_stream(
            &self,
            to: IdentityId,
            _call: CallId,
            _kind: MediaStreamKind,
        ) -> Result<Box<dyn MediaSendStream>, TransportError> {
            Ok(Box::new(RecordingStream {
                to,
                sent: Arc::clone(&self.sent),
            }))
        }
    }

    fn sealed_video(key: &CallKey, ssrc: u32, counter: u64, keyframe: bool) -> Vec<u8> {
        let mut flags = FLAG_END_OF_PICTURE;
        if keyframe {
            flags |= FLAG_KEYFRAME;
        }
        let header = FrameHeader {
            counter,
            ts: 0,
            ssrc,
            kind: MediaKind::Video,
            flags,
        };
        seal(key, header, format!("picture-{counter}").as_bytes()).unwrap()
    }

    fn sealed_control(key: &CallKey, counter: u64, message: &ControlMessage) -> Vec<u8> {
        let header = FrameHeader {
            counter,
            ts: 0,
            ssrc: 42,
            kind: MediaKind::Control,
            flags: 0,
        };
        seal(key, header, &message.encode()).unwrap()
    }

    async fn drain_sent(sent: &SentLog, expect: usize) -> Vec<(IdentityId, Vec<u8>)> {
        for _ in 0..400 {
            if sent.lock().await.len() >= expect {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        sent.lock().await.clone()
    }

    /// THE no-decrypt proof: frames are sealed under a key the pump does
    /// not hold, and the relay still repeats them byte-for-byte to every
    /// lane. If the relay path touched the ciphertext, this test could
    /// not pass.
    #[tokio::test(flavor = "multi_thread")]
    async fn relay_repeats_sealed_bytes_it_cannot_decrypt() {
        let sharer = identity(1);
        let (viewer_a, viewer_b) = (identity(2), identity(3));
        let sharer_key = CallKey::new([0xAA; 32]);
        let pump_key = CallKey::new([0xBB; 32]); // NOT the sharer's key

        let transport = Arc::new(RecordingStreamTransport::default());
        let sent = Arc::clone(&transport.sent);
        let fanout = VideoFanout::new_relay(transport, call());
        fanout.set_peers(vec![viewer_a, viewer_b]);

        let shared = PumpShared::new();
        shared.set_relay(Some(RelayTarget {
            sharer,
            fanout: Arc::clone(&fanout),
        }));

        let (tap_tx, tap_rx) = mpsc::channel(16);
        let (decode_tx, mut decode_rx) = mpsc::channel(16);
        let (events_tx, _events_rx) = mpsc::channel(16);
        let pump = tokio::spawn({
            let shared = Arc::clone(&shared);
            async move {
                run_video_pump(
                    tap_rx,
                    &pump_key,
                    shared,
                    decode_tx,
                    events_tx,
                    Duration::from_secs(60),
                )
                .await
            }
        });

        let ssrc = video_ssrc_of(&sharer);
        let frames: Vec<Vec<u8>> = (0..3)
            .map(|i| sealed_video(&sharer_key, ssrc, i, i == 0))
            .collect();
        for frame in &frames {
            tap_tx.send((sharer, frame.clone())).await.unwrap();
        }

        let sent = drain_sent(&sent, 6).await;
        assert_eq!(sent.len(), 6, "3 frames × 2 lanes: {}", sent.len());
        for viewer in [viewer_a, viewer_b] {
            let got: Vec<&Vec<u8>> = sent
                .iter()
                .filter(|(to, _)| *to == viewer)
                .map(|(_, bytes)| bytes)
                .collect();
            assert_eq!(got.len(), 3);
            for (sent_bytes, original) in got.iter().zip(&frames) {
                assert_eq!(
                    *sent_bytes, original,
                    "relayed bytes must be the original sealed bytes, untouched"
                );
            }
        }

        // The forwarder still watches locally: the decode path got the
        // frames too (attributed to the transport sender absent an
        // origins map).
        for _ in 0..3 {
            let (origin, _) = decode_rx.recv().await.unwrap();
            assert_eq!(origin, sharer);
        }

        drop(tap_tx);
        let stats = pump.await.unwrap();
        assert_eq!(stats.relayed, 3);
        assert_eq!(stats.to_decode, 3);
        assert_eq!(stats.rejected, 0);
    }

    /// A late lane is primed from the keyframe cache: it receives the
    /// whole cached run with no new broadcast and no sharer involvement.
    #[tokio::test(flavor = "multi_thread")]
    async fn late_lane_is_primed_from_the_keyframe_cache() {
        let transport = Arc::new(RecordingStreamTransport::default());
        let sent = Arc::clone(&transport.sent);
        let fanout = VideoFanout::new_relay(transport, call());
        fanout.set_peers(vec![identity(2)]);

        let frames: Vec<SealedVideoFrame> = (0u8..3)
            .map(|i| SealedVideoFrame {
                sealed: Arc::new(vec![i; 8]),
                keyframe: i == 0,
            })
            .collect();
        for frame in &frames {
            fanout.broadcast(frame);
        }
        let first = drain_sent(&sent, 3).await;
        assert_eq!(first.len(), 3);

        // The late joiner appears; nothing new is broadcast.
        fanout.set_peers(vec![identity(2), identity(9)]);
        let all = drain_sent(&sent, 6).await;
        let late: Vec<&Vec<u8>> = all
            .iter()
            .filter(|(to, _)| *to == identity(9))
            .map(|(_, bytes)| bytes)
            .collect();
        assert_eq!(late.len(), 3, "cached run served: {late:?}");
        for (got, frame) in late.iter().zip(&frames) {
            assert_eq!(**got, *frame.sealed.as_ref());
        }
        assert!(
            !fanout.keyframe_wanted(),
            "a primed lane must not demand an IDR from the sharer"
        );
    }

    /// Counter gaps become loss in the next feedback report; clean
    /// windows report received only; empty windows report nothing.
    #[tokio::test(start_paused = true)]
    async fn feedback_windows_count_received_and_lost() {
        let sharer = identity(1);
        let key = CallKey::new([5; 32]);
        let ssrc = video_ssrc_of(&sharer);

        let shared = PumpShared::new();
        let (tap_tx, tap_rx) = mpsc::channel(16);
        let (decode_tx, _decode_rx) = mpsc::channel(16);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let pump = tokio::spawn({
            let shared = Arc::clone(&shared);
            let key = key.clone();
            async move {
                run_video_pump(
                    tap_rx,
                    &key,
                    shared,
                    decode_tx,
                    events_tx,
                    Duration::from_secs(2),
                )
                .await
            }
        });

        // Counters 0, 1, then 5: three received, three lost (2, 3, 4).
        for counter in [0, 1, 5] {
            tap_tx
                .send((sharer, sealed_video(&key, ssrc, counter, counter == 0)))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        let event = events_rx.recv().await.unwrap();
        assert_eq!(
            event,
            PumpEvent::FeedbackDue {
                origin: sharer,
                report: FeedbackReport {
                    received: 3,
                    lost: 3,
                },
            }
        );

        // A clean follow-up window.
        tap_tx
            .send((sharer, sealed_video(&key, ssrc, 6, false)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        let event = events_rx.recv().await.unwrap();
        assert_eq!(
            event,
            PumpEvent::FeedbackDue {
                origin: sharer,
                report: FeedbackReport {
                    received: 1,
                    lost: 0,
                },
            }
        );

        // Empty windows say nothing.
        tokio::time::sleep(Duration::from_millis(4_200)).await;
        drop(tap_tx);
        let stats = pump.await.unwrap();
        assert!(events_rx.recv().await.is_none(), "no report for silence");
        assert_eq!(stats.to_decode, 4);
    }

    /// Authenticated control routes to the engine; forged control is
    /// rejected; relayed frames are attributed to their origin with the
    /// forwarder recorded as "via".
    #[tokio::test(flavor = "multi_thread")]
    async fn control_and_origin_attribution() {
        let sharer = identity(1);
        let forwarder = identity(2);
        let key = CallKey::new([5; 32]);
        let ssrc = video_ssrc_of(&sharer);

        let shared = PumpShared::new();
        shared.set_origins([(ssrc, sharer)].into_iter().collect());

        let (tap_tx, tap_rx) = mpsc::channel(16);
        let (decode_tx, mut decode_rx) = mpsc::channel(16);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let pump = tokio::spawn({
            let shared = Arc::clone(&shared);
            let key = key.clone();
            async move {
                run_video_pump(
                    tap_rx,
                    &key,
                    shared,
                    decode_tx,
                    events_tx,
                    Duration::from_secs(60),
                )
                .await
            }
        });

        // An authenticated assignment from the sharer.
        let assign = ControlMessage::ForwarderAssign {
            viewers: vec![identity(3)],
        };
        tap_tx
            .send((sharer, sealed_control(&key, 0, &assign)))
            .await
            .unwrap();
        let event = events_rx.recv().await.unwrap();
        assert_eq!(
            event,
            PumpEvent::Control {
                from: sharer,
                message: assign,
            }
        );

        // A forged one (wrong key) is dropped.
        let forged = sealed_control(&CallKey::new([6; 32]), 0, &ControlMessage::ForwarderRevoke);
        tap_tx.send((identity(9), forged)).await.unwrap();

        // A video frame relayed by the forwarder: attributed to the
        // sharer, with the forwarder as "via".
        tap_tx
            .send((forwarder, sealed_video(&key, ssrc, 0, true)))
            .await
            .unwrap();
        let (origin, _) = decode_rx.recv().await.unwrap();
        assert_eq!(origin, sharer);
        assert_eq!(shared.via_of(&sharer), Some(forwarder));

        // Direct delivery clears the via marker.
        tap_tx
            .send((sharer, sealed_video(&key, ssrc, 1, false)))
            .await
            .unwrap();
        let (origin, _) = decode_rx.recv().await.unwrap();
        assert_eq!(origin, sharer);
        assert_eq!(shared.via_of(&sharer), None);

        drop(tap_tx);
        let stats = pump.await.unwrap();
        assert_eq!(stats.control_in, 1);
        assert_eq!(stats.rejected, 1);
    }
}
