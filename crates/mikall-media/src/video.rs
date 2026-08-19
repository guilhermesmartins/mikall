//! The screen-share video pipeline: capture → static gate → encode ONCE →
//! seal ONCE → per-viewer lanes over unidirectional media streams; and on
//! the way in, tap → per-sender lane → decode → present.
//!
//! Everything here is pure or device-free: capture and the H.264 codec are
//! adapters behind the `hardware-video` feature ([`crate::capture`],
//! [`crate::codec_h264`]) speaking the [`VideoEncoder`]/[`VideoDecoder`]
//! ports, so the whole pipeline is testable with fakes.
//!
//! Design constraints (docs/streaming.md):
//! - The sharer encodes and seals each frame exactly once; [`VideoFanout`]
//!   is the *single* fan-out point. The forwarding-tree milestone replaces
//!   its peer set with elected forwarders without touching capture/encode.
//! - Per-viewer queues drop stale video instead of delaying fresh video,
//!   and never touch the global network command channel.
//! - H.264 P-frames chain: dropping one breaks decode until the next
//!   keyframe, so the queue drops *runs* (clear + wait for a keyframe),
//!   not single frames, and asks the shared encoder for an IDR.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, Notify};

use mikall_app::ports::{MediaStreamKind, MediaStreamTransport};
use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

use crate::frame::{
    open, seal, CallKey, FrameHeader, MediaKind, FLAG_END_OF_PICTURE, FLAG_KEYFRAME,
};

/// Video timestamps tick at the RTP video clock.
pub const VIDEO_CLOCK_HZ: u32 = 90_000;

/// Frames wider than this are the capture adapter's job to avoid; the
/// encoder port may refuse bigger pictures. Weak-machine ceiling for v1.
pub const MAX_WIDTH: u32 = 1280;

/// The audio sender derives its ssrc from the identity's first four bytes;
/// video XORs a constant so the same sender's two streams can never share
/// an AEAD nonce sequence under the one call key.
pub fn video_ssrc_of(id: &IdentityId) -> u32 {
    let b = id.as_bytes();
    u32::from_be_bytes([b[0], b[1], b[2], b[3]]) ^ 0x9E37_79B9
}

/// One captured picture, tightly packed 8-bit BGRA (the native macOS
/// capture format; converted to YUV inside the encoder adapter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedFrame {
    pub width: u32,
    pub height: u32,
    pub bgra: Vec<u8>,
}

/// H.264 wants even dimensions (4:2:0 chroma); capture may hand us odd
/// ones. Drop the last column/row rather than stretch: one pixel nobody
/// misses. Returns the frame untouched when already even.
pub fn crop_to_even(frame: CapturedFrame) -> CapturedFrame {
    let width = frame.width & !1;
    let height = frame.height & !1;
    if width == frame.width && height == frame.height {
        return frame;
    }
    let src_stride = (frame.width * 4) as usize;
    let dst_stride = (width * 4) as usize;
    let mut bgra = Vec::with_capacity(dst_stride * height as usize);
    for row in frame.bgra.chunks_exact(src_stride).take(height as usize) {
        bgra.extend_from_slice(&row[..dst_stride]);
    }
    CapturedFrame {
        width,
        height,
        bgra,
    }
}

/// Halve the picture (2×2 box filter) until it fits `max_width` — the
/// weak-machine safeguard when a capture backend cannot downscale at the
/// source. macOS capture is configured to ≤1280 already, so this is
/// normally a no-op.
pub fn downscale_to_max_width(mut frame: CapturedFrame, max_width: u32) -> CapturedFrame {
    while frame.width > max_width && frame.width >= 2 && frame.height >= 2 {
        let (w, h) = (frame.width as usize & !1, frame.height as usize & !1);
        let (dw, dh) = (w / 2, h / 2);
        let stride = frame.width as usize * 4;
        let mut out = vec![0u8; dw * dh * 4];
        for y in 0..dh {
            for x in 0..dw {
                for channel in 0..4 {
                    let a = frame.bgra[(2 * y) * stride + (2 * x) * 4 + channel] as u16;
                    let b = frame.bgra[(2 * y) * stride + (2 * x + 1) * 4 + channel] as u16;
                    let c = frame.bgra[(2 * y + 1) * stride + (2 * x) * 4 + channel] as u16;
                    let d = frame.bgra[(2 * y + 1) * stride + (2 * x + 1) * 4 + channel] as u16;
                    out[y * dw * 4 + x * 4 + channel] = ((a + b + c + d + 2) / 4) as u8;
                }
            }
        }
        frame = CapturedFrame {
            width: dw as u32,
            height: dh as u32,
            bgra: out,
        };
    }
    frame
}

/// One encoded picture out of the codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPicture {
    pub bytes: Vec<u8>,
    pub keyframe: bool,
}

/// One decoded picture for presentation, tightly packed 8-bit RGBA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPicture {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VideoError {
    #[error("encoder failure: {0}")]
    Encode(String),
    #[error("frame dimensions unsupported: {0}x{1}")]
    BadDimensions(u32, u32),
}

/// The codec's encode port. `force_keyframe` demands an IDR (new viewer,
/// or a lane recovering from drops). `None` means the codec skipped the
/// frame (rate control) — legal, nothing goes on the wire.
pub trait VideoEncoder: Send {
    fn encode(
        &mut self,
        frame: &CapturedFrame,
        force_keyframe: bool,
    ) -> Result<Option<EncodedPicture>, VideoError>;
}

/// The codec's decode port. `None` = not decodable (waiting for state,
/// corrupt packet) — the caller skips, it never fails the stream.
pub trait VideoDecoder: Send {
    fn decode(&mut self, packet: &[u8]) -> Option<DecodedPicture>;
}

/// Where decoded pictures land (the GUI's viewer pane in production, a
/// collector in tests).
pub trait VideoSink: Send {
    fn present(&mut self, from: IdentityId, picture: DecodedPicture);
}

/// Screen content is mostly static: an unchanged picture costs nothing —
/// it is simply not encoded. The comparison is exact bytes; macOS capture
/// additionally suppresses unchanged frames at the source, this gate
/// catches re-emitted heartbeats and any capture backend that does not.
#[derive(Debug, Default)]
pub struct StaticFrameGate {
    last: Option<CapturedFrame>,
}

impl StaticFrameGate {
    /// True when this frame differs from the last one that passed.
    pub fn changed(&mut self, frame: &CapturedFrame) -> bool {
        if self.last.as_ref() == Some(frame) {
            return false;
        }
        self.last = Some(frame.clone());
        true
    }
}

/// A sealed video frame ready for the wire, shared (not copied) between
/// per-viewer queues — sealed exactly once, relayable without the key.
#[derive(Debug, Clone)]
pub struct SealedVideoFrame {
    pub sealed: Arc<Vec<u8>>,
    pub keyframe: bool,
}

/// What became of a pushed frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Queued,
    /// The consumer stalled: the whole backlog (and this frame) was
    /// dropped, because H.264 deltas are useless once one is missing.
    /// The queue now waits for a keyframe.
    DroppedRun {
        dropped: usize,
    },
    /// Still waiting for a keyframe — deltas are skipped, not queued.
    SkippedAwaitingKeyframe,
}

/// Latest-run-wins outbound queue for one viewer. Bounded; overflow drops
/// the *run* (everything queued plus the newcomer) and demands a keyframe,
/// because sending a delta whose predecessor was dropped would only ship
/// garbage. A fresh queue starts awaiting a keyframe — that is what makes
/// a late joiner receive a decodable picture first.
#[derive(Debug)]
pub struct VideoSendQueue {
    frames: VecDeque<SealedVideoFrame>,
    capacity: usize,
    awaiting_keyframe: bool,
    dropped: u64,
}

impl VideoSendQueue {
    pub fn new(capacity: usize) -> Self {
        VideoSendQueue {
            frames: VecDeque::new(),
            capacity: capacity.max(1),
            awaiting_keyframe: true,
            dropped: 0,
        }
    }

    pub fn push(&mut self, frame: SealedVideoFrame) -> PushOutcome {
        if self.awaiting_keyframe && !frame.keyframe {
            self.dropped += 1;
            return PushOutcome::SkippedAwaitingKeyframe;
        }
        if self.frames.len() >= self.capacity {
            let dropped = self.frames.len() + 1;
            self.frames.clear();
            self.dropped += dropped as u64;
            self.awaiting_keyframe = true;
            if frame.keyframe {
                // A keyframe resets decode state anyway: keep it, the run
                // it starts is fresh.
                self.frames.push_back(frame);
                self.awaiting_keyframe = false;
                return PushOutcome::DroppedRun {
                    dropped: dropped - 1,
                };
            }
            return PushOutcome::DroppedRun { dropped };
        }
        if frame.keyframe {
            self.awaiting_keyframe = false;
        }
        self.frames.push_back(frame);
        PushOutcome::Queued
    }

    pub fn pop(&mut self) -> Option<SealedVideoFrame> {
        self.frames.pop_front()
    }

    /// A keyframe is needed before this queue will carry frames again.
    pub fn awaiting_keyframe(&self) -> bool {
        self.awaiting_keyframe
    }

    /// Drop everything queued and demand a keyframe — the consumer lost
    /// its decode state (stream reopen, send failure).
    pub fn reset_for_keyframe(&mut self) {
        self.dropped += self.frames.len() as u64;
        self.frames.clear();
        self.awaiting_keyframe = true;
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

/// One viewer's lane: its queue plus the task draining it into a
/// unidirectional stream. Dropping the lane aborts the task and closes
/// the stream.
#[derive(Debug)]
struct Lane {
    queue: Arc<Mutex<VideoSendQueue>>,
    wake: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Lane {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The single fan-out point of a share: the sender pushes each sealed
/// frame exactly once via [`VideoFanout::broadcast`]; every viewer has an
/// independent lane. **Forwarding-tree seam:** electing forwarders means
/// calling [`VideoFanout::set_peers`] with the forwarder(s) instead of all
/// viewers — capture, encode and seal never learn the difference. The
/// last-keyframe cache the spec puts on forwarders belongs beside the
/// lane queues here when that milestone lands.
pub struct VideoFanout {
    transport: Arc<dyn MediaStreamTransport>,
    call: CallId,
    lanes: Mutex<BTreeMap<IdentityId, Lane>>,
    /// Frames dropped across all lanes (stats).
    dropped_total: AtomicU64,
}

impl std::fmt::Debug for VideoFanout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VideoFanout")
            .field("call", &self.call)
            .finish_non_exhaustive()
    }
}

/// Per-lane queue depth: at 10 fps, four frames is 400 ms of backlog —
/// beyond that the viewer is better served by a fresh keyframe run.
const LANE_QUEUE_DEPTH: usize = 4;

impl VideoFanout {
    pub fn new(transport: Arc<dyn MediaStreamTransport>, call: CallId) -> Arc<Self> {
        Arc::new(VideoFanout {
            transport,
            call,
            lanes: Mutex::new(BTreeMap::new()),
            dropped_total: AtomicU64::new(0),
        })
    }

    /// Follow the roster: new peers get a lane (which starts by demanding
    /// a keyframe — the late-joiner path), leavers lose theirs.
    pub fn set_peers(self: &Arc<Self>, peers: Vec<IdentityId>) {
        let Ok(mut lanes) = self.lanes.lock() else {
            return;
        };
        lanes.retain(|peer, _| peers.contains(peer));
        for peer in peers {
            if lanes.contains_key(&peer) {
                continue;
            }
            let queue = Arc::new(Mutex::new(VideoSendQueue::new(LANE_QUEUE_DEPTH)));
            let wake = Arc::new(Notify::new());
            let task = tokio::spawn(run_lane(
                Arc::clone(&self.transport),
                self.call,
                peer,
                Arc::clone(&queue),
                Arc::clone(&wake),
            ));
            lanes.insert(peer, Lane { queue, wake, task });
        }
    }

    /// Push one sealed frame toward every lane. Never blocks and never
    /// copies the frame — lanes share the sealed bytes.
    pub fn broadcast(&self, frame: &SealedVideoFrame) {
        let Ok(lanes) = self.lanes.lock() else {
            return;
        };
        for lane in lanes.values() {
            if let Ok(mut queue) = lane.queue.lock() {
                if let PushOutcome::DroppedRun { dropped } = queue.push(frame.clone()) {
                    self.dropped_total
                        .fetch_add(dropped as u64, Ordering::Relaxed);
                }
            }
            lane.wake.notify_one();
        }
    }

    /// Does any lane need a keyframe (new viewer, or one recovering from a
    /// dropped run)? The encode loop consults this every frame and forces
    /// an IDR — the whole call shares the one encode.
    pub fn keyframe_wanted(&self) -> bool {
        let Ok(lanes) = self.lanes.lock() else {
            return false;
        };
        lanes.values().any(|lane| {
            lane.queue
                .lock()
                .map(|queue| queue.awaiting_keyframe())
                .unwrap_or(false)
        })
    }

    pub fn peer_count(&self) -> usize {
        self.lanes.lock().map(|lanes| lanes.len()).unwrap_or(0)
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }
}

/// One lane's drain loop: open the stream (retrying — the peer may still
/// be dialing), then pop-and-send forever. A send failure closes the
/// stream, demands a keyframe and reopens: frames are expendable, the
/// lane is not.
async fn run_lane(
    transport: Arc<dyn MediaStreamTransport>,
    call: CallId,
    peer: IdentityId,
    queue: Arc<Mutex<VideoSendQueue>>,
    wake: Arc<Notify>,
) {
    let mut stream = None;
    loop {
        // Stream first, frames second: nothing is consumed from the queue
        // until there is somewhere to send it, so the opening keyframe
        // survives however long the substream takes to come up.
        if stream.is_none() {
            match transport
                .open_stream(peer, call, MediaStreamKind::Video)
                .await
            {
                Ok(opened) => stream = Some(opened),
                Err(error) => {
                    tracing::debug!(%peer, %call, %error, "video lane: stream open failed");
                    // Whatever queued while unreachable is stale by the
                    // time the stream exists; restart from a keyframe.
                    if let Ok(mut queue) = queue.lock() {
                        queue.reset_for_keyframe();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            }
        }
        let frame = {
            let popped = queue.lock().ok().and_then(|mut queue| queue.pop());
            match popped {
                Some(frame) => frame,
                None => {
                    wake.notified().await;
                    continue;
                }
            }
        };
        if let Some(lane_stream) = stream.as_mut() {
            if let Err(error) = lane_stream.send(&frame.sealed).await {
                tracing::debug!(%peer, %call, %error, "video lane: send failed, reopening");
                stream = None;
                if let Ok(mut queue) = queue.lock() {
                    queue.reset_for_keyframe();
                }
            }
        }
    }
}

/// Sender statistics over one logging window.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VideoSenderStats {
    pub encoded: u64,
    pub keyframes: u64,
    pub skipped_static: u64,
    pub encoded_bytes: u64,
}

/// Pump captured frames through gate → encoder → seal → fan-out until the
/// capture channel closes. Returns totals. The frame is encoded and
/// sealed exactly once regardless of viewer count.
pub async fn run_video_sender(
    fanout: Arc<VideoFanout>,
    key: &CallKey,
    ssrc: u32,
    fps_hint: u32,
    mut frames: mpsc::Receiver<CapturedFrame>,
    mut encoder: Box<dyn VideoEncoder>,
) -> VideoSenderStats {
    let mut gate = StaticFrameGate::default();
    let mut counter: u64 = 0;
    let mut totals = VideoSenderStats::default();
    let mut window = VideoSenderStats::default();
    let mut window_start = std::time::Instant::now();
    let ts_step = VIDEO_CLOCK_HZ / fps_hint.max(1);
    while let Some(frame) = frames.recv().await {
        let keyframe_wanted = fanout.keyframe_wanted();
        // Static frames cost nothing — unless a viewer needs a keyframe,
        // which a static screen must not starve.
        if !gate.changed(&frame) && !keyframe_wanted {
            totals.skipped_static += 1;
            window.skipped_static += 1;
            continue;
        }
        let encoded = match encoder.encode(&frame, keyframe_wanted) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%error, "video tx: encode failed, stopping sender");
                break;
            }
        };
        let mut flags = FLAG_END_OF_PICTURE;
        if encoded.keyframe {
            flags |= FLAG_KEYFRAME;
        }
        let header = FrameHeader {
            counter,
            ts: (counter as u32).wrapping_mul(ts_step),
            ssrc,
            kind: MediaKind::Video,
            flags,
        };
        let Ok(sealed) = seal(key, header, &encoded.bytes) else {
            break;
        };
        counter += 1;
        totals.encoded += 1;
        window.encoded += 1;
        totals.encoded_bytes += sealed.len() as u64;
        window.encoded_bytes += sealed.len() as u64;
        if encoded.keyframe {
            totals.keyframes += 1;
            window.keyframes += 1;
        }
        fanout.broadcast(&SealedVideoFrame {
            sealed: Arc::new(sealed),
            keyframe: encoded.keyframe,
        });
        if window_start.elapsed() >= std::time::Duration::from_secs(1) {
            tracing::info!(
                fps = window.encoded,
                bytes_per_s = window.encoded_bytes,
                keyframes = window.keyframes,
                skipped_static = window.skipped_static,
                viewers = fanout.peer_count(),
                dropped_total = fanout.dropped_total(),
                "video tx"
            );
            window = VideoSenderStats::default();
            window_start = std::time::Instant::now();
        }
    }
    totals
}

/// Receiver statistics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VideoReceiverStats {
    pub presented: u64,
    /// Frames that failed AEAD authentication (dropped).
    pub rejected: u64,
    /// Deltas discarded while waiting for this sender's first keyframe.
    pub discarded_pre_keyframe: u64,
    /// Frames arriving with a non-advancing counter (dropped — streams
    /// are ordered, so this is a replay or a restarted sender).
    pub stale: u64,
}

/// One inbound sender's decode lane.
struct RxLane {
    decoder: Box<dyn VideoDecoder>,
    synced: bool,
    next_counter: u64,
    presented: u64,
    window_bytes: u64,
    window_frames: u64,
    window_start: std::time::Instant,
}

/// Drain a call's video tap into the sink until the tap closes or
/// `expect_frames` pictures have been presented. Streams are ordered per
/// sender, so there is no jitter buffer: verify, gate on the first
/// keyframe, decode, present. Each sender gets its own decoder.
pub async fn run_video_receiver(
    mut tap: mpsc::Receiver<(IdentityId, Vec<u8>)>,
    key: &CallKey,
    mut make_decoder: impl FnMut() -> Box<dyn VideoDecoder> + Send,
    sink: &mut dyn VideoSink,
    expect_frames: Option<u64>,
) -> VideoReceiverStats {
    let mut lanes: BTreeMap<IdentityId, RxLane> = BTreeMap::new();
    let mut stats = VideoReceiverStats::default();
    loop {
        if let Some(target) = expect_frames {
            if stats.presented >= target {
                break;
            }
        }
        let Some((from, sealed)) = tap.recv().await else {
            break;
        };
        let (header, payload) = match open(key, &sealed) {
            Ok(opened) => opened,
            Err(_) => {
                stats.rejected += 1;
                continue;
            }
        };
        if header.kind != MediaKind::Video {
            stats.rejected += 1;
            continue;
        }
        let lane = lanes.entry(from).or_insert_with(|| RxLane {
            decoder: make_decoder(),
            synced: false,
            next_counter: 0,
            presented: 0,
            window_bytes: 0,
            window_frames: 0,
            window_start: std::time::Instant::now(),
        });
        if lane.synced && header.counter < lane.next_counter {
            stats.stale += 1;
            continue;
        }
        let keyframe = header.flags & FLAG_KEYFRAME != 0;
        if !lane.synced && !keyframe {
            stats.discarded_pre_keyframe += 1;
            continue;
        }
        lane.synced = true;
        lane.next_counter = header.counter + 1;
        lane.window_bytes += sealed.len() as u64;
        lane.window_frames += 1;
        if let Some(picture) = lane.decoder.decode(&payload) {
            lane.presented += 1;
            stats.presented += 1;
            sink.present(from, picture);
        }
        if lane.window_start.elapsed() >= std::time::Duration::from_secs(1) {
            tracing::info!(
                peer = %from,
                fps = lane.window_frames,
                bytes_per_s = lane.window_bytes,
                presented = lane.presented,
                "video rx"
            );
            lane.window_bytes = 0;
            lane.window_frames = 0;
            lane.window_start = std::time::Instant::now();
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use async_trait::async_trait;
    use mikall_app::ports::{MediaSendStream, TransportError};
    use tokio::sync::Mutex as AsyncMutex;

    fn identity(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn call() -> CallId {
        CallId::from_bytes([7; 16])
    }

    fn bgra(w: u32, h: u32, fill: u8) -> CapturedFrame {
        CapturedFrame {
            width: w,
            height: h,
            bgra: vec![fill; (w * h * 4) as usize],
        }
    }

    fn sealed_frame(tag: u8, keyframe: bool) -> SealedVideoFrame {
        SealedVideoFrame {
            sealed: Arc::new(vec![tag]),
            keyframe,
        }
    }

    /// Deterministic fake codec: "encoding" prefixes a byte marking the
    /// frame type, "decoding" strips it. Keyframes carry the full frame;
    /// deltas only make sense after one.
    struct FakeEncoder {
        force_all_key: bool,
        frames_seen: u64,
    }

    impl VideoEncoder for FakeEncoder {
        fn encode(
            &mut self,
            frame: &CapturedFrame,
            force_keyframe: bool,
        ) -> Result<Option<EncodedPicture>, VideoError> {
            let keyframe = self.force_all_key || force_keyframe || self.frames_seen == 0;
            self.frames_seen += 1;
            let mut bytes = vec![u8::from(keyframe)];
            bytes.extend_from_slice(&frame.width.to_be_bytes());
            bytes.extend_from_slice(&frame.height.to_be_bytes());
            bytes.extend_from_slice(&frame.bgra);
            Ok(Some(EncodedPicture { bytes, keyframe }))
        }
    }

    struct FakeDecoder;

    impl VideoDecoder for FakeDecoder {
        fn decode(&mut self, packet: &[u8]) -> Option<DecodedPicture> {
            if packet.len() < 9 {
                return None;
            }
            let width = u32::from_be_bytes(packet[1..5].try_into().ok()?);
            let height = u32::from_be_bytes(packet[5..9].try_into().ok()?);
            Some(DecodedPicture {
                width,
                height,
                rgba: packet[9..].to_vec(),
            })
        }
    }

    struct CollectVideoSink {
        pictures: Vec<(IdentityId, DecodedPicture)>,
    }

    impl VideoSink for CollectVideoSink {
        fn present(&mut self, from: IdentityId, picture: DecodedPicture) {
            self.pictures.push((from, picture));
        }
    }

    type SentLog = Arc<AsyncMutex<Vec<(IdentityId, Vec<u8>)>>>;

    /// Transport whose streams record every sealed frame per peer.
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

    #[test]
    fn queue_starts_by_demanding_a_keyframe() {
        let mut queue = VideoSendQueue::new(4);
        assert!(queue.awaiting_keyframe());
        assert_eq!(
            queue.push(sealed_frame(1, false)),
            PushOutcome::SkippedAwaitingKeyframe
        );
        assert!(queue.is_empty());
        assert_eq!(queue.push(sealed_frame(2, true)), PushOutcome::Queued);
        assert!(!queue.awaiting_keyframe());
        assert_eq!(queue.push(sealed_frame(3, false)), PushOutcome::Queued);
        assert_eq!(queue.pop().unwrap().sealed[0], 2);
        assert_eq!(queue.pop().unwrap().sealed[0], 3);
        assert!(queue.pop().is_none());
    }

    #[test]
    fn stalled_consumer_drops_the_run_and_resumes_at_a_keyframe() {
        let mut queue = VideoSendQueue::new(2);
        assert_eq!(queue.push(sealed_frame(0, true)), PushOutcome::Queued);
        assert_eq!(queue.push(sealed_frame(1, false)), PushOutcome::Queued);
        // Nobody popped: the third frame overflows — everything goes, the
        // chain is broken, deltas are refused until the next keyframe.
        assert_eq!(
            queue.push(sealed_frame(2, false)),
            PushOutcome::DroppedRun { dropped: 3 }
        );
        assert!(queue.is_empty());
        assert!(queue.awaiting_keyframe());
        assert_eq!(
            queue.push(sealed_frame(3, false)),
            PushOutcome::SkippedAwaitingKeyframe
        );
        assert_eq!(queue.push(sealed_frame(4, true)), PushOutcome::Queued);
        assert_eq!(queue.pop().unwrap().sealed[0], 4);
        assert_eq!(queue.dropped(), 4);
    }

    #[test]
    fn overflow_by_a_keyframe_keeps_the_keyframe() {
        let mut queue = VideoSendQueue::new(2);
        queue.push(sealed_frame(0, true));
        queue.push(sealed_frame(1, false));
        assert_eq!(
            queue.push(sealed_frame(2, true)),
            PushOutcome::DroppedRun { dropped: 2 }
        );
        assert!(!queue.awaiting_keyframe());
        let kept = queue.pop().unwrap();
        assert_eq!(kept.sealed[0], 2);
        assert!(kept.keyframe);
    }

    #[test]
    fn static_gate_passes_changes_and_skips_repeats() {
        let mut gate = StaticFrameGate::default();
        let a = bgra(4, 4, 1);
        let b = bgra(4, 4, 2);
        assert!(gate.changed(&a));
        assert!(!gate.changed(&a));
        assert!(gate.changed(&b));
        assert!(!gate.changed(&b));
        assert!(gate.changed(&a));
    }

    #[tokio::test]
    async fn sender_seals_once_and_every_lane_gets_the_same_bytes() {
        let transport = Arc::new(RecordingStreamTransport::default());
        let sent = Arc::clone(&transport.sent);
        let fanout = VideoFanout::new(transport, call());
        fanout.set_peers(vec![identity(2), identity(3)]);

        let key = CallKey::new([9; 32]);
        let (frame_tx, frame_rx) = mpsc::channel(8);
        frame_tx.send(bgra(4, 4, 1)).await.unwrap();
        frame_tx.send(bgra(4, 4, 5)).await.unwrap();
        drop(frame_tx);
        let totals = run_video_sender(
            Arc::clone(&fanout),
            &key,
            77,
            10,
            frame_rx,
            Box::new(FakeEncoder {
                force_all_key: false,
                frames_seen: 0,
            }),
        )
        .await;
        assert_eq!(totals.encoded, 2);
        assert_eq!(totals.keyframes, 1);

        // Wait for both lanes to drain both frames.
        for _ in 0..200 {
            if sent.lock().await.len() == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let sent = sent.lock().await.clone();
        assert_eq!(sent.len(), 4, "2 frames × 2 viewers: {sent:?}");
        // The identical sealed bytes went to both peers: sealed once.
        let to_2: Vec<&Vec<u8>> = sent
            .iter()
            .filter(|(to, _)| *to == identity(2))
            .map(|(_, bytes)| bytes)
            .collect();
        let to_3: Vec<&Vec<u8>> = sent
            .iter()
            .filter(|(to, _)| *to == identity(3))
            .map(|(_, bytes)| bytes)
            .collect();
        assert_eq!(to_2, to_3);
        // And the flags say what they must: keyframe first, delta second.
        let (h0, _) = open(&key, to_2[0]).unwrap();
        assert_eq!(h0.kind, MediaKind::Video);
        assert_ne!(h0.flags & FLAG_KEYFRAME, 0);
        assert_ne!(h0.flags & FLAG_END_OF_PICTURE, 0);
        let (h1, _) = open(&key, to_2[1]).unwrap();
        assert_eq!(h1.flags & FLAG_KEYFRAME, 0);
    }

    #[tokio::test]
    async fn static_frames_are_skipped_but_a_late_joiner_forces_a_keyframe() {
        let transport = Arc::new(RecordingStreamTransport::default());
        let sent = Arc::clone(&transport.sent);
        let fanout = VideoFanout::new(transport, call());
        fanout.set_peers(vec![identity(2)]);
        let key = CallKey::new([9; 32]);
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let sender = tokio::spawn({
            let fanout = Arc::clone(&fanout);
            let key = CallKey::new([9; 32]);
            async move {
                run_video_sender(
                    fanout,
                    &key,
                    77,
                    10,
                    frame_rx,
                    Box::new(FakeEncoder {
                        force_all_key: false,
                        frames_seen: 0,
                    }),
                )
                .await
            }
        });
        let same = bgra(4, 4, 1);
        // First frame: encoded as the opening keyframe, viewer 2 gets it.
        frame_tx.send(same.clone()).await.unwrap();
        for _ in 0..200 {
            if sent.lock().await.len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(sent.lock().await.len(), 1);

        // Same frame again: static, nobody needs a keyframe → skipped.
        frame_tx.send(same.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(sent.lock().await.len(), 1, "static repeat must not send");

        // A late joiner appears. The screen is still static, but their
        // fresh lane demands a keyframe — the very same frame is now
        // encoded as an IDR and reaches both viewers.
        fanout.set_peers(vec![identity(2), identity(3)]);
        frame_tx.send(same.clone()).await.unwrap();
        for _ in 0..200 {
            if sent.lock().await.len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let sent_now = sent.lock().await.clone();
        assert_eq!(sent_now.len(), 3, "IDR to both viewers: {sent_now:?}");
        let (header, _) = open(&key, &sent_now[2].1).unwrap();
        assert_ne!(header.flags & FLAG_KEYFRAME, 0);

        drop(frame_tx);
        let totals = sender.await.unwrap();
        assert_eq!(totals.encoded, 2);
        assert_eq!(totals.keyframes, 2);
        assert_eq!(totals.skipped_static, 1);
    }

    #[tokio::test]
    async fn receiver_demuxes_interleaved_senders_and_gates_on_keyframes() {
        let key = CallKey::new([5; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(64);
        // Two senders interleaved. Sender 1 starts mid-stream (delta
        // first): its deltas are discarded until its keyframe. Sender 2
        // opens with a keyframe.
        let push = |n: u8, counter: u64, keyframe: bool, fill: u8| {
            let mut bytes = vec![u8::from(keyframe)];
            bytes.extend_from_slice(&4u32.to_be_bytes());
            bytes.extend_from_slice(&4u32.to_be_bytes());
            bytes.extend_from_slice(&[fill; 64]);
            let header = FrameHeader {
                counter,
                ts: 0,
                ssrc: u32::from(n),
                kind: MediaKind::Video,
                flags: FLAG_END_OF_PICTURE | if keyframe { FLAG_KEYFRAME } else { 0 },
            };
            let sealed = seal(&key, header, &bytes).unwrap();
            (identity(n), sealed)
        };
        let frames = vec![
            push(1, 5, false, 10), // pre-keyframe delta: discarded
            push(2, 0, true, 20),
            push(1, 6, true, 11),
            push(2, 1, false, 21),
            push(1, 7, false, 12),
        ];
        for frame in frames {
            tap_tx.send(frame).await.unwrap();
        }
        drop(tap_tx);

        let mut sink = CollectVideoSink {
            pictures: Vec::new(),
        };
        let stats =
            run_video_receiver(tap_rx, &key, || Box::new(FakeDecoder), &mut sink, None).await;
        assert_eq!(stats.presented, 4);
        assert_eq!(stats.discarded_pre_keyframe, 1);
        assert_eq!(stats.rejected, 0);
        let from_1: Vec<u8> = sink
            .pictures
            .iter()
            .filter(|(who, _)| *who == identity(1))
            .map(|(_, p)| p.rgba[0])
            .collect();
        let from_2: Vec<u8> = sink
            .pictures
            .iter()
            .filter(|(who, _)| *who == identity(2))
            .map(|(_, p)| p.rgba[0])
            .collect();
        assert_eq!(from_1, vec![11, 12]);
        assert_eq!(from_2, vec![20, 21]);
    }

    #[tokio::test]
    async fn receiver_rejects_wrong_key_and_audio_frames() {
        let key = CallKey::new([5; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(8);
        // A frame sealed under another key.
        let alien = seal(
            &CallKey::new([6; 32]),
            FrameHeader {
                counter: 0,
                ts: 0,
                ssrc: 1,
                kind: MediaKind::Video,
                flags: FLAG_KEYFRAME,
            },
            b"nope",
        )
        .unwrap();
        // An audio frame that somehow reached the video tap.
        let audio = seal(
            &key,
            FrameHeader {
                counter: 0,
                ts: 0,
                ssrc: 2,
                kind: MediaKind::Audio,
                flags: 0,
            },
            b"opus",
        )
        .unwrap();
        tap_tx.send((identity(1), alien)).await.unwrap();
        tap_tx.send((identity(2), audio)).await.unwrap();
        drop(tap_tx);
        let mut sink = CollectVideoSink {
            pictures: Vec::new(),
        };
        let stats =
            run_video_receiver(tap_rx, &key, || Box::new(FakeDecoder), &mut sink, None).await;
        assert_eq!(stats.presented, 0);
        assert_eq!(stats.rejected, 2);
    }

    #[test]
    fn crop_to_even_trims_the_odd_edge_only() {
        let mut frame = bgra(3, 3, 0);
        // Mark each pixel with its (x, y) so the crop is checkable.
        for y in 0..3u8 {
            for x in 0..3u8 {
                let i = (y as usize * 3 + x as usize) * 4;
                frame.bgra[i] = x;
                frame.bgra[i + 1] = y;
            }
        }
        let cropped = crop_to_even(frame);
        assert_eq!((cropped.width, cropped.height), (2, 2));
        assert_eq!(cropped.bgra.len(), 2 * 2 * 4);
        // Top-left 2×2 survives untouched.
        assert_eq!(&cropped.bgra[0..2], &[0, 0]);
        assert_eq!(&cropped.bgra[4..6], &[1, 0]);
        assert_eq!(&cropped.bgra[8..10], &[0, 1]);
        assert_eq!(&cropped.bgra[12..14], &[1, 1]);

        let even = bgra(4, 2, 9);
        assert_eq!(crop_to_even(even.clone()), even);
    }

    #[test]
    fn downscale_halves_until_the_width_fits() {
        let frame = bgra(8, 4, 100);
        let scaled = downscale_to_max_width(frame, 2);
        assert_eq!((scaled.width, scaled.height), (2, 1));
        // A uniform picture stays uniform under a box filter.
        assert!(scaled.bgra.iter().all(|b| *b == 100));

        let small = bgra(4, 4, 7);
        assert_eq!(downscale_to_max_width(small.clone(), 1280), small);
    }

    #[test]
    fn video_ssrc_differs_from_the_audio_ssrc_of_the_same_identity() {
        let id = identity(0xAB);
        let b = id.as_bytes();
        let audio = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        assert_ne!(video_ssrc_of(&id), audio);
    }
}
