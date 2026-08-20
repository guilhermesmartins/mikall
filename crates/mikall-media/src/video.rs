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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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

/// The current wall clock in 90 kHz video-clock units, wrapping u32
/// (one lap ≈ 13.25 h). Senders stamp every video frame's header `ts`
/// with this, so a receiver can estimate glass-to-glass latency as
/// `ts90k_now().wrapping_sub(header.ts)` — exact when both ends share a
/// wall clock (two instances on one machine), per-stage-only across
/// machines whose clocks drift.
pub fn ts90k_now() -> u32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    // 90 kHz ticks of the UNIX epoch, truncated to u32 (wrapping lap).
    (now.as_nanos().wrapping_mul(9) / 100_000) as u32
}

/// Milliseconds represented by a wrapping 90 kHz tick difference.
pub fn ts90k_diff_ms(later: u32, earlier: u32) -> f64 {
    f64::from(later.wrapping_sub(earlier)) / 90.0
}

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

/// Shape a captured frame for an encoder — the weak-machine width cap,
/// then even dimensions (4:2:0 chroma) — **copying only when something
/// actually changes**. macOS capture already delivers ≤1280-wide even
/// frames, so the hot path borrows: the only per-frame pixel pass left in
/// the encode loop is the codec's own colorspace read.
pub fn shape_for_encode(
    frame: &CapturedFrame,
    max_width: u32,
) -> std::borrow::Cow<'_, CapturedFrame> {
    if frame.width <= max_width && frame.width & 1 == 0 && frame.height & 1 == 0 {
        return std::borrow::Cow::Borrowed(frame);
    }
    std::borrow::Cow::Owned(crop_to_even(downscale_to_max_width(
        frame.clone(),
        max_width,
    )))
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

    /// Retarget the encoder's bitrate (the feedback loop's actuator).
    /// Takes effect on a following frame; codecs that cannot adapt ignore
    /// it — the default is an honest no-op.
    fn set_target_bitrate(&mut self, _bps: u32) {}
}

/// The codec's decode port. `None` = not decodable (waiting for state,
/// corrupt packet) — the caller skips, it never fails the stream.
pub trait VideoDecoder: Send {
    fn decode(&mut self, packet: &[u8]) -> Option<DecodedPicture>;
}

/// Where decoded pictures land (the GUI's viewer pane in production, a
/// collector in tests). `ts90k` is the sender's wall-clock capture stamp
/// from the frame header ([`ts90k_now`]) — frontends use it for the
/// glass-to-glass latency estimate at the moment they actually render.
pub trait VideoSink: Send {
    fn present(&mut self, from: IdentityId, picture: DecodedPicture, ts90k: u32);
}

/// Screen content is mostly static: an unchanged picture costs nothing —
/// it is simply not encoded. The comparison is exact (a collision-free
/// content hash); macOS capture additionally suppresses unchanged frames
/// at the source, this gate catches re-emitted heartbeats and any capture
/// backend that does not. Hashing instead of storing the frame keeps the
/// gate O(read) with zero per-frame allocation — the old clone-and-memcmp
/// wrote a full frame copy on every screen change, a measurable slice of
/// the encode-loop budget at 1280-wide.
#[derive(Debug, Default)]
pub struct StaticFrameGate {
    last: Option<(u32, u32, [u8; 32])>,
}

impl StaticFrameGate {
    /// True when this frame differs from the last one that passed.
    pub fn changed(&mut self, frame: &CapturedFrame) -> bool {
        let signature = (
            frame.width,
            frame.height,
            *blake3::hash(&frame.bgra).as_bytes(),
        );
        if self.last == Some(signature) {
            return false;
        }
        self.last = Some(signature);
        true
    }
}

/// A sealed video frame ready for the wire, shared (not copied) between
/// per-viewer queues — sealed exactly once, relayable without the key.
#[derive(Debug, Clone)]
pub struct SealedVideoFrame {
    pub sealed: Arc<Vec<u8>>,
    pub keyframe: bool,
    /// When this frame entered the fan-out (sealed by the sharer, or
    /// received by a relay). Lane drain loops read it to report
    /// queue-wait + send time per frame — the encode→send stage of the
    /// latency instrumentation.
    stamped: std::time::Instant,
}

impl SealedVideoFrame {
    pub fn new(sealed: Arc<Vec<u8>>, keyframe: bool) -> Self {
        SealedVideoFrame {
            sealed,
            keyframe,
            stamped: std::time::Instant::now(),
        }
    }

    /// How long ago this frame entered the fan-out.
    pub fn age(&self) -> std::time::Duration {
        self.stamped.elapsed()
    }
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
    /// Frames still in the queue from a [`VideoSendQueue::prime`]: a
    /// primed backlog (a whole cached keyframe run) may exceed the
    /// steady-state capacity, so the overflow check allows it through
    /// until the lane drains it.
    primed: usize,
}

impl VideoSendQueue {
    pub fn new(capacity: usize) -> Self {
        VideoSendQueue {
            frames: VecDeque::new(),
            capacity: capacity.max(1),
            awaiting_keyframe: true,
            dropped: 0,
            primed: 0,
        }
    }

    pub fn push(&mut self, frame: SealedVideoFrame) -> PushOutcome {
        if self.awaiting_keyframe && !frame.keyframe {
            self.dropped += 1;
            return PushOutcome::SkippedAwaitingKeyframe;
        }
        if self.frames.len() >= self.capacity + self.primed {
            let dropped = self.frames.len() + 1;
            self.frames.clear();
            self.primed = 0;
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

    /// Seed a fresh lane with a complete cached keyframe run (keyframe
    /// first — the caller guarantees run shape). The late joiner gets a
    /// decodable picture *now*, from the cache, without the sharer
    /// re-encoding an IDR.
    pub fn prime(&mut self, run: &[SealedVideoFrame]) {
        let Some(first) = run.first() else {
            return;
        };
        if !first.keyframe {
            return;
        }
        self.dropped += self.frames.len() as u64;
        self.frames.clear();
        // Re-stamp: cached frames may be seconds old, and a primed lane's
        // send timing must measure *this* lane, not the cache's age.
        self.frames.extend(run.iter().map(|frame| {
            let mut fresh = frame.clone();
            fresh.stamped = std::time::Instant::now();
            fresh
        }));
        self.primed = self.frames.len();
        self.awaiting_keyframe = false;
    }

    pub fn pop(&mut self) -> Option<SealedVideoFrame> {
        let popped = self.frames.pop_front();
        if popped.is_some() {
            self.primed = self.primed.saturating_sub(1);
        }
        popped
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
        self.primed = 0;
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

/// Cache of the last complete keyframe *run* (the keyframe plus every
/// delta since — H.264 deltas chain, so anything less than the whole run
/// cannot prime a decoder). Lives beside the lane queues on relay
/// fan-outs: a late-joining viewer's lane is primed from here instead of
/// making the sharer re-encode an IDR. Byte-capped: a run that outgrows
/// the cap is discarded whole (a partial run would decode to garbage) and
/// the next keyframe starts a fresh one.
#[derive(Debug)]
pub struct KeyframeCache {
    run: Vec<SealedVideoFrame>,
    bytes: usize,
    max_bytes: usize,
    complete: bool,
}

impl KeyframeCache {
    pub fn new(max_bytes: usize) -> Self {
        KeyframeCache {
            run: Vec::new(),
            bytes: 0,
            max_bytes,
            complete: false,
        }
    }

    pub fn observe(&mut self, frame: &SealedVideoFrame) {
        if frame.keyframe {
            self.run.clear();
            self.bytes = frame.sealed.len();
            self.run.push(frame.clone());
            // A single keyframe always fits by definition — without it
            // there is nothing to serve at all.
            self.complete = true;
            return;
        }
        if !self.complete {
            return;
        }
        if self.bytes + frame.sealed.len() > self.max_bytes {
            self.run.clear();
            self.bytes = 0;
            self.complete = false;
            return;
        }
        self.bytes += frame.sealed.len();
        self.run.push(frame.clone());
    }

    /// The complete run, keyframe first — or `None` when the cache cannot
    /// honestly serve a decodable picture.
    pub fn run(&self) -> Option<&[SealedVideoFrame]> {
        if self.complete {
            Some(&self.run)
        } else {
            None
        }
    }
}

/// One viewer's lane: its queue plus the task draining it into a
/// unidirectional stream. Dropping the lane aborts the task and closes
/// the stream.
#[derive(Debug)]
struct Lane {
    queue: Arc<Mutex<VideoSendQueue>>,
    wake: Arc<Notify>,
    /// Consecutive open/send failures — reset by any successful send.
    /// The sharer's watchdog reads this to treat a dead forwarder as
    /// unreachable and re-elect (v1 reachability signal).
    failures: Arc<AtomicU32>,
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
    /// Last-keyframe-run cache (relay fan-outs): primes late lanes
    /// without involving the sharer. `None` on the sharer's own fan-out,
    /// where forcing the shared encoder is the cheaper primer.
    cache: Option<Mutex<KeyframeCache>>,
    /// One-shot external IDR demand (a forwarder's `KeyframeRequest`),
    /// cleared when the next keyframe goes out.
    keyframe_demanded: AtomicBool,
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

/// Keyframe-run cache ceiling for relay fan-outs: comfortably one full
/// intra period of 1280-wide screen content (~15 s at the observed
/// ~350 kbps worst case), small enough to never matter in RAM.
pub const RELAY_CACHE_BYTES: usize = 4 * 1024 * 1024;

impl VideoFanout {
    pub fn new(transport: Arc<dyn MediaStreamTransport>, call: CallId) -> Arc<Self> {
        Self::build(transport, call, None)
    }

    /// A relay's fan-out: same lanes, plus the keyframe-run cache that
    /// serves late joiners without touching the sharer.
    pub fn new_relay(transport: Arc<dyn MediaStreamTransport>, call: CallId) -> Arc<Self> {
        Self::build(
            transport,
            call,
            Some(Mutex::new(KeyframeCache::new(RELAY_CACHE_BYTES))),
        )
    }

    fn build(
        transport: Arc<dyn MediaStreamTransport>,
        call: CallId,
        cache: Option<Mutex<KeyframeCache>>,
    ) -> Arc<Self> {
        Arc::new(VideoFanout {
            transport,
            call,
            lanes: Mutex::new(BTreeMap::new()),
            dropped_total: AtomicU64::new(0),
            cache,
            keyframe_demanded: AtomicBool::new(false),
        })
    }

    /// Follow the roster: new peers get a lane, leavers lose theirs. On a
    /// cached (relay) fan-out a fresh lane is primed with the last
    /// keyframe run — the late joiner sees a picture immediately; without
    /// a servable cache the lane starts by demanding a keyframe, exactly
    /// the M16 late-joiner path.
    pub fn set_peers(self: &Arc<Self>, peers: Vec<IdentityId>) {
        let Ok(mut lanes) = self.lanes.lock() else {
            return;
        };
        lanes.retain(|peer, _| peers.contains(peer));
        for peer in peers {
            if lanes.contains_key(&peer) {
                continue;
            }
            let mut queue = VideoSendQueue::new(LANE_QUEUE_DEPTH);
            if let Some(cache) = &self.cache {
                if let Ok(cache) = cache.lock() {
                    if let Some(run) = cache.run() {
                        queue.prime(run);
                    }
                }
            }
            let queue = Arc::new(Mutex::new(queue));
            let wake = Arc::new(Notify::new());
            let failures = Arc::new(AtomicU32::new(0));
            let task = tokio::spawn(run_lane(
                Arc::clone(&self.transport),
                self.call,
                peer,
                Arc::clone(&queue),
                Arc::clone(&wake),
                Arc::clone(&failures),
            ));
            // A primed lane has frames waiting before the task first
            // parks — wake it so the cached run leaves immediately.
            wake.notify_one();
            lanes.insert(
                peer,
                Lane {
                    queue,
                    wake,
                    failures,
                    task,
                },
            );
        }
    }

    /// Push one sealed frame toward every lane. Never blocks and never
    /// copies the frame — lanes share the sealed bytes.
    pub fn broadcast(&self, frame: &SealedVideoFrame) {
        if frame.keyframe {
            self.keyframe_demanded.store(false, Ordering::Relaxed);
        }
        if let Some(cache) = &self.cache {
            if let Ok(mut cache) = cache.lock() {
                cache.observe(frame);
            }
        }
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
    /// dropped run), or did a forwarder ask for one? The encode loop
    /// consults this every frame and forces an IDR — the whole call
    /// shares the one encode.
    pub fn keyframe_wanted(&self) -> bool {
        if self.keyframe_demanded.load(Ordering::Relaxed) {
            return true;
        }
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

    /// External IDR demand (a forwarder's relay lane that the cache could
    /// not prime). One-shot: cleared by the next broadcast keyframe.
    pub fn demand_keyframe(&self) {
        self.keyframe_demanded.store(true, Ordering::Relaxed);
    }

    /// Peers whose lane has failed `threshold`+ consecutive opens/sends —
    /// the transport-level unreachability signal the sharer's watchdog
    /// feeds into forwarder re-election.
    pub fn failing_peers(&self, threshold: u32) -> Vec<IdentityId> {
        let Ok(lanes) = self.lanes.lock() else {
            return Vec::new();
        };
        lanes
            .iter()
            .filter(|(_, lane)| lane.failures.load(Ordering::Relaxed) >= threshold)
            .map(|(peer, _)| *peer)
            .collect()
    }

    pub fn peer_count(&self) -> usize {
        self.lanes.lock().map(|lanes| lanes.len()).unwrap_or(0)
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }
}

/// A frame that cannot be written within this bound is a dead peer, not a
/// slow one: at 10 fps LAN/WAN a sealed frame leaves in milliseconds, but
/// writing to a silently killed peer *blocks forever* — the transport's
/// flow-control window fills and the send future never resolves, so
/// without a deadline the failure counter never moves and re-election
/// never fires.
const LANE_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
/// Opening a substream dials if needed; bound it the same way.
const LANE_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// One lane's drain loop: open the stream (retrying — the peer may still
/// be dialing), then pop-and-send forever. A send failure *or stall*
/// closes the stream, demands a keyframe and reopens: frames are
/// expendable, the lane is not.
async fn run_lane(
    transport: Arc<dyn MediaStreamTransport>,
    call: CallId,
    peer: IdentityId,
    queue: Arc<Mutex<VideoSendQueue>>,
    wake: Arc<Notify>,
    failures: Arc<AtomicU32>,
) {
    let mut stream = None;
    // Per-second send-stage stats: how long each frame spent between
    // entering the fan-out (seal/relay) and leaving on the wire — the
    // encode→send hop of the latency instrumentation.
    let mut win_start = std::time::Instant::now();
    let mut win_sends: u64 = 0;
    let mut win_tx_ms_sum: f64 = 0.0;
    let mut win_tx_ms_max: f64 = 0.0;
    loop {
        // Stream first, frames second: nothing is consumed from the queue
        // until there is somewhere to send it, so the opening keyframe
        // survives however long the substream takes to come up.
        if stream.is_none() {
            match tokio::time::timeout(
                LANE_OPEN_TIMEOUT,
                transport.open_stream(peer, call, MediaStreamKind::Video),
            )
            .await
            {
                Ok(Ok(opened)) => stream = Some(opened),
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %call, %error, "video lane: stream open failed");
                    failures.fetch_add(1, Ordering::Relaxed);
                    // Whatever queued while unreachable is stale by the
                    // time the stream exists; restart from a keyframe.
                    if let Ok(mut queue) = queue.lock() {
                        queue.reset_for_keyframe();
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                Err(_) => {
                    tracing::debug!(%peer, %call, "video lane: stream open timed out");
                    failures.fetch_add(1, Ordering::Relaxed);
                    if let Ok(mut queue) = queue.lock() {
                        queue.reset_for_keyframe();
                    }
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
            match tokio::time::timeout(LANE_SEND_TIMEOUT, lane_stream.send(&frame.sealed)).await {
                Ok(Ok(())) => {
                    failures.store(0, Ordering::Relaxed);
                    let tx_ms = frame.age().as_secs_f64() * 1_000.0;
                    win_sends += 1;
                    win_tx_ms_sum += tx_ms;
                    win_tx_ms_max = win_tx_ms_max.max(tx_ms);
                    if win_start.elapsed() >= std::time::Duration::from_secs(1) {
                        tracing::info!(
                            %peer,
                            sends = win_sends,
                            tx_ms_avg = format!("{:.2}", win_tx_ms_sum / win_sends as f64).as_str(),
                            tx_ms_max = format!("{win_tx_ms_max:.2}").as_str(),
                            "video lane"
                        );
                        win_start = std::time::Instant::now();
                        win_sends = 0;
                        win_tx_ms_sum = 0.0;
                        win_tx_ms_max = 0.0;
                    }
                }
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %call, %error, "video lane: send failed, reopening");
                    failures.fetch_add(1, Ordering::Relaxed);
                    stream = None;
                    if let Ok(mut queue) = queue.lock() {
                        queue.reset_for_keyframe();
                    }
                }
                Err(_) => {
                    tracing::debug!(%peer, %call, "video lane: send stalled, reopening");
                    failures.fetch_add(1, Ordering::Relaxed);
                    stream = None;
                    if let Ok(mut queue) = queue.lock() {
                        queue.reset_for_keyframe();
                    }
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
    /// Captured frames dropped unencoded because a newer capture was
    /// already pending (latest-wins on the capture channel).
    pub skipped_stale: u64,
    pub encoded_bytes: u64,
}

/// Live knobs of a running video sender. The feedback loop's consumer
/// side: the engine stores the adapted target here and the encode loop
/// applies it on the next frame — no restart, no channel round trip.
#[derive(Debug)]
pub struct VideoSenderControl {
    bitrate_bps: AtomicU32,
}

impl VideoSenderControl {
    pub fn new(initial_bps: u32) -> Arc<Self> {
        Arc::new(VideoSenderControl {
            bitrate_bps: AtomicU32::new(initial_bps),
        })
    }

    pub fn set_bitrate(&self, bps: u32) {
        self.bitrate_bps.store(bps, Ordering::Relaxed);
    }

    pub fn bitrate(&self) -> u32 {
        self.bitrate_bps.load(Ordering::Relaxed)
    }
}

/// Pump captured frames through gate → encoder → seal → fan-out until the
/// capture channel closes. Returns totals. The frame is encoded and
/// sealed exactly once regardless of viewer count.
///
/// Latest-wins on the capture channel: when more than one captured frame
/// is pending (the encoder fell behind the capture cadence), everything
/// but the newest is dropped *before* encoding — encoding stale pictures
/// only adds latency to every viewer. Raw captures carry no inter-frame
/// state, so skipping them is always safe (unlike encoded deltas).
pub async fn run_video_sender(
    fanout: Arc<VideoFanout>,
    key: &CallKey,
    ssrc: u32,
    _fps_hint: u32,
    control: Arc<VideoSenderControl>,
    mut frames: mpsc::Receiver<CapturedFrame>,
    mut encoder: Box<dyn VideoEncoder>,
) -> VideoSenderStats {
    let mut gate = StaticFrameGate::default();
    let mut counter: u64 = 0;
    let mut totals = VideoSenderStats::default();
    let mut window = VideoSenderStats::default();
    let mut window_start = std::time::Instant::now();
    let mut window_enc_ms_sum: f64 = 0.0;
    let mut window_enc_ms_max: f64 = 0.0;
    let mut applied_bitrate = control.bitrate();
    while let Some(mut frame) = frames.recv().await {
        // Drain to the newest pending capture (latest-wins).
        while let Ok(newer) = frames.try_recv() {
            frame = newer;
            totals.skipped_stale += 1;
            window.skipped_stale += 1;
        }
        let wanted_bitrate = control.bitrate();
        if wanted_bitrate != applied_bitrate {
            tracing::info!(
                from_bps = applied_bitrate,
                to_bps = wanted_bitrate,
                "video tx: adapting encoder bitrate"
            );
            encoder.set_target_bitrate(wanted_bitrate);
            applied_bitrate = wanted_bitrate;
        }
        let keyframe_wanted = fanout.keyframe_wanted();
        // Static frames cost nothing — unless a viewer needs a keyframe,
        // which a static screen must not starve.
        if !gate.changed(&frame) && !keyframe_wanted {
            totals.skipped_static += 1;
            window.skipped_static += 1;
            continue;
        }
        // Stamp *before* the encode: this is (approximately) when the
        // pixels left the glass — the drain above keeps the gap between
        // capture and this point within one frame interval.
        let ts = ts90k_now();
        let enc_start = std::time::Instant::now();
        let encoded = match encoder.encode(&frame, keyframe_wanted) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%error, "video tx: encode failed, stopping sender");
                break;
            }
        };
        let enc_ms = enc_start.elapsed().as_secs_f64() * 1_000.0;
        window_enc_ms_sum += enc_ms;
        window_enc_ms_max = window_enc_ms_max.max(enc_ms);
        let mut flags = FLAG_END_OF_PICTURE;
        if encoded.keyframe {
            flags |= FLAG_KEYFRAME;
        }
        let header = FrameHeader {
            counter,
            ts,
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
        fanout.broadcast(&SealedVideoFrame::new(Arc::new(sealed), encoded.keyframe));
        if window_start.elapsed() >= std::time::Duration::from_secs(1) {
            let enc_avg = if window.encoded > 0 {
                window_enc_ms_sum / window.encoded as f64
            } else {
                0.0
            };
            tracing::info!(
                fps = window.encoded,
                bytes_per_s = window.encoded_bytes,
                keyframes = window.keyframes,
                skipped_static = window.skipped_static,
                skipped_stale = window.skipped_stale,
                enc_ms_avg = format!("{enc_avg:.2}").as_str(),
                enc_ms_max = format!("{window_enc_ms_max:.2}").as_str(),
                tx_lanes = fanout.peer_count(),
                dropped_total = fanout.dropped_total(),
                bitrate_bps = applied_bitrate,
                "video tx"
            );
            window = VideoSenderStats::default();
            window_enc_ms_sum = 0.0;
            window_enc_ms_max = 0.0;
            window_start = std::time::Instant::now();
        }
    }
    totals
}

/// Receiver statistics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VideoReceiverStats {
    pub presented: u64,
    /// Frames decoded (the H.264 delta chain needs every frame) but not
    /// presented because a newer picture of the same sender was already
    /// pending — the latest-wins render path.
    pub presented_skipped: u64,
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
    // Per-second stage timing: receive→decode queue wait, decode cost,
    // and the glass-to-glass estimate at present time (sender wall-clock
    // stamp → shared wall clock on one machine).
    window_q_ms_sum: f64,
    window_q_ms_max: f64,
    window_dec_ms_sum: f64,
    window_dec_ms_max: f64,
    window_g2g_ms_sum: f64,
    window_g2g_ms_max: f64,
    window_presented: u64,
}

/// A sealed frame headed for the local decoder, stamped when it arrived
/// off the network (the pump's tap) so the receive→decode queue wait is
/// measurable.
pub type TappedFrame = (IdentityId, Vec<u8>, std::time::Instant);

/// Drain a call's video tap into the sink until the tap closes or
/// `expect_frames` pictures have been presented. Streams are ordered per
/// sender, so there is no jitter buffer: verify, gate on the first
/// keyframe, decode, present. Each sender gets its own decoder.
///
/// Latest-wins presentation: every arrived frame is *decoded* (H.264
/// deltas chain — skipping one would corrupt the stream until the next
/// IDR), but when several frames are pending at once only the newest
/// decoded picture per sender is *presented*. A viewer therefore always
/// renders the freshest arrived picture instead of replaying a backlog.
pub async fn run_video_receiver(
    mut tap: mpsc::Receiver<TappedFrame>,
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
        let Some(first) = tap.recv().await else {
            break;
        };
        // Gather everything already pending: the batch is decoded in
        // order, but only its newest picture per sender is presented.
        let mut batch = vec![first];
        while let Ok(more) = tap.try_recv() {
            batch.push(more);
        }
        // Newest decodable picture per sender in this batch, with its
        // capture stamp.
        let mut freshest: BTreeMap<IdentityId, (DecodedPicture, u32)> = BTreeMap::new();
        for (from, sealed, arrived) in batch {
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
                window_q_ms_sum: 0.0,
                window_q_ms_max: 0.0,
                window_dec_ms_sum: 0.0,
                window_dec_ms_max: 0.0,
                window_g2g_ms_sum: 0.0,
                window_g2g_ms_max: 0.0,
                window_presented: 0,
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
            let q_ms = arrived.elapsed().as_secs_f64() * 1_000.0;
            lane.window_q_ms_sum += q_ms;
            lane.window_q_ms_max = lane.window_q_ms_max.max(q_ms);
            let dec_start = std::time::Instant::now();
            let decoded = lane.decoder.decode(&payload);
            let dec_ms = dec_start.elapsed().as_secs_f64() * 1_000.0;
            lane.window_dec_ms_sum += dec_ms;
            lane.window_dec_ms_max = lane.window_dec_ms_max.max(dec_ms);
            if let Some(picture) = decoded {
                if freshest.insert(from, (picture, header.ts)).is_some() {
                    // An older decoded picture of this sender was pending:
                    // superseded before it ever reached the sink.
                    stats.presented_skipped += 1;
                }
            }
        }
        for (from, (picture, ts)) in freshest {
            if let Some(lane) = lanes.get_mut(&from) {
                present(&mut stats, lane, sink, from, picture, ts);
            }
        }
        for (from, lane) in lanes.iter_mut() {
            if lane.window_frames > 0
                && lane.window_start.elapsed() >= std::time::Duration::from_secs(1)
            {
                let frames = lane.window_frames as f64;
                let presented = lane.window_presented.max(1) as f64;
                tracing::info!(
                    peer = %from,
                    fps = lane.window_frames,
                    bytes_per_s = lane.window_bytes,
                    presented = lane.presented,
                    q_ms_avg = format!("{:.2}", lane.window_q_ms_sum / frames).as_str(),
                    q_ms_max = format!("{:.2}", lane.window_q_ms_max).as_str(),
                    dec_ms_avg = format!("{:.2}", lane.window_dec_ms_sum / frames).as_str(),
                    dec_ms_max = format!("{:.2}", lane.window_dec_ms_max).as_str(),
                    g2g_ms_avg = format!("{:.1}", lane.window_g2g_ms_sum / presented).as_str(),
                    g2g_ms_max = format!("{:.1}", lane.window_g2g_ms_max).as_str(),
                    "video rx"
                );
                lane.window_bytes = 0;
                lane.window_frames = 0;
                lane.window_q_ms_sum = 0.0;
                lane.window_q_ms_max = 0.0;
                lane.window_dec_ms_sum = 0.0;
                lane.window_dec_ms_max = 0.0;
                lane.window_g2g_ms_sum = 0.0;
                lane.window_g2g_ms_max = 0.0;
                lane.window_presented = 0;
                lane.window_start = std::time::Instant::now();
            }
        }
    }
    stats
}

/// Hand one picture to the sink and account for it (shared by the
/// baseline per-frame path and the latest-wins batch path).
fn present(
    stats: &mut VideoReceiverStats,
    lane: &mut RxLane,
    sink: &mut dyn VideoSink,
    from: IdentityId,
    picture: DecodedPicture,
    ts90k: u32,
) {
    let g2g_ms = ts90k_diff_ms(ts90k_now(), ts90k);
    lane.window_g2g_ms_sum += g2g_ms;
    lane.window_g2g_ms_max = lane.window_g2g_ms_max.max(g2g_ms);
    lane.window_presented += 1;
    lane.presented += 1;
    stats.presented += 1;
    sink.present(from, picture, ts90k);
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
        SealedVideoFrame::new(Arc::new(vec![tag]), keyframe)
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
        fn present(&mut self, from: IdentityId, picture: DecodedPicture, _ts90k: u32) {
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

    /// A peer killed silently never errors — its sends just stall on a
    /// full flow-control window. The lane's deadline must turn that into
    /// counted failures, or forwarder re-election can never fire.
    #[tokio::test(start_paused = true)]
    async fn a_silently_dead_peer_becomes_a_failing_lane() {
        struct StallingStream;

        #[async_trait]
        impl MediaSendStream for StallingStream {
            async fn send(&mut self, _sealed_frame: &[u8]) -> Result<(), TransportError> {
                std::future::pending().await
            }
        }

        struct StallingTransport;

        #[async_trait]
        impl MediaStreamTransport for StallingTransport {
            async fn open_stream(
                &self,
                _to: IdentityId,
                _call: CallId,
                _kind: MediaStreamKind,
            ) -> Result<Box<dyn MediaSendStream>, TransportError> {
                Ok(Box::new(StallingStream))
            }
        }

        let fanout = VideoFanout::new(Arc::new(StallingTransport), call());
        fanout.set_peers(vec![identity(2)]);
        assert!(fanout.failing_peers(3).is_empty());
        // Keep feeding keyframes the way a live share would (each failed
        // run demands one); the stalled sends must accumulate failures.
        for _ in 0..8 {
            fanout.broadcast(&sealed_frame(1, true));
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        }
        assert_eq!(
            fanout.failing_peers(3),
            vec![identity(2)],
            "a stalled lane must count as unreachable"
        );
    }

    #[test]
    fn priming_seeds_a_run_that_survives_the_next_broadcast() {
        let mut queue = VideoSendQueue::new(2);
        let run: Vec<SealedVideoFrame> = vec![
            sealed_frame(0, true),
            sealed_frame(1, false),
            sealed_frame(2, false),
            sealed_frame(3, false),
        ];
        queue.prime(&run);
        assert!(!queue.awaiting_keyframe());
        assert_eq!(queue.len(), 4);
        // A live frame lands on top of the primed backlog without nuking
        // it, even though 4 > capacity 2 — the allowance covers the run.
        assert_eq!(queue.push(sealed_frame(4, false)), PushOutcome::Queued);
        let drained: Vec<u8> = std::iter::from_fn(|| queue.pop().map(|f| f.sealed[0])).collect();
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);
        // Fully drained, the allowance is spent: steady-state capacity
        // rules again (keyframe, delta, then overflow on the third).
        queue.push(sealed_frame(5, true));
        queue.push(sealed_frame(6, false));
        assert!(matches!(
            queue.push(sealed_frame(7, false)),
            PushOutcome::DroppedRun { .. }
        ));
    }

    #[test]
    fn priming_refuses_runs_that_do_not_open_with_a_keyframe() {
        let mut queue = VideoSendQueue::new(2);
        queue.prime(&[sealed_frame(1, false)]);
        assert!(queue.is_empty());
        assert!(queue.awaiting_keyframe());
        queue.prime(&[]);
        assert!(queue.awaiting_keyframe());
    }

    #[test]
    fn keyframe_cache_keeps_whole_runs_and_discards_overflow() {
        let mut cache = KeyframeCache::new(20);
        // Deltas before any keyframe: nothing to serve.
        cache.observe(&sealed_frame(1, false));
        assert!(cache.run().is_none());
        // A run forms.
        cache.observe(&sealed_frame(2, true));
        cache.observe(&sealed_frame(3, false));
        let run = cache.run().unwrap();
        assert_eq!(run.len(), 2);
        assert!(run[0].keyframe);
        // A new keyframe restarts the run.
        cache.observe(&sealed_frame(4, true));
        assert_eq!(cache.run().unwrap().len(), 1);
        // Overflow discards the whole run — a partial run is garbage.
        let big = SealedVideoFrame::new(Arc::new(vec![9; 64]), false);
        cache.observe(&big);
        assert!(cache.run().is_none());
        // The next keyframe starts serving again.
        cache.observe(&sealed_frame(5, true));
        assert_eq!(cache.run().unwrap().len(), 1);
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
        let sender = tokio::spawn({
            let fanout = Arc::clone(&fanout);
            let key = key.clone();
            async move {
                run_video_sender(
                    fanout,
                    &key,
                    77,
                    10,
                    VideoSenderControl::new(1_500_000),
                    frame_rx,
                    Box::new(FakeEncoder {
                        force_all_key: false,
                        frames_seen: 0,
                    }),
                )
                .await
            }
        });
        // Paced like a live capture: wait for each frame to reach both
        // lanes before sending the next, so the sender's latest-wins
        // drain never collapses them.
        frame_tx.send(bgra(4, 4, 1)).await.unwrap();
        for _ in 0..200 {
            if sent.lock().await.len() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        frame_tx.send(bgra(4, 4, 5)).await.unwrap();
        for _ in 0..200 {
            if sent.lock().await.len() == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        drop(frame_tx);
        let totals = sender.await.unwrap();
        assert_eq!(totals.encoded, 2);
        assert_eq!(totals.keyframes, 1);

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
                    VideoSenderControl::new(1_500_000),
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
        for (from, sealed) in frames {
            tap_tx
                .send((from, sealed, std::time::Instant::now()))
                .await
                .unwrap();
        }
        drop(tap_tx);

        let mut sink = CollectVideoSink {
            pictures: Vec::new(),
        };
        let stats =
            run_video_receiver(tap_rx, &key, || Box::new(FakeDecoder), &mut sink, None).await;
        // Everything pending arrives as one backlog batch: every frame is
        // decoded (the delta chain), but only the *newest* picture per
        // sender reaches the sink — latest-wins rendering.
        assert_eq!(stats.presented, 2);
        assert_eq!(stats.presented_skipped, 2);
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
        assert_eq!(from_1, vec![12], "only sender 1's newest picture");
        assert_eq!(from_2, vec![21], "only sender 2's newest picture");
    }

    /// Frames that arrive one at a time (a live share at capture cadence)
    /// are presented one at a time — latest-wins only collapses an actual
    /// backlog, never a healthy stream.
    #[tokio::test]
    async fn receiver_presents_every_frame_when_none_are_backlogged() {
        let key = CallKey::new([5; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(64);
        let push = |counter: u64, keyframe: bool, fill: u8| {
            let mut bytes = vec![u8::from(keyframe)];
            bytes.extend_from_slice(&4u32.to_be_bytes());
            bytes.extend_from_slice(&4u32.to_be_bytes());
            bytes.extend_from_slice(&[fill; 64]);
            let header = FrameHeader {
                counter,
                ts: 0,
                ssrc: 1,
                kind: MediaKind::Video,
                flags: FLAG_END_OF_PICTURE | if keyframe { FLAG_KEYFRAME } else { 0 },
            };
            (identity(1), seal(&key, header, &bytes).unwrap())
        };
        let receiver = tokio::spawn({
            let key = key.clone();
            async move {
                let mut sink = CollectVideoSink {
                    pictures: Vec::new(),
                };
                let stats =
                    run_video_receiver(tap_rx, &key, || Box::new(FakeDecoder), &mut sink, Some(3))
                        .await;
                (stats, sink.pictures)
            }
        });
        for (counter, fill) in [(0u64, 10u8), (1, 11), (2, 12)] {
            let (from, sealed) = push(counter, counter == 0, fill);
            tap_tx
                .send((from, sealed, std::time::Instant::now()))
                .await
                .unwrap();
            // Let the receiver drain before the next frame exists.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        drop(tap_tx);
        let (stats, pictures) = receiver.await.unwrap();
        assert_eq!(stats.presented, 3);
        assert_eq!(stats.presented_skipped, 0);
        let fills: Vec<u8> = pictures.iter().map(|(_, p)| p.rgba[0]).collect();
        assert_eq!(fills, vec![10, 11, 12]);
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
        tap_tx
            .send((identity(1), alien, std::time::Instant::now()))
            .await
            .unwrap();
        tap_tx
            .send((identity(2), audio, std::time::Instant::now()))
            .await
            .unwrap();
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
