//! The voice pipeline: source → codec → sealed frames → transport, and
//! tap → per-sender jitter → codec (with concealment) → sink.
//!
//! `AudioSource`/`AudioSink` are the hardware ports (cpal on a real
//! machine, fixtures in tests); `AudioCodec` is Opus under the
//! `hardware-audio` feature and raw PCM otherwise. Frames reach the sender
//! through a channel so a real microphone (which produces frames on its
//! own callback thread, at its own pace) and a test fixture drive the same
//! loop.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use mikall_app::ports::MediaTransport;
use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

use crate::frame::{open, seal, CallKey, FrameHeader, MediaKind};
use crate::jitter::{JitterBuffer, Popped};
use crate::pcm;

/// 20 ms of mono audio at 48 kHz.
pub const FRAME_SAMPLES: usize = 960;

/// Frames per pipeline second — the cadence of the liveness logs.
const FRAMES_PER_SECOND: u64 = 50;

pub trait AudioSource: Send {
    /// The next 20 ms PCM frame, or `None` when the stream ends.
    fn next_frame(&mut self) -> Option<Vec<i16>>;
}

pub trait AudioSink: Send {
    fn play(&mut self, pcm: &[i16]);
}

pub trait AudioCodec: Send {
    fn encode(&mut self, pcm: &[i16]) -> Vec<u8>;
    /// `None` = packet lost: produce a concealment frame.
    fn decode(&mut self, packet: Option<&[u8]>) -> Vec<i16>;
}

/// Uncompressed PCM codec (little-endian i16). Concealment = silence.
#[derive(Debug, Default)]
pub struct PcmCodec;

impl AudioCodec for PcmCodec {
    fn encode(&mut self, pcm: &[i16]) -> Vec<u8> {
        pcm.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    fn decode(&mut self, packet: Option<&[u8]>) -> Vec<i16> {
        match packet {
            Some(bytes) => bytes
                .chunks_exact(2)
                .map(|c| i16::from_le_bytes([c[0], c[1]]))
                .collect(),
            None => vec![0; FRAME_SAMPLES],
        }
    }
}

/// Deterministic tone source for tests.
#[derive(Debug)]
pub struct SineSource {
    frames_left: usize,
    phase: f32,
}

impl SineSource {
    pub fn new(frames: usize) -> Self {
        SineSource {
            frames_left: frames,
            phase: 0.0,
        }
    }
}

impl AudioSource for SineSource {
    fn next_frame(&mut self) -> Option<Vec<i16>> {
        if self.frames_left == 0 {
            return None;
        }
        self.frames_left -= 1;
        let mut out = Vec::with_capacity(FRAME_SAMPLES);
        for _ in 0..FRAME_SAMPLES {
            out.push((self.phase.sin() * 8000.0) as i16);
            self.phase += 2.0 * std::f32::consts::PI * 440.0 / 48_000.0;
        }
        Some(out)
    }
}

/// Collects played PCM for assertions.
#[derive(Debug, Default)]
pub struct CollectSink {
    pub samples: Vec<i16>,
    pub concealed_frames: usize,
}

impl AudioSink for CollectSink {
    fn play(&mut self, pcm: &[i16]) {
        self.samples.extend_from_slice(pcm);
    }
}

/// Live controls of one outbound stream, shared between the call session
/// and the running sender: the peer set follows the roster, and mute is a
/// real switch the sender consults on every frame.
#[derive(Debug)]
pub struct SenderControl {
    muted: AtomicBool,
    peers: std::sync::RwLock<Vec<IdentityId>>,
    sent: AtomicU64,
}

impl SenderControl {
    pub fn new(peers: Vec<IdentityId>) -> Arc<Self> {
        Arc::new(SenderControl {
            muted: AtomicBool::new(false),
            peers: std::sync::RwLock::new(peers),
            sent: AtomicU64::new(0),
        })
    }

    /// Local mute: the stream stays alive and paced (counters keep
    /// advancing, so AEAD nonces stay dense and receivers' jitter buffers
    /// stay in steady state) but every frame is silence — the real mic
    /// samples are zeroed *before* they ever reach the encoder.
    pub fn set_muted(&self, muted: bool) {
        self.muted.store(muted, Ordering::Relaxed);
    }

    pub fn muted(&self) -> bool {
        self.muted.load(Ordering::Relaxed)
    }

    /// Follow the roster: joins and leaves mid-call retarget the sender
    /// without restarting it.
    pub fn set_peers(&self, peers: Vec<IdentityId>) {
        if let Ok(mut guard) = self.peers.write() {
            *guard = peers;
        }
    }

    fn peers(&self) -> Vec<IdentityId> {
        self.peers.read().map(|p| p.clone()).unwrap_or_default()
    }

    /// Frames sealed and sent so far (liveness instrumentation).
    pub fn frames_sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
}

/// Adapt a synchronous [`AudioSource`] to the sender's frame channel.
/// `pace` throttles to real time when set (tests run unpaced); hardware
/// capture feeds the channel directly from its callback thread instead.
pub fn frames_from_source(
    mut source: Box<dyn AudioSource>,
    pace: Option<Duration>,
) -> mpsc::Receiver<Vec<i16>> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        while let Some(frame) = source.next_frame() {
            if tx.send(frame).await.is_err() {
                break;
            }
            if let Some(pace) = pace {
                tokio::time::sleep(pace).await;
            }
        }
    });
    rx
}

/// Pump PCM frames into sealed frames toward every peer until the frame
/// channel closes. Returns the number of frames sent. Mute and the peer
/// set are read live from `control` on every frame.
pub async fn run_sender(
    transport: Arc<dyn MediaTransport>,
    call: CallId,
    key: &CallKey,
    ssrc: u32,
    control: Arc<SenderControl>,
    mut frames: mpsc::Receiver<Vec<i16>>,
    mut codec: Box<dyn AudioCodec>,
) -> u64 {
    let mut counter: u64 = 0;
    let mut window_payload_bytes: usize = 0;
    while let Some(mut pcm) = frames.recv().await {
        if control.muted() {
            pcm.iter_mut().for_each(|s| *s = 0);
        }
        let packet = codec.encode(&pcm);
        let header = FrameHeader {
            counter,
            ts: (counter as u32).wrapping_mul(FRAME_SAMPLES as u32),
            ssrc,
            kind: MediaKind::Audio,
            flags: 0,
        };
        window_payload_bytes += packet.len();
        let Ok(sealed) = seal(key, header, &packet) else {
            break;
        };
        for peer in control.peers() {
            let _ = transport.send_frame(peer, call, sealed.clone()).await;
        }
        counter += 1;
        control.sent.store(counter, Ordering::Relaxed);
        if counter % FRAMES_PER_SECOND == 0 {
            tracing::debug!(
                call = %call,
                ssrc,
                sent = counter,
                muted = control.muted(),
                window_payload_bytes,
                "voice tx: +{FRAMES_PER_SECOND} frames"
            );
            window_payload_bytes = 0;
        }
    }
    counter
}

/// One inbound sender stream: its own reorder buffer and decoder state,
/// so a mesh peer's loss or reordering never disturbs another's.
struct Lane {
    jitter: JitterBuffer,
    codec: Box<dyn AudioCodec>,
    played: u64,
    concealed: u64,
    /// Sum of squared samples over the current log window.
    energy: f64,
    energy_samples: u64,
}

/// Drain a call's media tap into the sink until the tap closes or
/// `expect_frames` frames (including concealed ones) have been played.
/// Each sending peer gets its own jitter buffer and codec instance from
/// `make_codec`.
pub async fn run_receiver(
    mut tap: mpsc::Receiver<(IdentityId, Vec<u8>)>,
    key: &CallKey,
    mut make_codec: impl FnMut() -> Box<dyn AudioCodec> + Send,
    sink: &mut dyn AudioSink,
    expect_frames: Option<u64>,
) -> ReceiverStats {
    let mut lanes: std::collections::BTreeMap<IdentityId, Lane> = std::collections::BTreeMap::new();
    let mut stats = ReceiverStats::default();
    loop {
        if let Some(target) = expect_frames {
            if stats.played + stats.concealed >= target {
                break;
            }
        }
        let Some((from, sealed)) = tap.recv().await else {
            break;
        };
        let lane = lanes.entry(from).or_insert_with(|| Lane {
            jitter: JitterBuffer::new(3),
            codec: make_codec(),
            played: 0,
            concealed: 0,
            energy: 0.0,
            energy_samples: 0,
        });
        match open(key, &sealed) {
            Ok((header, payload)) => lane.jitter.push(header.counter, payload),
            Err(_) => {
                stats.rejected += 1;
                continue;
            }
        }
        loop {
            let decoded = match lane.jitter.pop_next() {
                Popped::Frame(packet) => {
                    lane.played += 1;
                    stats.played += 1;
                    lane.codec.decode(Some(&packet))
                }
                Popped::Missing => {
                    lane.concealed += 1;
                    stats.concealed += 1;
                    lane.codec.decode(None)
                }
                Popped::Waiting => break,
            };
            lane.energy += decoded
                .iter()
                .map(|s| f64::from(*s) * f64::from(*s))
                .sum::<f64>();
            lane.energy_samples += decoded.len() as u64;
            sink.play(&decoded);
            if (lane.played + lane.concealed) % FRAMES_PER_SECOND == 0 {
                let rms = if lane.energy_samples == 0 {
                    0.0
                } else {
                    (lane.energy / lane.energy_samples as f64).sqrt()
                };
                tracing::debug!(
                    peer = %from,
                    played = lane.played,
                    concealed = lane.concealed,
                    rejected = stats.rejected,
                    rms = format!("{rms:.1}"),
                    "voice rx: +{FRAMES_PER_SECOND} frames"
                );
                lane.energy = 0.0;
                lane.energy_samples = 0;
            }
        }
    }
    stats
}

/// Root-mean-square energy of a decoded frame — re-exported convenience
/// for sinks that want their own liveness metering.
pub fn frame_rms(pcm: &[i16]) -> f64 {
    pcm::rms(pcm)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverStats {
    pub played: u64,
    pub concealed: u64,
    /// Frames that failed AEAD authentication (dropped).
    pub rejected: u64,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use async_trait::async_trait;
    use mikall_app::ports::TransportError;
    use tokio::sync::Mutex;

    fn identity(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn call() -> CallId {
        CallId::from_bytes([9; 16])
    }

    /// Loopback transport: every sent frame lands on the tap, tagged with
    /// the sending identity it was configured with.
    struct LoopbackTransport {
        from: IdentityId,
        tap: mpsc::Sender<(IdentityId, Vec<u8>)>,
    }

    impl std::fmt::Debug for LoopbackTransport {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LoopbackTransport").finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl MediaTransport for LoopbackTransport {
        async fn send_frame(
            &self,
            _to: IdentityId,
            _call: CallId,
            sealed_frame: Vec<u8>,
        ) -> Result<(), TransportError> {
            let _ = self.tap.send((self.from, sealed_frame)).await;
            Ok(())
        }
    }

    /// Transport that records which peers each frame went to.
    #[derive(Debug, Default)]
    struct RecordingTransport {
        sent_to: Mutex<Vec<IdentityId>>,
    }

    #[async_trait]
    impl MediaTransport for RecordingTransport {
        async fn send_frame(
            &self,
            to: IdentityId,
            _call: CallId,
            _sealed_frame: Vec<u8>,
        ) -> Result<(), TransportError> {
            self.sent_to.lock().await.push(to);
            Ok(())
        }
    }

    #[tokio::test]
    async fn muted_sender_ships_silence_not_mic_audio() {
        let key = CallKey::new([3; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(64);
        let transport = Arc::new(LoopbackTransport {
            from: identity(1),
            tap: tap_tx,
        });

        let control = SenderControl::new(vec![identity(2)]);
        control.set_muted(true);
        let frames = frames_from_source(Box::new(SineSource::new(5)), None);
        let sent = run_sender(
            transport,
            call(),
            &key,
            7,
            Arc::clone(&control),
            frames,
            Box::new(PcmCodec),
        )
        .await;
        assert_eq!(sent, 5);
        assert_eq!(control.frames_sent(), 5);

        let mut sink = CollectSink::default();
        let stats = run_receiver(tap_rx, &key, || Box::new(PcmCodec), &mut sink, Some(5)).await;
        assert_eq!(stats.played, 5);
        assert_eq!(stats.rejected, 0);
        // The tone was zeroed before encoding: pure silence on the wire.
        assert!(sink.samples.iter().all(|s| *s == 0));
        assert_eq!(sink.samples.len(), 5 * FRAME_SAMPLES);
    }

    #[tokio::test]
    async fn unmuting_mid_stream_resumes_real_audio() {
        let key = CallKey::new([3; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(64);
        let transport = Arc::new(LoopbackTransport {
            from: identity(1),
            tap: tap_tx,
        });

        let control = SenderControl::new(vec![identity(2)]);
        control.set_muted(true);
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let sender = tokio::spawn({
            let control = Arc::clone(&control);
            let key = CallKey::new([3; 32]);
            async move {
                run_sender(
                    transport,
                    call(),
                    &key,
                    7,
                    control,
                    frame_rx,
                    Box::new(PcmCodec),
                )
                .await
            }
        });
        let tone: Vec<i16> = SineSource::new(1).next_frame().unwrap();
        frame_tx.send(tone.clone()).await.unwrap();
        // Wait until the muted frame went out, then flip the switch.
        while control.frames_sent() < 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        control.set_muted(false);
        frame_tx.send(tone.clone()).await.unwrap();
        drop(frame_tx);
        assert_eq!(sender.await.unwrap(), 2);

        let mut sink = CollectSink::default();
        run_receiver(tap_rx, &key, || Box::new(PcmCodec), &mut sink, Some(2)).await;
        let (first, second) = sink.samples.split_at(FRAME_SAMPLES);
        assert!(first.iter().all(|s| *s == 0), "muted frame must be silence");
        assert_eq!(second, &tone[..], "unmuted frame carries the mic again");
    }

    #[tokio::test]
    async fn sender_follows_live_peer_changes() {
        let key = CallKey::new([3; 32]);
        let transport = Arc::new(RecordingTransport::default());
        let control = SenderControl::new(vec![identity(2)]);
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let sender = tokio::spawn({
            let transport = Arc::clone(&transport);
            let control = Arc::clone(&control);
            async move {
                run_sender(
                    transport,
                    call(),
                    &key,
                    7,
                    control,
                    frame_rx,
                    Box::new(PcmCodec),
                )
                .await
            }
        });
        frame_tx.send(vec![1; FRAME_SAMPLES]).await.unwrap();
        while control.frames_sent() < 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        control.set_peers(vec![identity(2), identity(3)]);
        frame_tx.send(vec![1; FRAME_SAMPLES]).await.unwrap();
        drop(frame_tx);
        sender.await.unwrap();
        let sent_to = transport.sent_to.lock().await.clone();
        assert_eq!(sent_to, vec![identity(2), identity(2), identity(3)]);
    }

    #[tokio::test]
    async fn receiver_demuxes_interleaved_senders() {
        let key = CallKey::new([5; 32]);
        let (tap_tx, tap_rx) = mpsc::channel(64);

        // Two mesh peers, same shared call key, independent counters —
        // their frames interleave on the tap.
        for counter in 0..4u64 {
            for (n, ssrc) in [(1u8, 10u32), (2u8, 20u32)] {
                let pcm = vec![i16::from(n); FRAME_SAMPLES];
                let packet = PcmCodec.encode(&pcm);
                let header = FrameHeader {
                    counter,
                    ts: 0,
                    ssrc,
                    kind: MediaKind::Audio,
                    flags: 0,
                };
                let sealed = seal(&key, header, &packet).unwrap();
                tap_tx.send((identity(n), sealed)).await.unwrap();
            }
        }
        drop(tap_tx);

        let mut sink = CollectSink::default();
        let stats = run_receiver(tap_rx, &key, || Box::new(PcmCodec), &mut sink, None).await;
        // With per-sender jitter buffers the identical counters never
        // collide: all 8 frames play, none concealed, none dropped late.
        assert_eq!(stats.played, 8);
        assert_eq!(stats.concealed, 0);
        assert_eq!(stats.rejected, 0);
        let ones = sink.samples.iter().filter(|s| **s == 1).count();
        let twos = sink.samples.iter().filter(|s| **s == 2).count();
        assert_eq!(ones, 4 * FRAME_SAMPLES);
        assert_eq!(twos, 4 * FRAME_SAMPLES);
    }
}
