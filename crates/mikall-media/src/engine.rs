//! The voice pipeline: source → codec → sealed frames → transport, and
//! tap → jitter → codec (with concealment) → sink.
//!
//! `AudioSource`/`AudioSink` are the hardware ports (cpal on a real
//! machine, fixtures in tests); `AudioCodec` is Opus under the
//! `hardware-audio` feature and raw PCM otherwise.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use mikall_app::ports::MediaTransport;
use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

use crate::frame::{open, seal, CallKey, FrameHeader, MediaKind};
use crate::jitter::{JitterBuffer, Popped};

/// 20 ms of mono audio at 48 kHz.
pub const FRAME_SAMPLES: usize = 960;

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

/// Pump the local source into sealed frames toward every peer. Returns the
/// number of frames sent. `pace` throttles to real time when set (tests run
/// unpaced).
#[allow(clippy::too_many_arguments)]
pub async fn run_sender(
    transport: Arc<dyn MediaTransport>,
    call: CallId,
    key: &CallKey,
    ssrc: u32,
    peers: &[IdentityId],
    mut source: Box<dyn AudioSource>,
    mut codec: Box<dyn AudioCodec>,
    pace: Option<Duration>,
) -> u64 {
    let mut counter: u64 = 0;
    while let Some(pcm) = source.next_frame() {
        let packet = codec.encode(&pcm);
        let header = FrameHeader {
            counter,
            ts: (counter as u32).wrapping_mul(FRAME_SAMPLES as u32),
            ssrc,
            kind: MediaKind::Audio,
            flags: 0,
        };
        let Ok(sealed) = seal(key, header, &packet) else {
            break;
        };
        for peer in peers {
            let _ = transport.send_frame(*peer, call, sealed.clone()).await;
        }
        counter += 1;
        if let Some(pace) = pace {
            tokio::time::sleep(pace).await;
        }
    }
    counter
}

/// Drain a call's media tap into the sink until the tap closes or
/// `expect_frames` frames (including concealed ones) have been played.
pub async fn run_receiver(
    mut tap: mpsc::Receiver<(IdentityId, Vec<u8>)>,
    key: &CallKey,
    mut codec: Box<dyn AudioCodec>,
    sink: &mut dyn AudioSink,
    expect_frames: Option<u64>,
) -> ReceiverStats {
    let mut jitter = JitterBuffer::new(3);
    let mut stats = ReceiverStats::default();
    loop {
        if let Some(target) = expect_frames {
            if stats.played + stats.concealed >= target {
                break;
            }
        }
        let Some((_, sealed)) = tap.recv().await else {
            break;
        };
        match open(key, &sealed) {
            Ok((header, payload)) => jitter.push(header.counter, payload),
            Err(_) => {
                stats.rejected += 1;
                continue;
            }
        }
        loop {
            match jitter.pop_next() {
                Popped::Frame(packet) => {
                    sink.play(&codec.decode(Some(&packet)));
                    stats.played += 1;
                }
                Popped::Missing => {
                    sink.play(&codec.decode(None));
                    stats.concealed += 1;
                }
                Popped::Waiting => break,
            }
        }
    }
    stats
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverStats {
    pub played: u64,
    pub concealed: u64,
    /// Frames that failed AEAD authentication (dropped).
    pub rejected: u64,
}
