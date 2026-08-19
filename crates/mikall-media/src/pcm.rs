//! Pure PCM shaping between device formats and the pipeline's canonical
//! 48 kHz mono `i16` frames. Everything here is deterministic and tested
//! without hardware; the cpal adapters (feature `hardware-audio`) are thin
//! glue over these functions.

use crate::engine::FRAME_SAMPLES;

/// The pipeline's canonical sample rate.
pub const PIPELINE_RATE: u32 = 48_000;

/// Average interleaved channels down to mono.
pub fn downmix(interleaved: &[f32], channels: usize) -> Vec<f32> {
    if channels <= 1 {
        return interleaved.to_vec();
    }
    interleaved
        .chunks_exact(channels)
        .map(|frame| frame.iter().sum::<f32>() / channels as f32)
        .collect()
}

/// Streaming linear resampler (mono `f32`). Keeps a fractional read
/// position and the last sample across calls so chunk boundaries do not
/// click. Identity when the rates match.
#[derive(Debug)]
pub struct Resampler {
    from: u32,
    to: u32,
    /// Fractional position into the *virtual* stream `[prev, input...]`.
    pos: f64,
    prev: f32,
    primed: bool,
}

impl Resampler {
    pub fn new(from: u32, to: u32) -> Self {
        Resampler {
            from: from.max(1),
            to: to.max(1),
            pos: 0.0,
            prev: 0.0,
            primed: false,
        }
    }

    pub fn is_identity(&self) -> bool {
        self.from == self.to
    }

    pub fn push(&mut self, input: &[f32], out: &mut Vec<f32>) {
        if self.is_identity() {
            out.extend_from_slice(input);
            return;
        }
        if input.is_empty() {
            return;
        }
        if !self.primed {
            self.prev = input[0];
            self.primed = true;
        }
        // Virtual stream: prev at index 0, input at indices 1..=len.
        let step = f64::from(self.from) / f64::from(self.to);
        while self.pos + 1e-9 < input.len() as f64 {
            let base = self.pos.floor();
            let frac = (self.pos - base) as f32;
            let idx = base as usize;
            let a = if idx == 0 { self.prev } else { input[idx - 1] };
            let b = input[idx.min(input.len() - 1)];
            out.push(a + (b - a) * frac);
            self.pos += step;
        }
        self.pos -= input.len() as f64;
        self.prev = input[input.len() - 1];
    }
}

/// Accumulates arbitrary-size sample runs into exact
/// [`FRAME_SAMPLES`]-sample `i16` frames.
#[derive(Debug, Default)]
pub struct FrameChunker {
    pending: Vec<i16>,
}

impl FrameChunker {
    pub fn push(&mut self, samples: &[f32], mut on_frame: impl FnMut(Vec<i16>)) {
        for s in samples {
            self.pending.push(f32_to_i16(*s));
            if self.pending.len() == FRAME_SAMPLES {
                on_frame(core::mem::take(&mut self.pending));
            }
        }
    }
}

pub fn f32_to_i16(s: f32) -> i16 {
    (s.clamp(-1.0, 1.0) * 32767.0) as i16
}

pub fn i16_to_f32(s: i16) -> f32 {
    f32::from(s) / 32768.0
}

/// Root-mean-square energy of a PCM frame, in the i16 domain. A live mic
/// in a quiet room still shows a small nonzero, varying noise floor —
/// which is exactly what makes this useful as liveness evidence.
pub fn rms(pcm: &[i16]) -> f64 {
    if pcm.is_empty() {
        return 0.0;
    }
    let sum: f64 = pcm.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
    (sum / pcm.len() as f64).sqrt()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn downmix_averages_channels() {
        let stereo = [0.5, -0.5, 1.0, 0.0];
        assert_eq!(downmix(&stereo, 2), vec![0.0, 0.5]);
        assert_eq!(downmix(&stereo, 1), stereo.to_vec());
    }

    #[test]
    fn identity_resampler_is_a_passthrough() {
        let mut rs = Resampler::new(48_000, 48_000);
        let mut out = Vec::new();
        rs.push(&[0.1, 0.2, 0.3], &mut out);
        assert_eq!(out, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn resampler_produces_the_expected_rate() {
        // 44.1 kHz -> 48 kHz over one second of chunked input.
        let mut rs = Resampler::new(44_100, 48_000);
        let input: Vec<f32> = (0..44_100).map(|i| (i as f32 * 0.01).sin() * 0.5).collect();
        let mut out = Vec::new();
        for chunk in input.chunks(441) {
            rs.push(chunk, &mut out);
        }
        let expected = 48_000;
        assert!(
            (out.len() as i64 - expected).unsigned_abs() <= 2,
            "expected ~{expected} samples, got {}",
            out.len()
        );
    }

    #[test]
    fn resampler_interpolates_without_jumps() {
        // A ramp stays a ramp after resampling: successive deltas stay small.
        let mut rs = Resampler::new(44_100, 48_000);
        let ramp: Vec<f32> = (0..4410).map(|i| i as f32 / 4410.0).collect();
        let mut out = Vec::new();
        for chunk in ramp.chunks(100) {
            rs.push(chunk, &mut out);
        }
        for pair in out.windows(2) {
            assert!(
                (pair[1] - pair[0]).abs() < 0.001,
                "discontinuity: {} -> {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn chunker_emits_exact_frames() {
        let mut chunker = FrameChunker::default();
        let mut frames = Vec::new();
        let samples = vec![0.25_f32; FRAME_SAMPLES + FRAME_SAMPLES / 2];
        chunker.push(&samples, |f| frames.push(f));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), FRAME_SAMPLES);
        chunker.push(&samples[..FRAME_SAMPLES / 2], |f| frames.push(f));
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn sample_conversion_clamps_and_roundtrips() {
        assert_eq!(f32_to_i16(2.0), 32767);
        assert_eq!(f32_to_i16(-2.0), -32767);
        assert_eq!(f32_to_i16(0.0), 0);
        let s = i16_to_f32(16384);
        assert!((s - 0.5).abs() < 0.001);
    }

    #[test]
    fn rms_of_silence_is_zero_and_of_tone_is_not() {
        assert_eq!(rms(&[0; 960]), 0.0);
        let tone: Vec<i16> = (0..960)
            .map(|i| ((i as f32 * 0.1).sin() * 8000.0) as i16)
            .collect();
        assert!(rms(&tone) > 1000.0);
    }
}
