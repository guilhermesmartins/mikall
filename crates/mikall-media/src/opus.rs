//! Opus codec adapter (libopus via `audiopus`): 48 kHz mono, VOIP tuning,
//! in-band FEC for ~5% expected loss, decoder-side PLC on gaps.

use audiopus::coder::{Decoder, Encoder};
use audiopus::{Application, Channels, SampleRate};

use crate::engine::{AudioCodec, FRAME_SAMPLES};

#[derive(Debug, thiserror::Error)]
pub enum OpusError {
    #[error("opus init failed: {0}")]
    Init(String),
}

pub struct OpusCodec {
    encoder: Encoder,
    decoder: Decoder,
    encode_buf: Vec<u8>,
}

impl std::fmt::Debug for OpusCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpusCodec").finish_non_exhaustive()
    }
}

impl OpusCodec {
    pub fn new() -> Result<Self, OpusError> {
        let mut encoder = Encoder::new(SampleRate::Hz48000, Channels::Mono, Application::Voip)
            .map_err(|e| OpusError::Init(e.to_string()))?;
        let _ = encoder.set_inband_fec(true);
        let _ = encoder.set_packet_loss_perc(5);
        let decoder = Decoder::new(SampleRate::Hz48000, Channels::Mono)
            .map_err(|e| OpusError::Init(e.to_string()))?;
        Ok(OpusCodec {
            encoder,
            decoder,
            encode_buf: vec![0u8; 4000],
        })
    }
}

impl AudioCodec for OpusCodec {
    fn encode(&mut self, pcm: &[i16]) -> Vec<u8> {
        match self.encoder.encode(pcm, &mut self.encode_buf) {
            Ok(len) => self.encode_buf[..len].to_vec(),
            Err(_) => Vec::new(),
        }
    }

    fn decode(&mut self, packet: Option<&[u8]>) -> Vec<i16> {
        let mut out = vec![0i16; FRAME_SAMPLES];
        match self.decoder.decode(packet, &mut out[..], false) {
            Ok(samples) => {
                out.truncate(samples);
                out
            }
            Err(_) => vec![0i16; FRAME_SAMPLES],
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::engine::{AudioSource, SineSource};

    #[test]
    fn opus_roundtrip_produces_audio() {
        let mut codec = OpusCodec::new().unwrap();
        let mut source = SineSource::new(3);
        let mut decoded_total = 0usize;
        while let Some(pcm) = source.next_frame() {
            let packet = codec.encode(&pcm);
            assert!(!packet.is_empty(), "encoder produced no bytes");
            assert!(packet.len() < 400, "voice frame should compress well");
            let decoded = codec.decode(Some(&packet));
            assert_eq!(decoded.len(), FRAME_SAMPLES);
            decoded_total += decoded.len();
        }
        assert_eq!(decoded_total, 3 * FRAME_SAMPLES);
    }

    #[test]
    fn packet_loss_concealment_yields_a_frame() {
        let mut codec = OpusCodec::new().unwrap();
        let pcm = SineSource::new(1).next_frame().unwrap();
        let packet = codec.encode(&pcm);
        let _ = codec.decode(Some(&packet));
        let concealed = codec.decode(None);
        assert_eq!(concealed.len(), FRAME_SAMPLES);
    }
}
