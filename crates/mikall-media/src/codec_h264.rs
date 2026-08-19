//! H.264 codec adapters over the bundled Cisco OpenH264 (feature
//! `hardware-video`).
//!
//! Why software OpenH264 for v1 and not VideoToolbox: at the pipeline's
//! deliberate operating point (≤1280 wide, 5–15 fps, screen content) the
//! measured encode cost is a few milliseconds per frame — affordable even
//! on the weak-machine target — while the crate needs no OS frameworks,
//! encodes *and* decodes on every platform a viewer might run, and its
//! `ScreenContentRealTime` mode is tuned for exactly this material.
//! Hardware VideoToolbox slots in later behind the same
//! [`VideoEncoder`]/[`VideoDecoder`] ports when higher fps or resolution
//! justify the platform glue.

use openh264::decoder::Decoder;
use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, FrameType, UsageType};
use openh264::formats::{BgraSliceU8, YUVBuffer, YUVSource};
use openh264::OpenH264API;

use crate::video::{
    crop_to_even, downscale_to_max_width, CapturedFrame, DecodedPicture, EncodedPicture,
    VideoDecoder, VideoEncoder, VideoError, MAX_WIDTH,
};

/// Target bitrate: generous for 1280-wide screen content at 10 fps, small
/// enough for a slow uplink to carry one copy (the whole point of sending
/// each frame once).
const TARGET_BITRATE_BPS: u32 = 1_500_000;

/// Safety-net IDR interval in frames (~15 s at 10 fps). Late joiners get
/// an immediate forced IDR through the fan-out; this only bounds how long
/// a silently desynced viewer stays broken.
const INTRA_PERIOD_FRAMES: u32 = 150;

/// OpenH264 encoder behind the [`VideoEncoder`] port.
pub struct H264Encoder {
    encoder: Encoder,
    yuv: Option<YUVBuffer>,
    fps: u32,
}

impl std::fmt::Debug for H264Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H264Encoder").finish_non_exhaustive()
    }
}

impl H264Encoder {
    pub fn new(fps: u32) -> Result<Self, VideoError> {
        let config = EncoderConfig::new()
            .usage_type(UsageType::ScreenContentRealTime)
            .max_frame_rate(FrameRate::from_hz(fps.max(1) as f32))
            .bitrate(BitRate::from_bps(TARGET_BITRATE_BPS))
            .intra_frame_period(openh264::encoder::IntraFramePeriod::from_num_frames(
                INTRA_PERIOD_FRAMES,
            ));
        let encoder = Encoder::with_api_config(OpenH264API::from_source(), config)
            .map_err(|e| VideoError::Encode(e.to_string()))?;
        Ok(H264Encoder {
            encoder,
            yuv: None,
            fps,
        })
    }
}

impl VideoEncoder for H264Encoder {
    fn encode(
        &mut self,
        frame: &CapturedFrame,
        force_keyframe: bool,
    ) -> Result<Option<EncodedPicture>, VideoError> {
        if frame.width == 0 || frame.height == 0 || frame.bgra.is_empty() {
            return Err(VideoError::BadDimensions(frame.width, frame.height));
        }
        // Normalize: the weak-machine width cap, then even dimensions.
        let shaped = crop_to_even(downscale_to_max_width(frame.clone(), MAX_WIDTH));
        let (w, h) = (shaped.width as usize, shaped.height as usize);
        if shaped.bgra.len() != w * h * 4 {
            return Err(VideoError::BadDimensions(frame.width, frame.height));
        }
        let recreate = self
            .yuv
            .as_ref()
            .is_none_or(|yuv| yuv.dimensions() != (w, h));
        if recreate {
            self.yuv = Some(YUVBuffer::new(w, h));
        }
        let Some(yuv) = self.yuv.as_mut() else {
            return Err(VideoError::BadDimensions(frame.width, frame.height));
        };
        yuv.read_bgra8(BgraSliceU8::new(&shaped.bgra, (w, h)));
        if force_keyframe {
            self.encoder.force_intra_frame();
        }
        let bitstream = self
            .encoder
            .encode(yuv)
            .map_err(|e| VideoError::Encode(e.to_string()))?;
        let keyframe = match bitstream.frame_type() {
            FrameType::IDR | FrameType::I => true,
            FrameType::P | FrameType::IPMixed => false,
            FrameType::Skip | FrameType::Invalid => return Ok(None),
        };
        let bytes = bitstream.to_vec();
        if bytes.is_empty() {
            return Ok(None);
        }
        let _ = self.fps; // recorded for future rate adaptation
        Ok(Some(EncodedPicture { bytes, keyframe }))
    }
}

/// OpenH264 decoder behind the [`VideoDecoder`] port. Decode trouble is
/// per-frame and non-fatal: the caller skips until the stream recovers
/// (next keyframe).
pub struct H264Decoder {
    decoder: Decoder,
}

impl std::fmt::Debug for H264Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H264Decoder").finish_non_exhaustive()
    }
}

impl H264Decoder {
    pub fn new() -> Result<Self, VideoError> {
        let decoder = Decoder::new().map_err(|e| VideoError::Encode(e.to_string()))?;
        Ok(H264Decoder { decoder })
    }
}

impl VideoDecoder for H264Decoder {
    fn decode(&mut self, packet: &[u8]) -> Option<DecodedPicture> {
        match self.decoder.decode(packet) {
            Ok(Some(yuv)) => {
                let (width, height) = yuv.dimensions();
                let mut rgba = vec![0u8; width * height * 4];
                yuv.write_rgba8(&mut rgba);
                Some(DecodedPicture {
                    width: width as u32,
                    height: height as u32,
                    rgba,
                })
            }
            Ok(None) => None,
            Err(error) => {
                tracing::debug!(%error, "video rx: undecodable frame skipped");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// A synthetic gradient so the roundtrip has real luminance structure.
    fn gradient(width: u32, height: u32) -> CapturedFrame {
        let mut bgra = Vec::with_capacity((width * height * 4) as usize);
        for y in 0..height {
            for x in 0..width {
                bgra.extend_from_slice(&[
                    (x % 256) as u8,
                    (y % 256) as u8,
                    ((x + y) % 256) as u8,
                    255,
                ]);
            }
        }
        CapturedFrame {
            width,
            height,
            bgra,
        }
    }

    #[test]
    fn encode_decode_roundtrip_yields_the_same_picture_shape() {
        let mut encoder = H264Encoder::new(10).unwrap();
        let mut decoder = H264Decoder::new().unwrap();
        let frame = gradient(320, 180);

        let first = encoder.encode(&frame, false).unwrap().unwrap();
        assert!(first.keyframe, "the opening frame must be an IDR");
        let picture = decoder.decode(&first.bytes).unwrap();
        assert_eq!((picture.width, picture.height), (320, 180));
        assert_eq!(picture.rgba.len(), 320 * 180 * 4);
        // Lossy, but it must still look like our gradient: the top-left
        // pixel is near-black, the bottom-right is bright.
        let tl = &picture.rgba[0..3];
        let br_at = (180 * 320 - 1) * 4;
        let br = &picture.rgba[br_at..br_at + 3];
        assert!(tl.iter().all(|c| *c < 96), "top-left too bright: {tl:?}");
        let sum = |px: &[u8]| px.iter().map(|c| u32::from(*c)).sum::<u32>();
        assert!(sum(br) > sum(tl), "gradient direction lost");

        // A delta frame follows and decodes too.
        let mut second_frame = frame.clone();
        second_frame.bgra[0] = 200;
        let second = encoder.encode(&second_frame, false).unwrap().unwrap();
        assert!(!second.keyframe);
        assert!(decoder.decode(&second.bytes).is_some());
    }

    #[test]
    fn forcing_a_keyframe_mid_stream_yields_an_idr() {
        let mut encoder = H264Encoder::new(10).unwrap();
        let frame = gradient(320, 180);
        let _ = encoder.encode(&frame, false).unwrap().unwrap();
        let forced = encoder.encode(&frame, true).unwrap().unwrap();
        assert!(forced.keyframe, "force_keyframe must produce an IDR");
    }

    #[test]
    fn odd_dimensions_are_cropped_not_fatal() {
        let mut encoder = H264Encoder::new(10).unwrap();
        let mut decoder = H264Decoder::new().unwrap();
        let odd = gradient(321, 181);
        let encoded = encoder.encode(&odd, false).unwrap().unwrap();
        let picture = decoder.decode(&encoded.bytes).unwrap();
        assert_eq!((picture.width, picture.height), (320, 180));
    }
}
