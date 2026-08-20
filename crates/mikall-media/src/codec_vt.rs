//! Hardware H.264 encoding via VideoToolbox (macOS), behind the same
//! [`VideoEncoder`] port as the software OpenH264 adapter — the upgrade
//! the M16 codec seam was designed for.
//!
//! Why VideoToolbox: the dedicated media engine encodes a 1280-wide frame
//! in ~1–3 ms at near-zero CPU, where software OpenH264 costs a two-digit
//! millisecond slice of every frame budget on the weak-machine target.
//! That per-frame saving is *latency for every viewer on every frame*,
//! and it frees the sharer's CPU for the app being shared.
//!
//! Crate choice: the maintained `objc2-*` framework bindings
//! (`objc2-video-toolbox` + core-media/video/foundation) — typed CF smart
//! pointers ([`CFRetained`]) and generated signatures instead of
//! hand-rolled FFI. There is no higher-level safe VT encoder crate that is
//! maintained and MSRV-compatible, so the raw calls live here, contained
//! in this one adapter file behind a scoped `allow(unsafe_code)`.
//!
//! Fallback story (decided by the *caller*, `mikall-node`): construction
//! probes a real session — on any failure the engine falls back to
//! [`crate::codec_h264::H264Encoder`], so non-macOS builds and Macs with
//! broken/absent hardware encoders keep working unchanged. The wire
//! format is identical: Annex B H.264 (SPS/PPS prepended to every IDR)
//! that the OpenH264 software decoder on every viewer already consumes.
#![allow(unsafe_code)] // Contained here: VideoToolbox is a C API; every call site upholds the documented invariants.

use std::ffi::{c_int, c_void};
use std::ptr::{self, NonNull};
use std::sync::Mutex;

use objc2_core_foundation::{
    kCFBooleanFalse, kCFBooleanTrue, CFBoolean, CFDictionary, CFNumber, CFRetained, CFString,
    CFType,
};
use objc2_core_media::{
    kCMSampleAttachmentKey_NotSync, kCMTimeInvalid, kCMVideoCodecType_H264, CMSampleBuffer, CMTime,
    CMTimeFlags, CMVideoFormatDescriptionGetH264ParameterSetAtIndex,
};
use objc2_core_video::{
    kCVPixelFormatType_32BGRA, CVPixelBuffer, CVPixelBufferCreate, CVPixelBufferGetBaseAddress,
    CVPixelBufferGetBytesPerRow, CVPixelBufferLockBaseAddress, CVPixelBufferLockFlags,
    CVPixelBufferUnlockBaseAddress,
};
use objc2_video_toolbox::{
    kVTCompressionPropertyKey_AllowFrameReordering, kVTCompressionPropertyKey_AverageBitRate,
    kVTCompressionPropertyKey_ExpectedFrameRate, kVTCompressionPropertyKey_MaxFrameDelayCount,
    kVTCompressionPropertyKey_MaxKeyFrameInterval, kVTCompressionPropertyKey_ProfileLevel,
    kVTCompressionPropertyKey_RealTime, kVTEncodeFrameOptionKey_ForceKeyFrame,
    kVTProfileLevel_H264_Main_AutoLevel, VTCompressionSession, VTEncodeInfoFlags,
    VTSessionSetProperty,
};

use crate::video::{
    shape_for_encode, CapturedFrame, EncodedPicture, VideoEncoder, VideoError, MAX_WIDTH,
};

/// Safety-net IDR cadence in frames, matching the software adapter's
/// `INTRA_PERIOD_FRAMES` (~10 s at 15 fps): late joiners get an on-demand
/// IDR through the fan-out; this only bounds how long a silently desynced
/// viewer stays broken.
const MAX_KEYFRAME_INTERVAL_FRAMES: i32 = 150;

/// Probe dimensions for eager availability detection: the production
/// capture geometry, so a session that builds here builds for real frames.
const PROBE_WIDTH: i32 = 1280;
const PROBE_HEIGHT: i32 = 720;

/// What the VT output callback observed for one submitted frame.
enum VtEmitted {
    Picture(EncodedPicture),
    /// The rate controller dropped the frame (legal — nothing on the wire).
    Dropped,
    /// Encoding failed with this OSStatus.
    Error(i32),
}

/// The callback's landing zone. Boxed so its address is stable for the
/// session's `refcon` no matter how the encoder value moves.
struct SharedOutput(Mutex<Vec<VtEmitted>>);

struct ActiveSession {
    session: CFRetained<VTCompressionSession>,
    width: usize,
    height: usize,
}

/// VideoToolbox hardware H.264 encoder behind the [`VideoEncoder`] port.
pub struct VtH264Encoder {
    /// Lazily rebuilt when the capture geometry changes; created eagerly
    /// at probe dimensions so hardware availability fails at *construction*
    /// (where the caller can still choose the software fallback).
    active: Option<ActiveSession>,
    output: Box<SharedOutput>,
    fps: u32,
    bitrate_bps: u32,
    pending_bitrate_bps: Option<u32>,
    /// Frame counter, doubling as the presentation clock (pts = n/fps).
    counter: i64,
}

// SAFETY: VideoToolbox sessions are documented thread-safe, the encoder is
// driven from exactly one task at a time (the video sender loop), and every
// other field is plain owned data. The `CFRetained` session handle is just
// a refcounted pointer.
unsafe impl Send for VtH264Encoder {}

impl std::fmt::Debug for VtH264Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VtH264Encoder").finish_non_exhaustive()
    }
}

impl Drop for VtH264Encoder {
    fn drop(&mut self) {
        // Invalidate before the refcon box can die: VT guarantees no
        // callback runs after invalidate returns.
        self.teardown_session();
    }
}

impl VtH264Encoder {
    /// Build the encoder, eagerly creating (and configuring) a real
    /// compression session as the hardware-availability probe. `Err` here
    /// is the signal to fall back to software OpenH264.
    pub fn new(fps: u32, bitrate_bps: u32) -> Result<Self, VideoError> {
        let mut encoder = VtH264Encoder {
            active: None,
            output: Box::new(SharedOutput(Mutex::new(Vec::new()))),
            fps: fps.max(1),
            bitrate_bps,
            pending_bitrate_bps: None,
            counter: 0,
        };
        let session = encoder.build_session(PROBE_WIDTH, PROBE_HEIGHT)?;
        encoder.active = Some(ActiveSession {
            session,
            width: PROBE_WIDTH as usize,
            height: PROBE_HEIGHT as usize,
        });
        Ok(encoder)
    }

    fn teardown_session(&mut self) {
        if let Some(active) = self.active.take() {
            unsafe {
                // Flush, then deterministic teardown (callbacks are done
                // once invalidate returns).
                active.session.complete_frames(kCMTimeInvalid);
                active.session.invalidate();
            }
        }
        if let Ok(mut queue) = self.output.0.lock() {
            queue.clear();
        }
    }

    fn build_session(
        &self,
        width: i32,
        height: i32,
    ) -> Result<CFRetained<VTCompressionSession>, VideoError> {
        let refcon: *mut c_void = (&*self.output as *const SharedOutput)
            .cast_mut()
            .cast::<c_void>();
        let mut session_ptr: *mut VTCompressionSession = ptr::null_mut();
        // SAFETY: the callback matches VTCompressionOutputCallback and the
        // refcon points at the boxed SharedOutput, which outlives the
        // session (Drop invalidates the session first).
        let status = unsafe {
            VTCompressionSession::create(
                None,
                width,
                height,
                kCMVideoCodecType_H264,
                None,
                None,
                None,
                Some(vt_output_callback),
                refcon,
                NonNull::from(&mut session_ptr),
            )
        };
        if status != 0 {
            return Err(VideoError::Encode(format!(
                "VTCompressionSessionCreate failed (OSStatus {status}) — no usable hardware encoder"
            )));
        }
        let session_ptr = NonNull::new(session_ptr).ok_or_else(|| {
            VideoError::Encode("VTCompressionSessionCreate returned a null session".into())
        })?;
        // SAFETY: create hands back a +1 reference we now own.
        let session = unsafe { CFRetained::from_raw(session_ptr) };
        self.configure(&session)?;
        Ok(session)
    }

    /// Apply the low-latency session properties. Property support varies
    /// by encoder generation; the two that define the latency contract
    /// (real-time mode, no frame reordering) are required, the rest are
    /// best-effort with a log line.
    fn configure(&self, session: &CFRetained<VTCompressionSession>) -> Result<(), VideoError> {
        let yes = cf_bool(true)?;
        let no = cf_bool(false)?;
        // RealTime: schedule the encode for deadline (frame cadence) over
        // throughput — VT may otherwise batch for efficiency, which is
        // exactly the latency we are here to remove.
        set_property(session, unsafe { kVTCompressionPropertyKey_RealTime }, yes)?;
        // AllowFrameReordering = false: zero B-frames, so encode order ==
        // decode order == display order. B-frames would force the decoder
        // to hold pictures back — structural latency the pipeline's
        // "newest frame wins" design cannot tolerate. (Also what keeps
        // the sealed-frame counter monotone with presentation.)
        set_property(
            session,
            unsafe { kVTCompressionPropertyKey_AllowFrameReordering },
            no,
        )?;
        // Main profile (auto level): with reordering off there are no
        // B-frames anyway, and Main's CABAC entropy coding buys ~10-15 %
        // bitrate over Baseline at identical quality — bytes that would
        // otherwise queue at the sharer's uplink. Every viewer decodes
        // with OpenH264, which handles Main.
        try_set_property(
            session,
            unsafe { kVTCompressionPropertyKey_ProfileLevel },
            unsafe { kVTProfileLevel_H264_Main_AutoLevel }.as_ref(),
            "ProfileLevel",
        );
        // MaxFrameDelayCount = 0: the compressor may not sit on frames —
        // every submitted frame is emitted before the next arrives.
        // Belt-and-braces with RealTime; unsupported on some encoders,
        // best-effort.
        try_set_property(
            session,
            unsafe { kVTCompressionPropertyKey_MaxFrameDelayCount },
            CFNumber::new_i32(0).as_ref(),
            "MaxFrameDelayCount",
        );
        try_set_property(
            session,
            unsafe { kVTCompressionPropertyKey_MaxKeyFrameInterval },
            CFNumber::new_i32(MAX_KEYFRAME_INTERVAL_FRAMES).as_ref(),
            "MaxKeyFrameInterval",
        );
        try_set_property(
            session,
            unsafe { kVTCompressionPropertyKey_ExpectedFrameRate },
            CFNumber::new_i32(self.fps as i32).as_ref(),
            "ExpectedFrameRate",
        );
        self.apply_bitrate(session, self.bitrate_bps)?;
        Ok(())
    }

    /// The feedback loop's actuator on hardware: VT exposes AverageBitRate
    /// as a *dynamic* session property — the rate controller re-targets
    /// within a frame or two, with no session rebuild and no forced IDR.
    /// Strictly better than the software path's rebuild-with-opening-IDR:
    /// the delta chain continues and no lane needs to resync.
    fn apply_bitrate(
        &self,
        session: &CFRetained<VTCompressionSession>,
        bps: u32,
    ) -> Result<(), VideoError> {
        set_property(
            session,
            unsafe { kVTCompressionPropertyKey_AverageBitRate },
            CFNumber::new_i32(bps.min(i32::MAX as u32) as i32).as_ref(),
        )
    }

    /// Encode one shaped frame through the active session. `Ok(None)`
    /// means VT's rate control dropped the frame — legal, nothing on the
    /// wire.
    fn encode_via_session(
        &mut self,
        shaped: &CapturedFrame,
        force_keyframe: bool,
    ) -> Result<Option<EncodedPicture>, VideoError> {
        let (w, h) = (shaped.width as usize, shaped.height as usize);
        let rebuild = self
            .active
            .as_ref()
            .is_none_or(|active| active.width != w || active.height != h);
        if rebuild {
            self.teardown_session();
            let session = self.build_session(w as i32, h as i32)?;
            self.active = Some(ActiveSession {
                session,
                width: w,
                height: h,
            });
            tracing::info!(width = w, height = h, "video tx: VT session (re)built");
        }
        if let Some(bps) = self.pending_bitrate_bps.take() {
            if bps != self.bitrate_bps {
                if let Some(active) = &self.active {
                    self.apply_bitrate(&active.session, bps)?;
                    tracing::info!(bitrate_bps = bps, "video tx: VT bitrate retargeted live");
                }
                self.bitrate_bps = bps;
            }
        }
        let Some(active) = &self.active else {
            return Err(VideoError::Encode("VT session missing".into()));
        };
        let pixels = bgra_pixel_buffer(shaped)?;
        let frame_properties = if force_keyframe {
            let yes = cf_bool(true)?;
            let value: &CFType = yes.as_ref();
            Some(CFDictionary::from_slices(
                &[unsafe { kVTEncodeFrameOptionKey_ForceKeyFrame }],
                &[value],
            ))
        } else {
            None
        };
        let pts = CMTime {
            value: self.counter,
            timescale: self.fps as i32,
            flags: CMTimeFlags::Valid,
            epoch: 0,
        };
        let duration = CMTime {
            value: 1,
            timescale: self.fps as i32,
            flags: CMTimeFlags::Valid,
            epoch: 0,
        };
        // SAFETY: pixel buffer, times and properties are fully formed; the
        // callback contract is upheld by vt_output_callback.
        let status = unsafe {
            active.session.encode_frame(
                pixels.as_ref(),
                pts,
                duration,
                frame_properties.as_deref().map(|d| d.as_opaque()),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if status != 0 {
            return Err(VideoError::Encode(format!(
                "VTCompressionSessionEncodeFrame failed (OSStatus {status})"
            )));
        }
        // Synchronous completion: at real-time settings the hardware
        // finishes in low single-digit milliseconds, and the pipeline's
        // encode→seal→fan-out flow wants the bytes now (frames must never
        // queue inside the codec — that is hidden latency).
        let status = unsafe { active.session.complete_frames(kCMTimeInvalid) };
        if status != 0 {
            return Err(VideoError::Encode(format!(
                "VTCompressionSessionCompleteFrames failed (OSStatus {status})"
            )));
        }
        self.counter += 1;
        let emitted = self.output.0.lock().ok().and_then(|mut queue| {
            if queue.is_empty() {
                None
            } else {
                Some(queue.drain(..).next_back())
            }
        });
        match emitted.flatten() {
            Some(VtEmitted::Picture(picture)) => Ok(Some(picture)),
            Some(VtEmitted::Dropped) | None => Ok(None),
            Some(VtEmitted::Error(status)) => Err(VideoError::Encode(format!(
                "VT output callback reported OSStatus {status}"
            ))),
        }
    }
}

impl VideoEncoder for VtH264Encoder {
    fn encode(
        &mut self,
        frame: &CapturedFrame,
        force_keyframe: bool,
    ) -> Result<Option<EncodedPicture>, VideoError> {
        if frame.width == 0 || frame.height == 0 || frame.bgra.is_empty() {
            return Err(VideoError::BadDimensions(frame.width, frame.height));
        }
        let shaped = shape_for_encode(frame, MAX_WIDTH);
        match self.encode_via_session(&shaped, force_keyframe) {
            Ok(picture) => Ok(picture),
            Err(error) => {
                // One in-place recovery: hardware sessions can die under
                // GPU resets or display reconfiguration. Rebuild once and
                // retry this frame (the fresh session opens with an IDR,
                // which every lane accepts as a new run); a second failure
                // is real and surfaces to the caller.
                tracing::warn!(%error, "video tx: VT encode failed, rebuilding session once");
                self.teardown_session();
                self.encode_via_session(&shaped, force_keyframe)
            }
        }
    }

    fn set_target_bitrate(&mut self, bps: u32) {
        self.pending_bitrate_bps = Some(bps);
    }
}

/// The C output callback: runs on VT's thread, extracts Annex B bytes and
/// the keyframe flag from the sample buffer, and parks them for the
/// encode call that is blocked in `complete_frames`. Must never panic
/// (unwinding into VideoToolbox is UB) — every failure degrades to
/// `Dropped`/`Error` values instead.
unsafe extern "C-unwind" fn vt_output_callback(
    refcon: *mut c_void,
    _source_refcon: *mut c_void,
    status: i32,
    flags: VTEncodeInfoFlags,
    sample: *mut CMSampleBuffer,
) {
    let Some(output) = (unsafe { refcon.cast::<SharedOutput>().as_ref() }) else {
        return;
    };
    let emitted = if status != 0 {
        VtEmitted::Error(status)
    } else if flags.contains(VTEncodeInfoFlags::FrameDropped) || sample.is_null() {
        VtEmitted::Dropped
    } else {
        // SAFETY: non-null sample buffer, valid for the callback's extent.
        match unsafe { annex_b_of(&*sample) } {
            Some(picture) => VtEmitted::Picture(picture),
            None => VtEmitted::Dropped,
        }
    };
    if let Ok(mut queue) = output.0.lock() {
        queue.push(emitted);
    }
}

/// Convert one compressed CMSampleBuffer (AVCC: length-prefixed NALUs +
/// out-of-band parameter sets) into the Annex B byte stream the sealed
/// frame protocol carries and OpenH264 decodes: start-code-delimited
/// NALUs, with SPS/PPS prepended to every keyframe so an IDR alone is
/// decodable — exactly what the late-joiner and keyframe-cache paths
/// require of a keyframe.
unsafe fn annex_b_of(sample: &CMSampleBuffer) -> Option<EncodedPicture> {
    const START_CODE: [u8; 4] = [0, 0, 0, 1];
    let keyframe = unsafe { is_sync_sample(sample) };
    let format = unsafe { sample.format_description() }?;

    // NAL length-field width and parameter-set count ride on the format
    // description.
    let mut parameter_sets: usize = 0;
    let mut nal_length_field: c_int = 4;
    let status = unsafe {
        CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
            &format,
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            &mut parameter_sets,
            &mut nal_length_field,
        )
    };
    if status != 0 || !(1..=4).contains(&nal_length_field) {
        return None;
    }

    let data = unsafe { sample.data_buffer() }?;
    let length = unsafe { data.data_length() };
    let mut avcc = vec![0u8; length];
    let destination = NonNull::new(avcc.as_mut_ptr().cast::<c_void>())?;
    if unsafe { data.copy_data_bytes(0, length, destination) } != 0 {
        return None;
    }

    let mut annex_b = Vec::with_capacity(length + 128);
    if keyframe {
        for index in 0..parameter_sets {
            let mut set_ptr: *const u8 = ptr::null();
            let mut set_len: usize = 0;
            let status = unsafe {
                CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
                    &format,
                    index,
                    &mut set_ptr,
                    &mut set_len,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if status != 0 || set_ptr.is_null() || set_len == 0 {
                return None;
            }
            annex_b.extend_from_slice(&START_CODE);
            annex_b.extend_from_slice(unsafe { std::slice::from_raw_parts(set_ptr, set_len) });
        }
    }
    let nal_length_field = nal_length_field as usize;
    let mut offset = 0;
    while offset + nal_length_field <= avcc.len() {
        let mut nal_len = 0usize;
        for byte in &avcc[offset..offset + nal_length_field] {
            nal_len = (nal_len << 8) | usize::from(*byte);
        }
        offset += nal_length_field;
        if nal_len == 0 || offset + nal_len > avcc.len() {
            break;
        }
        annex_b.extend_from_slice(&START_CODE);
        annex_b.extend_from_slice(&avcc[offset..offset + nal_len]);
        offset += nal_len;
    }
    if annex_b.is_empty() {
        return None;
    }
    Some(EncodedPicture {
        bytes: annex_b,
        keyframe,
    })
}

/// A sample is a keyframe unless its attachments carry NotSync = true
/// (absent attachments mean sync, per CoreMedia convention).
unsafe fn is_sync_sample(sample: &CMSampleBuffer) -> bool {
    let Some(attachments) = (unsafe { sample.sample_attachments_array(false) }) else {
        return true;
    };
    if attachments.count() == 0 {
        return true;
    }
    let first = unsafe { attachments.value_at_index(0) }.cast::<CFDictionary>();
    let Some(first) = (unsafe { first.as_ref() }) else {
        return true;
    };
    let key: *const CFString = unsafe { kCMSampleAttachmentKey_NotSync };
    let value = unsafe { first.value(key.cast::<c_void>()) };
    let not_sync = value.cast::<CFBoolean>();
    match unsafe { not_sync.as_ref() } {
        Some(flag) => !flag.value(),
        None => true,
    }
}

/// Wrap the frame's tightly-packed BGRA rows into a CVPixelBuffer.
/// One stride-aware row copy — it *replaces* the software path's
/// BGRA→YUV conversion pass (VideoToolbox takes BGRA natively and does
/// the colorspace conversion in hardware), so the hardware path still
/// touches each pixel exactly once on the CPU.
fn bgra_pixel_buffer(shaped: &CapturedFrame) -> Result<CFRetained<CVPixelBuffer>, VideoError> {
    let (w, h) = (shaped.width as usize, shaped.height as usize);
    if shaped.bgra.len() != w * h * 4 {
        return Err(VideoError::BadDimensions(shaped.width, shaped.height));
    }
    let mut buffer_ptr: *mut CVPixelBuffer = ptr::null_mut();
    // SAFETY: out-pointer is valid; BGRA is a supported format.
    let status = unsafe {
        CVPixelBufferCreate(
            None,
            w,
            h,
            kCVPixelFormatType_32BGRA,
            None,
            NonNull::from(&mut buffer_ptr),
        )
    };
    if status != 0 {
        return Err(VideoError::Encode(format!(
            "CVPixelBufferCreate failed (CVReturn {status})"
        )));
    }
    let buffer_ptr = NonNull::new(buffer_ptr)
        .ok_or_else(|| VideoError::Encode("CVPixelBufferCreate returned null".into()))?;
    // SAFETY: +1 reference from create.
    let buffer = unsafe { CFRetained::from_raw(buffer_ptr) };
    if unsafe { CVPixelBufferLockBaseAddress(&buffer, CVPixelBufferLockFlags(0)) } != 0 {
        return Err(VideoError::Encode(
            "CVPixelBufferLockBaseAddress failed".into(),
        ));
    }
    let base = CVPixelBufferGetBaseAddress(&buffer);
    let stride = CVPixelBufferGetBytesPerRow(&buffer);
    let src_stride = w * 4;
    if base.is_null() || stride < src_stride {
        unsafe { CVPixelBufferUnlockBaseAddress(&buffer, CVPixelBufferLockFlags(0)) };
        return Err(VideoError::Encode("CVPixelBuffer layout unusable".into()));
    }
    // SAFETY: destination rows are within the locked buffer (stride ≥
    // src_stride, h rows), source rows within shaped.bgra (length checked).
    unsafe {
        let dst = base.cast::<u8>();
        for row in 0..h {
            ptr::copy_nonoverlapping(
                shaped.bgra.as_ptr().add(row * src_stride),
                dst.add(row * stride),
                src_stride,
            );
        }
        CVPixelBufferUnlockBaseAddress(&buffer, CVPixelBufferLockFlags(0));
    }
    Ok(buffer)
}

fn cf_bool(value: bool) -> Result<&'static CFBoolean, VideoError> {
    // SAFETY: reading a CF constant static.
    let constant = if value {
        unsafe { kCFBooleanTrue }
    } else {
        unsafe { kCFBooleanFalse }
    };
    constant.ok_or_else(|| VideoError::Encode("CFBoolean constants unavailable".into()))
}

/// Set a property that the latency contract depends on — failure is an
/// error (the caller then falls back to software rather than run VT in a
/// high-latency configuration).
fn set_property(
    session: &CFRetained<VTCompressionSession>,
    key: &CFString,
    value: &CFType,
) -> Result<(), VideoError> {
    // SAFETY: a VTCompressionSession *is* a VTSession (VTSession = CFType
    // alias in these bindings); key/value are valid CF objects.
    let status = unsafe { VTSessionSetProperty(session.as_ref(), key, Some(value)) };
    if status != 0 {
        return Err(VideoError::Encode(format!(
            "VTSessionSetProperty({key}) failed (OSStatus {status})"
        )));
    }
    Ok(())
}

/// Set a nice-to-have property; support varies by encoder generation, so
/// refusal is logged and tolerated.
fn try_set_property(
    session: &CFRetained<VTCompressionSession>,
    key: &CFString,
    value: &CFType,
    name: &str,
) {
    if let Err(error) = set_property(session, key, value) {
        tracing::debug!(%error, property = name, "video tx: optional VT property refused");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::codec_h264::H264Decoder;
    use crate::video::VideoDecoder;

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

    fn vt_or_skip(fps: u32) -> Option<VtH264Encoder> {
        match VtH264Encoder::new(fps, 1_500_000) {
            Ok(encoder) => Some(encoder),
            Err(error) => {
                // CI/VMs without a hardware encoder: the production path
                // falls back to software in exactly this case.
                eprintln!("VideoToolbox unavailable here, skipping: {error}");
                None
            }
        }
    }

    #[test]
    fn vt_encodes_and_openh264_decodes_the_picture() {
        let Some(mut encoder) = vt_or_skip(15) else {
            return;
        };
        let mut decoder = H264Decoder::new().unwrap();
        let frame = gradient(320, 180);
        let first = encoder.encode(&frame, false).unwrap().unwrap();
        assert!(first.keyframe, "the opening frame must be an IDR");
        // The Annex B stream must decode on the exact software decoder
        // every viewer runs.
        let picture = decoder.decode(&first.bytes).unwrap();
        assert_eq!((picture.width, picture.height), (320, 180));
        // Gradient structure survives (lossy): brighter bottom-right.
        let sum = |px: &[u8]| px.iter().map(|c| u32::from(*c)).sum::<u32>();
        let tl = &picture.rgba[0..3];
        let br_at = (180 * 320 - 1) * 4;
        let br = &picture.rgba[br_at..br_at + 3];
        assert!(sum(br) > sum(tl), "gradient direction lost");

        // Deltas follow and decode too.
        let mut second_frame = frame.clone();
        second_frame.bgra[0] = 200;
        let second = encoder.encode(&second_frame, false).unwrap();
        if let Some(second) = second {
            assert!(!second.keyframe);
            assert!(decoder.decode(&second.bytes).is_some());
        }
    }

    #[test]
    fn vt_forces_a_keyframe_on_request() {
        let Some(mut encoder) = vt_or_skip(15) else {
            return;
        };
        let frame = gradient(320, 180);
        let _ = encoder.encode(&frame, false).unwrap();
        let mut changed = frame.clone();
        changed.bgra[0] = 99;
        let _ = encoder.encode(&changed, false).unwrap();
        let forced = encoder.encode(&frame, true).unwrap().unwrap();
        assert!(forced.keyframe, "force_keyframe must produce an IDR");
        // And a keyframe must be self-contained: a *fresh* decoder accepts
        // it with no prior state (the late-joiner/keyframe-cache contract).
        let mut fresh = H264Decoder::new().unwrap();
        assert!(fresh.decode(&forced.bytes).is_some());
    }

    #[test]
    fn vt_bitrate_retarget_needs_no_rebuild_and_no_idr() {
        let Some(mut encoder) = vt_or_skip(15) else {
            return;
        };
        let frame = gradient(320, 180);
        let _ = encoder.encode(&frame, false).unwrap();
        encoder.set_target_bitrate(400_000);
        let mut changed = frame.clone();
        changed.bgra[0] = 50;
        // The retarget applies live: the next frame encodes without error
        // and without a forced keyframe (no resync needed).
        let after = encoder.encode(&changed, false).unwrap();
        if let Some(after) = after {
            assert!(!after.keyframe, "a live retarget must not force an IDR");
        }
    }

    #[test]
    fn vt_handles_dimension_changes_by_rebuilding() {
        let Some(mut encoder) = vt_or_skip(15) else {
            return;
        };
        let _ = encoder.encode(&gradient(320, 180), false).unwrap();
        // New geometry mid-stream: rebuilt session, opening IDR.
        let switched = encoder.encode(&gradient(640, 360), false).unwrap().unwrap();
        assert!(switched.keyframe, "a rebuilt session must open with an IDR");
        let mut decoder = H264Decoder::new().unwrap();
        let picture = decoder.decode(&switched.bytes).unwrap();
        assert_eq!((picture.width, picture.height), (640, 360));
    }

    #[test]
    fn odd_dimensions_are_cropped_not_fatal() {
        let Some(mut encoder) = vt_or_skip(15) else {
            return;
        };
        let odd = gradient(321, 181);
        let encoded = encoder.encode(&odd, false).unwrap().unwrap();
        let mut decoder = H264Decoder::new().unwrap();
        let picture = decoder.decode(&encoded.bytes).unwrap();
        assert_eq!((picture.width, picture.height), (320, 180));
    }
}
