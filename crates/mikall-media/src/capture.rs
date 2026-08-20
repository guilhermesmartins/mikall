//! Screen capture via `scap` (ScreenCaptureKit on macOS), feature
//! `hardware-video`.
//!
//! Crate choice: of the maintained options (`scap`, raw `screencapturekit`
//! bindings, CoreGraphics polling), `scap` is the only one exposing a
//! safe, blocking, fps-throttled BGRA frame loop with built-in permission
//! preflight — and CoreGraphics capture is deprecated on the macOS
//! versions this targets. It captures at `Resolution::_720p`, i.e. width
//! 1280: the weak-machine downscale happens at the source, not in our
//! pixels.
//!
//! Like the cpal devices, the capturer lives on its own named thread (its
//! engine is not `Send`) and pushes [`CapturedFrame`]s into a channel the
//! encoder loop drains. ScreenCaptureKit itself suppresses unchanged
//! frames (idle ticks arrive as empty markers), which this adapter turns
//! into ~1 Hz re-emits of the last picture so a late joiner on a static
//! screen still gets its keyframe.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::video::CapturedFrame;

/// Capture rate with the software encoder: the spec's 5–15 fps band,
/// split down the middle — the safe cadence for the weak-machine target.
pub const CAPTURE_FPS: u32 = 10;

/// Capture rate with hardware encoding: the spec's upper band. The
/// dedicated encoder makes the per-frame cost near-free, so the extra
/// frames buy smoothness *and* lower latency (a fresh frame exists 66 ms
/// after a change instead of 100 ms) without touching the CPU budget.
pub const CAPTURE_FPS_HW: u32 = 15;

/// How often the last picture is re-emitted while the screen is static,
/// so keyframe demands (late joiners) are never starved.
const HEARTBEAT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CaptureError {
    #[error("screen recording permission denied")]
    PermissionDenied,
    #[error("screen capture unsupported on this system")]
    Unsupported,
    #[error("screen capture failed to start: {0}")]
    Failed(String),
}

/// Keepalive for the capture thread. Dropping it asks the thread to stop;
/// the thread then stops the ScreenCaptureKit stream (the macOS capture
/// indicator goes away) and closes the frame channel. The thread notices
/// on the next frame or idle tick — in practice immediately, since the
/// very act of stopping a share repaints the screen.
#[derive(Debug)]
pub struct ScapCapture {
    stop: Arc<AtomicBool>,
}

impl Drop for ScapCapture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl ScapCapture {
    /// Start capturing the main display. The first call in a process may
    /// trigger the macOS Screen Recording permission prompt; denial is an
    /// error here, never a crash.
    pub fn start(fps: u32) -> Result<(Self, mpsc::Receiver<CapturedFrame>), CaptureError> {
        if !scap::is_supported() {
            return Err(CaptureError::Unsupported);
        }
        if !scap::has_permission() && !scap::request_permission() {
            return Err(CaptureError::PermissionDenied);
        }

        let (frame_tx, frame_rx) = mpsc::channel::<CapturedFrame>(4);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), CaptureError>>();

        std::thread::Builder::new()
            .name("mikall-screen".into())
            .spawn(move || capture_thread(fps, frame_tx, thread_stop, ready_tx))
            .map_err(|e| CaptureError::Failed(e.to_string()))?;

        match ready_rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => Ok((ScapCapture { stop }, frame_rx)),
            Ok(Err(error)) => Err(error),
            // The thread died before reporting (scap's engine panics on
            // some SCK failures) — honest error, contained to the thread.
            Err(_) => Err(CaptureError::Failed("capture engine did not start".into())),
        }
    }
}

fn capture_thread(
    fps: u32,
    frame_tx: mpsc::Sender<CapturedFrame>,
    stop: Arc<AtomicBool>,
    ready_tx: std::sync::mpsc::Sender<Result<(), CaptureError>>,
) {
    let options = scap::capturer::Options {
        fps,
        show_cursor: true,
        show_highlight: false,
        target: None, // main display
        crop_area: None,
        output_type: scap::frame::FrameType::BGRAFrame,
        output_resolution: scap::capturer::Resolution::_720p,
        excluded_targets: None,
        captures_audio: false,
        exclude_current_process_audio: false,
    };
    let mut capturer = match scap::capturer::Capturer::build(options) {
        Ok(capturer) => capturer,
        Err(error) => {
            let mapped = match error {
                scap::capturer::CapturerBuildError::PermissionNotGranted => {
                    CaptureError::PermissionDenied
                }
                scap::capturer::CapturerBuildError::NotSupported => CaptureError::Unsupported,
            };
            let _ = ready_tx.send(Err(mapped));
            return;
        }
    };
    capturer.start_capture();
    let _ = ready_tx.send(Ok(()));
    tracing::info!(fps, "video: screen capture started (≤1280 wide BGRA)");

    let mut last_full: Option<CapturedFrame> = None;
    let mut last_emit = Instant::now();
    // Per-second capture stats: produced frames and frames dropped
    // because the encoder had not drained the channel (capture→encode
    // backlog — the first hop of the latency instrumentation).
    let mut win_start = Instant::now();
    let mut win_frames: u64 = 0;
    let mut win_dropped: u64 = 0;
    loop {
        if stop.load(Ordering::Relaxed) || frame_tx.is_closed() {
            break;
        }
        if win_frames > 0 && win_start.elapsed() >= Duration::from_secs(1) {
            tracing::info!(
                fps = win_frames,
                dropped_backlog = win_dropped,
                "video capture"
            );
            win_start = Instant::now();
            win_frames = 0;
            win_dropped = 0;
        }
        let frame = match capturer.get_next_frame() {
            Ok(frame) => frame,
            Err(_) => break, // engine gone
        };
        let scap::frame::Frame::Video(scap::frame::VideoFrame::BGRA(bgra)) = frame else {
            continue;
        };
        if bgra.data.is_empty() {
            // Idle tick: the screen did not change. Re-emit the last
            // picture at heartbeat pace so downstream keyframe demands
            // (and the stop flag) are still serviced.
            if last_emit.elapsed() >= HEARTBEAT {
                if let Some(repeat) = last_full.clone() {
                    let _ = frame_tx.try_send(repeat);
                    last_emit = Instant::now();
                }
            }
            continue;
        }
        if bgra.width <= 0 || bgra.height <= 0 {
            continue;
        }
        let captured = CapturedFrame {
            width: bgra.width as u32,
            height: bgra.height as u32,
            bgra: bgra.data,
        };
        last_full = Some(captured.clone());
        last_emit = Instant::now();
        win_frames += 1;
        // Never block the capture thread; a full channel means the
        // encoder stalled and this frame is best dropped.
        if frame_tx.try_send(captured).is_err() {
            win_dropped += 1;
        }
    }
    capturer.stop_capture();
    tracing::info!("video: screen capture stopped");
}
