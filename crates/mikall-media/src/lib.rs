//! Media engine for mikall calls.
//!
//! - [`frame`]: the sealed frame protocol — 18-byte authenticated header +
//!   ChaCha20-Poly1305 payload, nonce = ssrc ‖ counter.
//! - [`jitter`]: reorder buffer with loss concealment signaling.
//! - [`engine`]: the voice pipeline over the `AudioSource`/`AudioSink`/
//!   `AudioCodec` ports, with per-sender receive lanes and a live mute
//!   switch on the sender.
//! - [`pcm`]: pure format shaping (downmix, resample, frame chunking,
//!   RMS) between device formats and the canonical 48 kHz mono frames —
//!   deterministic, tested without hardware.
//! - [`opus`] / [`hardware`] (feature `hardware-audio`): the Opus codec
//!   adapter and the cpal microphone/speaker adapters over the same ports.
//! - [`video`]: the screen-share pipeline — static-frame gate, one
//!   encode + one seal per frame, per-viewer latest-run-wins lanes over
//!   unidirectional media streams ([`VideoFanout`](video::VideoFanout) is
//!   the fan-out seam the forwarding tree will replace), and the ordered
//!   per-sender receive path. Pure and fake-codec-testable.
//! - [`capture`] / [`codec_h264`] (feature `hardware-video`): the scap
//!   screen-capture adapter and the OpenH264 encoder/decoder over the
//!   [`video`] ports.
//!
//! Group calls are a full mesh (domain-capped at 8): each participant runs
//! one sender toward every peer and one receiver per call. Video is
//! different by design (docs/streaming.md): one encode, per-viewer lanes,
//! stale frames dropped in whole runs.

pub mod engine;
pub mod frame;
pub mod jitter;
pub mod pcm;
pub mod video;

#[cfg(feature = "hardware-audio")]
pub mod hardware;
#[cfg(feature = "hardware-audio")]
pub mod opus;

#[cfg(feature = "hardware-video")]
pub mod capture;
#[cfg(feature = "hardware-video")]
pub mod codec_h264;
