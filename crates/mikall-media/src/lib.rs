//! Media engine for mikall calls.
//!
//! - [`frame`]: the sealed frame protocol — 18-byte authenticated header +
//!   ChaCha20-Poly1305 payload, nonce = ssrc ‖ counter.
//! - [`jitter`]: reorder buffer with loss concealment signaling.
//! - [`engine`]: the voice pipeline over the `AudioSource`/`AudioSink`/
//!   `AudioCodec` ports.
//! - [`opus`] (feature `hardware-audio`): the Opus codec adapter. Real
//!   microphone/speaker capture plugs into the same ports (cpal adapter is
//!   the documented next step; it needs real hardware to be tested against).
//!
//! Group calls are a full mesh (domain-capped at 8): each participant runs
//! one sender toward every peer and one receiver per call.

pub mod engine;
pub mod frame;
pub mod jitter;

#[cfg(feature = "hardware-audio")]
pub mod opus;
