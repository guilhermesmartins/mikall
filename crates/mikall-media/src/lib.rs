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
//!
//! Group calls are a full mesh (domain-capped at 8): each participant runs
//! one sender toward every peer and one receiver per call.

pub mod engine;
pub mod frame;
pub mod jitter;
pub mod pcm;

#[cfg(feature = "hardware-audio")]
pub mod hardware;
#[cfg(feature = "hardware-audio")]
pub mod opus;
