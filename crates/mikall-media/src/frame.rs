//! Sealed media frames.
//!
//! Layout on the wire: an 18-byte plaintext header (authenticated as AAD)
//! followed by the ChaCha20-Poly1305 ciphertext of the payload. The nonce
//! is `ssrc (4) ‖ counter (8)` — unique per sender stream as long as each
//! sender uses a distinct ssrc within the call, which the engine derives
//! from its identity.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::ChaCha20Poly1305;

pub const HEADER_LEN: usize = 18;

/// Header flag bit 0: last frame of a video picture (every M16 video frame
/// is one whole picture, so the bit is always set on video).
pub const FLAG_END_OF_PICTURE: u8 = 0b0000_0001;
/// Header flag bit 1: this video frame is a keyframe (IDR) — decodable
/// with no prior state. In the AAD, so a forwarder can trust it without
/// decrypting: exactly what the keyframe cache of the forwarding-tree
/// milestone needs.
pub const FLAG_KEYFRAME: u8 = 0b0000_0010;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Audio,
    Video,
}

impl MediaKind {
    fn to_byte(self) -> u8 {
        match self {
            MediaKind::Audio => 0,
            MediaKind::Video => 1,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(MediaKind::Audio),
            1 => Some(MediaKind::Video),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Monotonic per-sender frame counter — also the AEAD nonce material,
    /// so it must never repeat for one (key, ssrc).
    pub counter: u64,
    /// Media timestamp in 48 kHz units (audio) or 90 kHz (video).
    pub ts: u32,
    /// Sender stream id within the call.
    pub ssrc: u32,
    pub kind: MediaKind,
    /// Bit 0: end-of-picture (video). Others reserved.
    pub flags: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("frame too short")]
    TooShort,
    #[error("unknown media kind")]
    BadKind,
    #[error("decryption failed (wrong key or tampered frame)")]
    Decrypt,
    #[error("encryption failed")]
    Encrypt,
}

#[derive(Debug, Clone)]
pub struct CallKey([u8; 32]);

impl CallKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        CallKey(bytes)
    }
}

impl FrameHeader {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[..8].copy_from_slice(&self.counter.to_be_bytes());
        out[8..12].copy_from_slice(&self.ts.to_be_bytes());
        out[12..16].copy_from_slice(&self.ssrc.to_be_bytes());
        out[16] = self.kind.to_byte();
        out[17] = self.flags;
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_LEN {
            return Err(FrameError::TooShort);
        }
        let mut u64buf = [0u8; 8];
        u64buf.copy_from_slice(&bytes[..8]);
        let mut u32buf = [0u8; 4];
        u32buf.copy_from_slice(&bytes[8..12]);
        let ts = u32::from_be_bytes(u32buf);
        u32buf.copy_from_slice(&bytes[12..16]);
        Ok(FrameHeader {
            counter: u64::from_be_bytes(u64buf),
            ts,
            ssrc: u32::from_be_bytes(u32buf),
            kind: MediaKind::from_byte(bytes[16]).ok_or(FrameError::BadKind)?,
            flags: bytes[17],
        })
    }

    fn nonce(&self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..4].copy_from_slice(&self.ssrc.to_be_bytes());
        nonce[4..].copy_from_slice(&self.counter.to_be_bytes());
        nonce
    }
}

/// Encrypt one frame.
pub fn seal(key: &CallKey, header: FrameHeader, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    let cipher = ChaCha20Poly1305::new((&key.0).into());
    let header_bytes = header.encode();
    let ciphertext = cipher
        .encrypt(
            (&header.nonce()).into(),
            Payload {
                msg: payload,
                aad: &header_bytes,
            },
        )
        .map_err(|_| FrameError::Encrypt)?;
    let mut out = Vec::with_capacity(HEADER_LEN + ciphertext.len());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Authenticate and decrypt one frame.
pub fn open(key: &CallKey, sealed: &[u8]) -> Result<(FrameHeader, Vec<u8>), FrameError> {
    let header = FrameHeader::decode(sealed)?;
    let cipher = ChaCha20Poly1305::new((&key.0).into());
    let payload = cipher
        .decrypt(
            (&header.nonce()).into(),
            Payload {
                msg: &sealed[HEADER_LEN..],
                aad: &sealed[..HEADER_LEN],
            },
        )
        .map_err(|_| FrameError::Decrypt)?;
    Ok((header, payload))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn header(counter: u64) -> FrameHeader {
        FrameHeader {
            counter,
            ts: 960,
            ssrc: 0xC0FFEE,
            kind: MediaKind::Audio,
            flags: 0,
        }
    }

    #[test]
    fn seal_open_roundtrip() {
        let key = CallKey::new([7; 32]);
        let sealed = seal(&key, header(1), b"opus frame bytes").unwrap();
        let (opened_header, payload) = open(&key, &sealed).unwrap();
        assert_eq!(opened_header, header(1));
        assert_eq!(payload, b"opus frame bytes");
    }

    #[test]
    fn wrong_key_fails() {
        let sealed = seal(&CallKey::new([7; 32]), header(1), b"secret").unwrap();
        assert_eq!(
            open(&CallKey::new([8; 32]), &sealed),
            Err(FrameError::Decrypt)
        );
    }

    #[test]
    fn tampered_header_fails_authentication() {
        let key = CallKey::new([7; 32]);
        let mut sealed = seal(&key, header(5), b"payload").unwrap();
        sealed[17] ^= 0x01; // flip a flag bit (AAD covers the header)
        assert_eq!(open(&key, &sealed), Err(FrameError::Decrypt));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = CallKey::new([7; 32]);
        let mut sealed = seal(&key, header(5), b"payload").unwrap();
        let last = sealed.len() - 1;
        sealed[last] ^= 0xFF;
        assert_eq!(open(&key, &sealed), Err(FrameError::Decrypt));
    }

    #[test]
    fn short_frame_is_rejected() {
        assert_eq!(
            open(&CallKey::new([7; 32]), &[0u8; 5]),
            Err(FrameError::TooShort)
        );
    }
}
