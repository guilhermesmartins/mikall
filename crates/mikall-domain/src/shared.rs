//! Cross-context primitive value objects.

use core::fmt;

/// An identity is its Ed25519 public key. 32 opaque bytes at the domain level;
/// key material and signature checking live behind adapter ports.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityId([u8; 32]);

impl IdentityId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        IdentityId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for IdentityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdentityId({self})")
    }
}

impl fmt::Display for IdentityId {
    /// Short hex prefix — enough to disambiguate in logs, not for security
    /// decisions (that is what [`Fingerprint`] rendering is for).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// A human-checkable digest of an identity key (BLAKE3 of the public key,
/// computed by the crypto adapter). Rendered as eight groups of four
/// Crockford-base32 characters for out-of-band verification.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint([u8; 32]);

const BASE32: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

impl Fingerprint {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Fingerprint(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// `xxxx-xxxx-xxxx-xxxx-xxxx-xxxx-xxxx-xxxx`: 32 base32 chars covering the
    /// first 160 bits of the digest.
    pub fn display_groups(&self) -> String {
        let mut out = String::with_capacity(39);
        let mut acc: u32 = 0;
        let mut acc_bits = 0u32;
        let mut emitted = 0usize;
        for byte in self.0 {
            acc = (acc << 8) | u32::from(byte);
            acc_bits += 8;
            while acc_bits >= 5 && emitted < 32 {
                acc_bits -= 5;
                let idx = ((acc >> acc_bits) & 0x1f) as usize;
                if emitted > 0 && emitted % 4 == 0 {
                    out.push('-');
                }
                out.push(BASE32[idx] as char);
                emitted += 1;
            }
            if emitted >= 32 {
                break;
            }
        }
        out
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fingerprint({})", self.display_groups())
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_groups())
    }
}

/// Lamport timestamp with a total order: `(counter, author)`.
/// `ts_hint` wall-clock values are advisory only and never used for ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LamportStamp {
    counter: u64,
    author: IdentityId,
}

impl LamportStamp {
    pub const fn new(counter: u64, author: IdentityId) -> Self {
        LamportStamp { counter, author }
    }

    pub const fn counter(&self) -> u64 {
        self.counter
    }

    pub const fn author(&self) -> IdentityId {
        self.author
    }

    /// The stamp a node assigns to its next event after having observed
    /// `observed_max`.
    pub fn next_after(observed_max: u64, author: IdentityId) -> Self {
        LamportStamp {
            counter: observed_max.saturating_add(1),
            author,
        }
    }
}

/// An Ed25519 signature over canonical wire bytes. Opaque to the domain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature([u8; 64]);

impl Signature {
    pub const fn from_bytes(bytes: [u8; 64]) -> Self {
        Signature(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature(..)")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn id(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    #[test]
    fn fingerprint_renders_eight_groups() {
        let fp = Fingerprint::from_bytes([0xAB; 32]);
        let s = fp.display_groups();
        assert_eq!(s.split('-').count(), 8);
        assert!(s.split('-').all(|g| g.len() == 4));
    }

    #[test]
    fn lamport_total_order_breaks_ties_by_author() {
        let a = LamportStamp::new(5, id(1));
        let b = LamportStamp::new(5, id(2));
        let c = LamportStamp::new(6, id(1));
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn next_after_saturates() {
        let s = LamportStamp::next_after(u64::MAX, id(1));
        assert_eq!(s.counter(), u64::MAX);
    }
}
