//! Identity bounded context: contacts, trust, petnames.
//!
//! There is no registration authority in a serverless network: an identity is
//! a keypair and nothing more. Names are local labels ([`Petname`]) — never
//! global truths. Trust is earned per contact via TOFU pinning or explicit
//! out-of-band fingerprint verification.

use core::fmt;

use crate::messaging::Nickname;
use crate::shared::{Fingerprint, IdentityId};

/// A locally chosen label for a contact. 1..=64 bytes, no control characters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Petname(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PetnameError {
    #[error("petname must not be empty")]
    Empty,
    #[error("petname exceeds 64 bytes")]
    TooLong,
    #[error("petname contains a control character")]
    ControlCharacter,
}

impl Petname {
    pub fn parse(raw: &str) -> Result<Self, PetnameError> {
        if raw.is_empty() {
            return Err(PetnameError::Empty);
        }
        if raw.len() > 64 {
            return Err(PetnameError::TooLong);
        }
        if raw.chars().any(char::is_control) {
            return Err(PetnameError::ControlCharacter);
        }
        Ok(Petname(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Petname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How much we trust that a contact's current key really belongs to the
/// person we think it does. Exhaustive by design: every UI and gateway must
/// handle all four states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustLevel {
    /// Seen, never pinned.
    Unverified,
    /// Trust-on-first-use: the key seen at `first_seen_ms` is pinned; any
    /// future change raises [`IdentityEvent::ContactKeyChanged`].
    TofuPinned { first_seen_ms: u64 },
    /// Fingerprint verified out of band.
    Verified { verified_at_ms: u64 },
    /// All traffic dropped locally.
    Blocked,
}

/// A known remote identity (aggregate root).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contact {
    id: IdentityId,
    fingerprint: Fingerprint,
    trust: TrustLevel,
    petname: Option<Petname>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContactError {
    #[error("contact is blocked; unblock before changing trust")]
    Blocked,
}

impl Contact {
    /// First sighting of an identity: TOFU-pin its key immediately.
    pub fn first_seen(
        id: IdentityId,
        fingerprint: Fingerprint,
        now_ms: u64,
    ) -> (Self, IdentityEvent) {
        let contact = Contact {
            id,
            fingerprint,
            trust: TrustLevel::TofuPinned {
                first_seen_ms: now_ms,
            },
            petname: None,
        };
        (contact, IdentityEvent::ContactAdded { id })
    }

    pub fn id(&self) -> IdentityId {
        self.id
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    pub fn trust(&self) -> TrustLevel {
        self.trust
    }

    pub fn petname(&self) -> Option<&Petname> {
        self.petname.as_ref()
    }

    pub fn set_petname(&mut self, petname: Petname) {
        self.petname = Some(petname);
    }

    pub fn is_blocked(&self) -> bool {
        matches!(self.trust, TrustLevel::Blocked)
    }

    /// Mark the fingerprint as verified out of band.
    pub fn verify(&mut self, now_ms: u64) -> Result<IdentityEvent, ContactError> {
        match self.trust {
            TrustLevel::Blocked => Err(ContactError::Blocked),
            TrustLevel::Unverified
            | TrustLevel::TofuPinned { .. }
            | TrustLevel::Verified { .. } => {
                self.trust = TrustLevel::Verified {
                    verified_at_ms: now_ms,
                };
                Ok(IdentityEvent::ContactVerified { id: self.id })
            }
        }
    }

    pub fn block(&mut self) -> IdentityEvent {
        self.trust = TrustLevel::Blocked;
        IdentityEvent::ContactBlocked { id: self.id }
    }

    /// The TOFU alarm: the identity presented a different key than the one we
    /// pinned. Trust collapses to `Unverified`; the frontend must warn loudly.
    pub fn key_changed(&mut self, new_fingerprint: Fingerprint) -> IdentityEvent {
        let old = self.fingerprint;
        self.fingerprint = new_fingerprint;
        match self.trust {
            TrustLevel::Blocked => {}
            TrustLevel::Unverified
            | TrustLevel::TofuPinned { .. }
            | TrustLevel::Verified { .. } => self.trust = TrustLevel::Unverified,
        }
        IdentityEvent::ContactKeyChanged {
            id: self.id,
            old_fingerprint: old,
            new_fingerprint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityEvent {
    /// The local profile's announced nickname changed (from any frontend).
    NicknameChanged {
        nickname: Nickname,
    },
    ContactAdded {
        id: IdentityId,
    },
    ContactVerified {
        id: IdentityId,
    },
    ContactBlocked {
        id: IdentityId,
    },
    ContactKeyChanged {
        id: IdentityId,
        old_fingerprint: Fingerprint,
        new_fingerprint: Fingerprint,
    },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn contact() -> Contact {
        Contact::first_seen(
            IdentityId::from_bytes([1; 32]),
            Fingerprint::from_bytes([2; 32]),
            1_000,
        )
        .0
    }

    #[test]
    fn first_seen_is_tofu_pinned() {
        assert_eq!(
            contact().trust(),
            TrustLevel::TofuPinned {
                first_seen_ms: 1_000
            }
        );
    }

    #[test]
    fn key_change_collapses_trust_and_reports_both_fingerprints() {
        let mut c = contact();
        c.verify(2_000).unwrap();
        let event = c.key_changed(Fingerprint::from_bytes([9; 32]));
        assert_eq!(c.trust(), TrustLevel::Unverified);
        match event {
            IdentityEvent::ContactKeyChanged {
                old_fingerprint,
                new_fingerprint,
                ..
            } => {
                assert_eq!(old_fingerprint, Fingerprint::from_bytes([2; 32]));
                assert_eq!(new_fingerprint, Fingerprint::from_bytes([9; 32]));
            }
            IdentityEvent::NicknameChanged { .. }
            | IdentityEvent::ContactAdded { .. }
            | IdentityEvent::ContactVerified { .. }
            | IdentityEvent::ContactBlocked { .. } => panic!("wrong event"),
        }
    }

    #[test]
    fn blocked_contact_cannot_be_verified() {
        let mut c = contact();
        c.block();
        assert_eq!(c.verify(3_000), Err(ContactError::Blocked));
        assert!(c.is_blocked());
    }

    #[test]
    fn petname_rejects_control_chars_and_empty() {
        assert!(Petname::parse("").is_err());
        assert!(Petname::parse("evil\u{7}name").is_err());
        assert!(Petname::parse(&"x".repeat(65)).is_err());
        assert_eq!(Petname::parse("Miku ♪").unwrap().as_str(), "Miku ♪");
    }
}
