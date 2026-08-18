//! Presence bounded context. Serverless presence is gossip-derived and
//! best-effort: peers beacon periodically and expire when silent.

use core::fmt;
use std::collections::BTreeMap;

use crate::shared::IdentityId;

/// An away message: 1..=200 bytes, single line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwayMessage(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AwayMessageError {
    #[error("away message must not be empty")]
    Empty,
    #[error("away message exceeds 200 bytes")]
    TooLong,
    #[error("away message must be a single line without control characters")]
    ControlCharacter,
}

impl AwayMessage {
    pub fn parse(raw: &str) -> Result<Self, AwayMessageError> {
        if raw.is_empty() {
            return Err(AwayMessageError::Empty);
        }
        if raw.len() > 200 {
            return Err(AwayMessageError::TooLong);
        }
        if raw.chars().any(char::is_control) {
            return Err(AwayMessageError::ControlCharacter);
        }
        Ok(AwayMessage(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AwayMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceState {
    Online,
    Away { message: Option<AwayMessage> },
    Offline { last_seen_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceEvent {
    PeerCameOnline { who: IdentityId },
    PeerWentAway { who: IdentityId },
    PeerWentOffline { who: IdentityId },
}

/// Presence roster for one channel (or the contact list).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Roster {
    entries: BTreeMap<IdentityId, PresenceState>,
}

impl Roster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state_of(&self, id: &IdentityId) -> Option<&PresenceState> {
        self.entries.get(id)
    }

    pub fn entries(&self) -> impl Iterator<Item = (IdentityId, &PresenceState)> + '_ {
        self.entries.iter().map(|(id, s)| (*id, s))
    }

    /// Apply an observed beacon/state; returns an event only on transitions
    /// worth surfacing (repeat beacons are silent).
    pub fn observe(&mut self, who: IdentityId, state: PresenceState) -> Option<PresenceEvent> {
        let previous = self.entries.insert(who, state.clone());
        match (previous, state) {
            (Some(PresenceState::Online), PresenceState::Online) => None,
            (Some(PresenceState::Away { .. }), PresenceState::Away { .. }) => None,
            (Some(PresenceState::Offline { .. }), PresenceState::Offline { .. })
            | (None, PresenceState::Offline { .. }) => None,
            (_, PresenceState::Online) => Some(PresenceEvent::PeerCameOnline { who }),
            (_, PresenceState::Away { .. }) => Some(PresenceEvent::PeerWentAway { who }),
            (_, PresenceState::Offline { .. }) => Some(PresenceEvent::PeerWentOffline { who }),
        }
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
    fn transitions_emit_events_repeats_do_not() {
        let mut roster = Roster::new();
        assert_eq!(
            roster.observe(id(1), PresenceState::Online),
            Some(PresenceEvent::PeerCameOnline { who: id(1) })
        );
        assert_eq!(roster.observe(id(1), PresenceState::Online), None);
        assert_eq!(
            roster.observe(id(1), PresenceState::Away { message: None }),
            Some(PresenceEvent::PeerWentAway { who: id(1) })
        );
        assert_eq!(
            roster.observe(id(1), PresenceState::Offline { last_seen_ms: 5 }),
            Some(PresenceEvent::PeerWentOffline { who: id(1) })
        );
        assert_eq!(
            roster.observe(id(1), PresenceState::Offline { last_seen_ms: 9 }),
            None
        );
    }

    #[test]
    fn away_message_bounds() {
        assert!(AwayMessage::parse("").is_err());
        assert!(AwayMessage::parse(&"x".repeat(201)).is_err());
        assert!(AwayMessage::parse("gone\nfishing").is_err());
        assert!(AwayMessage::parse("brb, rehearsal").is_ok());
    }
}
