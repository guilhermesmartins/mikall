//! Direct-message threads.

use crate::shared::IdentityId;

use super::dag::{DagInsert, MessageDag};
use super::message::{Message, MessageId};

/// The unordered pair of participants in a DM thread, stored canonically
/// (smaller id first) so `(a, b)` and `(b, a)` are one value — two
/// representations of the same thread are unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DmKey {
    lower: IdentityId,
    higher: IdentityId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DmKeyError {
    #[error("a DM thread needs two distinct participants")]
    SelfThread,
}

impl DmKey {
    pub fn new(a: IdentityId, b: IdentityId) -> Result<Self, DmKeyError> {
        if a == b {
            return Err(DmKeyError::SelfThread);
        }
        let (lower, higher) = if a < b { (a, b) } else { (b, a) };
        Ok(DmKey { lower, higher })
    }

    pub fn participants(&self) -> (IdentityId, IdentityId) {
        (self.lower, self.higher)
    }

    pub fn involves(&self, id: &IdentityId) -> bool {
        self.lower == *id || self.higher == *id
    }

    pub fn other(&self, me: &IdentityId) -> Option<IdentityId> {
        if self.lower == *me {
            Some(self.higher)
        } else if self.higher == *me {
            Some(self.lower)
        } else {
            None
        }
    }
}

/// A DM conversation: the same causally ordered DAG as a channel, scoped to
/// two participants. Payload encryption happens at the adapter boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmThread {
    key: DmKey,
    dag: MessageDag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DmThreadError {
    #[error("author is not a participant of this DM thread")]
    NotAParticipant,
}

impl DmThread {
    pub fn new(key: DmKey) -> Self {
        DmThread {
            key,
            dag: MessageDag::new(),
        }
    }

    pub fn key(&self) -> DmKey {
        self.key
    }

    pub fn record_message(&mut self, message: Message) -> Result<DagInsert, DmThreadError> {
        if !self.key.involves(&message.author()) {
            return Err(DmThreadError::NotAParticipant);
        }
        Ok(self.dag.insert(message))
    }

    pub fn history(&self) -> Vec<&Message> {
        self.dag.linearize()
    }

    pub fn next_message_position(&self) -> (Vec<MessageId>, u64) {
        (self.dag.heads(), self.dag.max_lamport() + 1)
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
    fn dm_key_is_order_independent() {
        let ab = DmKey::new(id(1), id(2)).unwrap();
        let ba = DmKey::new(id(2), id(1)).unwrap();
        assert_eq!(ab, ba);
    }

    #[test]
    fn self_dm_is_unrepresentable() {
        assert_eq!(DmKey::new(id(1), id(1)), Err(DmKeyError::SelfThread));
    }

    #[test]
    fn other_participant_lookup() {
        let key = DmKey::new(id(1), id(2)).unwrap();
        assert_eq!(key.other(&id(1)), Some(id(2)));
        assert_eq!(key.other(&id(3)), None);
    }
}
