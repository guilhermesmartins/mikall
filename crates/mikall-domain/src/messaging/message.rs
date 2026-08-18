//! Messages and the verification boundary.

use core::fmt;

use crate::shared::{IdentityId, LamportStamp};

use super::values::MessageBody;

/// Content-derived message identity: BLAKE3 of the canonical signed envelope
/// bytes, computed at the adapter boundary. Unforgeable: two different
/// envelopes cannot share an id, and an id commits to author + content.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId([u8; 32]);

impl MessageId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        MessageId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageId({self})")
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Attestation that an inbound envelope's signature has been checked against
/// its author's public key.
///
/// This is the negative-space trust boundary: [`Message::received`] demands a
/// `Verified` value, and the **only** constructor is
/// [`Verified::attest_signature_checked`] — so every call site where remote
/// bytes become a domain `Message` is greppable and must sit next to a real
/// signature verification (the crypto adapter in production, a fake in BDD).
#[derive(Debug)]
#[must_use]
pub struct Verified(());

impl Verified {
    /// Caller vouches: the envelope this message was decoded from carried a
    /// valid signature by `author` over its canonical bytes.
    pub fn attest_signature_checked() -> Self {
        Verified(())
    }
}

/// A chat message. Exists only in two ways: authored locally, or received
/// from the network *after* signature verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    id: MessageId,
    author: IdentityId,
    lamport: u64,
    parents: Vec<MessageId>,
    body: MessageBody,
    /// Advisory wall clock (unix ms). Never used for ordering.
    ts_hint_ms: u64,
}

impl Message {
    /// A message this node is authoring right now.
    pub fn authored(
        id: MessageId,
        author: IdentityId,
        lamport: u64,
        parents: Vec<MessageId>,
        body: MessageBody,
        ts_hint_ms: u64,
    ) -> Self {
        Message {
            id,
            author,
            lamport,
            parents,
            body,
            ts_hint_ms,
        }
    }

    /// A message received from a peer. `_verified` proves the signature check
    /// happened at the boundary.
    pub fn received(
        _verified: Verified,
        id: MessageId,
        author: IdentityId,
        lamport: u64,
        parents: Vec<MessageId>,
        body: MessageBody,
        ts_hint_ms: u64,
    ) -> Self {
        Message {
            id,
            author,
            lamport,
            parents,
            body,
            ts_hint_ms,
        }
    }

    pub fn id(&self) -> MessageId {
        self.id
    }

    pub fn author(&self) -> IdentityId {
        self.author
    }

    pub fn lamport(&self) -> u64 {
        self.lamport
    }

    pub fn stamp(&self) -> LamportStamp {
        LamportStamp::new(self.lamport, self.author)
    }

    pub fn parents(&self) -> &[MessageId] {
        &self.parents
    }

    pub fn body(&self) -> &MessageBody {
        &self.body
    }

    pub fn ts_hint_ms(&self) -> u64 {
        self.ts_hint_ms
    }

    /// Deterministic display-order key: `(lamport, author, id)`.
    pub fn order_key(&self) -> (u64, IdentityId, MessageId) {
        (self.lamport, self.author, self.id)
    }
}
