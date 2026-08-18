//! The pure domain core of mikall.
//!
//! This crate is the hexagon's center: it knows nothing about libp2p, tokio,
//! iced, IRC sockets, or storage. It contains only value objects, entities,
//! aggregates, and domain events, all built so that illegal states are
//! unrepresentable (negative-space programming):
//!
//! - every value object has private fields and a single fallible `parse`/`new`
//!   constructor — an invalid `Nickname` or `ChannelName` cannot exist;
//! - state machines (`CallPhase`, `TransferPhase`) transition by consuming the
//!   current phase and matching exhaustively — no wildcard arms;
//! - bounded collections (`CallRoster`, `ChunkBitmap`) cannot exceed their
//!   limits by construction;
//! - remotely received messages require a [`messaging::Verified`] attestation
//!   token, so the only path from wire bytes to a domain `Message` goes
//!   through a signature check at the adapter boundary.
//!
//! Bounded contexts: [`identity`], [`messaging`], [`presence`], [`calls`],
//! [`transfer`], with cross-context primitives in [`shared`].

pub mod calls;
pub mod identity;
pub mod messaging;
pub mod presence;
pub mod shared;
pub mod transfer;

/// Union of every context's domain events, for projection by the application
/// layer into frontend-facing events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    Identity(identity::IdentityEvent),
    Messaging(messaging::MessagingEvent),
    Presence(presence::PresenceEvent),
    Calls(calls::CallEvent),
    Transfer(transfer::TransferEvent),
}

impl From<identity::IdentityEvent> for DomainEvent {
    fn from(e: identity::IdentityEvent) -> Self {
        DomainEvent::Identity(e)
    }
}
impl From<messaging::MessagingEvent> for DomainEvent {
    fn from(e: messaging::MessagingEvent) -> Self {
        DomainEvent::Messaging(e)
    }
}
impl From<presence::PresenceEvent> for DomainEvent {
    fn from(e: presence::PresenceEvent) -> Self {
        DomainEvent::Presence(e)
    }
}
impl From<calls::CallEvent> for DomainEvent {
    fn from(e: calls::CallEvent) -> Self {
        DomainEvent::Calls(e)
    }
}
impl From<transfer::TransferEvent> for DomainEvent {
    fn from(e: transfer::TransferEvent) -> Self {
        DomainEvent::Transfer(e)
    }
}
