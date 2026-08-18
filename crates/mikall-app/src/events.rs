//! Events the application layer emits toward frontends.

use mikall_domain::messaging::{ChannelName, MessageBody, MessageId, Nickname};
use mikall_domain::shared::IdentityId;
use mikall_domain::DomainEvent;
use tokio::sync::broadcast;

/// What frontends (iced UI, IRC gateway) subscribe to. Chat payloads are
/// enriched so a frontend can render them without reaching back into
/// aggregates.
#[derive(Debug, Clone)]
pub enum AppEvent {
    ChannelMessage {
        channel: ChannelName,
        author: IdentityId,
        author_nick: Option<Nickname>,
        message: MessageId,
        body: MessageBody,
        ts_hint_ms: u64,
    },
    DirectMessage {
        from: IdentityId,
        from_nick: Option<Nickname>,
        message: MessageId,
        body: MessageBody,
        ts_hint_ms: u64,
    },
    /// A completed inbound transfer was written to disk.
    TransferSaved {
        transfer: mikall_domain::transfer::TransferId,
        path: String,
    },
    Domain(DomainEvent),
}

/// Broadcast fan-out to any number of frontend subscribers. Slow subscribers
/// lag (bounded queue) rather than back-pressuring the node — a frontend can
/// always re-read history from the services.
#[derive(Debug, Clone)]
pub struct EventBus {
    tx: broadcast::Sender<AppEvent>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        EventBus { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: AppEvent) {
        // No subscribers is fine (headless node before any frontend attaches).
        let _ = self.tx.send(event);
    }

    pub fn publish_domain(&self, event: impl Into<DomainEvent>) {
        self.publish(AppEvent::Domain(event.into()));
    }
}
