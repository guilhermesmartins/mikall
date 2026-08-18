//! Persistence adapter: a single-file redb database.
//!
//! Layout:
//! - `messages`: key = channel id (32 bytes) ++ big-endian sequence (8
//!   bytes) so a prefix range scan replays a channel in arrival order;
//!   value = CBOR of [`StoredMessage`].
//! - `seq`: next sequence number per channel.
//!
//! Wire/domain types never touch redb directly — [`StoredMessage`] is this
//! adapter's own serde DTO.

mod blobs;

pub use blobs::FsBlobStore;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use mikall_app::ports::{MessageStore, StoreError, WireMessage};
use mikall_domain::messaging::{ChannelId, MessageId};
use mikall_domain::shared::IdentityId;

const MESSAGES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("messages");
const SEQ: TableDefinition<&[u8], u64> = TableDefinition::new("seq");

#[derive(Debug, Serialize, Deserialize)]
struct StoredMessage {
    id: [u8; 32],
    author: [u8; 32],
    lamport: u64,
    parents: Vec<[u8; 32]>,
    body: String,
    ts_hint_ms: u64,
}

impl From<&WireMessage> for StoredMessage {
    fn from(w: &WireMessage) -> Self {
        StoredMessage {
            id: *w.id.as_bytes(),
            author: *w.author.as_bytes(),
            lamport: w.lamport,
            parents: w.parents.iter().map(|p| *p.as_bytes()).collect(),
            body: w.body.clone(),
            ts_hint_ms: w.ts_hint_ms,
        }
    }
}

impl From<StoredMessage> for WireMessage {
    fn from(s: StoredMessage) -> Self {
        WireMessage {
            id: MessageId::from_bytes(s.id),
            author: IdentityId::from_bytes(s.author),
            lamport: s.lamport,
            parents: s.parents.into_iter().map(MessageId::from_bytes).collect(),
            body: s.body,
            ts_hint_ms: s.ts_hint_ms,
        }
    }
}

fn map_err<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Other(e.to_string())
}

#[derive(Debug)]
pub struct RedbStore {
    db: Arc<Database>,
}

impl RedbStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(map_err)?;
        }
        let db = Database::create(path).map_err(map_err)?;
        // Make sure the tables exist so first reads don't error.
        let tx = db.begin_write().map_err(map_err)?;
        {
            tx.open_table(MESSAGES).map_err(map_err)?;
            tx.open_table(SEQ).map_err(map_err)?;
        }
        tx.commit().map_err(map_err)?;
        Ok(RedbStore { db: Arc::new(db) })
    }

    fn message_key(channel: &ChannelId, seq: u64) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(channel.as_bytes());
        key[32..].copy_from_slice(&seq.to_be_bytes());
        key
    }
}

#[async_trait]
impl MessageStore for RedbStore {
    async fn save_channel_message(
        &self,
        channel: ChannelId,
        message: &WireMessage,
    ) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let stored = StoredMessage::from(message);
        tokio_free_blocking(move || {
            let tx = db.begin_write().map_err(map_err)?;
            {
                let mut seq_table = tx.open_table(SEQ).map_err(map_err)?;
                let seq = seq_table
                    .get(channel.as_bytes().as_slice())
                    .map_err(map_err)?
                    .map(|v| v.value())
                    .unwrap_or(0);
                seq_table
                    .insert(channel.as_bytes().as_slice(), seq + 1)
                    .map_err(map_err)?;

                let mut messages = tx.open_table(MESSAGES).map_err(map_err)?;
                let mut value = Vec::new();
                ciborium::into_writer(&stored, &mut value).map_err(map_err)?;
                let key = RedbStore::message_key(&channel, seq);
                messages
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(map_err)?;
            }
            tx.commit().map_err(map_err)
        })
    }

    async fn load_channel_messages(
        &self,
        channel: ChannelId,
    ) -> Result<Vec<WireMessage>, StoreError> {
        let db = Arc::clone(&self.db);
        tokio_free_blocking(move || {
            let tx = db.begin_read().map_err(map_err)?;
            let messages = tx.open_table(MESSAGES).map_err(map_err)?;
            let start = RedbStore::message_key(&channel, 0);
            let end = RedbStore::message_key(&channel, u64::MAX);
            let mut out = Vec::new();
            for entry in messages
                .range(start.as_slice()..=end.as_slice())
                .map_err(map_err)?
            {
                let (_, value) = entry.map_err(map_err)?;
                let stored: StoredMessage =
                    ciborium::from_reader(value.value()).map_err(map_err)?;
                out.push(WireMessage::from(stored));
            }
            Ok(out)
        })
    }
}

/// redb operations are short and synchronous; run them inline. (The name is
/// a reminder that nothing here awaits or blocks on I/O beyond page writes.)
fn tokio_free_blocking<T>(f: impl FnOnce() -> Result<T, StoreError>) -> Result<T, StoreError> {
    f()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn wire(n: u8) -> WireMessage {
        WireMessage {
            id: MessageId::from_bytes([n; 32]),
            author: IdentityId::from_bytes([1; 32]),
            lamport: u64::from(n),
            parents: if n == 1 {
                vec![]
            } else {
                vec![MessageId::from_bytes([n - 1; 32])]
            },
            body: format!("message {n}"),
            ts_hint_ms: 1000 + u64::from(n),
        }
    }

    #[tokio::test]
    async fn roundtrip_preserves_order_and_content() {
        let dir = std::env::temp_dir().join(format!("mikall-redb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = RedbStore::open(&dir.join("test.redb")).unwrap();
        let chan_a = ChannelId::from_bytes([0xA; 32]);
        let chan_b = ChannelId::from_bytes([0xB; 32]);

        for n in 1..=3 {
            store.save_channel_message(chan_a, &wire(n)).await.unwrap();
        }
        store.save_channel_message(chan_b, &wire(9)).await.unwrap();

        let loaded = store.load_channel_messages(chan_a).await.unwrap();
        assert_eq!(loaded, vec![wire(1), wire(2), wire(3)]);
        let other = store.load_channel_messages(chan_b).await.unwrap();
        assert_eq!(other, vec![wire(9)]);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
