//! Persistence adapter: a single-file redb database.
//!
//! Layout:
//! - `messages`: key = channel id (32 bytes) ++ big-endian sequence (8
//!   bytes) so a prefix range scan replays a channel in arrival order;
//!   value = CBOR of [`StoredMessage`].
//! - `seq`: next sequence number per channel.
//! - `profile`: a single row (`"local"`) holding the CBOR of
//!   [`StoredProfile`] — the persisted local profile record.
//!
//! Wire/domain types never touch redb directly — [`StoredMessage`] and
//! [`StoredProfile`] are this adapter's own serde DTOs.

mod blobs;

pub use blobs::FsBlobStore;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use mikall_app::ports::{
    GatewayPort, IrcGatewayConfig, MessageStore, ProfileRecord, ProfileStore, ProfileStoreError,
    StoreError, WireMessage,
};
use mikall_domain::messaging::{ChannelId, MessageId, Nickname};
use mikall_domain::shared::IdentityId;

const MESSAGES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("messages");
const SEQ: TableDefinition<&[u8], u64> = TableDefinition::new("seq");
const PROFILE: TableDefinition<&str, &[u8]> = TableDefinition::new("profile");

/// The single row key of the `profile` table: there is exactly one local
/// profile per database.
const PROFILE_KEY: &str = "local";

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

/// This adapter's DTO for the persisted profile. Every field carries
/// `#[serde(default)]` so records written before a field existed still
/// decode — the forward-compatibility contract of
/// [`mikall_app::ports::ProfileRecord`].
#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredProfile {
    #[serde(default)]
    nickname: Option<String>,
    #[serde(default)]
    irc: Option<StoredIrcGateway>,
    #[serde(default)]
    blocked: Vec<[u8; 32]>,
}

/// The IRC-gateway slice of [`StoredProfile`], mirroring
/// [`IrcGatewayConfig`] field for field.
#[derive(Debug, Serialize, Deserialize)]
struct StoredIrcGateway {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    port: u16,
    #[serde(default)]
    password: Option<String>,
}

impl From<&ProfileRecord> for StoredProfile {
    fn from(r: &ProfileRecord) -> Self {
        StoredProfile {
            nickname: r.nickname.as_ref().map(|n| n.as_str().to_owned()),
            irc: r.irc.as_ref().map(|irc| StoredIrcGateway {
                enabled: irc.enabled,
                port: irc.port.get(),
                password: irc.password.clone(),
            }),
            blocked: r.blocked.iter().map(|id| *id.as_bytes()).collect(),
        }
    }
}

impl TryFrom<StoredProfile> for ProfileRecord {
    type Error = ProfileStoreError;

    fn try_from(s: StoredProfile) -> Result<Self, Self::Error> {
        let nickname = s
            .nickname
            .as_deref()
            .map(Nickname::parse)
            .transpose()
            .map_err(|e| ProfileStoreError::Other(format!("stored nickname invalid: {e}")))?;
        let irc = s
            .irc
            .map(|irc| {
                Ok(IrcGatewayConfig {
                    enabled: irc.enabled,
                    port: GatewayPort::new(irc.port).map_err(|e| {
                        ProfileStoreError::Other(format!("stored gateway port invalid: {e}"))
                    })?,
                    password: irc.password,
                })
            })
            .transpose()?;
        Ok(ProfileRecord {
            nickname,
            irc,
            blocked: s.blocked.into_iter().map(IdentityId::from_bytes).collect(),
        })
    }
}

fn map_err<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Other(e.to_string())
}

fn map_profile_err<E: std::fmt::Display>(e: E) -> ProfileStoreError {
    ProfileStoreError::Other(e.to_string())
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
            tx.open_table(PROFILE).map_err(map_err)?;
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

#[async_trait]
impl ProfileStore for RedbStore {
    async fn load(&self) -> Result<ProfileRecord, ProfileStoreError> {
        let db = Arc::clone(&self.db);
        let stored = tokio_free_blocking(move || {
            let tx = db.begin_read().map_err(map_err)?;
            let profile = tx.open_table(PROFILE).map_err(map_err)?;
            match profile.get(PROFILE_KEY).map_err(map_err)? {
                None => Ok(None),
                Some(value) => {
                    let stored: StoredProfile =
                        ciborium::from_reader(value.value()).map_err(map_err)?;
                    Ok(Some(stored))
                }
            }
        })
        .map_err(map_profile_err)?;
        match stored {
            None => Ok(ProfileRecord::default()),
            Some(stored) => ProfileRecord::try_from(stored),
        }
    }

    async fn save(&self, record: &ProfileRecord) -> Result<(), ProfileStoreError> {
        let db = Arc::clone(&self.db);
        let stored = StoredProfile::from(record);
        tokio_free_blocking(move || {
            let tx = db.begin_write().map_err(map_err)?;
            {
                let mut profile = tx.open_table(PROFILE).map_err(map_err)?;
                let mut value = Vec::new();
                ciborium::into_writer(&stored, &mut value).map_err(map_err)?;
                profile
                    .insert(PROFILE_KEY, value.as_slice())
                    .map_err(map_err)?;
            }
            tx.commit().map_err(map_err)
        })
        .map_err(map_profile_err)
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

    #[tokio::test]
    async fn profile_loads_empty_then_roundtrips_and_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("mikall-redb-profile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb");

        let store = RedbStore::open(&path).unwrap();
        // A fresh database has no record yet: the default (no nickname).
        assert_eq!(store.load().await.unwrap(), ProfileRecord::default());

        let record = ProfileRecord {
            nickname: Some(Nickname::parse("miku").unwrap()),
            ..ProfileRecord::default()
        };
        store.save(&record).await.unwrap();
        assert_eq!(store.load().await.unwrap(), record);

        // Saving again overwrites the single row rather than appending.
        let renamed = ProfileRecord {
            nickname: Some(Nickname::parse("rin").unwrap()),
            ..ProfileRecord::default()
        };
        store.save(&renamed).await.unwrap();
        assert_eq!(store.load().await.unwrap(), renamed);

        // The restart shape: drop the database handle, reopen the same file.
        drop(store);
        let reopened = RedbStore::open(&path).unwrap();
        assert_eq!(reopened.load().await.unwrap(), renamed);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn irc_settings_and_blocklist_roundtrip_and_survive_reopen() {
        let dir = std::env::temp_dir().join(format!("mikall-redb-irc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb");

        let store = RedbStore::open(&path).unwrap();
        // A record written before the fields existed (nickname only) still
        // decodes: the settings come back as "never configured".
        store
            .save(&ProfileRecord {
                nickname: Some(Nickname::parse("miku").unwrap()),
                ..ProfileRecord::default()
            })
            .await
            .unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.irc, None);
        assert!(loaded.blocked.is_empty());

        // Read-modify-write in the settings writer keeps the nickname.
        let mut record = store.load().await.unwrap();
        record.irc = Some(IrcGatewayConfig {
            enabled: true,
            port: GatewayPort::new(6668).unwrap(),
            password: Some("negi".to_owned()),
        });
        record.blocked = vec![IdentityId::from_bytes([7; 32])];
        store.save(&record).await.unwrap();

        drop(store);
        let reopened = RedbStore::open(&path).unwrap();
        let loaded = reopened.load().await.unwrap();
        assert_eq!(loaded.nickname, Some(Nickname::parse("miku").unwrap()));
        let irc = loaded.irc.unwrap();
        assert!(irc.enabled);
        assert_eq!(irc.port.get(), 6668);
        assert_eq!(irc.password.as_deref(), Some("negi"));
        assert_eq!(loaded.blocked, vec![IdentityId::from_bytes([7; 32])]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn profile_and_messages_share_one_database() {
        let dir = std::env::temp_dir().join(format!("mikall-redb-shared-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = RedbStore::open(&dir.join("test.redb")).unwrap();
        let chan = ChannelId::from_bytes([0xC; 32]);

        store.save_channel_message(chan, &wire(1)).await.unwrap();
        store
            .save(&ProfileRecord {
                nickname: Some(Nickname::parse("miku").unwrap()),
                ..ProfileRecord::default()
            })
            .await
            .unwrap();

        assert_eq!(
            store.load_channel_messages(chan).await.unwrap(),
            vec![wire(1)]
        );
        assert_eq!(
            store.load().await.unwrap().nickname,
            Some(Nickname::parse("miku").unwrap())
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
