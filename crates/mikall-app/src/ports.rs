//! Driven ports: the traits adapters implement.
//!
//! Every trait here speaks domain types only — no libp2p, no codec, no
//! database type ever crosses this boundary. Production adapters live in
//! `mikall-net`, `mikall-store`, `mikall-crypto`, `mikall-media`; the BDD
//! suite provides in-memory fakes.

use async_trait::async_trait;

use mikall_domain::calls::CallId;
use mikall_domain::messaging::{ChannelId, ChannelName, MessageId, Nickname, Verified};
use mikall_domain::shared::{Fingerprint, IdentityId};
use mikall_domain::transfer::TransferId;

/// A message as it crosses the transport boundary, before it becomes a
/// domain [`mikall_domain::messaging::Message`]. Adapters must verify the
/// envelope signature and then attest via
/// [`mikall_domain::messaging::Verified`] when handing it to a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMessage {
    pub id: MessageId,
    pub author: IdentityId,
    pub lamport: u64,
    pub parents: Vec<MessageId>,
    pub body: String,
    pub ts_hint_ms: u64,
}

/// Everything that travels on a channel's gossip topic besides raw presence
/// beacons: chat messages and membership/topic operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelSignal {
    Message(WireMessage),
    Joined {
        who: IdentityId,
        nickname: Option<String>,
    },
    Left {
        who: IdentityId,
    },
    TopicChanged {
        by: IdentityId,
        topic: String,
    },
    /// Operator grant/revoke. `op: true` grants, `false` revokes. The
    /// receiving aggregate re-checks that `by` holds the founder seat.
    RoleChanged {
        by: IdentityId,
        target: IdentityId,
        op: bool,
    },
    Presence {
        who: IdentityId,
        away: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("not connected to any peers for this destination")]
    Unreachable,
    #[error("transport failure: {0}")]
    Other(String),
}

/// Outbound transport: publish to a channel's gossip topic or send an
/// (encrypted) direct envelope. Inbound delivery happens by the adapter
/// invoking an [`InboundHandler`] with a `Verified` attestation per item.
#[async_trait]
pub trait ChatTransport: Send + Sync {
    async fn subscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError>;
    async fn unsubscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError>;
    async fn publish(
        &self,
        channel: ChannelId,
        signal: ChannelSignal,
    ) -> Result<(), TransportError>;
    async fn send_dm(&self, to: IdentityId, message: WireMessage) -> Result<(), TransportError>;
}

/// The application side of inbound traffic. Transport adapters verify
/// envelope signatures, then call these with the attestation token.
#[async_trait]
pub trait InboundHandler: Send + Sync {
    async fn on_channel_signal(
        &self,
        channel: ChannelId,
        signal: ChannelSignal,
        verified: Verified,
    );
    async fn on_dm(&self, from: IdentityId, message: WireMessage, verified: Verified);
    async fn on_file_offer(
        &self,
        from: IdentityId,
        manifest: mikall_domain::transfer::FileManifest,
        verified: Verified,
    );
    async fn on_call_signal(
        &self,
        from: IdentityId,
        call: CallId,
        action: CallAction,
        verified: Verified,
    );
    /// Sealed media frame for a call. Authenticity comes from the per-call
    /// AEAD key, not an envelope signature.
    async fn on_media_frame(&self, from: IdentityId, call: CallId, sealed_frame: Vec<u8>);
}

/// A channel's discovery record: enough for a stranger to join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecord {
    pub id: ChannelId,
    pub name: ChannelName,
    pub founder: IdentityId,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DirectoryError {
    #[error("directory failure: {0}")]
    Other(String),
}

/// Serverless discovery: announce and look up channels (Kademlia DHT signed
/// records in production, a map in tests). First announce wins a name.
#[async_trait]
pub trait Directory: Send + Sync {
    async fn announce_channel(&self, record: ChannelRecord) -> Result<(), DirectoryError>;
    async fn lookup_channel(
        &self,
        name: &ChannelName,
    ) -> Result<Option<ChannelRecord>, DirectoryError>;
    async fn list_channels(&self) -> Result<Vec<ChannelRecord>, DirectoryError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("storage failure: {0}")]
    Other(String),
}

/// Local persistence of chat state (redb in production).
#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn save_channel_message(
        &self,
        channel: ChannelId,
        message: &WireMessage,
    ) -> Result<(), StoreError>;
    async fn load_channel_messages(
        &self,
        channel: ChannelId,
    ) -> Result<Vec<WireMessage>, StoreError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileStoreError {
    #[error("profile storage failure: {0}")]
    Other(String),
}

/// The locally persisted slice of the profile. Every field is `Option` +
/// `Default` so later milestones (IRC gateway config, local settings) add
/// fields here without changing the [`ProfileStore`] trait and without
/// invalidating records written before the field existed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfileRecord {
    /// The announced nickname, once the user has chosen one.
    pub nickname: Option<Nickname>,
}

/// Local persistence of the profile record (redb in production). Callers
/// mutate via read-modify-write — load, change a field, save — so fields
/// they don't know about survive the round trip.
#[async_trait]
pub trait ProfileStore: Send + Sync {
    /// The stored record, or [`ProfileRecord::default`] when none exists yet.
    async fn load(&self) -> Result<ProfileRecord, ProfileStoreError>;
    async fn save(&self, record: &ProfileRecord) -> Result<(), ProfileStoreError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyStoreError {
    #[error("keystore failure: {0}")]
    Other(String),
}

/// Custody of the local identity key. The domain never sees private key
/// material — only the public [`IdentityId`] and [`Fingerprint`].
#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Load or create the local identity.
    async fn local_identity(&self) -> Result<(IdentityId, Fingerprint), KeyStoreError>;
    /// Fingerprint of any identity key (BLAKE3 of the public key).
    fn fingerprint_of(&self, id: &IdentityId) -> Fingerprint;
}

/// Deterministic-in-tests generation of ids.
pub trait IdGen: Send + Sync {
    /// Content-derived message id (BLAKE3 of the canonical envelope in
    /// production).
    fn message_id(
        &self,
        author: &IdentityId,
        lamport: u64,
        parents: &[MessageId],
        body: &str,
    ) -> MessageId;
    /// Deterministic public-channel id from its canonical name.
    fn channel_id(&self, name: &ChannelName) -> ChannelId;
    fn call_id(&self) -> CallId;
    fn transfer_id(&self) -> TransferId;
    /// Fresh random per-call media AEAD key.
    fn call_key(&self) -> [u8; 32];
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// A call-signaling action carried as a signed direct envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallAction {
    Offer {
        participants: Vec<IdentityId>,
        /// Per-call AEAD key for media frames. Confidential to the direct
        /// Noise-encrypted connection today; moves inside the E2E layer
        /// with the ratchet upgrade.
        media_key: [u8; 32],
    },
    Accept,
    Decline,
    HangUp,
    ScreenShare {
        active: bool,
    },
}

/// Sends call signaling to a peer (signed direct envelopes in production).
#[async_trait]
pub trait CallSignaling: Send + Sync {
    async fn send(
        &self,
        to: IdentityId,
        call: CallId,
        action: CallAction,
    ) -> Result<(), TransportError>;
}

/// Sends sealed media frames to a call peer. Frames are AEAD-encrypted by
/// the media engine; the transport only moves bytes.
#[async_trait]
pub trait MediaTransport: Send + Sync {
    async fn send_frame(
        &self,
        to: IdentityId,
        call: CallId,
        sealed_frame: Vec<u8>,
    ) -> Result<(), TransportError>;
}

/// BLAKE3 chunk hashing (crypto adapter in production).
pub trait ChunkHasher: Send + Sync {
    fn hash_chunk(&self, bytes: &[u8]) -> mikall_domain::transfer::BlobHash;
}

/// Content-addressed chunk storage plus local file import/assembly.
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put_chunk(
        &self,
        root: mikall_domain::transfer::BlobHash,
        index: u32,
        bytes: &[u8],
    ) -> Result<(), StoreError>;
    async fn get_chunk(
        &self,
        root: mikall_domain::transfer::BlobHash,
        index: u32,
    ) -> Result<Option<Vec<u8>>, StoreError>;
    /// Chunk + hash a local file into the store, returning its manifest.
    async fn import(
        &self,
        path: &std::path::Path,
    ) -> Result<mikall_domain::transfer::FileManifest, StoreError>;
    /// Reassemble a completed blob into `dest`.
    async fn assemble(
        &self,
        manifest: &mikall_domain::transfer::FileManifest,
        dest: &std::path::Path,
    ) -> Result<(), StoreError>;
}

/// File-transfer wire operations: offers travel as signed direct envelopes;
/// chunks are fetched from any peer holding the blob.
#[async_trait]
pub trait FileTransport: Send + Sync {
    async fn send_offer(
        &self,
        to: IdentityId,
        manifest: mikall_domain::transfer::FileManifest,
    ) -> Result<(), TransportError>;
    async fn fetch_chunk(
        &self,
        from: IdentityId,
        root: mikall_domain::transfer::BlobHash,
        index: u32,
    ) -> Result<Vec<u8>, TransportError>;
}
