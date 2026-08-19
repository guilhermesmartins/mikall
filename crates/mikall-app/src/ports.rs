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
    /// Sealed video frame for a call, arriving on a unidirectional media
    /// stream. Same trust model as [`InboundHandler::on_media_frame`]; a
    /// separate method because video demuxes to its own per-call tap.
    async fn on_video_frame(&self, from: IdentityId, call: CallId, sealed_frame: Vec<u8>);
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

/// A TCP port the IRC gateway may listen on: non-privileged, user-space
/// (`1024..=65535`). The loopback-only rule lives in `mikall-irc`'s
/// `IrcBindAddr`; this object only rules out ports that would need root or
/// mean "pick for me" — a persisted setting must name a real port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayPort(u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("gateway port must be 1024..=65535")]
pub struct GatewayPortError;

impl GatewayPort {
    /// The IRC default, 6667.
    pub const DEFAULT: GatewayPort = GatewayPort(6667);

    pub fn new(port: u16) -> Result<Self, GatewayPortError> {
        if port >= 1024 {
            Ok(GatewayPort(port))
        } else {
            Err(GatewayPortError)
        }
    }

    pub fn get(self) -> u16 {
        self.0
    }
}

impl Default for GatewayPort {
    fn default() -> Self {
        GatewayPort::DEFAULT
    }
}

/// The persisted IRC-gateway choice: off by default, port 6667, no
/// password. One struct rather than three loose record fields so a save
/// can never tear the setting apart.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IrcGatewayConfig {
    /// Whether the gateway starts with the node.
    pub enabled: bool,
    pub port: GatewayPort,
    /// Recommended on shared machines — the gateway is plaintext on
    /// loopback and other local users can reach loopback ports.
    pub password: Option<String>,
}

/// A remembered peer's multiaddr, held as a validated string. The port
/// layer speaks no libp2p, so validation is syntactic — leading slash,
/// non-empty `/`-separated segments, no whitespace, bounded length — and
/// the network adapter does the real parse at dial time. Anything that
/// passes here round-trips the store safely and displays cleanly in a
/// saved-peers list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerAddrError {
    #[error("peer address is empty")]
    Empty,
    #[error("peer address must be a multiaddr starting with '/'")]
    NotAMultiaddr,
    #[error("peer address has an empty '/'-separated segment")]
    EmptySegment,
    #[error("peer address contains whitespace or control characters")]
    ForbiddenCharacter,
    #[error("peer address is longer than {} bytes", PeerAddr::MAX_LEN)]
    TooLong,
}

impl PeerAddr {
    /// Generous for any TCP/QUIC multiaddr with a `/p2p/` suffix, small
    /// enough that a persisted record stays bounded.
    pub const MAX_LEN: usize = 256;

    /// Smart constructor: trims, then checks multiaddr syntax.
    pub fn parse(s: &str) -> Result<Self, PeerAddrError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(PeerAddrError::Empty);
        }
        if s.len() > Self::MAX_LEN {
            return Err(PeerAddrError::TooLong);
        }
        let Some(rest) = s.strip_prefix('/') else {
            return Err(PeerAddrError::NotAMultiaddr);
        };
        if s.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(PeerAddrError::ForbiddenCharacter);
        }
        if rest.split('/').any(str::is_empty) {
            return Err(PeerAddrError::EmptySegment);
        }
        Ok(PeerAddr(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The locally persisted slice of the profile. Every field is `Option` +
/// `Default` (or an empty collection) so later milestones add fields here
/// without changing the [`ProfileStore`] trait and without invalidating
/// records written before the field existed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProfileRecord {
    /// The announced nickname, once the user has chosen one.
    pub nickname: Option<Nickname>,
    /// IRC gateway settings; `None` means never configured (defaults).
    pub irc: Option<IrcGatewayConfig>,
    /// Locally blocked identities — bans are local on a decentralized
    /// network, and they survive a restart.
    pub blocked: Vec<IdentityId>,
    /// Addresses of peers we successfully connected to — most recent
    /// first, deduped, capped at [`ProfileRecord::PEERS_CAP`]. The node
    /// redials these at boot, so a manual dial is a first-time-only act.
    pub peers: Vec<PeerAddr>,
}

impl ProfileRecord {
    /// How many remembered peers survive: the most recent 16. The cache
    /// exists to make reconnection automatic, not to archive every peer
    /// ever met, so it must never grow unbounded.
    pub const PEERS_CAP: usize = 16;

    /// Remember a peer address: a known one moves to the front, a new one
    /// is inserted there, and the oldest past the cap falls off.
    pub fn remember_peer(&mut self, addr: PeerAddr) {
        self.peers.retain(|known| known != &addr);
        self.peers.insert(0, addr);
        self.peers.truncate(Self::PEERS_CAP);
    }

    /// Forget a peer address. Returns whether the record changed (and so
    /// wants persisting).
    pub fn forget_peer(&mut self, addr: &PeerAddr) -> bool {
        let before = self.peers.len();
        self.peers.retain(|known| known != addr);
        self.peers.len() != before
    }
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
    /// The sender muted (or unmuted) their mic. Advisory roster state —
    /// the muted party enforces it locally by sending only silence.
    Mute {
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

/// Which media a unidirectional stream carries. Video rides streams as of
/// M16; voice still travels [`MediaTransport`] and moves here later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaStreamKind {
    Audio,
    Video,
}

/// One live unidirectional media lane toward a peer: sealed frames go in,
/// nothing comes back — real-time media wants no per-frame acks. Closing
/// is dropping.
#[async_trait]
pub trait MediaSendStream: Send {
    async fn send(&mut self, sealed_frame: &[u8]) -> Result<(), TransportError>;
}

/// Opens unidirectional media streams to call peers (one QUIC/yamux
/// substream per (sender, viewer, kind) in production). The media engine
/// runs one lane per viewer with its own queue, so a slow viewer can never
/// head-of-line-block the others or the signaling path.
#[async_trait]
pub trait MediaStreamTransport: Send + Sync {
    async fn open_stream(
        &self,
        to: IdentityId,
        call: CallId,
        kind: MediaStreamKind,
    ) -> Result<Box<dyn MediaSendStream>, TransportError>;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn gateway_port_accepts_user_space_only() {
        assert!(GatewayPort::new(1023).is_err());
        assert!(GatewayPort::new(0).is_err());
        assert_eq!(GatewayPort::new(1024).unwrap().get(), 1024);
        assert_eq!(GatewayPort::new(65535).unwrap().get(), 65535);
        assert_eq!(GatewayPort::default().get(), 6667);
    }

    #[test]
    fn profile_record_defaults_stay_quiet() {
        let record = ProfileRecord::default();
        assert_eq!(record.nickname, None);
        assert_eq!(record.irc, None);
        assert!(record.blocked.is_empty());
        assert!(record.peers.is_empty());
        let irc = IrcGatewayConfig::default();
        assert!(!irc.enabled);
        assert_eq!(irc.port.get(), 6667);
        assert_eq!(irc.password, None);
    }

    #[test]
    fn peer_addr_parses_multiaddrs_and_rejects_junk() {
        // Whitespace around a pasted address is forgiven, junk is not.
        let ok = PeerAddr::parse(" /ip4/127.0.0.1/tcp/4001 ").unwrap();
        assert_eq!(ok.as_str(), "/ip4/127.0.0.1/tcp/4001");
        assert!(PeerAddr::parse("/ip4/192.168.1.7/udp/4001/quic-v1/p2p/12D3KooWQvcGm").is_ok());

        assert_eq!(PeerAddr::parse(""), Err(PeerAddrError::Empty));
        assert_eq!(PeerAddr::parse("   "), Err(PeerAddrError::Empty));
        assert_eq!(
            PeerAddr::parse("localhost:4001"),
            Err(PeerAddrError::NotAMultiaddr)
        );
        assert_eq!(
            PeerAddr::parse("/ip4//tcp/4001"),
            Err(PeerAddrError::EmptySegment)
        );
        assert_eq!(
            PeerAddr::parse("/ip4/1.2.3.4/tcp/4001/"),
            Err(PeerAddrError::EmptySegment)
        );
        assert_eq!(PeerAddr::parse("/"), Err(PeerAddrError::EmptySegment));
        assert_eq!(
            PeerAddr::parse("/ip4/1.2.3.4 evil/tcp/4001"),
            Err(PeerAddrError::ForbiddenCharacter)
        );
        let long = format!("/dns4/{}/tcp/4001", "a".repeat(PeerAddr::MAX_LEN));
        assert_eq!(PeerAddr::parse(&long), Err(PeerAddrError::TooLong));
    }

    #[test]
    fn remember_peer_dedupes_moves_to_front_and_caps() {
        let addr = |s: &str| PeerAddr::parse(s).unwrap();
        let mut record = ProfileRecord::default();
        let first = addr("/ip4/10.0.0.1/tcp/4001");
        let second = addr("/ip4/10.0.0.2/tcp/4001");

        record.remember_peer(first.clone());
        record.remember_peer(second.clone());
        assert_eq!(record.peers, vec![second.clone(), first.clone()]);

        // Re-remembering moves to the front instead of duplicating.
        record.remember_peer(first.clone());
        assert_eq!(record.peers, vec![first, second]);

        // Only the most recent PEERS_CAP survive.
        for port in 0..40 {
            record.remember_peer(addr(&format!("/ip4/10.9.9.9/tcp/{port}")));
        }
        assert_eq!(record.peers.len(), ProfileRecord::PEERS_CAP);
        assert_eq!(record.peers[0].as_str(), "/ip4/10.9.9.9/tcp/39");
        assert_eq!(
            record.peers[ProfileRecord::PEERS_CAP - 1].as_str(),
            "/ip4/10.9.9.9/tcp/24"
        );
    }

    #[test]
    fn forget_peer_reports_whether_the_record_changed() {
        let mut record = ProfileRecord::default();
        let known = PeerAddr::parse("/ip4/10.0.0.1/tcp/4001").unwrap();
        let stranger = PeerAddr::parse("/ip4/10.0.0.2/tcp/4001").unwrap();
        record.remember_peer(known.clone());

        assert!(!record.forget_peer(&stranger));
        assert!(record.forget_peer(&known));
        assert!(!record.forget_peer(&known));
        assert!(record.peers.is_empty());
    }
}
