//! libp2p network adapter: gossipsub channel topics, Kademlia channel
//! directory, mDNS LAN discovery, and a request-response direct-message
//! protocol — all speaking the signed envelopes of [`wire`].
//!
//! The libp2p transport keypair is derived from the *same* Ed25519 secret
//! as the mikall identity, so a peer's `PeerId` is a pure function of its
//! `IdentityId`: DMs route without any lookup table.

pub mod wire;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use libp2p::kad::store::RecordStore as _;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{
    gossipsub, identify, identity, kad, mdns, noise, request_response, tcp, yamux, Multiaddr,
    PeerId, StreamProtocol, Swarm,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use mikall_app::ports::{
    BlobStore, CallAction, CallSignaling, ChannelRecord, ChannelSignal, ChatTransport, Directory,
    DirectoryError, FileTransport, InboundHandler, MediaSendStream, MediaStreamKind,
    MediaStreamTransport, MediaTransport, TransportError, WireMessage,
};
use mikall_crypto::LocalKeys;
use mikall_domain::calls::CallId;
use mikall_domain::messaging::{ChannelId, ChannelName};
use mikall_domain::shared::IdentityId;
use mikall_domain::transfer::{BlobHash, FileManifest};

use wire::{
    open_dm, open_envelope, seal_call, seal_dm, seal_offer, seal_signal, DmPayload, SignedEnvelope,
    MAX_ENVELOPE_BYTES,
};

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("failed to build swarm: {0}")]
    Build(String),
    #[error("network task is gone")]
    TaskGone,
}

/// `PeerId` is a pure function of the mikall identity.
pub fn peer_id_of(identity_id: &IdentityId) -> Option<PeerId> {
    identity::ed25519::PublicKey::try_from_bytes(identity_id.as_bytes())
        .ok()
        .map(|pk| PeerId::from_public_key(&identity::PublicKey::from(pk)))
}

/// Inverse mapping: recover the identity from an Ed25519 `PeerId` (inline
/// public key). Returns `None` for non-Ed25519 peers.
pub fn identity_of_peer(peer: &PeerId) -> Option<IdentityId> {
    let multihash = peer.as_ref();
    // Identity multihash (code 0x00) wraps the protobuf-encoded public key.
    if multihash.code() != 0 {
        return None;
    }
    let public = identity::PublicKey::try_decode_protobuf(multihash.digest()).ok()?;
    let ed = public.try_into_ed25519().ok()?;
    Some(IdentityId::from_bytes(ed.to_bytes()))
}

fn topic_of(channel: &ChannelId) -> gossipsub::IdentTopic {
    let mut hex = String::with_capacity(64);
    for b in channel.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    gossipsub::IdentTopic::new(format!("mikall/1/chan/{hex}"))
}

fn channel_record_key(name: &ChannelName) -> kad::RecordKey {
    kad::RecordKey::new(&format!("mikall:chan:v1:{}", name.as_str()))
}

#[derive(Debug, Serialize, Deserialize)]
struct ChannelRecordDto {
    id: Vec<u8>,
    name: String,
    founder: Vec<u8>,
}

fn encode_record(record: &ChannelRecord) -> Vec<u8> {
    let dto = ChannelRecordDto {
        id: record.id.as_bytes().to_vec(),
        name: record.name.as_str().to_owned(),
        founder: record.founder.as_bytes().to_vec(),
    };
    let mut out = Vec::new();
    let _ = ciborium::into_writer(&dto, &mut out);
    out
}

fn decode_record(bytes: &[u8]) -> Option<ChannelRecord> {
    let dto: ChannelRecordDto = ciborium::from_reader(bytes).ok()?;
    Some(ChannelRecord {
        id: ChannelId::from_bytes(dto.id.as_slice().try_into().ok()?),
        name: ChannelName::parse(&dto.name).ok()?,
        founder: IdentityId::from_bytes(dto.founder.as_slice().try_into().ok()?),
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct DmAck;

#[derive(Debug, Serialize, Deserialize)]
struct BlobRequest {
    root: Vec<u8>,
    index: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlobResponse {
    chunk: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MediaPacket {
    call: Vec<u8>,
    frame: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MediaAck;

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    kad: kad::Behaviour<kad::store::MemoryStore>,
    mdns: Toggle<mdns::tokio::Behaviour>,
    identify: identify::Behaviour,
    dm: request_response::cbor::Behaviour<SignedEnvelope, DmAck>,
    blob: request_response::cbor::Behaviour<BlobRequest, BlobResponse>,
    media: request_response::cbor::Behaviour<MediaPacket, MediaAck>,
    /// Unidirectional media streams (video today; voice migrates later).
    /// Opened and accepted through a [`libp2p_stream::Control`] entirely
    /// outside the swarm command loop, so media can never head-of-line
    /// block signaling — and vice versa.
    media_stream: libp2p_stream::Behaviour,
}

/// The unidirectional media-stream protocol: a 17-byte stream header
/// (call id ‖ kind), then length-prefixed sealed frames, no acks ever.
const MEDIA_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/mikall/media-stream/1");

/// Per-frame ceiling on the stream (a 1280-wide screen keyframe is a few
/// hundred KB; anything near this is hostile or corrupt).
const MAX_MEDIA_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NetConfig {
    pub listen: Vec<Multiaddr>,
    pub enable_mdns: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig {
            listen: vec![
                "/ip4/0.0.0.0/tcp/0"
                    .parse()
                    .unwrap_or_else(|_| Multiaddr::empty()),
                "/ip4/0.0.0.0/udp/0/quic-v1"
                    .parse()
                    .unwrap_or_else(|_| Multiaddr::empty()),
            ],
            enable_mdns: true,
        }
    }
}

type DmReply = oneshot::Sender<Result<(), TransportError>>;
type DmPending = HashMap<request_response::OutboundRequestId, DmReply>;
type BlobReply = oneshot::Sender<Result<Vec<u8>, TransportError>>;
type BlobPending = HashMap<request_response::OutboundRequestId, BlobReply>;
type LookupReply = oneshot::Sender<Result<Option<ChannelRecord>, DirectoryError>>;
type KadLookups = HashMap<kad::QueryId, (ChannelName, LookupReply)>;

enum Command {
    Subscribe(ChannelId, oneshot::Sender<Result<(), TransportError>>),
    Unsubscribe(ChannelId, oneshot::Sender<Result<(), TransportError>>),
    Publish(
        ChannelId,
        Box<ChannelSignal>,
        oneshot::Sender<Result<(), TransportError>>,
    ),
    SendDm(
        IdentityId,
        Box<WireMessage>,
        oneshot::Sender<Result<(), TransportError>>,
    ),
    AnnounceChannel(ChannelRecord, oneshot::Sender<Result<(), DirectoryError>>),
    LookupChannel(
        ChannelName,
        oneshot::Sender<Result<Option<ChannelRecord>, DirectoryError>>,
    ),
    ListChannels(oneshot::Sender<Vec<ChannelRecord>>),
    ListenAddrs(oneshot::Sender<Vec<Multiaddr>>),
    Dial(Multiaddr, oneshot::Sender<Result<(), TransportError>>),
    ConnectedPeers(oneshot::Sender<Vec<PeerId>>),
    MeshPeerCount(ChannelId, oneshot::Sender<usize>),
    SendOffer(
        IdentityId,
        Box<FileManifest>,
        oneshot::Sender<Result<(), TransportError>>,
    ),
    FetchChunk(IdentityId, BlobHash, u32, BlobReply),
    SendCallSignal(
        IdentityId,
        CallId,
        CallAction,
        oneshot::Sender<Result<(), TransportError>>,
    ),
    SendMedia(IdentityId, CallId, Vec<u8>),
}

#[derive(Clone)]
struct CommandSender(mpsc::Sender<Command>);

impl CommandSender {
    async fn send(&self, command: Command) -> Result<(), TransportError> {
        self.0
            .send(command)
            .await
            .map_err(|_| TransportError::Other("network task is gone".into()))
    }
}

/// Handle for binaries and tests: listen addresses, dialing, mesh state.
#[derive(Clone)]
pub struct NetControl {
    tx: CommandSender,
    pub local_peer_id: PeerId,
}

impl std::fmt::Debug for NetControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetControl")
            .field("local_peer_id", &self.local_peer_id)
            .finish_non_exhaustive()
    }
}

impl NetControl {
    pub async fn listen_addrs(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Command::ListenAddrs(tx)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn dial(&self, addr: Multiaddr) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Dial(addr, tx)).await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }

    /// Peers with at least one live connection right now. Lets callers
    /// (the boot redial task, network status surfaces) distinguish "dial
    /// initiated" — all [`NetControl::dial`] promises — from "connected".
    pub async fn connected_peers(&self) -> Vec<PeerId> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Command::ConnectedPeers(tx)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    pub async fn mesh_peer_count(&self, channel: ChannelId) -> usize {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(Command::MeshPeerCount(channel, tx))
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }
}

/// `ChatTransport` implementation over the swarm task.
pub struct NetTransport {
    tx: CommandSender,
}

impl std::fmt::Debug for NetTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetTransport").finish_non_exhaustive()
    }
}

#[async_trait]
impl ChatTransport for NetTransport {
    async fn subscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Subscribe(channel, tx)).await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }

    async fn unsubscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Unsubscribe(channel, tx)).await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }

    async fn publish(
        &self,
        channel: ChannelId,
        signal: ChannelSignal,
    ) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Publish(channel, Box::new(signal), tx))
            .await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }

    async fn send_dm(&self, to: IdentityId, message: WireMessage) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SendDm(to, Box::new(message), tx))
            .await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }
}

/// `FileTransport` implementation over the swarm task.
pub struct NetFileTransport {
    tx: CommandSender,
}

impl std::fmt::Debug for NetFileTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetFileTransport").finish_non_exhaustive()
    }
}

#[async_trait]
impl FileTransport for NetFileTransport {
    async fn send_offer(
        &self,
        to: IdentityId,
        manifest: FileManifest,
    ) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SendOffer(to, Box::new(manifest), tx))
            .await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }

    async fn fetch_chunk(
        &self,
        from: IdentityId,
        root: BlobHash,
        index: u32,
    ) -> Result<Vec<u8>, TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::FetchChunk(from, root, index, tx))
            .await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }
}

/// `CallSignaling` implementation over the swarm task.
pub struct NetCallSignaling {
    tx: CommandSender,
}

impl std::fmt::Debug for NetCallSignaling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetCallSignaling").finish_non_exhaustive()
    }
}

#[async_trait]
impl CallSignaling for NetCallSignaling {
    async fn send(
        &self,
        to: IdentityId,
        call: CallId,
        action: CallAction,
    ) -> Result<(), TransportError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SendCallSignal(to, call, action, tx))
            .await?;
        rx.await
            .map_err(|_| TransportError::Other("network task is gone".into()))?
    }
}

/// `MediaTransport` implementation: fire-and-forget sealed frames.
pub struct NetMediaTransport {
    tx: CommandSender,
}

impl std::fmt::Debug for NetMediaTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetMediaTransport").finish_non_exhaustive()
    }
}

#[async_trait]
impl MediaTransport for NetMediaTransport {
    async fn send_frame(
        &self,
        to: IdentityId,
        call: CallId,
        sealed_frame: Vec<u8>,
    ) -> Result<(), TransportError> {
        self.tx
            .send(Command::SendMedia(to, call, sealed_frame))
            .await
    }
}

/// `MediaStreamTransport` implementation: one unidirectional libp2p
/// substream per (viewer, kind), spoken directly through the stream
/// behaviour's control — the global command channel never carries a
/// video frame.
pub struct NetMediaStreams {
    control: libp2p_stream::Control,
}

impl std::fmt::Debug for NetMediaStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetMediaStreams").finish_non_exhaustive()
    }
}

/// An open outbound media stream. Dropping it closes the substream.
struct NetMediaSendStream {
    stream: libp2p::Stream,
}

#[async_trait]
impl MediaSendStream for NetMediaSendStream {
    async fn send(&mut self, sealed_frame: &[u8]) -> Result<(), TransportError> {
        use futures::AsyncWriteExt as _;
        if sealed_frame.len() > MAX_MEDIA_FRAME_BYTES {
            return Err(TransportError::Other("media frame too large".into()));
        }
        let len = sealed_frame.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        self.stream
            .write_all(sealed_frame)
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        self.stream
            .flush()
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl MediaStreamTransport for NetMediaStreams {
    async fn open_stream(
        &self,
        to: IdentityId,
        call: CallId,
        kind: MediaStreamKind,
    ) -> Result<Box<dyn MediaSendStream>, TransportError> {
        use futures::AsyncWriteExt as _;
        let peer = peer_id_of(&to)
            .ok_or_else(|| TransportError::Other("recipient identity is not a valid key".into()))?;
        let mut control = self.control.clone();
        let mut stream = control
            .open_stream(peer, MEDIA_STREAM_PROTOCOL)
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        let mut header = [0u8; 17];
        header[..16].copy_from_slice(call.as_bytes());
        header[16] = match kind {
            MediaStreamKind::Audio => 0,
            MediaStreamKind::Video => 1,
        };
        stream
            .write_all(&header)
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        Ok(Box::new(NetMediaSendStream { stream }))
    }
}

/// Accept inbound media streams and pump their frames to the handler.
/// One task per stream; a malformed or oversized frame ends that stream
/// only.
async fn run_media_stream_acceptor(
    mut incoming: libp2p_stream::IncomingStreams,
    handler: Arc<dyn InboundHandler>,
) {
    while let Some((peer, stream)) = incoming.next().await {
        let handler = Arc::clone(&handler);
        tokio::spawn(read_media_stream(peer, stream, handler));
    }
}

async fn read_media_stream(
    peer: PeerId,
    mut stream: libp2p::Stream,
    handler: Arc<dyn InboundHandler>,
) {
    use futures::AsyncReadExt as _;
    // The sender is authenticated by the transport (PeerId ↔ identity);
    // frame authenticity is the per-call AEAD, exactly as on the old path.
    let Some(from) = identity_of_peer(&peer) else {
        return;
    };
    let mut header = [0u8; 17];
    if stream.read_exact(&mut header).await.is_err() {
        return;
    }
    let Ok(call_bytes) = <[u8; 16]>::try_from(&header[..16]) else {
        return;
    };
    let call = CallId::from_bytes(call_bytes);
    let kind = header[16];
    let mut len_buf = [0u8; 4];
    loop {
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_MEDIA_FRAME_BYTES {
            break;
        }
        let mut frame = vec![0u8; len];
        if stream.read_exact(&mut frame).await.is_err() {
            break;
        }
        match kind {
            0 => handler.on_media_frame(from, call, frame).await,
            1 => handler.on_video_frame(from, call, frame).await,
            _ => break,
        }
    }
}

/// `Directory` implementation over Kademlia records.
pub struct NetDirectory {
    tx: CommandSender,
}

impl std::fmt::Debug for NetDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetDirectory").finish_non_exhaustive()
    }
}

#[async_trait]
impl Directory for NetDirectory {
    async fn announce_channel(&self, record: ChannelRecord) -> Result<(), DirectoryError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::AnnounceChannel(record, tx))
            .await
            .map_err(|e| DirectoryError::Other(e.to_string()))?;
        rx.await
            .map_err(|_| DirectoryError::Other("network task is gone".into()))?
    }

    async fn lookup_channel(
        &self,
        name: &ChannelName,
    ) -> Result<Option<ChannelRecord>, DirectoryError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::LookupChannel(name.clone(), tx))
            .await
            .map_err(|e| DirectoryError::Other(e.to_string()))?;
        rx.await
            .map_err(|_| DirectoryError::Other("network task is gone".into()))?
    }

    async fn list_channels(&self) -> Result<Vec<ChannelRecord>, DirectoryError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::ListChannels(tx))
            .await
            .map_err(|e| DirectoryError::Other(e.to_string()))?;
        Ok(rx.await.unwrap_or_default())
    }
}

/// Everything the composition root needs from the network stack.
pub struct NetStack {
    pub transport: Arc<NetTransport>,
    pub directory: Arc<NetDirectory>,
    pub files: Arc<NetFileTransport>,
    pub call_signaling: Arc<NetCallSignaling>,
    pub media: Arc<NetMediaTransport>,
    pub media_streams: Arc<NetMediaStreams>,
    pub control: NetControl,
    driver: NetDriver,
    incoming_media_streams: libp2p_stream::IncomingStreams,
}

impl std::fmt::Debug for NetStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetStack").finish_non_exhaustive()
    }
}

struct NetDriver {
    swarm: Swarm<Behaviour>,
    cmd_rx: mpsc::Receiver<Command>,
    keys: Arc<LocalKeys>,
}

impl NetStack {
    /// Build the swarm (not yet running). Call [`NetStack::start`] with the
    /// application's inbound handler to begin processing.
    pub fn build(keys: Arc<LocalKeys>, config: NetConfig) -> Result<Self, NetError> {
        let secret = keys.secret_bytes();
        let keypair = identity::Keypair::ed25519_from_bytes(*secret)
            .map_err(|e| NetError::Build(e.to_string()))?;
        let enable_mdns = config.enable_mdns;

        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| NetError::Build(e.to_string()))?
            .with_quic()
            .with_behaviour(|key| {
                let peer_id = PeerId::from(key.public());
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .max_transmit_size(MAX_ENVELOPE_BYTES + 1024)
                    .heartbeat_interval(Duration::from_millis(500))
                    .build()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;

                let mut kad_config = kad::Config::default();
                kad_config.set_query_timeout(Duration::from_secs(10));
                let mut kad = kad::Behaviour::with_config(
                    peer_id,
                    kad::store::MemoryStore::new(peer_id),
                    kad_config,
                );
                kad.set_mode(Some(kad::Mode::Server));

                let mdns = if enable_mdns {
                    Some(
                        mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                            .map_err(std::io::Error::other)?,
                    )
                } else {
                    None
                };

                let identify = identify::Behaviour::new(identify::Config::new(
                    "/mikall/1.0.0".to_owned(),
                    key.public(),
                ));

                let dm = request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/mikall/dm/1"),
                        request_response::ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                let blob = request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/mikall/blob/1"),
                        request_response::ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                let media = request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/mikall/media/1"),
                        request_response::ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                Ok(Behaviour {
                    gossipsub,
                    kad,
                    mdns: Toggle::from(mdns),
                    identify,
                    dm,
                    blob,
                    media,
                    media_stream: libp2p_stream::Behaviour::new(),
                })
            })
            .map_err(|e| NetError::Build(e.to_string()))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
            .build();

        for addr in &config.listen {
            if !addr.is_empty() {
                let _ = swarm.listen_on(addr.clone());
            }
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let sender = CommandSender(cmd_tx);
        let local_peer_id = *swarm.local_peer_id();

        // The stream control lives outside the swarm task: media senders
        // open substreams and the acceptor reads them without a single
        // command-channel round trip.
        let mut stream_control = swarm.behaviour().media_stream.new_control();
        let incoming_media_streams = stream_control
            .accept(MEDIA_STREAM_PROTOCOL)
            .map_err(|e| NetError::Build(e.to_string()))?;

        Ok(NetStack {
            transport: Arc::new(NetTransport { tx: sender.clone() }),
            directory: Arc::new(NetDirectory { tx: sender.clone() }),
            files: Arc::new(NetFileTransport { tx: sender.clone() }),
            call_signaling: Arc::new(NetCallSignaling { tx: sender.clone() }),
            media: Arc::new(NetMediaTransport { tx: sender.clone() }),
            media_streams: Arc::new(NetMediaStreams {
                control: stream_control,
            }),
            control: NetControl {
                tx: sender,
                local_peer_id,
            },
            driver: NetDriver {
                swarm,
                cmd_rx,
                keys,
            },
            incoming_media_streams,
        })
    }

    /// Spawn the swarm event loop, delivering verified inbound traffic to
    /// `handler` and serving blob-chunk requests from `blobs`; and the
    /// media-stream acceptor beside it.
    pub fn start(
        self,
        handler: Arc<dyn InboundHandler>,
        blobs: Arc<dyn BlobStore>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let NetStack {
            driver,
            incoming_media_streams,
            ..
        } = self;
        vec![
            tokio::spawn(driver.run(Arc::clone(&handler), blobs)),
            tokio::spawn(run_media_stream_acceptor(incoming_media_streams, handler)),
        ]
    }
}

impl NetDriver {
    async fn run(mut self, handler: Arc<dyn InboundHandler>, blobs: Arc<dyn BlobStore>) {
        // channel-id lookup for inbound topics
        let mut topics: HashMap<gossipsub::TopicHash, ChannelId> = HashMap::new();
        let mut dm_pending: DmPending = HashMap::new();
        let mut blob_pending: BlobPending = HashMap::new();
        let mut kad_lookups: KadLookups = HashMap::new();

        loop {
            tokio::select! {
                command = self.cmd_rx.recv() => {
                    let Some(command) = command else { break };
                    self.handle_command(command, &mut topics, &mut dm_pending, &mut blob_pending, &mut kad_lookups);
                }
                event = self.swarm.select_next_some() => {
                    self.handle_event(event, &topics, &mut dm_pending, &mut blob_pending, &mut kad_lookups, &handler, &blobs).await;
                }
            }
        }
    }

    fn handle_command(
        &mut self,
        command: Command,
        topics: &mut HashMap<gossipsub::TopicHash, ChannelId>,
        dm_pending: &mut DmPending,
        blob_pending: &mut BlobPending,
        kad_lookups: &mut KadLookups,
    ) {
        match command {
            Command::Subscribe(channel, reply) => {
                let topic = topic_of(&channel);
                topics.insert(topic.hash(), channel);
                let result = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .subscribe(&topic)
                    .map(|_| ())
                    .map_err(|e| TransportError::Other(e.to_string()));
                let _ = reply.send(result);
            }
            Command::Unsubscribe(channel, reply) => {
                let topic = topic_of(&channel);
                topics.remove(&topic.hash());
                let _ = self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
                let _ = reply.send(Ok(()));
            }
            Command::Publish(channel, signal, reply) => {
                let envelope = seal_signal(&self.keys, &signal);
                let mut bytes = Vec::new();
                let _ = ciborium::into_writer(&envelope, &mut bytes);
                let result = match self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic_of(&channel), bytes)
                {
                    Ok(_) => Ok(()),
                    // Nobody listening yet is not an error on a P2P network:
                    // the message is already recorded locally and will reach
                    // peers via sync once they appear.
                    Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => Ok(()),
                    Err(e) => Err(TransportError::Other(e.to_string())),
                };
                let _ = reply.send(result);
            }
            Command::SendDm(to, message, reply) => {
                let Some(peer) = peer_id_of(&to) else {
                    let _ = reply.send(Err(TransportError::Other(
                        "recipient identity is not a valid key".into(),
                    )));
                    return;
                };
                let envelope = seal_dm(&self.keys, &message);
                let request_id = self.swarm.behaviour_mut().dm.send_request(&peer, envelope);
                dm_pending.insert(request_id, reply);
            }
            Command::AnnounceChannel(record, reply) => {
                let key = channel_record_key(&record.name);
                let value = encode_record(&record);
                let kad_record = kad::Record::new(key, value);
                let result = self
                    .swarm
                    .behaviour_mut()
                    .kad
                    .put_record(kad_record, kad::Quorum::One)
                    .map(|_| ())
                    .map_err(|e| DirectoryError::Other(e.to_string()));
                let _ = reply.send(result);
            }
            Command::LookupChannel(name, reply) => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kad
                    .get_record(channel_record_key(&name));
                kad_lookups.insert(query_id, (name, reply));
            }
            Command::ListChannels(reply) => {
                let records: Vec<ChannelRecord> = self
                    .swarm
                    .behaviour_mut()
                    .kad
                    .store_mut()
                    .records()
                    .filter_map(|r| decode_record(&r.value))
                    .collect();
                let _ = reply.send(records);
            }
            Command::ListenAddrs(reply) => {
                let addrs: Vec<Multiaddr> = self.swarm.listeners().cloned().collect();
                let _ = reply.send(addrs);
            }
            Command::Dial(addr, reply) => {
                let result = self
                    .swarm
                    .dial(addr)
                    .map_err(|e| TransportError::Other(e.to_string()));
                let _ = reply.send(result);
            }
            Command::ConnectedPeers(reply) => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().copied().collect();
                let _ = reply.send(peers);
            }
            Command::MeshPeerCount(channel, reply) => {
                let topic = topic_of(&channel);
                let count = self
                    .swarm
                    .behaviour()
                    .gossipsub
                    .mesh_peers(&topic.hash())
                    .count();
                let _ = reply.send(count);
            }
            Command::SendOffer(to, manifest, reply) => {
                let Some(peer) = peer_id_of(&to) else {
                    let _ = reply.send(Err(TransportError::Other(
                        "recipient identity is not a valid key".into(),
                    )));
                    return;
                };
                let envelope = seal_offer(&self.keys, &manifest);
                let request_id = self.swarm.behaviour_mut().dm.send_request(&peer, envelope);
                dm_pending.insert(request_id, reply);
            }
            Command::SendCallSignal(to, call, action, reply) => {
                let Some(peer) = peer_id_of(&to) else {
                    let _ = reply.send(Err(TransportError::Other(
                        "recipient identity is not a valid key".into(),
                    )));
                    return;
                };
                let envelope = seal_call(&self.keys, call, &action);
                let request_id = self.swarm.behaviour_mut().dm.send_request(&peer, envelope);
                dm_pending.insert(request_id, reply);
            }
            Command::SendMedia(to, call, frame) => {
                if let Some(peer) = peer_id_of(&to) {
                    let packet = MediaPacket {
                        call: call.as_bytes().to_vec(),
                        frame,
                    };
                    let _ = self.swarm.behaviour_mut().media.send_request(&peer, packet);
                }
            }
            Command::FetchChunk(from, root, index, reply) => {
                let Some(peer) = peer_id_of(&from) else {
                    let _ = reply.send(Err(TransportError::Other(
                        "peer identity is not a valid key".into(),
                    )));
                    return;
                };
                let request = BlobRequest {
                    root: root.as_bytes().to_vec(),
                    index,
                };
                let request_id = self.swarm.behaviour_mut().blob.send_request(&peer, request);
                blob_pending.insert(request_id, reply);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_event(
        &mut self,
        event: SwarmEvent<BehaviourEvent>,
        topics: &HashMap<gossipsub::TopicHash, ChannelId>,
        dm_pending: &mut DmPending,
        blob_pending: &mut BlobPending,
        kad_lookups: &mut KadLookups,
        handler: &Arc<dyn InboundHandler>,
        blobs: &Arc<dyn BlobStore>,
    ) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                message,
                ..
            })) => {
                let Some(channel) = topics.get(&message.topic).copied() else {
                    return;
                };
                let Ok(envelope) =
                    ciborium::from_reader::<SignedEnvelope, _>(message.data.as_slice())
                else {
                    return;
                };
                match open_envelope(&envelope) {
                    Ok((signal, verified)) => {
                        handler.on_channel_signal(channel, signal, verified).await;
                    }
                    Err(err) => {
                        tracing::debug!("dropping invalid channel envelope: {err}");
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dm(request_response::Event::Message {
                message,
                ..
            })) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let _ = self.swarm.behaviour_mut().dm.send_response(channel, DmAck);
                    match open_dm(&request) {
                        Ok((from, DmPayload::Chat(wire_message), verified)) => {
                            handler.on_dm(from, wire_message, verified).await;
                        }
                        Ok((from, DmPayload::Offer(manifest), verified)) => {
                            handler.on_file_offer(from, manifest, verified).await;
                        }
                        Ok((from, DmPayload::Call(call, action), verified)) => {
                            handler.on_call_signal(from, call, action, verified).await;
                        }
                        Err(err) => {
                            tracing::debug!("dropping invalid DM envelope: {err}");
                        }
                    }
                }
                request_response::Message::Response { request_id, .. } => {
                    if let Some(reply) = dm_pending.remove(&request_id) {
                        let _ = reply.send(Ok(()));
                    }
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::Dm(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                if let Some(reply) = dm_pending.remove(&request_id) {
                    let _ = reply.send(Err(TransportError::Other(error.to_string())));
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Blob(request_response::Event::Message {
                message,
                ..
            })) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let chunk = match request.root.as_slice().try_into() {
                        Ok(root_bytes) => {
                            let root = BlobHash::from_bytes(root_bytes);
                            blobs.get_chunk(root, request.index).await.unwrap_or(None)
                        }
                        Err(_) => None,
                    };
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .blob
                        .send_response(channel, BlobResponse { chunk });
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    if let Some(reply) = blob_pending.remove(&request_id) {
                        let _ = reply.send(response.chunk.ok_or_else(|| {
                            TransportError::Other("peer does not have that chunk".into())
                        }));
                    }
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::Blob(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                if let Some(reply) = blob_pending.remove(&request_id) {
                    let _ = reply.send(Err(TransportError::Other(error.to_string())));
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Media(request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Request {
                        request, channel, ..
                    },
                ..
            })) => {
                let _ = self
                    .swarm
                    .behaviour_mut()
                    .media
                    .send_response(channel, MediaAck);
                if let Ok(call_bytes) = <[u8; 16]>::try_from(request.call.as_slice()) {
                    // Identify the sender by their transport key — the
                    // PeerId is derived from the identity key, and frame
                    // authenticity is enforced by the per-call AEAD.
                    if let Some(from) = identity_of_peer(&peer) {
                        handler
                            .on_media_frame(from, CallId::from_bytes(call_bytes), request.frame)
                            .await;
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer, addr) in peers {
                    self.swarm.behaviour_mut().kad.add_address(&peer, addr);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                for addr in info.listen_addrs {
                    self.swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                id,
                result,
                ..
            })) => match result {
                kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(found))) => {
                    if let Some((_, reply)) = kad_lookups.remove(&id) {
                        let _ = reply.send(Ok(decode_record(&found.record.value)));
                    }
                }
                kad::QueryResult::GetRecord(Ok(
                    kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. },
                )) => {
                    if let Some((_, reply)) = kad_lookups.remove(&id) {
                        let _ = reply.send(Ok(None));
                    }
                }
                kad::QueryResult::GetRecord(Err(_)) => {
                    if let Some((_, reply)) = kad_lookups.remove(&id) {
                        let _ = reply.send(Ok(None));
                    }
                }
                _ => {}
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!("listening on {address}");
            }
            _ => {}
        }
    }
}
