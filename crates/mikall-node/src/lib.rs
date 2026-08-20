//! The composition root. `NodeHandle` is the single object every frontend
//! (iced GUI, IRC gateway, the `mikalld` REPL, integration tests) drives —
//! all of them run against identical wiring by construction.

use std::path::PathBuf;
use std::sync::Arc;

use mikall_app::events::{AppEvent, EventBus};
use mikall_app::ports::{
    BlobStore, CallSignaling, ChatTransport, ChunkHasher, Clock, Directory, FileTransport, IdGen,
    KeyStore, MediaStreamTransport, MediaTransport, MessageStore, ProfileStore,
};
use mikall_app::services::{
    CallService, ChatService, DmService, IdentityService, InboundRouter, PresenceService, Profile,
    TransferService,
};
use mikall_crypto::{Blake3ChunkHasher, CryptoIdGen, FileKeyStore, LocalKeys, SystemClock};
use mikall_net::{NetConfig, NetControl, NetStack};
use mikall_store::{FsBlobStore, RedbStore};

pub mod video;
pub mod voice;
pub use video::{ts90k_diff_ms, ts90k_now, RemoteVideoFrame};
pub use voice::MediaAlert;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    Crypto(#[from] mikall_crypto::CryptoError),
    #[error(transparent)]
    Net(#[from] mikall_net::NetError),
    #[error("store error: {0}")]
    Store(String),
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Data directory: identity key + message database live here.
    pub data_dir: PathBuf,
    pub net: NetConfig,
    /// Run the voice engine (real mic/speaker audio for active calls).
    /// Only effective on `hardware-audio` builds; integration tests turn
    /// it off so no test ever opens a device or races the pipelines they
    /// drive by hand.
    pub call_audio: bool,
    /// Run the video engine (real screen capture and decode for shares).
    /// Only effective on `hardware-video` builds; same test discipline as
    /// `call_audio`.
    pub call_video: bool,
}

impl NodeConfig {
    pub fn at(data_dir: PathBuf) -> Self {
        NodeConfig {
            data_dir,
            net: NetConfig::default(),
            call_audio: true,
            call_video: true,
        }
    }
}

/// Every use-case service plus the event stream — the entire driving surface
/// of the hexagon.
#[derive(Clone)]
pub struct NodeHandle {
    pub identity: Arc<IdentityService>,
    pub chat: Arc<ChatService>,
    pub dm: Arc<DmService>,
    pub presence: Arc<PresenceService>,
    pub calls: Arc<CallService>,
    pub transfer: Arc<TransferService>,
    pub bus: EventBus,
    pub net: NetControl,
    /// Sends sealed media frames to call peers (used by the media engine).
    pub media: Arc<dyn MediaTransport>,
    /// Opens unidirectional media streams to call peers (used by the
    /// video engine; voice migrates here later).
    pub media_streams: Arc<dyn MediaStreamTransport>,
    /// Device trouble from the media engines (mic denied, no speaker,
    /// screen recording refused) — honest, non-fatal, for frontends to
    /// surface. Only hardware builds ever send; subscribing is always
    /// safe.
    media_alerts: tokio::sync::broadcast::Sender<MediaAlert>,
    /// Decoded frames of remote screen shares. Only the `hardware-video`
    /// build ever sends; subscribing is always safe.
    video_frames: tokio::sync::broadcast::Sender<RemoteVideoFrame>,
    /// Background tasks owned by this node: the swarm loop, the boot
    /// redial of remembered peers and (with `hardware-audio`) the voice
    /// engine. All aborted on shutdown.
    tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle").finish_non_exhaustive()
    }
}

impl NodeHandle {
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<AppEvent> {
        self.bus.subscribe()
    }

    /// Media-device trouble (mic denied, no output device, screen
    /// recording refused). Empty forever on builds without the hardware
    /// features.
    pub fn subscribe_media_alerts(&self) -> tokio::sync::broadcast::Receiver<MediaAlert> {
        self.media_alerts.subscribe()
    }

    /// Decoded pictures of remote screen shares, ready to render. Empty
    /// forever on builds without `hardware-video`.
    pub fn subscribe_video_frames(&self) -> tokio::sync::broadcast::Receiver<RemoteVideoFrame> {
        self.video_frames.subscribe()
    }

    /// Stop the background tasks and release the node's resources (key
    /// file, database lock). Required before re-opening the same data
    /// directory.
    pub fn shutdown(&self) {
        if let Ok(mut guard) = self.tasks.lock() {
            for task in guard.drain(..) {
                task.abort();
            }
        }
    }
}

/// Build and start a full node.
pub async fn start(config: NodeConfig) -> Result<NodeHandle, NodeError> {
    let keys = Arc::new(LocalKeys::load_or_generate(
        &config.data_dir.join("identity.key"),
    )?);
    let keystore: Arc<dyn KeyStore> = Arc::new(FileKeyStore::new(Arc::clone(&keys)));
    let idgen: Arc<dyn IdGen> = Arc::new(CryptoIdGen);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    // One redb database backs both the message log and the profile record.
    let redb = Arc::new(
        RedbStore::open(&config.data_dir.join("messages.redb"))
            .map_err(|e| NodeError::Store(e.to_string()))?,
    );
    let store: Arc<dyn MessageStore> = Arc::clone(&redb) as Arc<dyn MessageStore>;
    let profile_store: Arc<dyn ProfileStore> = redb;

    let blobs: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::new(config.data_dir.join("blobs"))
            .map_err(|e| NodeError::Store(e.to_string()))?,
    );
    let hasher: Arc<dyn ChunkHasher> = Arc::new(Blake3ChunkHasher);

    let net = NetStack::build(Arc::clone(&keys), config.net)?;
    let transport: Arc<dyn ChatTransport> = net.transport.clone();
    let files: Arc<dyn FileTransport> = net.files.clone();
    let call_signaling: Arc<dyn CallSignaling> = net.call_signaling.clone();
    let media: Arc<dyn MediaTransport> = net.media.clone();
    let media_streams: Arc<dyn MediaStreamTransport> = net.media_streams.clone();
    let directory: Arc<dyn Directory> = net.directory.clone();
    let control = net.control.clone();

    let bus = EventBus::new();
    let profile = Arc::new(Profile::new(keys.identity_id(), keys.fingerprint()));
    let identity = Arc::new(IdentityService::new(
        Arc::clone(&profile),
        profile_store,
        Arc::clone(&clock),
        bus.clone(),
    ));
    // Rehydrate the persisted profile (nickname) so a restarted node keeps
    // its name and the GUI skips onboarding.
    identity
        .hydrate()
        .await
        .map_err(|e| NodeError::Store(e.to_string()))?;
    let chat = Arc::new(ChatService::new(
        Arc::clone(&identity),
        Arc::clone(&transport),
        directory,
        store,
        Arc::clone(&keystore),
        Arc::clone(&idgen),
        Arc::clone(&clock),
        bus.clone(),
    ));
    let dm = Arc::new(DmService::new(
        Arc::clone(&identity),
        Arc::clone(&transport),
        Arc::clone(&keystore),
        Arc::clone(&idgen),
        Arc::clone(&clock),
        bus.clone(),
    ));
    let presence = Arc::new(PresenceService::new(
        Arc::clone(&identity),
        Arc::clone(&chat),
        Arc::clone(&transport),
        bus.clone(),
    ));
    let calls = Arc::new(CallService::new(
        Arc::clone(&identity),
        Arc::clone(&idgen),
        call_signaling,
        bus.clone(),
    ));
    let transfer = Arc::new(TransferService::new(
        Arc::clone(&idgen),
        bus.clone(),
        files,
        Arc::clone(&blobs),
        hasher,
        config.data_dir.join("downloads"),
    ));

    let router = Arc::new(InboundRouter::new(
        Arc::clone(&chat),
        Arc::clone(&dm),
        Arc::clone(&presence),
        Arc::clone(&transfer),
        Arc::clone(&calls),
    ));
    let net_tasks = net.start(router, blobs);
    // Redial remembered peers in the background: with mDNS often blocked
    // (macOS local-network permission), this is what makes connecting
    // automatic from the second boot on — for every frontend, with zero
    // UI involvement. Failures are quiet and the task ends on its own.
    let redial_task = tokio::spawn(redial_remembered_peers(
        Arc::clone(&identity),
        control.clone(),
    ));

    let (media_alerts, _) = tokio::sync::broadcast::channel(16);
    let (video_frames, _) = tokio::sync::broadcast::channel::<RemoteVideoFrame>(8);
    #[allow(unused_mut)]
    let mut tasks = net_tasks;
    tasks.push(redial_task);
    // Real call audio (mic → Opus → sealed frames, and back out the
    // speakers) for every frontend of this node — the GUI enables the
    // feature by default; `--no-default-features` builds stay device-free.
    #[cfg(feature = "hardware-audio")]
    if config.call_audio {
        tasks.push(voice::spawn_voice_engine(
            Arc::clone(&identity),
            Arc::clone(&calls),
            Arc::clone(&media),
            bus.clone(),
            media_alerts.clone(),
        ));
    }
    // Real screen-share video (capture → H.264 → sealed frames over
    // per-viewer streams, and back into the frame feed) — same feature
    // discipline as audio.
    #[cfg(feature = "hardware-video")]
    if config.call_video {
        tasks.push(video::spawn_video_engine(
            Arc::clone(&identity),
            Arc::clone(&calls),
            Arc::clone(&media_streams),
            bus.clone(),
            media_alerts.clone(),
            video_frames.clone(),
        ));
    }

    Ok(NodeHandle {
        identity,
        chat,
        dm,
        presence,
        calls,
        transfer,
        bus,
        net: control,
        media,
        media_streams,
        media_alerts,
        video_frames,
        tasks: Arc::new(std::sync::Mutex::new(tasks)),
    })
}

/// Dial every remembered peer with a short, bounded retry: three rounds, a
/// few seconds apart, then the task ends — a dead peer must never spam the
/// log forever. [`NetControl::dial`] only promises "dial initiated", so
/// each round after the first consults [`NetControl::connected_peers`] and
/// drops targets that made it; addresses without a `/p2p/` suffix cannot
/// be correlated with a connection and get a single round. All outcomes
/// are non-fatal; per-dial chatter stays at debug with one info summary.
async fn redial_remembered_peers(identity: Arc<IdentityService>, net: NetControl) {
    use libp2p::multiaddr::Protocol;
    use libp2p::{Multiaddr, PeerId};

    const ROUNDS: u32 = 3;
    const BASE_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

    let remembered = identity.known_peers().await;
    if remembered.is_empty() {
        return;
    }

    let mut targets: Vec<(Multiaddr, Option<PeerId>)> = Vec::new();
    for addr in &remembered {
        match addr.as_str().parse::<Multiaddr>() {
            Ok(multiaddr) => {
                // The *last* `/p2p/` component is the target peer: a
                // relayed `/…/p2p/RELAY/p2p-circuit/p2p/PEER` address
                // names the relay first, and we must not credit a mere
                // relay connection as "reconnected to the peer".
                let peer = multiaddr
                    .iter()
                    .filter_map(|p| match p {
                        Protocol::P2p(peer) => Some(peer),
                        _ => None,
                    })
                    .last();
                targets.push((multiaddr, peer));
            }
            Err(error) => {
                tracing::debug!(addr = addr.as_str(), %error, "skipping unparseable remembered peer");
            }
        }
    }
    let wanted: Vec<PeerId> = targets.iter().filter_map(|(_, peer)| *peer).collect();

    for round in 1..=ROUNDS {
        if round > 1 {
            tokio::time::sleep(BASE_DELAY * 2u32.pow(round - 2)).await;
            let connected = net.connected_peers().await;
            targets.retain(|(_, peer)| peer.is_some_and(|p| !connected.contains(&p)));
        }
        if targets.is_empty() {
            break;
        }
        for (addr, _) in &targets {
            match net.dial(addr.clone()).await {
                Ok(()) => tracing::debug!(%addr, round, "redialing remembered peer"),
                Err(error) => {
                    tracing::debug!(%addr, round, %error, "remembered peer dial failed");
                }
            }
        }
    }

    // One summary line once the dust settles, then the task is done.
    tokio::time::sleep(BASE_DELAY).await;
    let connected = net.connected_peers().await;
    let reconnected = wanted.iter().filter(|p| connected.contains(p)).count();
    tracing::info!(
        reconnected,
        remembered = remembered.len(),
        "boot redial of remembered peers finished"
    );
}
