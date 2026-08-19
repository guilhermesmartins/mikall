//! The composition root. `NodeHandle` is the single object every frontend
//! (iced GUI, IRC gateway, the `mikalld` REPL, integration tests) drives —
//! all of them run against identical wiring by construction.

use std::path::PathBuf;
use std::sync::Arc;

use mikall_app::events::{AppEvent, EventBus};
use mikall_app::ports::{
    BlobStore, CallSignaling, ChatTransport, ChunkHasher, Clock, Directory, FileTransport, IdGen,
    KeyStore, MediaTransport, MessageStore, ProfileStore,
};
use mikall_app::services::{
    CallService, ChatService, DmService, IdentityService, InboundRouter, PresenceService, Profile,
    TransferService,
};
use mikall_crypto::{Blake3ChunkHasher, CryptoIdGen, FileKeyStore, LocalKeys, SystemClock};
use mikall_net::{NetConfig, NetControl, NetStack};
use mikall_store::{FsBlobStore, RedbStore};

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
}

impl NodeConfig {
    pub fn at(data_dir: PathBuf) -> Self {
        NodeConfig {
            data_dir,
            net: NetConfig::default(),
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
    /// Background tasks owned by this node: the swarm loop and the boot
    /// redial of remembered peers. All aborted on shutdown.
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
    let net_task = net.start(router, blobs);
    // Redial remembered peers in the background: with mDNS often blocked
    // (macOS local-network permission), this is what makes connecting
    // automatic from the second boot on — for every frontend, with zero
    // UI involvement. Failures are quiet and the task ends on its own.
    let redial_task = tokio::spawn(redial_remembered_peers(
        Arc::clone(&identity),
        control.clone(),
    ));

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
        tasks: Arc::new(std::sync::Mutex::new(vec![net_task, redial_task])),
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
                let peer = multiaddr.iter().find_map(|p| match p {
                    Protocol::P2p(peer) => Some(peer),
                    _ => None,
                });
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
