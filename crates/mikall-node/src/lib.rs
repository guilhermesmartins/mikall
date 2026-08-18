//! The composition root. `NodeHandle` is the single object every frontend
//! (iced GUI, IRC gateway, the `mikalld` REPL, integration tests) drives —
//! all of them run against identical wiring by construction.

use std::path::PathBuf;
use std::sync::Arc;

use mikall_app::events::{AppEvent, EventBus};
use mikall_app::ports::{ChatTransport, Clock, Directory, IdGen, KeyStore, MessageStore};
use mikall_app::services::{
    CallService, ChatService, DmService, IdentityService, InboundRouter, PresenceService, Profile,
    TransferService,
};
use mikall_crypto::{CryptoIdGen, FileKeyStore, LocalKeys, SystemClock};
use mikall_net::{NetConfig, NetControl, NetStack};
use mikall_store::RedbStore;

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
    net_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
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

    /// Stop the network task and release the node's resources (key file,
    /// database lock). Required before re-opening the same data directory.
    pub fn shutdown(&self) {
        if let Ok(mut guard) = self.net_task.lock() {
            if let Some(task) = guard.take() {
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
    let store: Arc<dyn MessageStore> = Arc::new(
        RedbStore::open(&config.data_dir.join("messages.redb"))
            .map_err(|e| NodeError::Store(e.to_string()))?,
    );

    let net = NetStack::build(Arc::clone(&keys), config.net)?;
    let transport: Arc<dyn ChatTransport> = net.transport.clone();
    let directory: Arc<dyn Directory> = net.directory.clone();
    let control = net.control.clone();

    let bus = EventBus::new();
    let profile = Arc::new(Profile::new(keys.identity_id(), keys.fingerprint()));
    let identity = Arc::new(IdentityService::new(
        Arc::clone(&profile),
        Arc::clone(&clock),
        bus.clone(),
    ));
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
        bus.clone(),
    ));
    let transfer = Arc::new(TransferService::new(Arc::clone(&idgen), bus.clone()));

    let router = Arc::new(InboundRouter::new(
        Arc::clone(&chat),
        Arc::clone(&dm),
        Arc::clone(&presence),
    ));
    let net_task = net.start(router);

    Ok(NodeHandle {
        identity,
        chat,
        dm,
        presence,
        calls,
        transfer,
        bus,
        net: control,
        net_task: Arc::new(std::sync::Mutex::new(Some(net_task))),
    })
}
