//! In-memory adapters (fakes) and a deterministic multi-node universe for
//! driving the application layer in BDD scenarios — no sockets, no disk, no
//! crypto, and a **seeded, deterministic delivery order** so ordering
//! scenarios never flake.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, Mutex, RwLock};

use mikall_app::events::{AppEvent, EventBus};
use mikall_app::ports::{
    ChannelRecord, ChannelSignal, ChatTransport, Clock, Directory, DirectoryError, IdGen,
    InboundHandler, KeyStore, KeyStoreError, MessageStore, StoreError, TransportError, WireMessage,
};
use mikall_app::services::{
    CallService, ChatService, DmService, IdentityService, InboundRouter, PresenceService, Profile,
    TransferService,
};
use mikall_domain::calls::CallId;
use mikall_domain::messaging::{ChannelId, ChannelName, MessageId, Verified};
use mikall_domain::shared::{Fingerprint, IdentityId};
use mikall_domain::transfer::TransferId;

fn hash64(parts: &[&[u8]]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for part in parts {
        part.hash(&mut hasher);
    }
    hasher.finish()
}

fn spread(seed: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut state = seed | 1;
    for chunk in out.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    out
}

/// Deterministic id generation for tests.
#[derive(Debug, Default)]
pub struct TestIdGen {
    counter: AtomicU64,
}

impl IdGen for TestIdGen {
    fn message_id(
        &self,
        author: &IdentityId,
        lamport: u64,
        parents: &[MessageId],
        body: &str,
    ) -> MessageId {
        let mut parts: Vec<&[u8]> = vec![author.as_bytes(), body.as_bytes()];
        let lamport_bytes = lamport.to_le_bytes();
        parts.push(&lamport_bytes);
        let parent_bytes: Vec<[u8; 32]> = parents.iter().map(|p| *p.as_bytes()).collect();
        for p in &parent_bytes {
            parts.push(p);
        }
        MessageId::from_bytes(spread(hash64(&parts)))
    }

    fn channel_id(&self, name: &ChannelName) -> ChannelId {
        ChannelId::from_bytes(spread(hash64(&[b"chan", name.as_str().as_bytes()])))
    }

    fn call_id(&self) -> CallId {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        CallId::from_bytes(bytes)
    }

    fn transfer_id(&self) -> TransferId {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        bytes[8] = 0x54; // tag transfer ids apart from call ids
        TransferId::from_bytes(bytes)
    }
}

/// Monotonic fake clock.
#[derive(Debug, Default)]
pub struct TestClock {
    now: AtomicU64,
}

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.now.fetch_add(1, Ordering::Relaxed) + 1_700_000_000_000
    }
}

/// Fake keystore: fingerprints derive deterministically from identity bytes.
#[derive(Debug)]
pub struct FakeKeyStore {
    id: IdentityId,
}

impl FakeKeyStore {
    pub fn new(id: IdentityId) -> Self {
        FakeKeyStore { id }
    }

    pub fn derive_fingerprint(id: &IdentityId) -> Fingerprint {
        Fingerprint::from_bytes(spread(hash64(&[b"fp", id.as_bytes()])))
    }
}

#[async_trait]
impl KeyStore for FakeKeyStore {
    async fn local_identity(&self) -> Result<(IdentityId, Fingerprint), KeyStoreError> {
        Ok((self.id, Self::derive_fingerprint(&self.id)))
    }

    fn fingerprint_of(&self, id: &IdentityId) -> Fingerprint {
        Self::derive_fingerprint(id)
    }
}

/// Shared in-memory channel directory (the DHT stand-in).
#[derive(Debug, Default)]
pub struct InMemoryDirectory {
    records: RwLock<BTreeMap<ChannelName, ChannelRecord>>,
}

#[async_trait]
impl Directory for InMemoryDirectory {
    async fn announce_channel(&self, record: ChannelRecord) -> Result<(), DirectoryError> {
        // First announce wins, like a first-founder-wins DHT record.
        self.records
            .write()
            .await
            .entry(record.name.clone())
            .or_insert(record);
        Ok(())
    }

    async fn lookup_channel(
        &self,
        name: &ChannelName,
    ) -> Result<Option<ChannelRecord>, DirectoryError> {
        Ok(self.records.read().await.get(name).cloned())
    }

    async fn list_channels(&self) -> Result<Vec<ChannelRecord>, DirectoryError> {
        Ok(self.records.read().await.values().cloned().collect())
    }
}

/// Per-node in-memory message store.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    messages: RwLock<BTreeMap<ChannelId, Vec<WireMessage>>>,
}

#[async_trait]
impl MessageStore for InMemoryStore {
    async fn save_channel_message(
        &self,
        channel: ChannelId,
        message: &WireMessage,
    ) -> Result<(), StoreError> {
        self.messages
            .write()
            .await
            .entry(channel)
            .or_default()
            .push(message.clone());
        Ok(())
    }

    async fn load_channel_messages(
        &self,
        channel: ChannelId,
    ) -> Result<Vec<WireMessage>, StoreError> {
        Ok(self
            .messages
            .read()
            .await
            .get(&channel)
            .cloned()
            .unwrap_or_default())
    }
}

struct Endpoint {
    handler: Arc<dyn InboundHandler>,
    subscriptions: BTreeSet<ChannelId>,
}

enum Held {
    Channel {
        to: IdentityId,
        channel: ChannelId,
        signal: ChannelSignal,
    },
    Dm {
        to: IdentityId,
        from: IdentityId,
        message: WireMessage,
    },
}

/// The in-memory bus connecting every node in a scenario. Delivery is
/// synchronous and iterates nodes in `IdentityId` order — deterministic by
/// construction. Links can be "delayed": traffic is held in FIFO order until
/// flushed, which is how scenarios manufacture races and history gaps.
#[derive(Default)]
pub struct InMemoryNetwork {
    endpoints: RwLock<BTreeMap<IdentityId, Endpoint>>,
    delayed_links: RwLock<BTreeSet<(IdentityId, IdentityId)>>,
    held: Mutex<VecDeque<Held>>,
}

impl std::fmt::Debug for InMemoryNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryNetwork").finish_non_exhaustive()
    }
}

impl InMemoryNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn register(&self, id: IdentityId, handler: Arc<dyn InboundHandler>) {
        self.endpoints.write().await.insert(
            id,
            Endpoint {
                handler,
                subscriptions: BTreeSet::new(),
            },
        );
    }

    pub async fn delay_link(&self, a: IdentityId, b: IdentityId) {
        let mut links = self.delayed_links.write().await;
        links.insert((a, b));
        links.insert((b, a));
    }

    pub async fn restore_link(&self, a: IdentityId, b: IdentityId) {
        let mut links = self.delayed_links.write().await;
        links.remove(&(a, b));
        links.remove(&(b, a));
    }

    /// Deliver everything held, in the order it was sent.
    pub async fn flush_held(&self) {
        loop {
            let next = self.held.lock().await.pop_front();
            let Some(item) = next else { break };
            match item {
                Held::Channel {
                    to,
                    channel,
                    signal,
                } => {
                    let handler = {
                        let endpoints = self.endpoints.read().await;
                        endpoints.get(&to).map(|e| Arc::clone(&e.handler))
                    };
                    if let Some(handler) = handler {
                        handler
                            .on_channel_signal(
                                channel,
                                signal,
                                Verified::attest_signature_checked(),
                            )
                            .await;
                    }
                }
                Held::Dm { to, from, message } => {
                    let handler = {
                        let endpoints = self.endpoints.read().await;
                        endpoints.get(&to).map(|e| Arc::clone(&e.handler))
                    };
                    if let Some(handler) = handler {
                        handler
                            .on_dm(from, message, Verified::attest_signature_checked())
                            .await;
                    }
                }
            }
        }
    }

    async fn is_delayed(&self, from: IdentityId, to: IdentityId) -> bool {
        self.delayed_links.read().await.contains(&(from, to))
    }

    async fn publish_from(&self, from: IdentityId, channel: ChannelId, signal: ChannelSignal) {
        let recipients: Vec<(IdentityId, Arc<dyn InboundHandler>)> = {
            let endpoints = self.endpoints.read().await;
            endpoints
                .iter()
                .filter(|(id, ep)| **id != from && ep.subscriptions.contains(&channel))
                .map(|(id, ep)| (*id, Arc::clone(&ep.handler)))
                .collect()
        };
        for (to, handler) in recipients {
            if self.is_delayed(from, to).await {
                self.held.lock().await.push_back(Held::Channel {
                    to,
                    channel,
                    signal: signal.clone(),
                });
            } else {
                handler
                    .on_channel_signal(
                        channel,
                        signal.clone(),
                        Verified::attest_signature_checked(),
                    )
                    .await;
            }
        }
    }

    async fn dm_from(
        &self,
        from: IdentityId,
        to: IdentityId,
        message: WireMessage,
    ) -> Result<(), TransportError> {
        if self.is_delayed(from, to).await {
            self.held
                .lock()
                .await
                .push_back(Held::Dm { to, from, message });
            return Ok(());
        }
        let handler = {
            let endpoints = self.endpoints.read().await;
            endpoints.get(&to).map(|e| Arc::clone(&e.handler))
        };
        match handler {
            Some(handler) => {
                handler
                    .on_dm(from, message, Verified::attest_signature_checked())
                    .await;
                Ok(())
            }
            None => Err(TransportError::Unreachable),
        }
    }
}

/// One node's view of the bus.
#[derive(Debug)]
pub struct NodeTransport {
    network: Arc<InMemoryNetwork>,
    id: IdentityId,
}

impl NodeTransport {
    pub fn new(network: Arc<InMemoryNetwork>, id: IdentityId) -> Self {
        NodeTransport { network, id }
    }
}

#[async_trait]
impl ChatTransport for NodeTransport {
    async fn subscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError> {
        let mut endpoints = self.network.endpoints.write().await;
        if let Some(ep) = endpoints.get_mut(&self.id) {
            ep.subscriptions.insert(channel);
        }
        Ok(())
    }

    async fn unsubscribe_channel(&self, channel: ChannelId) -> Result<(), TransportError> {
        let mut endpoints = self.network.endpoints.write().await;
        if let Some(ep) = endpoints.get_mut(&self.id) {
            ep.subscriptions.remove(&channel);
        }
        Ok(())
    }

    async fn publish(
        &self,
        channel: ChannelId,
        signal: ChannelSignal,
    ) -> Result<(), TransportError> {
        self.network.publish_from(self.id, channel, signal).await;
        Ok(())
    }

    async fn send_dm(&self, to: IdentityId, message: WireMessage) -> Result<(), TransportError> {
        self.network.dm_from(self.id, to, message).await
    }
}

/// A fully wired node: every service over in-memory adapters.
pub struct TestNode {
    pub id: IdentityId,
    pub identity: Arc<IdentityService>,
    pub chat: Arc<ChatService>,
    pub dm: Arc<DmService>,
    pub presence: Arc<PresenceService>,
    pub calls: Arc<CallService>,
    pub transfer: Arc<TransferService>,
    pub events: broadcast::Receiver<AppEvent>,
    pub collected: Vec<AppEvent>,
}

impl std::fmt::Debug for TestNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestNode").field("id", &self.id).finish()
    }
}

impl TestNode {
    /// Drain all events emitted since the last drain into `collected`.
    pub fn drain_events(&mut self) -> &[AppEvent] {
        while let Ok(event) = self.events.try_recv() {
            self.collected.push(event);
        }
        &self.collected
    }
}

/// Build a node whose identity derives deterministically from `seed_name`,
/// register it on the network, and set its nickname.
pub async fn spawn_node(
    network: &Arc<InMemoryNetwork>,
    directory: &Arc<InMemoryDirectory>,
    seed_name: &str,
) -> TestNode {
    let id = IdentityId::from_bytes(spread(hash64(&[b"id", seed_name.as_bytes()])));
    let keystore: Arc<dyn KeyStore> = Arc::new(FakeKeyStore::new(id));
    let idgen: Arc<dyn IdGen> = Arc::new(TestIdGen::default());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::default());
    let bus = EventBus::new();
    let events = bus.subscribe();

    let profile = Arc::new(Profile::new(id, FakeKeyStore::derive_fingerprint(&id)));
    let identity = Arc::new(IdentityService::new(
        Arc::clone(&profile),
        Arc::clone(&clock),
        bus.clone(),
    ));
    if let Ok(nick) = mikall_domain::messaging::Nickname::parse(seed_name) {
        identity.set_nickname(nick).await;
    }

    let transport: Arc<dyn ChatTransport> = Arc::new(NodeTransport::new(Arc::clone(network), id));
    let store: Arc<dyn MessageStore> = Arc::new(InMemoryStore::default());
    let dir: Arc<dyn Directory> = Arc::clone(directory) as Arc<dyn Directory>;

    let chat = Arc::new(ChatService::new(
        Arc::clone(&identity),
        Arc::clone(&transport),
        dir,
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
    network.register(id, router).await;

    TestNode {
        id,
        identity,
        chat,
        dm,
        presence,
        calls,
        transfer,
        events,
        collected: Vec::new(),
    }
}
