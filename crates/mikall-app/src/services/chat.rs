//! Channel chat use cases.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::messaging::{
    Channel, ChannelError, ChannelId, ChannelName, ChannelVisibility, Message, MessageBody,
    MessageBodyError, MessageId, MessagingEvent, Nickname, Role, Topic, TopicError, Verified,
};
use mikall_domain::shared::IdentityId;

use crate::events::{AppEvent, EventBus};
use crate::ports::{
    ChannelRecord, ChannelSignal, ChatTransport, Clock, Directory, DirectoryError, IdGen, KeyStore,
    MessageStore, StoreError, TransportError, WireMessage,
};

use super::identity::IdentityService;

/// A message ready for display by any frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedMessage {
    pub id: MessageId,
    pub author: IdentityId,
    pub author_nick: Option<Nickname>,
    pub body: MessageBody,
    pub ts_hint_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    pub id: IdentityId,
    pub role: Role,
    pub nick: Option<Nickname>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChatError {
    #[error("not joined to {0}")]
    NotJoined(String),
    #[error(transparent)]
    Body(#[from] MessageBodyError),
    #[error(transparent)]
    Topic(#[from] TopicError),
    #[error(transparent)]
    Channel(#[from] ChannelError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Directory(#[from] DirectoryError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct ChatService {
    identity: Arc<IdentityService>,
    transport: Arc<dyn ChatTransport>,
    directory: Arc<dyn Directory>,
    store: Arc<dyn MessageStore>,
    keystore: Arc<dyn KeyStore>,
    idgen: Arc<dyn IdGen>,
    clock: Arc<dyn Clock>,
    bus: EventBus,
    channels: Arc<RwLock<BTreeMap<ChannelId, Channel>>>,
    by_name: Arc<RwLock<BTreeMap<ChannelName, ChannelId>>>,
}

impl std::fmt::Debug for ChatService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatService").finish_non_exhaustive()
    }
}

impl ChatService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: Arc<IdentityService>,
        transport: Arc<dyn ChatTransport>,
        directory: Arc<dyn Directory>,
        store: Arc<dyn MessageStore>,
        keystore: Arc<dyn KeyStore>,
        idgen: Arc<dyn IdGen>,
        clock: Arc<dyn Clock>,
        bus: EventBus,
    ) -> Self {
        ChatService {
            identity,
            transport,
            directory,
            store,
            keystore,
            idgen,
            clock,
            bus,
            channels: Arc::new(RwLock::new(BTreeMap::new())),
            by_name: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Join a channel, creating it (as founder) if the directory has no
    /// record of the name. This is the `JOIN #name` use case.
    pub async fn join_channel(&self, name: ChannelName) -> Result<ChannelId, ChatError> {
        let me = self.identity.local_id();
        if let Some(id) = self.by_name.read().await.get(&name).copied() {
            return Ok(id); // already joined — idempotent like IRC JOIN
        }

        let record = self.directory.lookup_channel(&name).await?;
        let (channel_id, mut channel, events) = match record {
            Some(record) => {
                let (mut channel, mut events) = Channel::create(
                    record.id,
                    name.clone(),
                    record.founder,
                    ChannelVisibility::Public,
                );
                if record.founder != me {
                    events.push(channel.join(me)?);
                }
                (record.id, channel, events)
            }
            None => {
                let id = self.idgen.channel_id(&name);
                let (channel, events) =
                    Channel::create(id, name.clone(), me, ChannelVisibility::Public);
                self.directory
                    .announce_channel(ChannelRecord {
                        id,
                        name: name.clone(),
                        founder: me,
                    })
                    .await?;
                (id, channel, events)
            }
        };

        // Rehydrate any locally persisted history before going live.
        for wire in self.store.load_channel_messages(channel_id).await? {
            if !channel.is_member(&wire.author) {
                let _ = channel.join(wire.author);
            }
            if let Ok(body) = MessageBody::parse(&wire.body) {
                let message = Message::received(
                    Verified::attest_signature_checked(),
                    wire.id,
                    wire.author,
                    wire.lamport,
                    wire.parents,
                    body,
                    wire.ts_hint_ms,
                );
                let _ = channel.record_message(message, false);
            }
        }

        self.channels.write().await.insert(channel_id, channel);
        self.by_name.write().await.insert(name, channel_id);

        self.transport.subscribe_channel(channel_id).await?;
        let nickname = self
            .identity
            .nickname()
            .await
            .map(|n| n.as_str().to_owned());
        self.transport
            .publish(channel_id, ChannelSignal::Joined { who: me, nickname })
            .await?;
        for event in events {
            self.bus.publish_domain(event);
        }
        Ok(channel_id)
    }

    /// The `PART #name` use case. The founder detaches (unsubscribes)
    /// without giving up the founder seat — the founder role is immovable.
    pub async fn leave_channel(&self, name: &ChannelName) -> Result<(), ChatError> {
        let me = self.identity.local_id();
        let channel_id = self
            .by_name
            .write()
            .await
            .remove(name)
            .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;

        let mut channels = self.channels.write().await;
        if let Some(channel) = channels.get_mut(&channel_id) {
            match channel.role_of(&me) {
                Some(Role::Founder) | None => {}
                Some(Role::Op) | Some(Role::Member) => {
                    let event = channel.leave(me)?;
                    self.bus.publish_domain(event);
                }
            }
        }
        channels.remove(&channel_id);
        drop(channels);

        self.transport
            .publish(channel_id, ChannelSignal::Left { who: me })
            .await?;
        self.transport.unsubscribe_channel(channel_id).await?;
        self.bus.publish_domain(MessagingEvent::ChannelLeft {
            channel: channel_id,
            who: me,
        });
        Ok(())
    }

    /// The `PRIVMSG #name` use case.
    pub async fn post_message(
        &self,
        name: &ChannelName,
        raw_body: &str,
    ) -> Result<MessageId, ChatError> {
        let body = MessageBody::parse(raw_body)?;
        let me = self.identity.local_id();
        let channel_id = self.channel_id_of(name).await?;

        let (wire, events) = {
            let mut channels = self.channels.write().await;
            let channel = channels
                .get_mut(&channel_id)
                .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
            let (parents, lamport) = channel.next_message_position();
            let id = self.idgen.message_id(&me, lamport, &parents, body.as_str());
            let ts = self.clock.now_ms();
            let message = Message::authored(id, me, lamport, parents.clone(), body.clone(), ts);
            let events = channel.record_message(message, true)?;
            let wire = WireMessage {
                id,
                author: me,
                lamport,
                parents,
                body: body.as_str().to_owned(),
                ts_hint_ms: ts,
            };
            (wire, events)
        };

        self.store.save_channel_message(channel_id, &wire).await?;
        self.transport
            .publish(channel_id, ChannelSignal::Message(wire.clone()))
            .await?;

        for event in events {
            self.bus.publish_domain(event);
        }
        self.bus.publish(AppEvent::ChannelMessage {
            channel: name.clone(),
            author: me,
            author_nick: self.identity.nickname().await,
            message: wire.id,
            body,
            ts_hint_ms: wire.ts_hint_ms,
        });
        Ok(wire.id)
    }

    /// The `TOPIC #name :text` use case.
    pub async fn set_topic(&self, name: &ChannelName, raw_topic: &str) -> Result<(), ChatError> {
        let topic = Topic::parse(raw_topic)?;
        let me = self.identity.local_id();
        let channel_id = self.channel_id_of(name).await?;
        let event = {
            let mut channels = self.channels.write().await;
            let channel = channels
                .get_mut(&channel_id)
                .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
            channel.set_topic(me, topic.clone())?
        };
        self.transport
            .publish(
                channel_id,
                ChannelSignal::TopicChanged {
                    by: me,
                    topic: topic.as_str().to_owned(),
                },
            )
            .await?;
        self.bus.publish_domain(event);
        Ok(())
    }

    /// Grant/revoke op (the `MODE #c +o/-o` use case; founder only).
    pub async fn set_role(
        &self,
        name: &ChannelName,
        target: IdentityId,
        role: Role,
    ) -> Result<(), ChatError> {
        let me = self.identity.local_id();
        let channel_id = self.channel_id_of(name).await?;
        let event = {
            let mut channels = self.channels.write().await;
            let channel = channels
                .get_mut(&channel_id)
                .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
            channel.set_role(me, target, role)?
        };
        self.transport
            .publish(
                channel_id,
                ChannelSignal::RoleChanged {
                    by: me,
                    target,
                    op: matches!(role, Role::Op),
                },
            )
            .await?;
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn topic(&self, name: &ChannelName) -> Result<Topic, ChatError> {
        let channel_id = self.channel_id_of(name).await?;
        let channels = self.channels.read().await;
        let channel = channels
            .get(&channel_id)
            .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
        Ok(channel.topic().clone())
    }

    /// Deterministically ordered history.
    pub async fn history(&self, name: &ChannelName) -> Result<Vec<RenderedMessage>, ChatError> {
        let channel_id = self.channel_id_of(name).await?;
        let raw: Vec<(MessageId, IdentityId, MessageBody, u64)> = {
            let channels = self.channels.read().await;
            let channel = channels
                .get(&channel_id)
                .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
            channel
                .history()
                .into_iter()
                .map(|m| (m.id(), m.author(), m.body().clone(), m.ts_hint_ms()))
                .collect()
        };
        let mut out = Vec::with_capacity(raw.len());
        for (id, author, body, ts) in raw {
            out.push(RenderedMessage {
                id,
                author,
                author_nick: self.identity.nickname_of(&author).await,
                body,
                ts_hint_ms: ts,
            });
        }
        Ok(out)
    }

    pub async fn members(&self, name: &ChannelName) -> Result<Vec<MemberView>, ChatError> {
        let channel_id = self.channel_id_of(name).await?;
        let raw: Vec<(IdentityId, Role)> = {
            let channels = self.channels.read().await;
            let channel = channels
                .get(&channel_id)
                .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))?;
            channel.members().collect()
        };
        let mut out = Vec::with_capacity(raw.len());
        for (id, role) in raw {
            out.push(MemberView {
                id,
                role,
                nick: self.identity.nickname_of(&id).await,
            });
        }
        Ok(out)
    }

    pub async fn joined_channels(&self) -> Vec<ChannelName> {
        self.by_name.read().await.keys().cloned().collect()
    }

    /// The `LIST` use case: every channel the directory knows.
    pub async fn list_channels(&self) -> Result<Vec<ChannelRecord>, ChatError> {
        Ok(self.directory.list_channels().await?)
    }

    pub(crate) async fn channel_id_of(&self, name: &ChannelName) -> Result<ChannelId, ChatError> {
        self.by_name
            .read()
            .await
            .get(name)
            .copied()
            .ok_or_else(|| ChatError::NotJoined(name.as_str().to_owned()))
    }

    /// Reverse lookup of a joined channel's name.
    pub async fn name_of(&self, channel: ChannelId) -> Option<ChannelName> {
        self.by_name
            .read()
            .await
            .iter()
            .find(|(_, id)| **id == channel)
            .map(|(name, _)| name.clone())
    }

    /// Inbound path, called by the transport router with a verification
    /// attestation. Unknown channels are ignored (we are not subscribed).
    pub async fn receive_signal(
        &self,
        channel_id: ChannelId,
        signal: ChannelSignal,
        verified: Verified,
    ) {
        let Some(name) = self.name_of(channel_id).await else {
            return;
        };
        match signal {
            ChannelSignal::Message(wire) => {
                self.receive_message(channel_id, &name, wire, verified)
                    .await;
            }
            ChannelSignal::Joined { who, nickname } => {
                let nick = nickname.and_then(|n| Nickname::parse(&n).ok());
                self.identity
                    .observe_peer(who, self.keystore.fingerprint_of(&who), nick)
                    .await;
                let mut channels = self.channels.write().await;
                if let Some(channel) = channels.get_mut(&channel_id) {
                    if let Ok(event) = channel.join(who) {
                        self.bus.publish_domain(event);
                    }
                }
            }
            ChannelSignal::Left { who } => {
                let mut channels = self.channels.write().await;
                if let Some(channel) = channels.get_mut(&channel_id) {
                    if let Ok(event) = channel.leave(who) {
                        self.bus.publish_domain(event);
                    }
                }
            }
            ChannelSignal::TopicChanged { by, topic } => {
                let Ok(topic) = Topic::parse(&topic) else {
                    return;
                };
                let mut channels = self.channels.write().await;
                if let Some(channel) = channels.get_mut(&channel_id) {
                    // Role check happens inside the aggregate: non-ops are
                    // rejected and their topic change is simply ignored.
                    if let Ok(event) = channel.set_topic(by, topic) {
                        self.bus.publish_domain(event);
                    }
                }
            }
            ChannelSignal::RoleChanged { by, target, op } => {
                let mut channels = self.channels.write().await;
                if let Some(channel) = channels.get_mut(&channel_id) {
                    // The aggregate re-validates: only the founder's grants
                    // take effect; forged grants are simply ignored.
                    let role = if op { Role::Op } else { Role::Member };
                    if let Ok(event) = channel.set_role(by, target, role) {
                        self.bus.publish_domain(event);
                    }
                }
            }
            ChannelSignal::Presence { .. } => {
                // Handled by PresenceService via the router.
            }
        }
    }

    async fn receive_message(
        &self,
        channel_id: ChannelId,
        name: &ChannelName,
        wire: WireMessage,
        verified: Verified,
    ) {
        if self.identity.is_blocked(&wire.author).await {
            return;
        }
        let Ok(body) = MessageBody::parse(&wire.body) else {
            return; // malformed bodies never enter the domain
        };
        self.identity
            .observe_peer(
                wire.author,
                self.keystore.fingerprint_of(&wire.author),
                None,
            )
            .await;

        let events = {
            let mut channels = self.channels.write().await;
            let Some(channel) = channels.get_mut(&channel_id) else {
                return;
            };
            // Gossip may deliver a message before the join signal.
            if !channel.is_member(&wire.author) {
                if let Ok(event) = channel.join(wire.author) {
                    self.bus.publish_domain(event);
                }
            }
            let message = Message::received(
                verified,
                wire.id,
                wire.author,
                wire.lamport,
                wire.parents.clone(),
                body.clone(),
                wire.ts_hint_ms,
            );
            match channel.record_message(message, false) {
                Ok(events) => events,
                Err(_) => return,
            }
        };

        let mut applied = false;
        for event in &events {
            if matches!(
                event,
                MessagingEvent::MessagePosted { .. } | MessagingEvent::MessageReceived { .. }
            ) {
                applied = true;
            }
        }
        if applied {
            let _ = self.store.save_channel_message(channel_id, &wire).await;
            self.bus.publish(AppEvent::ChannelMessage {
                channel: name.clone(),
                author: wire.author,
                author_nick: self.identity.nickname_of(&wire.author).await,
                message: wire.id,
                body,
                ts_hint_ms: wire.ts_hint_ms,
            });
        }
        for event in events {
            self.bus.publish_domain(event);
        }
    }
}
