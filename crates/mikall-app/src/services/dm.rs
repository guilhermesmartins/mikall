//! Direct-message use cases. Payload encryption is the transport/crypto
//! adapter's duty; the service works with plaintext domain bodies that exist
//! only on this node.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::messaging::{
    DmKey, DmKeyError, DmThread, Message, MessageBody, MessageBodyError, MessageId, Verified,
};
use mikall_domain::shared::IdentityId;

use crate::events::{AppEvent, EventBus};
use crate::ports::{ChatTransport, Clock, IdGen, KeyStore, TransportError, WireMessage};

use super::chat::RenderedMessage;
use super::identity::IdentityService;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DmError {
    #[error(transparent)]
    Key(#[from] DmKeyError),
    #[error(transparent)]
    Body(#[from] MessageBodyError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("recipient is blocked")]
    Blocked,
}

#[derive(Clone)]
pub struct DmService {
    identity: Arc<IdentityService>,
    transport: Arc<dyn ChatTransport>,
    keystore: Arc<dyn KeyStore>,
    idgen: Arc<dyn IdGen>,
    clock: Arc<dyn Clock>,
    bus: EventBus,
    threads: Arc<RwLock<BTreeMap<DmKey, DmThread>>>,
}

impl std::fmt::Debug for DmService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmService").finish_non_exhaustive()
    }
}

impl DmService {
    pub fn new(
        identity: Arc<IdentityService>,
        transport: Arc<dyn ChatTransport>,
        keystore: Arc<dyn KeyStore>,
        idgen: Arc<dyn IdGen>,
        clock: Arc<dyn Clock>,
        bus: EventBus,
    ) -> Self {
        DmService {
            identity,
            transport,
            keystore,
            idgen,
            clock,
            bus,
            threads: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// The `PRIVMSG nick` use case.
    pub async fn send_dm(&self, to: IdentityId, raw_body: &str) -> Result<MessageId, DmError> {
        if self.identity.is_blocked(&to).await {
            return Err(DmError::Blocked);
        }
        let body = MessageBody::parse(raw_body)?;
        let me = self.identity.local_id();
        let key = DmKey::new(me, to)?;

        let wire = {
            let mut threads = self.threads.write().await;
            let thread = threads.entry(key).or_insert_with(|| DmThread::new(key));
            let (parents, lamport) = thread.next_message_position();
            let id = self.idgen.message_id(&me, lamport, &parents, body.as_str());
            let ts = self.clock.now_ms();
            let message = Message::authored(id, me, lamport, parents.clone(), body.clone(), ts);
            // Author is a participant by construction of `key`.
            let _ = thread.record_message(message);
            WireMessage {
                id,
                author: me,
                lamport,
                parents,
                body: body.as_str().to_owned(),
                ts_hint_ms: ts,
            }
        };

        self.transport.send_dm(to, wire.clone()).await?;
        self.bus.publish(AppEvent::DirectMessage {
            from: me,
            from_nick: self.identity.nickname().await,
            message: wire.id,
            body,
            ts_hint_ms: wire.ts_hint_ms,
        });
        Ok(wire.id)
    }

    /// Inbound path from the transport router.
    pub async fn receive_dm(&self, from: IdentityId, wire: WireMessage, verified: Verified) {
        if self.identity.is_blocked(&from).await {
            return;
        }
        if wire.author != from {
            return; // a DM claiming another author is discarded outright
        }
        let Ok(body) = MessageBody::parse(&wire.body) else {
            return;
        };
        let me = self.identity.local_id();
        let Ok(key) = DmKey::new(me, from) else {
            return;
        };
        self.identity
            .observe_peer(from, self.keystore.fingerprint_of(&from), None)
            .await;

        let recorded = {
            let mut threads = self.threads.write().await;
            let thread = threads.entry(key).or_insert_with(|| DmThread::new(key));
            let message = Message::received(
                verified,
                wire.id,
                wire.author,
                wire.lamport,
                wire.parents.clone(),
                body.clone(),
                wire.ts_hint_ms,
            );
            thread.record_message(message).is_ok()
        };
        if recorded {
            self.bus.publish(AppEvent::DirectMessage {
                from,
                from_nick: self.identity.nickname_of(&from).await,
                message: wire.id,
                body,
                ts_hint_ms: wire.ts_hint_ms,
            });
        }
    }

    pub async fn history(&self, with: IdentityId) -> Result<Vec<RenderedMessage>, DmError> {
        let me = self.identity.local_id();
        let key = DmKey::new(me, with)?;
        let raw: Vec<(MessageId, IdentityId, MessageBody, u64)> = {
            let threads = self.threads.read().await;
            match threads.get(&key) {
                None => Vec::new(),
                Some(thread) => thread
                    .history()
                    .into_iter()
                    .map(|m| (m.id(), m.author(), m.body().clone(), m.ts_hint_ms()))
                    .collect(),
            }
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
}
