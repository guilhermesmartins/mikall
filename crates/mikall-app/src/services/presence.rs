//! Presence use cases. Best-effort by nature on a serverless network.

use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::presence::{AwayMessage, AwayMessageError, PresenceState, Roster};
use mikall_domain::shared::IdentityId;

use crate::events::EventBus;
use crate::ports::{ChannelSignal, ChatTransport, TransportError};

use super::chat::ChatService;
use super::identity::IdentityService;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PresenceError {
    #[error(transparent)]
    Away(#[from] AwayMessageError),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

#[derive(Clone)]
pub struct PresenceService {
    identity: Arc<IdentityService>,
    chat: Arc<ChatService>,
    transport: Arc<dyn ChatTransport>,
    bus: EventBus,
    roster: Arc<RwLock<Roster>>,
}

impl std::fmt::Debug for PresenceService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresenceService").finish_non_exhaustive()
    }
}

impl PresenceService {
    pub fn new(
        identity: Arc<IdentityService>,
        chat: Arc<ChatService>,
        transport: Arc<dyn ChatTransport>,
        bus: EventBus,
    ) -> Self {
        PresenceService {
            identity,
            chat,
            transport,
            bus,
            roster: Arc::new(RwLock::new(Roster::new())),
        }
    }

    /// The `AWAY [:message]` use case: `None` clears away.
    pub async fn set_away(&self, message: Option<&str>) -> Result<(), PresenceError> {
        let away = match message {
            Some(raw) => Some(AwayMessage::parse(raw)?),
            None => None,
        };
        let me = self.identity.local_id();
        let state = match &away {
            Some(msg) => PresenceState::Away {
                message: Some(msg.clone()),
            },
            None => PresenceState::Online,
        };
        if let Some(event) = self.roster.write().await.observe(me, state) {
            self.bus.publish_domain(event);
        }
        let wire_away = away.map(|a| a.as_str().to_owned());
        for name in self.chat.joined_channels().await {
            if let Ok(id) = self.chat.channel_id_of(&name).await {
                self.transport
                    .publish(
                        id,
                        ChannelSignal::Presence {
                            who: me,
                            away: wire_away.clone(),
                        },
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// Inbound presence beacon.
    pub async fn observe(&self, who: IdentityId, away: Option<String>) {
        let state = match away {
            Some(raw) => match AwayMessage::parse(&raw) {
                Ok(msg) => PresenceState::Away { message: Some(msg) },
                Err(_) => PresenceState::Away { message: None },
            },
            None => PresenceState::Online,
        };
        if let Some(event) = self.roster.write().await.observe(who, state) {
            self.bus.publish_domain(event);
        }
    }

    pub async fn state_of(&self, id: &IdentityId) -> PresenceState {
        self.roster
            .read()
            .await
            .state_of(id)
            .cloned()
            .unwrap_or(PresenceState::Offline { last_seen_ms: 0 })
    }
}
