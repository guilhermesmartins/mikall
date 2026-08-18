//! The inbound router: the single place where transport adapters hand
//! verified traffic to the application.

use std::sync::Arc;

use async_trait::async_trait;

use mikall_domain::messaging::{ChannelId, Verified};
use mikall_domain::shared::IdentityId;

use crate::ports::{ChannelSignal, InboundHandler, WireMessage};

use super::chat::ChatService;
use super::dm::DmService;
use super::presence::PresenceService;
use super::transfer::TransferService;

#[derive(Clone)]
pub struct InboundRouter {
    chat: Arc<ChatService>,
    dm: Arc<DmService>,
    presence: Arc<PresenceService>,
    transfer: Arc<TransferService>,
}

impl std::fmt::Debug for InboundRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundRouter").finish_non_exhaustive()
    }
}

impl InboundRouter {
    pub fn new(
        chat: Arc<ChatService>,
        dm: Arc<DmService>,
        presence: Arc<PresenceService>,
        transfer: Arc<TransferService>,
    ) -> Self {
        InboundRouter {
            chat,
            dm,
            presence,
            transfer,
        }
    }
}

#[async_trait]
impl InboundHandler for InboundRouter {
    async fn on_channel_signal(
        &self,
        channel: ChannelId,
        signal: ChannelSignal,
        verified: Verified,
    ) {
        if let ChannelSignal::Presence { who, away } = &signal {
            self.presence.observe(*who, away.clone()).await;
        }
        self.chat.receive_signal(channel, signal, verified).await;
    }

    async fn on_dm(&self, from: IdentityId, message: WireMessage, verified: Verified) {
        self.dm.receive_dm(from, message, verified).await;
    }

    async fn on_file_offer(
        &self,
        from: IdentityId,
        manifest: mikall_domain::transfer::FileManifest,
        _verified: Verified,
    ) {
        self.transfer.offered_to_us(from, manifest).await;
    }
}
