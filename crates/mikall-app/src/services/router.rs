//! The inbound router: the single place where transport adapters hand
//! verified traffic to the application.

use std::sync::Arc;

use async_trait::async_trait;

use mikall_domain::messaging::{ChannelId, Verified};
use mikall_domain::shared::IdentityId;

use crate::ports::{CallAction, ChannelSignal, InboundHandler, WireMessage};
use mikall_domain::calls::CallId;

use super::calls::CallService;
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
    calls: Arc<CallService>,
}

impl std::fmt::Debug for InboundRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundRouter").finish_non_exhaustive()
    }
}

impl InboundRouter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chat: Arc<ChatService>,
        dm: Arc<DmService>,
        presence: Arc<PresenceService>,
        transfer: Arc<TransferService>,
        calls: Arc<CallService>,
    ) -> Self {
        InboundRouter {
            chat,
            dm,
            presence,
            transfer,
            calls,
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

    async fn on_call_signal(
        &self,
        from: IdentityId,
        call: CallId,
        action: CallAction,
        _verified: Verified,
    ) {
        self.calls.receive_signal(from, call, action).await;
    }

    async fn on_media_frame(&self, from: IdentityId, call: CallId, sealed_frame: Vec<u8>) {
        self.calls.receive_media(from, call, sealed_frame).await;
    }

    async fn on_video_frame(&self, from: IdentityId, call: CallId, sealed_frame: Vec<u8>) {
        self.calls.receive_video(from, call, sealed_frame).await;
    }
}
