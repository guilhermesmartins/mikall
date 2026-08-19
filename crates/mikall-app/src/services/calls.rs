//! Call use cases: the `Call` aggregate driven by signed signaling
//! envelopes between peers. Media frames are routed to per-call taps for
//! the media engine; the mesh-size invariant and lifecycle live in the
//! domain aggregate.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use mikall_domain::calls::{Call, CallError, CallEvent, CallId, CallPhase, MediaState};
use mikall_domain::shared::IdentityId;

use crate::events::EventBus;
use crate::ports::{CallAction, CallSignaling, IdGen, TransportError};

use super::identity::IdentityService;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallServiceError {
    #[error("unknown call")]
    UnknownCall,
    #[error(transparent)]
    Call(#[from] CallError),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

/// Read-model of a call for frontends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallSnapshot {
    pub id: CallId,
    pub initiator: IdentityId,
    pub phase: CallPhase,
    pub participants: Vec<(IdentityId, MediaState)>,
    /// Offered-but-not-joined: rung (initially or mid-call) and yet to
    /// answer. Frontends render these as "ringing" and must not re-ring.
    pub invited: Vec<IdentityId>,
}

type MediaTap = mpsc::Sender<(IdentityId, Vec<u8>)>;

#[derive(Clone)]
pub struct CallService {
    identity: Arc<IdentityService>,
    idgen: Arc<dyn IdGen>,
    signaling: Arc<dyn CallSignaling>,
    bus: EventBus,
    calls: Arc<RwLock<BTreeMap<CallId, Call>>>,
    media_taps: Arc<RwLock<BTreeMap<CallId, MediaTap>>>,
    call_keys: Arc<RwLock<BTreeMap<CallId, [u8; 32]>>>,
}

impl std::fmt::Debug for CallService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallService").finish_non_exhaustive()
    }
}

impl CallService {
    pub fn new(
        identity: Arc<IdentityService>,
        idgen: Arc<dyn IdGen>,
        signaling: Arc<dyn CallSignaling>,
        bus: EventBus,
    ) -> Self {
        CallService {
            identity,
            idgen,
            signaling,
            bus,
            calls: Arc::new(RwLock::new(BTreeMap::new())),
            media_taps: Arc::new(RwLock::new(BTreeMap::new())),
            call_keys: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Ring one or more peers — or nobody: an empty list opens a solo
    /// stage, active immediately with only us, peers rung later via
    /// [`CallService::invite`]. The mesh cap (8) is enforced by the roster
    /// as participants actually join.
    pub async fn start_call(&self, offered_to: Vec<IdentityId>) -> CallId {
        let id = self.idgen.call_id();
        let me = self.identity.local_id();
        let media_key = self.idgen.call_key();
        let (call, event) = Call::offer(id, me, offered_to.clone());
        self.calls.write().await.insert(id, call);
        self.call_keys.write().await.insert(id, media_key);
        self.bus.publish_domain(event);
        for peer in offered_to {
            let _ = self
                .signaling
                .send(
                    peer,
                    id,
                    CallAction::Offer {
                        participants: vec![me],
                        media_key,
                    },
                )
                .await;
        }
        id
    }

    /// The AEAD key protecting this call's media frames.
    pub async fn media_key(&self, id: CallId) -> Option<[u8; 32]> {
        self.call_keys.read().await.get(&id).copied()
    }

    /// Ring more peers into an ongoing call. Each fresh invitee gets the
    /// same `CallAction::Offer` envelope as an initial ring — carrying the
    /// call's existing media key — so their incoming-offer flow (and their
    /// ability to seal/unseal frames on accept) is unchanged.
    pub async fn invite(&self, id: CallId, peers: Vec<IdentityId>) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let media_key = self
            .media_key(id)
            .await
            .ok_or(CallServiceError::UnknownCall)?;
        let (event, participants) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let event = call.invite(me, &peers)?;
            let participants: Vec<IdentityId> =
                call.roster().members().map(|(who, _)| who).collect();
            (event, participants)
        };
        // `Call::invite` always yields `CallInvited` with the deduplicated
        // fresh invitees — exactly who needs an offer envelope.
        let fresh = if let CallEvent::CallInvited { to, .. } = &event {
            to.clone()
        } else {
            Vec::new()
        };
        self.bus.publish_domain(event);
        for peer in fresh {
            let _ = self
                .signaling
                .send(
                    peer,
                    id,
                    CallAction::Offer {
                        participants: participants.clone(),
                        media_key,
                    },
                )
                .await;
        }
        Ok(())
    }

    /// Accept a call that was offered to us.
    pub async fn accept_incoming(&self, id: CallId) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let (events, initiator) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let events = call.accept(me)?;
            call.connected()?;
            (events, call.initiator())
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        self.signaling
            .send(initiator, id, CallAction::Accept)
            .await?;
        Ok(())
    }

    /// Decline a call that was offered to us.
    pub async fn decline_incoming(&self, id: CallId) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let (events, initiator) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let initiator = call.initiator();
            (call.decline(me)?, initiator)
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        self.signaling
            .send(initiator, id, CallAction::Decline)
            .await?;
        Ok(())
    }

    /// Hang up: end locally and tell every other participant.
    pub async fn hang_up_all(&self, id: CallId) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let (event, peers) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let peers: Vec<IdentityId> = call
                .roster()
                .members()
                .map(|(who, _)| who)
                .filter(|who| *who != me)
                .collect();
            (call.hang_up()?, peers)
        };
        self.bus.publish_domain(event);
        self.media_taps.write().await.remove(&id);
        for peer in peers {
            let _ = self.signaling.send(peer, id, CallAction::HangUp).await;
        }
        Ok(())
    }

    /// Toggle our screen share and tell the call.
    pub async fn share_screen(&self, id: CallId, active: bool) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let (event, peers) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let peers: Vec<IdentityId> = call
                .roster()
                .members()
                .map(|(who, _)| who)
                .filter(|who| *who != me)
                .collect();
            (call.set_screen_sharing(me, active)?, peers)
        };
        self.bus.publish_domain(event);
        for peer in peers {
            let _ = self
                .signaling
                .send(peer, id, CallAction::ScreenShare { active })
                .await;
        }
        Ok(())
    }

    /// Toggle our mic mute and tell the call. The enforcement is local —
    /// the media engine sends silence while muted — this records the state
    /// in the aggregate and advises every peer's roster.
    pub async fn set_muted(&self, id: CallId, muted: bool) -> Result<(), CallServiceError> {
        let me = self.identity.local_id();
        let (event, peers) = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            let peers: Vec<IdentityId> = call
                .roster()
                .members()
                .map(|(who, _)| who)
                .filter(|who| *who != me)
                .collect();
            (call.set_mic_muted(me, muted)?, peers)
        };
        self.bus.publish_domain(event);
        for peer in peers {
            let _ = self
                .signaling
                .send(peer, id, CallAction::Mute { active: muted })
                .await;
        }
        Ok(())
    }

    /// Inbound signaling from the router (envelope already verified).
    pub async fn receive_signal(&self, from: IdentityId, id: CallId, action: CallAction) {
        let mut events: Vec<CallEvent> = Vec::new();
        {
            let mut calls = self.calls.write().await;
            match action {
                CallAction::Offer { media_key, .. } => {
                    // Fresh offer, or a re-ring of a call we declined or
                    // left earlier (its ended aggregate is superseded).
                    let fresh = match calls.get(&id) {
                        None => true,
                        Some(call) => matches!(call.phase(), CallPhase::Ended { .. }),
                    };
                    if fresh {
                        let me = self.identity.local_id();
                        let (call, event) = Call::offer(id, from, vec![me]);
                        calls.insert(id, call);
                        self.call_keys.write().await.insert(id, media_key);
                        events.push(event);
                    }
                }
                CallAction::Accept => {
                    if let Some(call) = calls.get_mut(&id) {
                        if let Ok(mut accepted) = call.accept(from) {
                            events.append(&mut accepted);
                            let _ = call.connected();
                        }
                    }
                }
                CallAction::Decline => {
                    if let Some(call) = calls.get_mut(&id) {
                        if let Ok(mut declined) = call.decline(from) {
                            events.append(&mut declined);
                        }
                    }
                }
                CallAction::HangUp => {
                    if let Some(call) = calls.get_mut(&id) {
                        match call.leave(from) {
                            Ok(mut left) => events.append(&mut left),
                            // Never seated: an invited peer ringing off is a
                            // decline, and must not tear down a call others
                            // are still in. Only a peer neither seated nor
                            // invited hanging up ends our copy outright.
                            Err(_) => match call.decline(from) {
                                Ok(mut declined) => events.append(&mut declined),
                                Err(_) => {
                                    if let Ok(event) = call.hang_up() {
                                        events.push(event);
                                    }
                                }
                            },
                        }
                        // The media tap lives as long as the call does — a
                        // single participant leaving must not silence it.
                        if matches!(call.phase(), CallPhase::Ended { .. }) {
                            self.media_taps.write().await.remove(&id);
                        }
                    } else {
                        self.media_taps.write().await.remove(&id);
                    }
                }
                CallAction::ScreenShare { active } => {
                    if let Some(call) = calls.get_mut(&id) {
                        if let Ok(event) = call.set_screen_sharing(from, active) {
                            events.push(event);
                        }
                    }
                }
                CallAction::Mute { active } => {
                    if let Some(call) = calls.get_mut(&id) {
                        if let Ok(event) = call.set_mic_muted(from, active) {
                            events.push(event);
                        }
                    }
                }
            }
        }
        for event in events {
            self.bus.publish_domain(event);
        }
    }

    /// The media engine registers here to receive this call's inbound
    /// sealed frames.
    pub async fn media_tap(&self, id: CallId) -> mpsc::Receiver<(IdentityId, Vec<u8>)> {
        let (tx, rx) = mpsc::channel(256);
        self.media_taps.write().await.insert(id, tx);
        rx
    }

    /// Inbound sealed media frame from the router.
    pub async fn receive_media(&self, from: IdentityId, id: CallId, sealed_frame: Vec<u8>) {
        let tap = self.media_taps.read().await.get(&id).cloned();
        if let Some(tap) = tap {
            let _ = tap.try_send((from, sealed_frame));
        }
    }

    /// Peers to send media to (everyone in the roster but us).
    pub async fn media_peers(&self, id: CallId) -> Result<Vec<IdentityId>, CallServiceError> {
        let me = self.identity.local_id();
        let calls = self.calls.read().await;
        let call = calls.get(&id).ok_or(CallServiceError::UnknownCall)?;
        Ok(call
            .roster()
            .members()
            .map(|(who, _)| who)
            .filter(|who| *who != me)
            .collect())
    }

    // ------ direct aggregate drivers (BDD + local flows) ------

    pub async fn accept(&self, id: CallId, by: IdentityId) -> Result<(), CallServiceError> {
        let events = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.accept(by)?
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        Ok(())
    }

    pub async fn decline(&self, id: CallId, by: IdentityId) -> Result<(), CallServiceError> {
        let events = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.decline(by)?
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        Ok(())
    }

    pub async fn mark_connected(&self, id: CallId) -> Result<(), CallServiceError> {
        let mut calls = self.calls.write().await;
        let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
        call.connected()?;
        Ok(())
    }

    pub async fn join(&self, id: CallId, who: IdentityId) -> Result<(), CallServiceError> {
        let event = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.join(who)?
        };
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn leave(&self, id: CallId, who: IdentityId) -> Result<(), CallServiceError> {
        let events = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.leave(who)?
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        Ok(())
    }

    pub async fn hang_up(&self, id: CallId) -> Result<(), CallServiceError> {
        let event = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.hang_up()?
        };
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn set_screen_sharing(
        &self,
        id: CallId,
        who: IdentityId,
        sharing: bool,
    ) -> Result<(), CallServiceError> {
        let event = {
            let mut calls = self.calls.write().await;
            let call = calls.get_mut(&id).ok_or(CallServiceError::UnknownCall)?;
            call.set_screen_sharing(who, sharing)?
        };
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn snapshot(&self, id: CallId) -> Result<CallSnapshot, CallServiceError> {
        let calls = self.calls.read().await;
        let call = calls.get(&id).ok_or(CallServiceError::UnknownCall)?;
        Ok(CallSnapshot {
            id: call.id(),
            initiator: call.initiator(),
            phase: call.phase().clone(),
            participants: call.roster().members().collect(),
            invited: call.invited().collect(),
        })
    }
}
