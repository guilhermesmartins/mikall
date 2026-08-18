//! Call use cases. In M0–M4 this drives the `Call` aggregate (signaling and
//! media transports arrive with the media milestone); the mesh-size
//! invariant and the call lifecycle are fully enforced here already.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::calls::{Call, CallError, CallId, CallPhase, MediaState};
use mikall_domain::shared::IdentityId;

use crate::events::EventBus;
use crate::ports::IdGen;

use super::identity::IdentityService;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallServiceError {
    #[error("unknown call")]
    UnknownCall,
    #[error(transparent)]
    Call(#[from] CallError),
}

/// Read-model of a call for frontends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallSnapshot {
    pub id: CallId,
    pub initiator: IdentityId,
    pub phase: CallPhase,
    pub participants: Vec<(IdentityId, MediaState)>,
}

#[derive(Clone)]
pub struct CallService {
    identity: Arc<IdentityService>,
    idgen: Arc<dyn IdGen>,
    bus: EventBus,
    calls: Arc<RwLock<BTreeMap<CallId, Call>>>,
}

impl std::fmt::Debug for CallService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallService").finish_non_exhaustive()
    }
}

impl CallService {
    pub fn new(identity: Arc<IdentityService>, idgen: Arc<dyn IdGen>, bus: EventBus) -> Self {
        CallService {
            identity,
            idgen,
            bus,
            calls: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn start_call(&self, offered_to: Vec<IdentityId>) -> CallId {
        let id = self.idgen.call_id();
        let (call, event) = Call::offer(id, self.identity.local_id(), offered_to);
        self.calls.write().await.insert(id, call);
        self.bus.publish_domain(event);
        id
    }

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
        })
    }
}
