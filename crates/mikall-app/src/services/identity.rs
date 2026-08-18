//! Identity use cases: the local identity, contacts, trust.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::identity::{Contact, ContactError, IdentityEvent, TrustLevel};
use mikall_domain::messaging::Nickname;
use mikall_domain::shared::{Fingerprint, IdentityId};

use crate::events::EventBus;
use crate::ports::Clock;

/// Shared local profile: who we are and what we know about others.
#[derive(Debug)]
pub struct Profile {
    id: IdentityId,
    fingerprint: Fingerprint,
    nickname: RwLock<Option<Nickname>>,
    contacts: RwLock<BTreeMap<IdentityId, Contact>>,
    /// Nicknames peers have announced for themselves (labels, never truths).
    nicks: RwLock<BTreeMap<IdentityId, Nickname>>,
}

impl Profile {
    pub fn new(id: IdentityId, fingerprint: Fingerprint) -> Self {
        Profile {
            id,
            fingerprint,
            nickname: RwLock::new(None),
            contacts: RwLock::new(BTreeMap::new()),
            nicks: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn id(&self) -> IdentityId {
        self.id
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdentityServiceError {
    #[error("unknown contact")]
    UnknownContact,
    #[error(transparent)]
    Contact(#[from] ContactError),
}

#[derive(Clone)]
pub struct IdentityService {
    profile: Arc<Profile>,
    clock: Arc<dyn Clock>,
    bus: EventBus,
}

impl std::fmt::Debug for IdentityService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityService").finish_non_exhaustive()
    }
}

impl IdentityService {
    pub fn new(profile: Arc<Profile>, clock: Arc<dyn Clock>, bus: EventBus) -> Self {
        IdentityService {
            profile,
            clock,
            bus,
        }
    }

    pub fn local_id(&self) -> IdentityId {
        self.profile.id()
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.profile.fingerprint()
    }

    pub async fn set_nickname(&self, nickname: Nickname) {
        *self.profile.nickname.write().await = Some(nickname);
    }

    pub async fn nickname(&self) -> Option<Nickname> {
        self.profile.nickname.read().await.clone()
    }

    /// The label we display for an identity: their announced nickname.
    pub async fn nickname_of(&self, id: &IdentityId) -> Option<Nickname> {
        if *id == self.profile.id() {
            return self.profile.nickname.read().await.clone();
        }
        self.profile.nicks.read().await.get(id).cloned()
    }

    /// Record a sighting of a peer: TOFU-pin on first contact, raise the
    /// key-change alarm when the fingerprint differs from the pinned one.
    pub async fn observe_peer(
        &self,
        id: IdentityId,
        fingerprint: Fingerprint,
        nickname: Option<Nickname>,
    ) {
        if id == self.profile.id() {
            return;
        }
        if let Some(nick) = nickname {
            self.profile.nicks.write().await.insert(id, nick);
        }
        let mut contacts = self.profile.contacts.write().await;
        match contacts.get_mut(&id) {
            None => {
                let (contact, event) = Contact::first_seen(id, fingerprint, self.clock.now_ms());
                contacts.insert(id, contact);
                self.bus.publish_domain(event);
            }
            Some(contact) => {
                if contact.fingerprint() != fingerprint {
                    let event = contact.key_changed(fingerprint);
                    self.bus.publish_domain(event);
                }
            }
        }
    }

    pub async fn verify_contact(&self, id: &IdentityId) -> Result<(), IdentityServiceError> {
        let mut contacts = self.profile.contacts.write().await;
        let contact = contacts
            .get_mut(id)
            .ok_or(IdentityServiceError::UnknownContact)?;
        let event = contact.verify(self.clock.now_ms())?;
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn block(&self, id: IdentityId) {
        let mut contacts = self.profile.contacts.write().await;
        let contact = contacts.entry(id).or_insert_with(|| {
            Contact::first_seen(id, Fingerprint::from_bytes([0; 32]), self.clock.now_ms()).0
        });
        let event = contact.block();
        self.bus.publish_domain(event);
    }

    pub async fn is_blocked(&self, id: &IdentityId) -> bool {
        self.profile
            .contacts
            .read()
            .await
            .get(id)
            .is_some_and(Contact::is_blocked)
    }

    pub async fn trust_of(&self, id: &IdentityId) -> Option<TrustLevel> {
        self.profile
            .contacts
            .read()
            .await
            .get(id)
            .map(Contact::trust)
    }

    pub async fn fingerprint_of_contact(&self, id: &IdentityId) -> Option<Fingerprint> {
        self.profile
            .contacts
            .read()
            .await
            .get(id)
            .map(Contact::fingerprint)
    }

    /// Emitted events for tests and frontends that want raw identity events.
    pub fn publish_event(&self, event: IdentityEvent) {
        self.bus.publish_domain(event);
    }
}
