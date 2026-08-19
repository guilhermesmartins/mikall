//! Identity use cases: the local identity, contacts, trust.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::identity::{Contact, ContactError, IdentityEvent, TrustLevel};
use mikall_domain::messaging::Nickname;
use mikall_domain::shared::{Fingerprint, IdentityId};

use crate::events::EventBus;
use crate::ports::{Clock, IrcGatewayConfig, ProfileStore, ProfileStoreError};

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
    store: Arc<dyn ProfileStore>,
    clock: Arc<dyn Clock>,
    bus: EventBus,
}

impl std::fmt::Debug for IdentityService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityService").finish_non_exhaustive()
    }
}

impl IdentityService {
    pub fn new(
        profile: Arc<Profile>,
        store: Arc<dyn ProfileStore>,
        clock: Arc<dyn Clock>,
        bus: EventBus,
    ) -> Self {
        IdentityService {
            profile,
            store,
            clock,
            bus,
        }
    }

    /// Load the persisted profile record into memory. The composition root
    /// calls this once at boot, before any frontend attaches — no events
    /// are published (no `NicknameChanged`, no `ContactBlocked`), frontends
    /// read the result via [`IdentityService::nickname`],
    /// [`IdentityService::is_blocked`], and friends.
    pub async fn hydrate(&self) -> Result<(), ProfileStoreError> {
        let record = self.store.load().await?;
        if let Some(nick) = record.nickname {
            *self.profile.nickname.write().await = Some(nick);
        }
        let mut contacts = self.profile.contacts.write().await;
        for id in record.blocked {
            let contact = contacts.entry(id).or_insert_with(|| {
                Contact::first_seen(id, Fingerprint::from_bytes([0; 32]), self.clock.now_ms()).0
            });
            // Replaying a past decision, not making a new one: the block
            // event was published when the user chose it.
            let _ = contact.block();
        }
        Ok(())
    }

    pub fn local_id(&self) -> IdentityId {
        self.profile.id()
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.profile.fingerprint()
    }

    pub async fn set_nickname(&self, nickname: Nickname) {
        *self.profile.nickname.write().await = Some(nickname.clone());
        // Persistence is best-effort: read-modify-write keeps fields this
        // service doesn't own, and a failed page write never blocks the
        // rename or its event — the worst outcome is re-onboarding after a
        // restart, never a wrong nickname.
        let mut record = self.store.load().await.unwrap_or_default();
        record.nickname = Some(nickname.clone());
        let _ = self.store.save(&record).await;
        self.bus
            .publish_domain(IdentityEvent::NicknameChanged { nickname });
    }

    pub async fn nickname(&self) -> Option<Nickname> {
        self.profile.nickname.read().await.clone()
    }

    /// The persisted IRC-gateway choice, defaults when never configured.
    /// Best-effort like every profile read: an unreadable record answers
    /// with the defaults (gateway off) rather than blocking boot.
    pub async fn irc_config(&self) -> IrcGatewayConfig {
        self.store
            .load()
            .await
            .unwrap_or_default()
            .irc
            .unwrap_or_default()
    }

    /// Persist the IRC-gateway choice. All profile writes funnel through
    /// this service and read-modify-write the record, so the nickname and
    /// blocklist survive a gateway save (and vice versa).
    pub async fn set_irc_config(&self, config: IrcGatewayConfig) {
        let mut record = self.store.load().await.unwrap_or_default();
        record.irc = Some(config);
        let _ = self.store.save(&record).await;
    }

    /// The label we display for an identity: their announced nickname.
    pub async fn nickname_of(&self, id: &IdentityId) -> Option<Nickname> {
        if *id == self.profile.id() {
            return self.profile.nickname.read().await.clone();
        }
        self.profile.nicks.read().await.get(id).cloned()
    }

    /// Reverse lookup: the identity that most recently announced `nick`.
    /// Nicknames are labels, not identities — collisions resolve to the
    /// first match in id order, and `WHOIS` shows the fingerprint.
    pub async fn identity_by_nickname(&self, nick: &str) -> Option<IdentityId> {
        self.profile
            .nicks
            .read()
            .await
            .iter()
            .find(|(_, n)| n.as_str().eq_ignore_ascii_case(nick))
            .map(|(id, _)| *id)
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
        {
            let mut contacts = self.profile.contacts.write().await;
            let contact = contacts.entry(id).or_insert_with(|| {
                Contact::first_seen(id, Fingerprint::from_bytes([0; 32]), self.clock.now_ms()).0
            });
            let event = contact.block();
            self.bus.publish_domain(event);
        }
        // A block is a user choice, so it persists (best-effort,
        // read-modify-write — same discipline as `set_nickname`).
        let mut record = self.store.load().await.unwrap_or_default();
        if !record.blocked.contains(&id) {
            record.blocked.push(id);
            let _ = self.store.save(&record).await;
        }
    }

    /// Undo a block by forgetting the contact entirely: the identity drops
    /// off the blocklist and, on next sighting, is TOFU-pinned again like
    /// any stranger. No domain trust transition exists out of `Blocked` —
    /// pretending the old pin was still trustworthy would be a lie, so we
    /// honestly start over.
    pub async fn unblock(&self, id: IdentityId) {
        {
            let mut contacts = self.profile.contacts.write().await;
            if !contacts.get(&id).is_some_and(Contact::is_blocked) {
                return;
            }
            contacts.remove(&id);
        }
        let mut record = self.store.load().await.unwrap_or_default();
        if record.blocked.contains(&id) {
            record.blocked.retain(|blocked| *blocked != id);
            let _ = self.store.save(&record).await;
        }
    }

    /// Every blocked identity with its announced nickname, if one is known
    /// — for blocklist management surfaces.
    pub async fn blocked_contacts(&self) -> Vec<(IdentityId, Option<Nickname>)> {
        let contacts = self.profile.contacts.read().await;
        let nicks = self.profile.nicks.read().await;
        contacts
            .values()
            .filter(|contact| contact.is_blocked())
            .map(|contact| (contact.id(), nicks.get(&contact.id()).cloned()))
            .collect()
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
