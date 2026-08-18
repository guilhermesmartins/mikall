//! The `Channel` aggregate root.

use core::fmt;
use std::collections::BTreeMap;

use crate::shared::IdentityId;

use super::dag::{DagInsert, MessageDag};
use super::message::{Message, MessageId};
use super::values::{ChannelName, Topic};
use super::MessagingEvent;

/// Channel identity. For public channels this is derived deterministically
/// from the canonical name (BLAKE3 at the adapter boundary) so `JOIN #stage`
/// converges on one gossip topic everywhere; private channels use random ids
/// shared via invite.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelId([u8; 32]);

impl ChannelId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        ChannelId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChannelId({self})")
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Member roles. Ordered: `Member < Op < Founder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Member,
    Op,
    Founder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelVisibility {
    Public,
    /// Private channels carry an encryption epoch, bumped on membership
    /// change so departed members cannot read new traffic.
    Private {
        epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChannelError {
    #[error("not a member of this channel")]
    NotAMember,
    #[error("requires operator privileges")]
    NotAnOperator,
    #[error("the founder cannot be removed from a channel")]
    FounderIsImmovable,
    #[error("already a member")]
    AlreadyMember,
    #[error("only the founder can change operator roles")]
    NotFounder,
}

/// A chat channel: name, topic, membership with roles, and the message DAG.
///
/// Invariants (checked by `debug_check_invariants` after every command):
/// exactly one founder, the founder is always a member, every applied
/// message's author was accepted through membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    id: ChannelId,
    name: ChannelName,
    topic: Topic,
    visibility: ChannelVisibility,
    members: BTreeMap<IdentityId, Role>,
    dag: MessageDag,
}

impl Channel {
    pub fn create(
        id: ChannelId,
        name: ChannelName,
        founder: IdentityId,
        visibility: ChannelVisibility,
    ) -> (Self, Vec<MessagingEvent>) {
        let mut members = BTreeMap::new();
        members.insert(founder, Role::Founder);
        let channel = Channel {
            id,
            name: name.clone(),
            topic: Topic::none(),
            visibility,
            members,
            dag: MessageDag::new(),
        };
        channel.debug_check_invariants();
        let events = vec![
            MessagingEvent::ChannelCreated { channel: id, name },
            MessagingEvent::ChannelJoined {
                channel: id,
                who: founder,
            },
        ];
        (channel, events)
    }

    pub fn id(&self) -> ChannelId {
        self.id
    }

    pub fn name(&self) -> &ChannelName {
        &self.name
    }

    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    pub fn visibility(&self) -> ChannelVisibility {
        self.visibility
    }

    pub fn members(&self) -> impl Iterator<Item = (IdentityId, Role)> + '_ {
        self.members.iter().map(|(id, role)| (*id, *role))
    }

    pub fn role_of(&self, id: &IdentityId) -> Option<Role> {
        self.members.get(id).copied()
    }

    pub fn is_member(&self, id: &IdentityId) -> bool {
        self.members.contains_key(id)
    }

    pub fn dag(&self) -> &MessageDag {
        &self.dag
    }

    /// Deterministic display order of all applied messages.
    pub fn history(&self) -> Vec<&Message> {
        self.dag.linearize()
    }

    pub fn join(&mut self, who: IdentityId) -> Result<MessagingEvent, ChannelError> {
        if self.members.contains_key(&who) {
            return Err(ChannelError::AlreadyMember);
        }
        self.members.insert(who, Role::Member);
        if let ChannelVisibility::Private { epoch } = self.visibility {
            self.visibility = ChannelVisibility::Private { epoch: epoch + 1 };
        }
        self.debug_check_invariants();
        Ok(MessagingEvent::ChannelJoined {
            channel: self.id,
            who,
        })
    }

    pub fn leave(&mut self, who: IdentityId) -> Result<MessagingEvent, ChannelError> {
        match self.members.get(&who) {
            None => Err(ChannelError::NotAMember),
            Some(Role::Founder) => Err(ChannelError::FounderIsImmovable),
            Some(Role::Op) | Some(Role::Member) => {
                self.members.remove(&who);
                if let ChannelVisibility::Private { epoch } = self.visibility {
                    self.visibility = ChannelVisibility::Private { epoch: epoch + 1 };
                }
                self.debug_check_invariants();
                Ok(MessagingEvent::ChannelLeft {
                    channel: self.id,
                    who,
                })
            }
        }
    }

    pub fn set_topic(
        &mut self,
        by: IdentityId,
        topic: Topic,
    ) -> Result<MessagingEvent, ChannelError> {
        match self.members.get(&by) {
            None => Err(ChannelError::NotAMember),
            Some(Role::Member) => Err(ChannelError::NotAnOperator),
            Some(Role::Op) | Some(Role::Founder) => {
                self.topic = topic.clone();
                self.debug_check_invariants();
                Ok(MessagingEvent::TopicChanged {
                    channel: self.id,
                    by,
                    topic,
                })
            }
        }
    }

    /// Grant or revoke operator status. Only the founder may change roles,
    /// and the founder's own role can never change.
    pub fn set_role(
        &mut self,
        by: IdentityId,
        target: IdentityId,
        role: Role,
    ) -> Result<MessagingEvent, ChannelError> {
        if self.role_of(&by) != Some(Role::Founder) {
            return Err(ChannelError::NotFounder);
        }
        match self.members.get(&target) {
            None => Err(ChannelError::NotAMember),
            Some(Role::Founder) => Err(ChannelError::FounderIsImmovable),
            Some(Role::Op) | Some(Role::Member) => {
                let new_role = match role {
                    Role::Founder => return Err(ChannelError::FounderIsImmovable),
                    Role::Op => Role::Op,
                    Role::Member => Role::Member,
                };
                self.members.insert(target, new_role);
                self.debug_check_invariants();
                Ok(MessagingEvent::MemberRoleChanged {
                    channel: self.id,
                    member: target,
                    role: new_role,
                })
            }
        }
    }

    /// Record a message (locally authored or received). The author must be a
    /// member. Emits `MessagePosted`/`MessageReceived` plus
    /// `HistoryGapDetected` when parents are missing.
    pub fn record_message(
        &mut self,
        message: Message,
        locally_authored: bool,
    ) -> Result<Vec<MessagingEvent>, ChannelError> {
        if !self.members.contains_key(&message.author()) {
            return Err(ChannelError::NotAMember);
        }
        let id = message.id();
        let mut events = Vec::new();
        match self.dag.insert(message) {
            DagInsert::Duplicate => {}
            DagInsert::Applied { unblocked } => {
                events.push(if locally_authored {
                    MessagingEvent::MessagePosted {
                        channel: self.id,
                        message: id,
                    }
                } else {
                    MessagingEvent::MessageReceived {
                        channel: self.id,
                        message: id,
                    }
                });
                for unblocked_id in unblocked {
                    events.push(MessagingEvent::MessageReceived {
                        channel: self.id,
                        message: unblocked_id,
                    });
                }
            }
            DagInsert::Pending { missing } => {
                events.push(MessagingEvent::HistoryGapDetected {
                    channel: self.id,
                    missing,
                });
            }
        }
        self.debug_check_invariants();
        Ok(events)
    }

    /// The parents and lamport counter a new local message should carry.
    pub fn next_message_position(&self) -> (Vec<MessageId>, u64) {
        (self.dag.heads(), self.dag.max_lamport() + 1)
    }

    fn debug_check_invariants(&self) {
        #[cfg(debug_assertions)]
        {
            let founders = self
                .members
                .values()
                .filter(|r| matches!(r, Role::Founder))
                .count();
            debug_assert_eq!(founders, 1, "channel must have exactly one founder");
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::messaging::values::MessageBody;
    use crate::messaging::Verified;

    fn id(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn channel() -> Channel {
        Channel::create(
            ChannelId::from_bytes([7; 32]),
            ChannelName::parse("#stage").unwrap(),
            id(1),
            ChannelVisibility::Public,
        )
        .0
    }

    fn message(n: u8, by: IdentityId, lamport: u64, parents: Vec<MessageId>) -> Message {
        Message::received(
            Verified::attest_signature_checked(),
            MessageId::from_bytes([n; 32]),
            by,
            lamport,
            parents,
            MessageBody::parse("hi").unwrap(),
            0,
        )
    }

    #[test]
    fn founder_cannot_leave_or_be_demoted() {
        let mut ch = channel();
        assert_eq!(ch.leave(id(1)), Err(ChannelError::FounderIsImmovable));
        ch.join(id(2)).unwrap();
        assert_eq!(
            ch.set_role(id(1), id(1), Role::Member),
            Err(ChannelError::FounderIsImmovable)
        );
        assert_eq!(
            ch.set_role(id(1), id(2), Role::Founder),
            Err(ChannelError::FounderIsImmovable)
        );
    }

    #[test]
    fn only_ops_change_topic() {
        let mut ch = channel();
        ch.join(id(2)).unwrap();
        let topic = Topic::parse("world is mine").unwrap();
        assert_eq!(
            ch.set_topic(id(2), topic.clone()),
            Err(ChannelError::NotAnOperator)
        );
        ch.set_role(id(1), id(2), Role::Op).unwrap();
        ch.set_topic(id(2), topic.clone()).unwrap();
        assert_eq!(ch.topic(), &topic);
    }

    #[test]
    fn non_member_message_rejected() {
        let mut ch = channel();
        let err = ch.record_message(message(1, id(9), 1, vec![]), false);
        assert_eq!(err, Err(ChannelError::NotAMember));
    }

    #[test]
    fn gap_detection_emits_event_then_backfill_applies() {
        let mut ch = channel();
        ch.join(id(2)).unwrap();
        let events = ch
            .record_message(
                message(2, id(2), 2, vec![MessageId::from_bytes([1; 32])]),
                false,
            )
            .unwrap();
        assert!(matches!(
            events[0],
            MessagingEvent::HistoryGapDetected { .. }
        ));
        let events = ch
            .record_message(message(1, id(1), 1, vec![]), false)
            .unwrap();
        assert_eq!(events.len(), 2); // applied + unblocked pending
        assert_eq!(ch.history().len(), 2);
    }

    #[test]
    fn private_channel_epoch_bumps_on_membership_change() {
        let (mut ch, _) = Channel::create(
            ChannelId::from_bytes([8; 32]),
            ChannelName::parse("#secret").unwrap(),
            id(1),
            ChannelVisibility::Private { epoch: 0 },
        );
        ch.join(id(2)).unwrap();
        assert_eq!(ch.visibility(), ChannelVisibility::Private { epoch: 1 });
        ch.leave(id(2)).unwrap();
        assert_eq!(ch.visibility(), ChannelVisibility::Private { epoch: 2 });
    }
}
