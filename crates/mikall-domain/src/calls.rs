//! Calls bounded context.
//!
//! Group calls are a full mesh: with no server there is no SFU, so every
//! participant sends media to every other. The mesh limit (8) is therefore a
//! *domain invariant* enforced by [`CallRoster`]'s construction, not a UI
//! suggestion — a 9th participant is unrepresentable.

use core::fmt;
use std::collections::BTreeMap;

use crate::shared::IdentityId;

/// Call identity (UUIDv7 bytes generated via the `IdGen` port).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallId([u8; 16]);

impl CallId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        CallId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CallId({self})")
    }
}

impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaState {
    pub mic_muted: bool,
    pub deafened: bool,
    pub sharing_screen: bool,
}

/// Bounded participant set: 1..=[`CallRoster::MAX_PARTICIPANTS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRoster {
    participants: BTreeMap<IdentityId, MediaState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RosterError {
    #[error("mesh call limit of {max} participants reached", max = CallRoster::MAX_PARTICIPANTS)]
    Full,
    #[error("already in the call")]
    AlreadyPresent,
    #[error("not in the call")]
    NotPresent,
}

impl CallRoster {
    /// Full-mesh ceiling: ~N-1 uplinks per member at ~32 kbps Opus each.
    pub const MAX_PARTICIPANTS: usize = 8;

    pub fn solo(initiator: IdentityId) -> Self {
        let mut participants = BTreeMap::new();
        participants.insert(initiator, MediaState::default());
        CallRoster { participants }
    }

    pub fn len(&self) -> usize {
        self.participants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    pub fn contains(&self, id: &IdentityId) -> bool {
        self.participants.contains_key(id)
    }

    pub fn members(&self) -> impl Iterator<Item = (IdentityId, MediaState)> + '_ {
        self.participants.iter().map(|(id, s)| (*id, *s))
    }

    pub fn add(&mut self, id: IdentityId) -> Result<(), RosterError> {
        if self.participants.contains_key(&id) {
            return Err(RosterError::AlreadyPresent);
        }
        if self.participants.len() >= Self::MAX_PARTICIPANTS {
            return Err(RosterError::Full);
        }
        self.participants.insert(id, MediaState::default());
        Ok(())
    }

    pub fn remove(&mut self, id: &IdentityId) -> Result<(), RosterError> {
        self.participants
            .remove(id)
            .map(|_| ())
            .ok_or(RosterError::NotPresent)
    }

    pub fn update_media(
        &mut self,
        id: &IdentityId,
        f: impl FnOnce(&mut MediaState),
    ) -> Result<MediaState, RosterError> {
        let state = self
            .participants
            .get_mut(id)
            .ok_or(RosterError::NotPresent)?;
        f(state);
        Ok(*state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    HungUp,
    Declined,
    Failed,
    LastParticipantLeft,
}

/// Call lifecycle. Transitions consume the current phase and are matched
/// exhaustively — an illegal transition is a compile-visible `Err`, never a
/// silent state overwrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallPhase {
    /// Offer sent, waiting for the callee(s).
    Ringing { offered_to: Vec<IdentityId> },
    /// Accepted; media transports are being established.
    Connecting,
    /// Media flowing.
    Active,
    /// Terminal.
    Ended { reason: EndReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid call transition: {action} while {state}")]
pub struct InvalidTransition {
    pub action: &'static str,
    pub state: &'static str,
}

impl CallPhase {
    fn name(&self) -> &'static str {
        match self {
            CallPhase::Ringing { .. } => "ringing",
            CallPhase::Connecting => "connecting",
            CallPhase::Active => "active",
            CallPhase::Ended { .. } => "ended",
        }
    }

    pub fn accept(self) -> Result<CallPhase, InvalidTransition> {
        match self {
            CallPhase::Ringing { .. } => Ok(CallPhase::Connecting),
            CallPhase::Connecting | CallPhase::Active | CallPhase::Ended { .. } => {
                Err(InvalidTransition {
                    action: "accept",
                    state: self.name(),
                })
            }
        }
    }

    pub fn decline(self) -> Result<CallPhase, InvalidTransition> {
        match self {
            CallPhase::Ringing { .. } => Ok(CallPhase::Ended {
                reason: EndReason::Declined,
            }),
            CallPhase::Connecting | CallPhase::Active | CallPhase::Ended { .. } => {
                Err(InvalidTransition {
                    action: "decline",
                    state: self.name(),
                })
            }
        }
    }

    pub fn connected(self) -> Result<CallPhase, InvalidTransition> {
        match self {
            CallPhase::Connecting => Ok(CallPhase::Active),
            CallPhase::Ringing { .. } | CallPhase::Active | CallPhase::Ended { .. } => {
                Err(InvalidTransition {
                    action: "connected",
                    state: self.name(),
                })
            }
        }
    }

    pub fn hang_up(self) -> Result<CallPhase, InvalidTransition> {
        match self {
            CallPhase::Ringing { .. } | CallPhase::Connecting | CallPhase::Active => {
                Ok(CallPhase::Ended {
                    reason: EndReason::HungUp,
                })
            }
            CallPhase::Ended { .. } => Err(InvalidTransition {
                action: "hang_up",
                state: self.name(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallEvent {
    CallOffered {
        call: CallId,
        by: IdentityId,
        to: Vec<IdentityId>,
    },
    CallAccepted {
        call: CallId,
        by: IdentityId,
    },
    CallDeclined {
        call: CallId,
        by: IdentityId,
    },
    ParticipantJoined {
        call: CallId,
        who: IdentityId,
    },
    ParticipantLeft {
        call: CallId,
        who: IdentityId,
    },
    ScreenShareStarted {
        call: CallId,
        who: IdentityId,
    },
    ScreenShareStopped {
        call: CallId,
        who: IdentityId,
    },
    CallEnded {
        call: CallId,
        reason: EndReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CallError {
    #[error(transparent)]
    Roster(#[from] RosterError),
    #[error(transparent)]
    Transition(#[from] InvalidTransition),
    #[error("call has ended")]
    Ended,
}

/// The `Call` aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    id: CallId,
    initiator: IdentityId,
    roster: CallRoster,
    phase: CallPhase,
}

impl Call {
    pub fn offer(
        id: CallId,
        initiator: IdentityId,
        offered_to: Vec<IdentityId>,
    ) -> (Self, CallEvent) {
        let call = Call {
            id,
            initiator,
            roster: CallRoster::solo(initiator),
            phase: CallPhase::Ringing {
                offered_to: offered_to.clone(),
            },
        };
        let event = CallEvent::CallOffered {
            call: id,
            by: initiator,
            to: offered_to,
        };
        (call, event)
    }

    pub fn id(&self) -> CallId {
        self.id
    }

    pub fn initiator(&self) -> IdentityId {
        self.initiator
    }

    pub fn phase(&self) -> &CallPhase {
        &self.phase
    }

    pub fn roster(&self) -> &CallRoster {
        &self.roster
    }

    fn take_phase(&mut self) -> CallPhase {
        core::mem::replace(
            &mut self.phase,
            CallPhase::Ended {
                reason: EndReason::Failed,
            },
        )
    }

    fn transition(
        &mut self,
        f: impl FnOnce(CallPhase) -> Result<CallPhase, InvalidTransition>,
    ) -> Result<(), InvalidTransition> {
        let current = self.take_phase();
        match f(current.clone()) {
            Ok(next) => {
                self.phase = next;
                Ok(())
            }
            Err(err) => {
                self.phase = current;
                Err(err)
            }
        }
    }

    pub fn accept(&mut self, by: IdentityId) -> Result<Vec<CallEvent>, CallError> {
        self.transition(CallPhase::accept)?;
        self.roster.add(by)?;
        Ok(vec![
            CallEvent::CallAccepted { call: self.id, by },
            CallEvent::ParticipantJoined {
                call: self.id,
                who: by,
            },
        ])
    }

    pub fn decline(&mut self, by: IdentityId) -> Result<Vec<CallEvent>, CallError> {
        self.transition(CallPhase::decline)?;
        Ok(vec![
            CallEvent::CallDeclined { call: self.id, by },
            CallEvent::CallEnded {
                call: self.id,
                reason: EndReason::Declined,
            },
        ])
    }

    pub fn connected(&mut self) -> Result<(), CallError> {
        self.transition(CallPhase::connected)?;
        Ok(())
    }

    /// Someone joins an active group call.
    pub fn join(&mut self, who: IdentityId) -> Result<CallEvent, CallError> {
        match self.phase {
            CallPhase::Active | CallPhase::Connecting => {
                self.roster.add(who)?;
                Ok(CallEvent::ParticipantJoined { call: self.id, who })
            }
            CallPhase::Ringing { .. } | CallPhase::Ended { .. } => Err(CallError::Ended),
        }
    }

    pub fn leave(&mut self, who: IdentityId) -> Result<Vec<CallEvent>, CallError> {
        self.roster.remove(&who)?;
        let mut events = vec![CallEvent::ParticipantLeft { call: self.id, who }];
        if self.roster.len() <= 1 {
            let current = self.take_phase();
            self.phase = match current {
                CallPhase::Ended { reason } => CallPhase::Ended { reason },
                CallPhase::Ringing { .. } | CallPhase::Connecting | CallPhase::Active => {
                    CallPhase::Ended {
                        reason: EndReason::LastParticipantLeft,
                    }
                }
            };
            events.push(CallEvent::CallEnded {
                call: self.id,
                reason: EndReason::LastParticipantLeft,
            });
        }
        Ok(events)
    }

    pub fn hang_up(&mut self) -> Result<CallEvent, CallError> {
        self.transition(CallPhase::hang_up)?;
        Ok(CallEvent::CallEnded {
            call: self.id,
            reason: EndReason::HungUp,
        })
    }

    pub fn set_screen_sharing(
        &mut self,
        who: IdentityId,
        sharing: bool,
    ) -> Result<CallEvent, CallError> {
        match self.phase {
            CallPhase::Active => {
                self.roster
                    .update_media(&who, |m| m.sharing_screen = sharing)?;
                Ok(if sharing {
                    CallEvent::ScreenShareStarted { call: self.id, who }
                } else {
                    CallEvent::ScreenShareStopped { call: self.id, who }
                })
            }
            CallPhase::Ringing { .. } | CallPhase::Connecting | CallPhase::Ended { .. } => {
                Err(CallError::Ended)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn id(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn active_call() -> Call {
        let (mut call, _) = Call::offer(CallId::from_bytes([1; 16]), id(1), vec![id(2)]);
        call.accept(id(2)).unwrap();
        call.connected().unwrap();
        call
    }

    #[test]
    fn ninth_participant_is_unrepresentable() {
        let mut call = active_call();
        for n in 3..=8 {
            call.join(id(n)).unwrap();
        }
        assert_eq!(call.roster().len(), 8);
        assert_eq!(call.join(id(9)), Err(CallError::Roster(RosterError::Full)));
    }

    #[test]
    fn cannot_accept_twice() {
        let (mut call, _) = Call::offer(CallId::from_bytes([1; 16]), id(1), vec![id(2)]);
        call.accept(id(2)).unwrap();
        assert!(matches!(call.accept(id(3)), Err(CallError::Transition(_))));
    }

    #[test]
    fn ended_call_rejects_everything() {
        let mut call = active_call();
        call.hang_up().unwrap();
        assert!(matches!(call.hang_up(), Err(CallError::Transition(_))));
        assert_eq!(call.join(id(5)), Err(CallError::Ended));
    }

    #[test]
    fn last_leaver_ends_the_call() {
        let mut call = active_call();
        let events = call.leave(id(2)).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(call.phase(), CallPhase::Ended { .. }));
    }

    #[test]
    fn screen_share_only_while_active() {
        let (mut call, _) = Call::offer(CallId::from_bytes([1; 16]), id(1), vec![id(2)]);
        assert_eq!(call.set_screen_sharing(id(1), true), Err(CallError::Ended));
        call.accept(id(2)).unwrap();
        call.connected().unwrap();
        let event = call.set_screen_sharing(id(1), true).unwrap();
        assert!(matches!(event, CallEvent::ScreenShareStarted { .. }));
    }
}
