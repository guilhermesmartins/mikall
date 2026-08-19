//! Calls bounded context.
//!
//! Group calls are a full mesh: with no server there is no SFU, so every
//! participant sends media to every other. The mesh limit (8) is therefore a
//! *domain invariant* enforced by [`CallRoster`]'s construction, not a UI
//! suggestion — a 9th participant is unrepresentable.
//!
//! A call opened with nobody to ring is a *solo stage*: legal, immediately
//! [`CallPhase::Active`] with only its opener, who rings people later via
//! [`Call::invite`]. Staying alone is a choice — a solo stage never ends by
//! "last participant left" while the opener holds it; only hanging up does.

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};

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
    /// A solo stage opened: active immediately with only its opener.
    CallOpened {
        call: CallId,
        by: IdentityId,
    },
    /// Peers rung into an ongoing call (mid-call invite).
    CallInvited {
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
    #[error("peer was not offered this call")]
    NotInvited,
    #[error("everyone named is already in the call or already invited")]
    AlreadyInvited,
    #[error(
        "no free seat: {seated} in the call and {pending} already invited of {max}",
        max = CallRoster::MAX_PARTICIPANTS
    )]
    NoFreeSeat { seated: usize, pending: usize },
}

/// The `Call` aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    id: CallId,
    initiator: IdentityId,
    roster: CallRoster,
    /// Offered-but-not-joined: everyone rung (at offer time or mid-call)
    /// who has neither accepted into the roster nor declined. Accepting
    /// requires membership here — an uninvited accept is unrepresentable.
    invited: BTreeSet<IdentityId>,
    /// Opened with nobody to ring: the opener holds the stage alone by
    /// choice, so "last participant left" never fires while they stay.
    solo_stage: bool,
    phase: CallPhase,
}

impl Call {
    /// Ring `offered_to`. An empty list is a [`Call::open_solo`] — a
    /// ringing call with nobody to answer it is unrepresentable.
    pub fn offer(
        id: CallId,
        initiator: IdentityId,
        offered_to: Vec<IdentityId>,
    ) -> (Self, CallEvent) {
        if offered_to.is_empty() {
            return Self::open_solo(id, initiator);
        }
        let call = Call {
            id,
            initiator,
            roster: CallRoster::solo(initiator),
            invited: offered_to.iter().copied().collect(),
            solo_stage: false,
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

    /// Open a solo stage: active immediately with only the opener, who
    /// rings people later via [`Call::invite`].
    pub fn open_solo(id: CallId, initiator: IdentityId) -> (Self, CallEvent) {
        let call = Call {
            id,
            initiator,
            roster: CallRoster::solo(initiator),
            invited: BTreeSet::new(),
            solo_stage: true,
            phase: CallPhase::Active,
        };
        let event = CallEvent::CallOpened {
            call: id,
            by: initiator,
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

    /// Offered-but-not-joined peers (initial ring plus mid-call invites).
    pub fn invited(&self) -> impl Iterator<Item = IdentityId> + '_ {
        self.invited.iter().copied()
    }

    pub fn is_solo_stage(&self) -> bool {
        self.solo_stage
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

    /// An invited peer picks up. The first pickup moves a ring to
    /// [`CallPhase::Connecting`]; later pickups (a second callee, a
    /// mid-call invitee) land in the already connecting/active call.
    pub fn accept(&mut self, by: IdentityId) -> Result<Vec<CallEvent>, CallError> {
        if matches!(self.phase, CallPhase::Ended { .. }) {
            return Err(CallError::Ended);
        }
        if !self.invited.contains(&by) {
            return Err(CallError::NotInvited);
        }
        self.roster.add(by)?;
        self.invited.remove(&by);
        if matches!(self.phase, CallPhase::Ringing { .. }) {
            self.transition(CallPhase::accept)?;
        }
        Ok(vec![
            CallEvent::CallAccepted { call: self.id, by },
            CallEvent::ParticipantJoined {
                call: self.id,
                who: by,
            },
        ])
    }

    /// An invited peer turns the call down. The call itself dies only when
    /// the decline leaves nobody to wait for: still ringing with no other
    /// callee pending. An invitee declining an ongoing call (or one of
    /// several ringing callees) never tears it down for those in it.
    pub fn decline(&mut self, by: IdentityId) -> Result<Vec<CallEvent>, CallError> {
        if matches!(self.phase, CallPhase::Ended { .. }) {
            return Err(CallError::Ended);
        }
        if !self.invited.remove(&by) {
            return Err(CallError::NotInvited);
        }
        let mut events = vec![CallEvent::CallDeclined { call: self.id, by }];
        if let CallPhase::Ringing { offered_to } = &mut self.phase {
            offered_to.retain(|peer| *peer != by);
        }
        if matches!(self.phase, CallPhase::Ringing { .. }) && self.invited.is_empty() {
            self.transition(CallPhase::decline)?;
            events.push(CallEvent::CallEnded {
                call: self.id,
                reason: EndReason::Declined,
            });
        }
        Ok(events)
    }

    /// Ring more peers into an ongoing call. Peers already in the roster
    /// or already invited are not re-rung; the mesh cap is respected at
    /// invite time counting both seats taken and invites outstanding.
    pub fn invite(&mut self, by: IdentityId, to: &[IdentityId]) -> Result<CallEvent, CallError> {
        match self.phase {
            CallPhase::Connecting | CallPhase::Active => {}
            CallPhase::Ringing { .. } => {
                return Err(CallError::Transition(InvalidTransition {
                    action: "invite",
                    state: self.phase.name(),
                }));
            }
            CallPhase::Ended { .. } => return Err(CallError::Ended),
        }
        let mut fresh: Vec<IdentityId> = Vec::new();
        for peer in to.iter().copied() {
            if !self.roster.contains(&peer)
                && !self.invited.contains(&peer)
                && !fresh.contains(&peer)
            {
                fresh.push(peer);
            }
        }
        if fresh.is_empty() {
            return Err(CallError::AlreadyInvited);
        }
        if self.roster.len() + self.invited.len() + fresh.len() > CallRoster::MAX_PARTICIPANTS {
            return Err(CallError::NoFreeSeat {
                seated: self.roster.len(),
                pending: self.invited.len(),
            });
        }
        self.invited.extend(fresh.iter().copied());
        Ok(CallEvent::CallInvited {
            call: self.id,
            by,
            to: fresh,
        })
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
        // On a solo stage the opener alone is not "last participant left":
        // staying is their choice, hanging up is how they end it.
        let opener_holds_the_stage =
            self.solo_stage && self.roster.len() == 1 && self.roster.contains(&self.initiator);
        if self.roster.len() <= 1 && !opener_holds_the_stage {
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
        // Neither an uninvited peer nor the already-seated callee can accept.
        assert_eq!(call.accept(id(3)), Err(CallError::NotInvited));
        assert_eq!(call.accept(id(2)), Err(CallError::NotInvited));
    }

    #[test]
    fn solo_start_is_active_with_only_the_opener() {
        let (call, event) = Call::offer(CallId::from_bytes([2; 16]), id(1), vec![]);
        assert!(matches!(call.phase(), CallPhase::Active));
        assert_eq!(call.roster().len(), 1);
        assert!(call.roster().contains(&id(1)));
        assert!(call.is_solo_stage());
        assert!(matches!(event, CallEvent::CallOpened { by, .. } if by == id(1)));
    }

    #[test]
    fn solo_stage_survives_its_guest_leaving() {
        let (mut call, _) = Call::open_solo(CallId::from_bytes([2; 16]), id(1));
        call.invite(id(1), &[id(2)]).unwrap();
        call.accept(id(2)).unwrap();
        assert_eq!(call.roster().len(), 2);
        let events = call.leave(id(2)).unwrap();
        assert_eq!(
            events,
            vec![CallEvent::ParticipantLeft {
                call: call.id(),
                who: id(2),
            }]
        );
        assert!(matches!(call.phase(), CallPhase::Active));
        assert_eq!(call.roster().len(), 1);
    }

    #[test]
    fn invitee_decline_leaves_the_call_alive() {
        let (mut call, _) = Call::open_solo(CallId::from_bytes([2; 16]), id(1));
        call.invite(id(1), &[id(2)]).unwrap();
        let events = call.decline(id(2)).unwrap();
        assert_eq!(
            events,
            vec![CallEvent::CallDeclined {
                call: call.id(),
                by: id(2),
            }]
        );
        assert!(matches!(call.phase(), CallPhase::Active));
        assert_eq!(call.invited().count(), 0);
    }

    #[test]
    fn invite_respects_the_mesh_cap() {
        let mut call = active_call();
        for n in 3..=8 {
            call.join(id(n)).unwrap();
        }
        assert_eq!(
            call.invite(id(1), &[id(9)]),
            Err(CallError::NoFreeSeat {
                seated: 8,
                pending: 0,
            })
        );
        // Outstanding invites claim seats too: 1 seated + 7 invited is full.
        let (mut solo, _) = Call::open_solo(CallId::from_bytes([3; 16]), id(1));
        let invitees: Vec<IdentityId> = (2..=8).map(id).collect();
        solo.invite(id(1), &invitees).unwrap();
        assert_eq!(
            solo.invite(id(1), &[id(9)]),
            Err(CallError::NoFreeSeat {
                seated: 1,
                pending: 7,
            })
        );
    }

    #[test]
    fn invite_skips_the_seated_and_already_invited() {
        let mut call = active_call();
        let event = call.invite(id(1), &[id(1), id(2), id(3), id(3)]).unwrap();
        assert!(matches!(
            &event,
            CallEvent::CallInvited { to, .. } if *to == vec![id(3)]
        ));
        assert_eq!(call.invite(id(1), &[id(3)]), Err(CallError::AlreadyInvited));
    }

    #[test]
    fn invite_is_for_ongoing_calls_only() {
        let (mut ringing, _) = Call::offer(CallId::from_bytes([4; 16]), id(1), vec![id(2)]);
        assert!(matches!(
            ringing.invite(id(1), &[id(3)]),
            Err(CallError::Transition(_))
        ));
        let mut ended = active_call();
        ended.hang_up().unwrap();
        assert_eq!(ended.invite(id(1), &[id(3)]), Err(CallError::Ended));
    }

    #[test]
    fn second_callee_of_a_group_ring_seats_after_the_first() {
        let (mut call, _) = Call::offer(CallId::from_bytes([5; 16]), id(1), vec![id(2), id(3)]);
        call.accept(id(2)).unwrap();
        call.connected().unwrap();
        call.accept(id(3)).unwrap();
        assert_eq!(call.roster().len(), 3);
        assert!(matches!(call.phase(), CallPhase::Active));
    }

    #[test]
    fn one_of_two_ringing_callees_declining_keeps_the_ring_alive() {
        let (mut call, _) = Call::offer(CallId::from_bytes([6; 16]), id(1), vec![id(2), id(3)]);
        let events = call.decline(id(2)).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(call.phase(), CallPhase::Ringing { .. }));
        // The last pending callee declining ends the ring.
        let events = call.decline(id(3)).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            call.phase(),
            CallPhase::Ended {
                reason: EndReason::Declined,
            }
        ));
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
