//! The Lamport-stamped hash DAG that gives every node the same view of
//! history without a server (protocol codename: `negi`).
//!
//! Each message links its parents (the DAG heads its author saw). Display
//! order is a topological sort tie-broken by `(lamport, author, id)`, which
//! is deterministic for any arrival order of the same message set — the
//! property the BDD ordering scenarios and proptests pin down.

use std::collections::{BTreeMap, BTreeSet};

use super::message::{Message, MessageId};

/// Outcome of inserting a message into the DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagInsert {
    /// Message applied; any pending descendants unblocked by it were applied
    /// too (their ids are included).
    Applied { unblocked: Vec<MessageId> },
    /// Message is buffered until the listed parents arrive (history gap).
    Pending { missing: Vec<MessageId> },
    /// Already known (applied or pending) — gossip duplicates are normal.
    Duplicate,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageDag {
    applied: BTreeMap<MessageId, Message>,
    /// Messages waiting for missing parents.
    pending: BTreeMap<MessageId, Message>,
    /// missing parent id -> pending children waiting on it.
    waiting_on: BTreeMap<MessageId, BTreeSet<MessageId>>,
    /// Applied messages that no applied message links to yet.
    heads: BTreeSet<MessageId>,
    max_lamport: u64,
}

impl MessageDag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.applied.len()
    }

    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    pub fn contains(&self, id: &MessageId) -> bool {
        self.applied.contains_key(id) || self.pending.contains_key(id)
    }

    pub fn get(&self, id: &MessageId) -> Option<&Message> {
        self.applied.get(id)
    }

    /// Current heads — the parents a newly authored message should link.
    pub fn heads(&self) -> Vec<MessageId> {
        self.heads.iter().copied().collect()
    }

    /// Highest Lamport counter observed among applied messages.
    pub fn max_lamport(&self) -> u64 {
        self.max_lamport
    }

    /// Parents referenced by buffered messages that we still don't have —
    /// what a sync request should fetch.
    pub fn missing_parents(&self) -> Vec<MessageId> {
        self.waiting_on.keys().copied().collect()
    }

    pub fn insert(&mut self, message: Message) -> DagInsert {
        let id = message.id();
        if self.contains(&id) {
            return DagInsert::Duplicate;
        }

        let missing: Vec<MessageId> = message
            .parents()
            .iter()
            .filter(|p| !self.applied.contains_key(*p))
            .copied()
            .collect();

        if !missing.is_empty() {
            for parent in &missing {
                self.waiting_on.entry(*parent).or_default().insert(id);
            }
            self.pending.insert(id, message);
            return DagInsert::Pending { missing };
        }

        self.apply(message);
        let unblocked = self.drain_unblocked(id);
        DagInsert::Applied { unblocked }
    }

    fn apply(&mut self, message: Message) {
        let id = message.id();
        for parent in message.parents() {
            self.heads.remove(parent);
        }
        // A head only if nothing already applied links to it (possible when a
        // child arrived first and was pending).
        self.heads.insert(id);
        self.max_lamport = self.max_lamport.max(message.lamport());
        self.applied.insert(id, message);
        self.debug_check_invariants();
    }

    /// After `arrived` was applied, cascade-apply pending messages whose
    /// parents are now all present. Returns applied ids in application order.
    fn drain_unblocked(&mut self, arrived: MessageId) -> Vec<MessageId> {
        let mut applied_now = Vec::new();
        let mut queue = vec![arrived];
        while let Some(parent) = queue.pop() {
            let Some(waiters) = self.waiting_on.remove(&parent) else {
                continue;
            };
            for waiter_id in waiters {
                let ready = self
                    .pending
                    .get(&waiter_id)
                    .is_some_and(|m| m.parents().iter().all(|p| self.applied.contains_key(p)));
                if ready {
                    if let Some(message) = self.pending.remove(&waiter_id) {
                        self.apply(message);
                        applied_now.push(waiter_id);
                        queue.push(waiter_id);
                    }
                } else if self.pending.contains_key(&waiter_id) {
                    // Still blocked on some other parent: re-register under
                    // each still-missing parent.
                    let still_missing: Vec<MessageId> = self
                        .pending
                        .get(&waiter_id)
                        .map(|m| {
                            m.parents()
                                .iter()
                                .filter(|p| !self.applied.contains_key(*p))
                                .copied()
                                .collect()
                        })
                        .unwrap_or_default();
                    for missing in still_missing {
                        self.waiting_on
                            .entry(missing)
                            .or_default()
                            .insert(waiter_id);
                    }
                }
            }
        }
        applied_now
    }

    /// Deterministic linearization: Kahn's topological sort with the ready
    /// set ordered by `(lamport, author, id)`.
    pub fn linearize(&self) -> Vec<&Message> {
        let mut indegree: BTreeMap<MessageId, usize> = BTreeMap::new();
        let mut children: BTreeMap<MessageId, Vec<MessageId>> = BTreeMap::new();
        for (id, message) in &self.applied {
            let applied_parents = message
                .parents()
                .iter()
                .filter(|p| self.applied.contains_key(*p))
                .count();
            indegree.insert(*id, applied_parents);
            for parent in message.parents() {
                if self.applied.contains_key(parent) {
                    children.entry(*parent).or_default().push(*id);
                }
            }
        }

        let mut ready: BTreeSet<(u64, crate::shared::IdentityId, MessageId)> = indegree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .filter_map(|(id, _)| self.applied.get(id).map(Message::order_key))
            .collect();

        let mut out = Vec::with_capacity(self.applied.len());
        while let Some(key) = ready.iter().next().copied() {
            ready.remove(&key);
            let (_, _, id) = key;
            if let Some(message) = self.applied.get(&id) {
                out.push(message);
            }
            for child in children.get(&id).into_iter().flatten() {
                if let Some(deg) = indegree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        if let Some(m) = self.applied.get(child) {
                            ready.insert(m.order_key());
                        }
                    }
                }
            }
        }
        out
    }

    fn debug_check_invariants(&self) {
        #[cfg(debug_assertions)]
        {
            // Every head is applied and no applied message has an applied
            // child pointing at it.
            for head in &self.heads {
                debug_assert!(self.applied.contains_key(head), "head not applied");
            }
            let mut linked: BTreeSet<MessageId> = BTreeSet::new();
            for message in self.applied.values() {
                for parent in message.parents() {
                    linked.insert(*parent);
                }
            }
            for head in &self.heads {
                debug_assert!(!linked.contains(head), "head {head:?} has an applied child");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::messaging::values::MessageBody;
    use crate::shared::IdentityId;
    use proptest::prelude::*;

    fn author(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    fn mid(n: u8) -> MessageId {
        MessageId::from_bytes([n; 32])
    }

    fn msg(id: u8, by: u8, lamport: u64, parents: Vec<MessageId>) -> Message {
        Message::authored(
            mid(id),
            author(by),
            lamport,
            parents,
            MessageBody::parse(&format!("m{id}")).unwrap(),
            0,
        )
    }

    #[test]
    fn genesis_message_becomes_head() {
        let mut dag = MessageDag::new();
        assert_eq!(
            dag.insert(msg(1, 1, 1, vec![])),
            DagInsert::Applied { unblocked: vec![] }
        );
        assert_eq!(dag.heads(), vec![mid(1)]);
        assert_eq!(dag.max_lamport(), 1);
    }

    #[test]
    fn duplicate_is_reported() {
        let mut dag = MessageDag::new();
        dag.insert(msg(1, 1, 1, vec![]));
        assert_eq!(dag.insert(msg(1, 1, 1, vec![])), DagInsert::Duplicate);
    }

    #[test]
    fn out_of_order_arrival_buffers_then_applies() {
        let mut dag = MessageDag::new();
        let child = msg(2, 1, 2, vec![mid(1)]);
        assert_eq!(
            dag.insert(child),
            DagInsert::Pending {
                missing: vec![mid(1)]
            }
        );
        assert_eq!(dag.missing_parents(), vec![mid(1)]);
        assert_eq!(
            dag.insert(msg(1, 1, 1, vec![])),
            DagInsert::Applied {
                unblocked: vec![mid(2)]
            }
        );
        assert_eq!(dag.heads(), vec![mid(2)]);
        assert!(dag.missing_parents().is_empty());
        assert_eq!(dag.len(), 2);
    }

    #[test]
    fn deep_pending_chain_cascades() {
        let mut dag = MessageDag::new();
        dag.insert(msg(3, 1, 3, vec![mid(2)]));
        dag.insert(msg(2, 1, 2, vec![mid(1)]));
        let result = dag.insert(msg(1, 1, 1, vec![]));
        assert_eq!(
            result,
            DagInsert::Applied {
                unblocked: vec![mid(2), mid(3)]
            }
        );
        assert_eq!(dag.heads(), vec![mid(3)]);
    }

    #[test]
    fn concurrent_messages_merge_at_next_message() {
        let mut dag = MessageDag::new();
        dag.insert(msg(1, 1, 1, vec![]));
        dag.insert(msg(2, 2, 2, vec![mid(1)]));
        dag.insert(msg(3, 3, 2, vec![mid(1)])); // concurrent with 2
        assert_eq!(dag.heads().len(), 2);
        dag.insert(msg(4, 1, 3, vec![mid(2), mid(3)]));
        assert_eq!(dag.heads(), vec![mid(4)]);
        let order: Vec<MessageId> = dag.linearize().iter().map(|m| m.id()).collect();
        assert_eq!(order, vec![mid(1), mid(2), mid(3), mid(4)]);
    }

    #[test]
    fn linearize_ties_break_by_author_then_id() {
        let mut dag = MessageDag::new();
        dag.insert(msg(9, 2, 1, vec![]));
        dag.insert(msg(5, 1, 1, vec![]));
        let order: Vec<MessageId> = dag.linearize().iter().map(|m| m.id()).collect();
        // Same lamport: author 1 sorts before author 2.
        assert_eq!(order, vec![mid(5), mid(9)]);
    }

    proptest! {
        /// The core decentralization property: any arrival order of the same
        /// message set linearizes identically.
        #[test]
        fn linearization_is_arrival_order_independent(seed in 0u64..1000) {
            // Build a fixed little DAG: root, two concurrent children, a merge,
            // and a second author chain.
            let messages = vec![
                msg(1, 1, 1, vec![]),
                msg(2, 1, 2, vec![mid(1)]),
                msg(3, 2, 2, vec![mid(1)]),
                msg(4, 2, 3, vec![mid(2), mid(3)]),
                msg(5, 1, 4, vec![mid(4)]),
                msg(6, 3, 1, vec![]),
            ];

            // Reference order: insert in definition order.
            let mut reference = MessageDag::new();
            for m in &messages {
                reference.insert(m.clone());
            }
            let expected: Vec<MessageId> =
                reference.linearize().iter().map(|m| m.id()).collect();

            // Shuffled arrival driven by the seed (deterministic xorshift).
            let mut order: Vec<usize> = (0..messages.len()).collect();
            let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            for i in (1..order.len()).rev() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let j = (state as usize) % (i + 1);
                order.swap(i, j);
            }

            let mut shuffled = MessageDag::new();
            for idx in order {
                shuffled.insert(messages[idx].clone());
            }
            let got: Vec<MessageId> = shuffled.linearize().iter().map(|m| m.id()).collect();
            prop_assert_eq!(got, expected);
        }
    }
}
