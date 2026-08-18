//! Cucumber runner: the Gherkin features under `tests/features/` drive the
//! application services over the in-memory universe from `mikall_bdd`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use cucumber::{given, then, when, World};

use mikall_app::events::AppEvent;
use mikall_app::services::{CallServiceError, ChatError, TransferServiceError};
use mikall_bdd::{spawn_node, InMemoryDirectory, InMemoryNetwork, TestNode};
use mikall_domain::calls::{CallError, CallId, CallPhase, RosterError};
use mikall_domain::identity::{IdentityEvent, TrustLevel};
use mikall_domain::messaging::{ChannelName, MessageId, MessagingEvent, Role};
use mikall_domain::presence::PresenceState;
use mikall_domain::shared::{Fingerprint, IdentityId};
use mikall_domain::transfer::{
    BlobHash, FileManifest, FileName, TransferId, TransferPhase, CHUNK_SIZE,
};
use mikall_domain::DomainEvent;

#[derive(cucumber::World)]
#[world(init = Self::fresh)]
struct MikallWorld {
    network: Arc<InMemoryNetwork>,
    directory: Arc<InMemoryDirectory>,
    nodes: BTreeMap<String, TestNode>,
    last_post: Option<Result<MessageId, ChatError>>,
    last_topic: Option<Result<(), ChatError>>,
    call: Option<CallId>,
    call_node: Option<String>,
    call_peers: Vec<IdentityId>,
    transfer: Option<TransferId>,
    transfer_node: Option<String>,
}

impl std::fmt::Debug for MikallWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MikallWorld")
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl MikallWorld {
    fn fresh() -> Self {
        MikallWorld {
            network: InMemoryNetwork::new(),
            directory: Arc::new(InMemoryDirectory::default()),
            nodes: BTreeMap::new(),
            last_post: None,
            last_topic: None,
            call: None,
            call_node: None,
            call_peers: Vec::new(),
            transfer: None,
            transfer_node: None,
        }
    }

    async fn ensure_node(&mut self, name: &str) {
        if !self.nodes.contains_key(name) {
            let node = spawn_node(&self.network, &self.directory, name).await;
            self.nodes.insert(name.to_owned(), node);
        }
    }

    fn node(&self, name: &str) -> &TestNode {
        self.nodes
            .get(name)
            .unwrap_or_else(|| panic!("node {name} not spawned"))
    }

    fn node_mut(&mut self, name: &str) -> &mut TestNode {
        self.nodes
            .get_mut(name)
            .unwrap_or_else(|| panic!("node {name} not spawned"))
    }

    fn id_of(&self, name: &str) -> IdentityId {
        self.node(name).id
    }

    /// A deterministic identity for a peer that exists only as a call/DM
    /// counterpart (never spawned as a full node).
    fn synthetic_id(tag: &str, n: u8) -> IdentityId {
        let mut bytes = [n; 32];
        for (slot, byte) in bytes.iter_mut().zip(tag.bytes()) {
            *slot ^= byte;
        }
        IdentityId::from_bytes(bytes)
    }
}

fn channel(name: &str) -> ChannelName {
    ChannelName::parse(name).unwrap()
}

fn manifest(name: &str, chunks: u64) -> FileManifest {
    FileManifest::new(
        FileName::parse(name).unwrap(),
        CHUNK_SIZE * chunks,
        BlobHash::from_bytes([0xAA; 32]),
        (0..chunks)
            .map(|i| BlobHash::from_bytes([i as u8; 32]))
            .collect(),
    )
    .unwrap()
}

// ---------------------------------------------------------------- givens --

#[given(expr = "a fresh node {string}")]
async fn fresh_node(world: &mut MikallWorld, name: String) {
    world.ensure_node(&name).await;
}

#[given(expr = "peers {string} and {string} are online")]
async fn two_online(world: &mut MikallWorld, a: String, b: String) {
    world.ensure_node(&a).await;
    world.ensure_node(&b).await;
}

#[given(expr = "peers {string} and {string} are online and joined {string}")]
async fn two_joined(world: &mut MikallWorld, a: String, b: String, chan: String) {
    world.ensure_node(&a).await;
    world.ensure_node(&b).await;
    for name in [&a, &b] {
        world
            .node(name)
            .chat
            .join_channel(channel(&chan))
            .await
            .unwrap();
    }
}

#[given(expr = "peers {string}, {string} and {string} are online and joined {string}")]
async fn three_joined(world: &mut MikallWorld, a: String, b: String, c: String, chan: String) {
    for name in [&a, &b, &c] {
        world.ensure_node(name).await;
        world
            .node(name)
            .chat
            .join_channel(channel(&chan))
            .await
            .unwrap();
    }
}

#[given(expr = "network delivery between {string} and {string} is delayed")]
async fn delay_link(world: &mut MikallWorld, a: String, b: String) {
    let (ida, idb) = (world.id_of(&a), world.id_of(&b));
    world.network.delay_link(ida, idb).await;
}

#[given(expr = "{string} was offered the file {string} of {int} chunks by {string}")]
async fn offered_file(
    world: &mut MikallWorld,
    who: String,
    file: String,
    chunks: u64,
    from: String,
) {
    world.ensure_node(&who).await;
    let from_id = MikallWorld::synthetic_id(&from, 0x77);
    let id = world
        .node(&who)
        .transfer
        .offered_to_us(from_id, manifest(&file, chunks))
        .await;
    world.transfer = Some(id);
    world.transfer_node = Some(who);
}

// ----------------------------------------------------------------- whens --

#[when(expr = "{string} posts {string} in {string}")]
async fn posts(world: &mut MikallWorld, who: String, text: String, chan: String) {
    world
        .node(&who)
        .chat
        .post_message(&channel(&chan), &text)
        .await
        .unwrap();
}

#[when(expr = "{string} tries to post {string} in {string}")]
async fn tries_to_post(world: &mut MikallWorld, who: String, text: String, chan: String) {
    let result = world
        .node(&who)
        .chat
        .post_message(&channel(&chan), &text)
        .await;
    world.last_post = Some(result);
}

#[when(expr = "{string} tries to post an IRC injection payload in {string}")]
async fn tries_injection(world: &mut MikallWorld, who: String, chan: String) {
    let result = world
        .node(&who)
        .chat
        .post_message(&channel(&chan), "hi\r\nJOIN #evil")
        .await;
    world.last_post = Some(result);
}

#[when(expr = "{string} sends the direct message {string} to {string}")]
async fn sends_dm(world: &mut MikallWorld, from: String, text: String, to: String) {
    let to_id = world.id_of(&to);
    // Blocked recipients yield an error; scenarios that block assert on the
    // receiving side, so both outcomes are acceptable here.
    let _ = world.node(&from).dm.send_dm(to_id, &text).await;
}

#[when(expr = "{string} verifies {string}'s fingerprint out of band")]
async fn verifies_fingerprint(world: &mut MikallWorld, who: String, target: String) {
    let target_id = world.id_of(&target);
    world
        .node(&who)
        .identity
        .verify_contact(&target_id)
        .await
        .unwrap();
}

#[when(expr = "{string} observes a changed key for {string}")]
async fn observes_key_change(world: &mut MikallWorld, who: String, target: String) {
    let target_id = world.id_of(&target);
    let rotated = Fingerprint::from_bytes([0xE7; 32]);
    world
        .node(&who)
        .identity
        .observe_peer(target_id, rotated, None)
        .await;
}

#[when(expr = "{string} blocks {string}")]
async fn blocks(world: &mut MikallWorld, who: String, target: String) {
    let target_id = world.id_of(&target);
    world.node(&who).identity.block(target_id).await;
}

#[when(
    expr = "network delivery between {string} and {string} is restored and held traffic is flushed"
)]
async fn restore_and_flush(world: &mut MikallWorld, a: String, b: String) {
    let (ida, idb) = (world.id_of(&a), world.id_of(&b));
    world.network.restore_link(ida, idb).await;
    world.network.flush_held().await;
}

#[when(expr = "network delivery between {string} and {string} is restored without flushing")]
async fn restore_no_flush(world: &mut MikallWorld, a: String, b: String) {
    let (ida, idb) = (world.id_of(&a), world.id_of(&b));
    world.network.restore_link(ida, idb).await;
}

#[when(expr = "held traffic is flushed")]
async fn flush_held(world: &mut MikallWorld) {
    world.network.flush_held().await;
}

#[when(expr = "{string} sets the topic of {string} to {string}")]
async fn sets_topic(world: &mut MikallWorld, who: String, chan: String, topic: String) {
    world
        .node(&who)
        .chat
        .set_topic(&channel(&chan), &topic)
        .await
        .unwrap();
}

#[when(expr = "{string} tries to set the topic of {string} to {string}")]
async fn tries_topic(world: &mut MikallWorld, who: String, chan: String, topic: String) {
    let result = world
        .node(&who)
        .chat
        .set_topic(&channel(&chan), &topic)
        .await;
    world.last_topic = Some(result);
}

#[when(expr = "{string} grants operator status to {string} in {string}")]
async fn grants_op(world: &mut MikallWorld, who: String, target: String, chan: String) {
    let target_id = world.id_of(&target);
    world
        .node(&who)
        .chat
        .set_role(&channel(&chan), target_id, Role::Op)
        .await
        .unwrap();
}

#[when(expr = "{string} sets away with message {string}")]
async fn sets_away(world: &mut MikallWorld, who: String, message: String) {
    world
        .node(&who)
        .presence
        .set_away(Some(&message))
        .await
        .unwrap();
}

#[when(expr = "{string} clears away")]
async fn clears_away(world: &mut MikallWorld, who: String) {
    world.node(&who).presence.set_away(None).await.unwrap();
}

#[when(expr = "{string} starts a call and {int} peers join it")]
async fn call_with_joiners(world: &mut MikallWorld, who: String, joiners: u8) {
    world.ensure_node(&who).await;
    let calls = Arc::clone(&world.node(&who).calls);
    let call = calls.start_call(vec![]).await;
    world.call_peers = (0..joiners)
        .map(|n| MikallWorld::synthetic_id("callpeer", n + 100))
        .collect();
    calls.accept(call, world.call_peers[0]).await.unwrap();
    calls.mark_connected(call).await.unwrap();
    for peer in &world.call_peers[1..joiners as usize] {
        calls.join(call, *peer).await.unwrap();
    }
    world.call = Some(call);
    world.call_node = Some(who);
}

#[when(expr = "{string} starts a call to {string}")]
async fn call_to(world: &mut MikallWorld, who: String, callee: String) {
    world.ensure_node(&who).await;
    let callee_id = MikallWorld::synthetic_id(&callee, 0x33);
    let call = world.node(&who).calls.start_call(vec![callee_id]).await;
    world.call = Some(call);
    world.call_node = Some(who);
    world.call_peers = vec![callee_id];
}

#[when(expr = "the callee declines the call")]
async fn callee_declines(world: &mut MikallWorld) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let callee = world.call_peers[0];
    world.node(&node).calls.decline(call, callee).await.unwrap();
}

#[when(expr = "the callee accepts the call")]
async fn callee_accepts(world: &mut MikallWorld) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let callee = world.call_peers[0];
    world.node(&node).calls.accept(call, callee).await.unwrap();
}

#[when(expr = "{string} accepts the transfer")]
async fn accepts_transfer(world: &mut MikallWorld, who: String) {
    let id = world.transfer.unwrap();
    world.node(&who).transfer.accept(id).await.unwrap();
}

#[when(expr = "{string} rejects the transfer")]
async fn rejects_transfer(world: &mut MikallWorld, who: String) {
    let id = world.transfer.unwrap();
    world.node(&who).transfer.reject(id).await.unwrap();
}

#[when(expr = "chunk {int} arrives and verifies")]
async fn chunk_verifies(world: &mut MikallWorld, index: u32) {
    let id = world.transfer.unwrap();
    let node = world.transfer_node.clone().unwrap();
    world
        .node(&node)
        .transfer
        .chunk_verified(id, index, true)
        .await
        .unwrap();
}

#[when(expr = "chunk {int} arrives corrupted")]
async fn chunk_corrupted(world: &mut MikallWorld, index: u32) {
    let id = world.transfer.unwrap();
    let node = world.transfer_node.clone().unwrap();
    let result = world
        .node(&node)
        .transfer
        .chunk_verified(id, index, false)
        .await;
    assert!(result.is_err(), "corrupt chunk must be rejected");
}

// ----------------------------------------------------------------- thens --

#[then(expr = "{string} has a fingerprint of eight groups of four characters")]
async fn fingerprint_format(world: &mut MikallWorld, who: String) {
    let display = world.node(&who).identity.fingerprint().display_groups();
    assert_eq!(display.split('-').count(), 8, "{display}");
    assert!(display.split('-').all(|g| g.len() == 4), "{display}");
}

#[then(expr = "{string} trusts {string} at level {string}")]
async fn trust_level(world: &mut MikallWorld, who: String, target: String, level: String) {
    let target_id = world.id_of(&target);
    let trust = world.node(&who).identity.trust_of(&target_id).await;
    let ok = match level.as_str() {
        "tofu" => matches!(trust, Some(TrustLevel::TofuPinned { .. })),
        "verified" => matches!(trust, Some(TrustLevel::Verified { .. })),
        "unverified" => matches!(trust, Some(TrustLevel::Unverified)),
        "blocked" => matches!(trust, Some(TrustLevel::Blocked)),
        other => panic!("unknown trust level {other}"),
    };
    assert!(ok, "expected {level}, got {trust:?}");
}

#[then(expr = "{string} saw a key-change alarm for {string}")]
async fn saw_key_change(world: &mut MikallWorld, who: String, target: String) {
    let target_id = world.id_of(&target);
    let events = world.node_mut(&who).drain_events().to_vec();
    let seen = events.iter().any(|e| {
        matches!(
            e,
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::ContactKeyChanged {
                id, ..
            })) if *id == target_id
        )
    });
    assert!(seen, "no ContactKeyChanged event for {target}");
}

#[then(expr = "{string} has no direct messages from {string}")]
async fn no_dms_from(world: &mut MikallWorld, who: String, from: String) {
    let from_id = world.id_of(&from);
    let history = world.node(&who).dm.history(from_id).await.unwrap();
    assert!(history.is_empty(), "expected empty DM history: {history:?}");
}

#[then(expr = "{string} has the direct message {string} from {string}")]
async fn has_dm(world: &mut MikallWorld, who: String, text: String, from: String) {
    let from_id = world.id_of(&from);
    let history = world.node(&who).dm.history(from_id).await.unwrap();
    assert!(
        history
            .iter()
            .any(|m| m.body.as_str() == text && m.author == from_id),
        "DM {text:?} from {from} not found in {history:?}"
    );
}

#[then(expr = "{string} sees {int} message(s) in the DM thread with {string}")]
async fn dm_count(world: &mut MikallWorld, who: String, count: usize, with: String) {
    let with_id = world.id_of(&with);
    let history = world.node(&who).dm.history(with_id).await.unwrap();
    assert_eq!(history.len(), count, "{history:?}");
}

#[then(expr = "{string} sees the DM history with {string} as {string}")]
async fn dm_history_is(world: &mut MikallWorld, who: String, with: String, expected: String) {
    let with_id = world.id_of(&with);
    let history = world.node(&who).dm.history(with_id).await.unwrap();
    let got: Vec<&str> = history.iter().map(|m| m.body.as_str()).collect();
    let want: Vec<&str> = expected.split(", ").collect();
    assert_eq!(got, want);
}

#[then(expr = "{string} sees {string} in {string} from {string}")]
async fn sees_message(
    world: &mut MikallWorld,
    who: String,
    text: String,
    chan: String,
    from: String,
) {
    let from_id = world.id_of(&from);
    let history = world
        .node(&who)
        .chat
        .history(&channel(&chan))
        .await
        .unwrap();
    assert!(
        history
            .iter()
            .any(|m| m.body.as_str() == text && m.author == from_id),
        "{text:?} from {from} not in {chan}: {history:?}"
    );
}

#[then(expr = "{string} is the founder of {string}")]
async fn is_founder(world: &mut MikallWorld, who: String, chan: String) {
    let id = world.id_of(&who);
    let members = world
        .node(&who)
        .chat
        .members(&channel(&chan))
        .await
        .unwrap();
    assert!(members
        .iter()
        .any(|m| m.id == id && m.role == Role::Founder));
}

#[then(expr = "{string} is a member of {string}")]
async fn is_member(world: &mut MikallWorld, who: String, chan: String) {
    let id = world.id_of(&who);
    let members = world
        .node(&who)
        .chat
        .members(&channel(&chan))
        .await
        .unwrap();
    assert!(members.iter().any(|m| m.id == id));
}

#[then(expr = "the topic change is rejected")]
async fn topic_rejected(world: &mut MikallWorld) {
    assert!(matches!(world.last_topic.take(), Some(Err(_))));
}

#[then(expr = "the message is rejected")]
async fn message_rejected(world: &mut MikallWorld) {
    assert!(matches!(world.last_post.take(), Some(Err(_))));
}

#[then(expr = "{string} sees the topic {string} for {string}")]
async fn sees_topic(world: &mut MikallWorld, who: String, topic: String, chan: String) {
    let got = world.node(&who).chat.topic(&channel(&chan)).await.unwrap();
    assert_eq!(got.as_str(), topic);
}

#[then(expr = "all peers display the same history for {string}")]
async fn same_history(world: &mut MikallWorld, chan: String) {
    let mut histories = Vec::new();
    let names: Vec<String> = world.nodes.keys().cloned().collect();
    for name in names {
        if let Ok(history) = world.node(&name).chat.history(&channel(&chan)).await {
            let ids: Vec<MessageId> = history.iter().map(|m| m.id).collect();
            histories.push((name, ids));
        }
    }
    assert!(histories.len() >= 2, "need at least two joined peers");
    let (first_name, reference) = &histories[0];
    for (name, ids) in &histories[1..] {
        assert_eq!(
            ids, reference,
            "{name} and {first_name} disagree on {chan} order"
        );
    }
}

#[then(expr = "{string} sees {int} message(s) in {string}")]
async fn message_count(world: &mut MikallWorld, who: String, count: usize, chan: String) {
    let history = world
        .node(&who)
        .chat
        .history(&channel(&chan))
        .await
        .unwrap();
    assert_eq!(history.len(), count, "{history:?}");
}

#[then(expr = "{string} reports a history gap in {string}")]
async fn reports_gap(world: &mut MikallWorld, who: String, chan: String) {
    let chan_name = channel(&chan);
    let _ = chan_name;
    let events = world.node_mut(&who).drain_events().to_vec();
    let seen = events.iter().any(|e| {
        matches!(
            e,
            AppEvent::Domain(DomainEvent::Messaging(
                MessagingEvent::HistoryGapDetected { .. }
            ))
        )
    });
    assert!(seen, "no HistoryGapDetected event on {who}");
}

#[then(expr = "{string} sees {string} as away with message {string}")]
async fn sees_away(world: &mut MikallWorld, who: String, target: String, message: String) {
    let target_id = world.id_of(&target);
    let state = world.node(&who).presence.state_of(&target_id).await;
    match state {
        PresenceState::Away { message: Some(msg) } => assert_eq!(msg.as_str(), message),
        other => panic!("expected away with message, got {other:?}"),
    }
}

#[then(expr = "{string} sees {string} as online")]
async fn sees_online(world: &mut MikallWorld, who: String, target: String) {
    let target_id = world.id_of(&target);
    let state = world.node(&who).presence.state_of(&target_id).await;
    assert_eq!(state, PresenceState::Online);
}

#[then(expr = "the call has {int} participants")]
async fn call_participants(world: &mut MikallWorld, count: usize) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let snapshot = world.node(&node).calls.snapshot(call).await.unwrap();
    assert_eq!(snapshot.participants.len(), count);
}

#[then(expr = "a 9th participant is rejected with the mesh-limit error")]
async fn ninth_rejected(world: &mut MikallWorld) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let ninth = MikallWorld::synthetic_id("callpeer", 250);
    let result = world.node(&node).calls.join(call, ninth).await;
    assert_eq!(
        result,
        Err(CallServiceError::Call(CallError::Roster(RosterError::Full)))
    );
}

#[then(expr = "the call is ended")]
async fn call_ended(world: &mut MikallWorld) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let snapshot = world.node(&node).calls.snapshot(call).await.unwrap();
    assert!(matches!(snapshot.phase, CallPhase::Ended { .. }));
}

#[then(expr = "accepting the call again is rejected")]
async fn accept_again_rejected(world: &mut MikallWorld) {
    let call = world.call.unwrap();
    let node = world.call_node.clone().unwrap();
    let extra = MikallWorld::synthetic_id("callpeer", 251);
    let result = world.node(&node).calls.accept(call, extra).await;
    assert!(matches!(result, Err(CallServiceError::Call(_))));
}

#[then(expr = "the transfer is complete")]
async fn transfer_complete(world: &mut MikallWorld) {
    let id = world.transfer.unwrap();
    let node = world.transfer_node.clone().unwrap();
    let progress = world.node(&node).transfer.progress(id).await.unwrap();
    assert_eq!(progress.phase, TransferPhase::Complete);
    assert_eq!(progress.have_chunks, progress.total_chunks);
}

#[then(expr = "the transfer reports {int} missing chunk(s)")]
async fn transfer_missing(world: &mut MikallWorld, count: usize) {
    let id = world.transfer.unwrap();
    let node = world.transfer_node.clone().unwrap();
    let missing = world.node(&node).transfer.missing_chunks(id).await.unwrap();
    assert_eq!(missing.len(), count, "{missing:?}");
}

#[then(expr = "accepting the transfer afterwards is rejected")]
async fn accept_after_reject(world: &mut MikallWorld) {
    let id = world.transfer.unwrap();
    let node = world.transfer_node.clone().unwrap();
    let result = world.node(&node).transfer.accept(id).await;
    assert!(matches!(result, Err(TransferServiceError::Transfer(_))));
}

#[tokio::main]
async fn main() {
    MikallWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit("tests/features")
        .await;
}
