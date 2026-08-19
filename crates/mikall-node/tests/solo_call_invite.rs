//! Integration: solo stage + mid-call invites between two real nodes.
//! A opens a call with nobody else there (active immediately, alone),
//! rings B mid-call, B accepts — both converge on Active with 2 and share
//! the media key carried in the invite's offer envelope. B leaving does
//! not end A's stage; a re-ring that B declines does not either.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::events::AppEvent;
use mikall_domain::calls::{CallEvent, CallPhase};
use mikall_domain::DomainEvent;
use mikall_net::NetConfig;
use mikall_node::{start, NodeConfig, NodeHandle};

fn test_config(dir: &Path) -> NodeConfig {
    NodeConfig {
        data_dir: dir.to_path_buf(),
        net: NetConfig {
            listen: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            enable_mdns: false,
        },
        // Tests drive media pipelines by hand; never open real devices.
        call_audio: false,
        call_video: false,
    }
}

async fn connect(a: &NodeHandle, b: &NodeHandle) {
    let addrs = a.net.listen_addrs().await;
    let target = format!("{}/p2p/{}", addrs[0], a.net.local_peer_id);
    b.net.dial(target.parse().unwrap()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn solo_stage_and_mid_call_invite() {
    let base = std::env::temp_dir().join(format!("mikall-solo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let alice = start(test_config(&base.join("a"))).await.unwrap();
    let bob = start(test_config(&base.join("b"))).await.unwrap();
    connect(&alice, &bob).await;

    let mut bob_events = bob.subscribe_events();

    // A opens a solo stage: no offers, active immediately, alone.
    let call = alice.calls.start_call(vec![]).await;
    let a = alice.calls.snapshot(call).await.unwrap();
    assert!(matches!(a.phase, CallPhase::Active), "{a:?}");
    assert_eq!(a.participants.len(), 1);
    assert!(a.invited.is_empty());

    // A rings B mid-call; A's aggregate tracks the outstanding invite.
    alice
        .calls
        .invite(call, vec![bob.identity.local_id()])
        .await
        .unwrap();
    let a = alice.calls.snapshot(call).await.unwrap();
    assert_eq!(a.invited, vec![bob.identity.local_id()]);
    assert!(matches!(a.phase, CallPhase::Active));

    // B's incoming-offer flow is the ordinary one.
    let offered = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), bob_events.recv())
            .await
            .expect("no call offer arrived")
            .unwrap();
        if let AppEvent::Domain(DomainEvent::Calls(CallEvent::CallOffered { call, by, .. })) = event
        {
            assert_eq!(by, alice.identity.local_id());
            break call;
        }
    };
    assert_eq!(offered, call);

    // B accepts; both sides converge on Active with 2 participants.
    bob.calls.accept_incoming(call).await.unwrap();
    for _ in 0..100 {
        let a = alice.calls.snapshot(call).await.unwrap();
        let b = bob.calls.snapshot(call).await.unwrap();
        if matches!(a.phase, CallPhase::Active)
            && matches!(b.phase, CallPhase::Active)
            && a.participants.len() == 2
            && b.participants.len() == 2
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let a = alice.calls.snapshot(call).await.unwrap();
    assert!(matches!(a.phase, CallPhase::Active));
    assert_eq!(a.participants.len(), 2, "{a:?}");
    assert!(a.invited.is_empty(), "invite must clear on accept: {a:?}");

    // The invitee holds the same per-call AEAD key (carried in the offer
    // the invite sent), so media would seal/unseal exactly as in M10.
    assert_eq!(
        alice.calls.media_key(call).await.unwrap(),
        bob.calls.media_key(call).await.unwrap()
    );

    // B hangs up. A opened this stage solo — being alone again is not
    // "last participant left"; the stage stays active.
    bob.calls.hang_up_all(call).await.unwrap();
    for _ in 0..100 {
        let a = alice.calls.snapshot(call).await.unwrap();
        if a.participants.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let a = alice.calls.snapshot(call).await.unwrap();
    assert!(matches!(a.phase, CallPhase::Active), "{a:?}");
    assert_eq!(a.participants.len(), 1);

    // A rings B again; this time B declines. The stage still stands.
    alice
        .calls
        .invite(call, vec![bob.identity.local_id()])
        .await
        .unwrap();
    for _ in 0..100 {
        if matches!(
            bob.calls.snapshot(call).await.unwrap().phase,
            CallPhase::Ringing { .. }
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bob.calls.decline_incoming(call).await.unwrap();
    for _ in 0..100 {
        if alice.calls.snapshot(call).await.unwrap().invited.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let a = alice.calls.snapshot(call).await.unwrap();
    assert!(
        matches!(a.phase, CallPhase::Active),
        "invitee decline must not end the stage: {a:?}"
    );
    assert_eq!(a.participants.len(), 1);
    assert!(a.invited.is_empty());

    // Hanging up is how the opener ends their own stage.
    alice.calls.hang_up_all(call).await.unwrap();
    assert!(matches!(
        alice.calls.snapshot(call).await.unwrap().phase,
        CallPhase::Ended { .. }
    ));

    alice.shutdown();
    bob.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
