//! Integration: call signaling and sealed voice frames between two real
//! nodes. A rings B, B accepts (both aggregates go Active), then A streams
//! a sine tone through the media pipeline — sealed with the per-call key
//! carried in the offer — and B's receiver plays back the identical PCM.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::events::AppEvent;
use mikall_domain::calls::{CallEvent, CallPhase};
use mikall_domain::DomainEvent;
use mikall_media::engine::{
    frames_from_source, run_receiver, run_sender, AudioSource, CollectSink, PcmCodec,
    SenderControl, SineSource,
};
use mikall_media::frame::CallKey;
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
async fn call_signaling_and_sealed_voice_frames() {
    let base = std::env::temp_dir().join(format!("mikall-voice-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let alice = start(test_config(&base.join("a"))).await.unwrap();
    let bob = start(test_config(&base.join("b"))).await.unwrap();
    connect(&alice, &bob).await;

    let mut bob_events = bob.subscribe_events();

    // A rings B.
    let call = alice.calls.start_call(vec![bob.identity.local_id()]).await;

    // B sees the offer.
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
    assert!(matches!(
        alice.calls.snapshot(call).await.unwrap().phase,
        CallPhase::Active
    ));

    // Both sides hold the same per-call AEAD key (carried in the offer).
    let alice_key = alice.calls.media_key(call).await.unwrap();
    let bob_key = bob.calls.media_key(call).await.unwrap();
    assert_eq!(alice_key, bob_key);

    // Screen-share state propagates through signaling.
    alice.calls.share_screen(call, true).await.unwrap();
    for _ in 0..100 {
        let b = bob.calls.snapshot(call).await.unwrap();
        let sharing = b
            .participants
            .iter()
            .any(|(who, media)| *who == alice.identity.local_id() && media.sharing_screen);
        if sharing {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let b = bob.calls.snapshot(call).await.unwrap();
    assert!(
        b.participants
            .iter()
            .any(|(who, media)| *who == alice.identity.local_id() && media.sharing_screen),
        "screen share never propagated: {b:?}"
    );

    // B taps the call's media; A streams 10 frames of tone.
    let tap = bob.calls.media_tap(call).await;
    let expected_pcm: Vec<i16> = {
        let mut source = SineSource::new(10);
        let mut all = Vec::new();
        while let Some(frame) = source.next_frame() {
            all.extend_from_slice(&frame);
        }
        all
    };

    let receiver = tokio::spawn(async move {
        let key = CallKey::new(bob_key);
        let mut sink = CollectSink::default();
        let stats = run_receiver(tap, &key, || Box::new(PcmCodec), &mut sink, Some(10)).await;
        (stats, sink.samples)
    });

    let control = SenderControl::new(vec![bob.identity.local_id()]);
    let frames = frames_from_source(Box::new(SineSource::new(10)), None);
    let sent = run_sender(
        alice.media.clone(),
        call,
        &CallKey::new(alice_key),
        1,
        control,
        frames,
        Box::new(PcmCodec),
    )
    .await;
    assert_eq!(sent, 10);

    let (stats, samples) = tokio::time::timeout(Duration::from_secs(15), receiver)
        .await
        .expect("receiver timed out")
        .unwrap();
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.played + stats.concealed, 10);
    assert_eq!(
        samples, expected_pcm,
        "PCM must survive seal → transport → jitter → decode byte-for-byte"
    );

    // Hang up propagates.
    alice.calls.hang_up_all(call).await.unwrap();
    for _ in 0..100 {
        if matches!(
            bob.calls.snapshot(call).await.unwrap().phase,
            CallPhase::Ended { .. }
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(matches!(
        bob.calls.snapshot(call).await.unwrap().phase,
        CallPhase::Ended { .. }
    ));

    alice.shutdown();
    bob.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
