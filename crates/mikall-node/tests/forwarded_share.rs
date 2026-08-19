//! Integration: the forwarding tree over three real nodes. The sharer
//! elects a forwarder through the aggregate, assigns it in-band over the
//! media streams, and sends each sealed frame exactly once; the forwarder
//! relays the sealed bytes — without ever holding a decrypt path in the
//! relay — to the viewer, who renders them byte-for-byte and reports
//! feedback that reaches the sharer. Then the forwarder dies and the
//! share survives: re-election falls back to direct fan-out and the
//! viewer's pictures resume from a fresh keyframe.
//!
//! The pipelines are wired by hand exactly as the `hardware-video` engine
//! wires them (tests never open devices); the deterministic fake codec
//! carries pixels byte-exact.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use mikall_domain::calls::CallPhase;
use mikall_domain::shared::IdentityId;
use mikall_media::control::{ControlMessage, ControlPlane};
use mikall_media::frame::CallKey;
use mikall_media::relay::{run_video_pump, PumpEvent, PumpShared, RelayTarget};
use mikall_media::video::{
    run_video_receiver, run_video_sender, video_ssrc_of, CapturedFrame, DecodedPicture,
    EncodedPicture, VideoDecoder, VideoEncoder, VideoError, VideoFanout, VideoSenderControl,
    VideoSink,
};
use mikall_net::NetConfig;
use mikall_node::{start, NodeConfig, NodeHandle};

fn test_config(dir: &Path) -> NodeConfig {
    NodeConfig {
        data_dir: dir.to_path_buf(),
        net: NetConfig {
            listen: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            enable_mdns: false,
            ..NetConfig::default()
        },
        call_audio: false,
        call_video: false,
    }
}

async fn connect(a: &NodeHandle, b: &NodeHandle) {
    let addrs = a.net.listen_addrs().await;
    let target = format!("{}/p2p/{}", addrs[0], a.net.local_peer_id);
    b.net.dial(target.parse().unwrap()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
}

struct FakeEncoder {
    frames_seen: u64,
}

impl VideoEncoder for FakeEncoder {
    fn encode(
        &mut self,
        frame: &CapturedFrame,
        force_keyframe: bool,
    ) -> Result<Option<EncodedPicture>, VideoError> {
        let keyframe = force_keyframe || self.frames_seen == 0;
        self.frames_seen += 1;
        let mut bytes = vec![u8::from(keyframe)];
        bytes.extend_from_slice(&frame.width.to_be_bytes());
        bytes.extend_from_slice(&frame.height.to_be_bytes());
        bytes.extend_from_slice(&frame.bgra);
        Ok(Some(EncodedPicture { bytes, keyframe }))
    }
}

struct FakeDecoder;

impl VideoDecoder for FakeDecoder {
    fn decode(&mut self, packet: &[u8]) -> Option<DecodedPicture> {
        if packet.len() < 9 {
            return None;
        }
        let width = u32::from_be_bytes(packet[1..5].try_into().ok()?);
        let height = u32::from_be_bytes(packet[5..9].try_into().ok()?);
        Some(DecodedPicture {
            width,
            height,
            rgba: packet[9..].to_vec(),
        })
    }
}

type Pictures = Arc<StdMutex<Vec<(IdentityId, Vec<u8>)>>>;

struct SharedSink {
    pictures: Pictures,
}

impl VideoSink for SharedSink {
    fn present(&mut self, from: IdentityId, picture: DecodedPicture) {
        if let Ok(mut pictures) = self.pictures.lock() {
            pictures.push((from, picture.rgba));
        }
    }
}

fn test_frame(step: u8) -> CapturedFrame {
    CapturedFrame {
        width: 16,
        height: 8,
        bgra: (0..16 * 8 * 4)
            .map(|i| (i as u8).wrapping_add(step))
            .collect(),
    }
}

async fn wait_for(mut check: impl FnMut() -> bool, what: &str) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarder_relays_and_the_share_survives_its_death() {
    let base = std::env::temp_dir().join(format!("mikall-fwd-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let sharer = start(test_config(&base.join("a"))).await.unwrap();
    let peer_b = start(test_config(&base.join("b"))).await.unwrap();
    let peer_c = start(test_config(&base.join("c"))).await.unwrap();
    // Full connectivity: the forwarder needs a path to the viewer too.
    connect(&sharer, &peer_b).await;
    connect(&sharer, &peer_c).await;
    connect(&peer_b, &peer_c).await;

    let a_id = sharer.identity.local_id();
    let b_id = peer_b.identity.local_id();
    let c_id = peer_c.identity.local_id();

    // A rings B and C; both accept.
    let call = sharer.calls.start_call(vec![b_id, c_id]).await;
    for callee in [&peer_b, &peer_c] {
        for _ in 0..100 {
            if callee.calls.accept_incoming(call).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for _ in 0..100 {
        let snapshot = sharer.calls.snapshot(call).await;
        if snapshot.is_ok_and(|s| matches!(s.phase, CallPhase::Active) && s.participants.len() == 3)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let key_bytes = sharer.calls.media_key(call).await.unwrap();

    // A shares; the deterministic election picks the lowest identity
    // among B and C — never A.
    sharer.calls.share_screen(call, true).await.unwrap();
    let elected = sharer.calls.reconcile_forwarder(call, &[]).await.unwrap();
    let forwarder_id = elected.expect("3-party call must elect a forwarder");
    assert_ne!(forwarder_id, a_id, "the sharer never forwards");
    assert_eq!(forwarder_id, b_id.min(c_id), "lowest identity wins");
    // Idempotent: reconciling again keeps the incumbent.
    assert_eq!(
        sharer.calls.reconcile_forwarder(call, &[]).await.unwrap(),
        Some(forwarder_id)
    );
    let (forwarder, viewer) = if forwarder_id == b_id {
        (&peer_b, &peer_c)
    } else {
        (&peer_c, &peer_b)
    };
    let viewer_id = if forwarder_id == b_id { c_id } else { b_id };

    // ---- wire the roles by hand, exactly as the engine does ----

    // Sharer: control plane + pump (to hear acks/feedback/requests).
    let a_control = ControlPlane::new(
        sharer.media_streams.clone(),
        call,
        CallKey::new(key_bytes),
        &a_id,
    );
    let a_shared = PumpShared::new();
    let (a_decode_tx, _a_decode_rx) = tokio::sync::mpsc::channel(64);
    let (a_events_tx, mut a_events_rx) = tokio::sync::mpsc::channel(64);
    let a_tap = sharer.calls.video_tap(call).await;
    let _a_pump = tokio::spawn({
        let key = CallKey::new(key_bytes);
        let shared = Arc::clone(&a_shared);
        async move {
            run_video_pump(
                a_tap,
                &key,
                shared,
                a_decode_tx,
                a_events_tx,
                Duration::from_secs(1),
            )
            .await
        }
    });
    let a_feedback: Arc<StdMutex<Vec<(IdentityId, u32, u32)>>> =
        Arc::new(StdMutex::new(Vec::new()));
    let a_acked = Arc::new(StdMutex::new(false));
    let _a_events = tokio::spawn({
        let a_feedback = Arc::clone(&a_feedback);
        let a_acked = Arc::clone(&a_acked);
        async move {
            while let Some(event) = a_events_rx.recv().await {
                if let PumpEvent::Control { from, message } = event {
                    match message {
                        ControlMessage::ForwarderAck => *a_acked.lock().unwrap() = true,
                        ControlMessage::Feedback(report) => {
                            a_feedback
                                .lock()
                                .unwrap()
                                .push((from, report.received, report.lost))
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    // Forwarder: pump + relay-on-assign (its own view is dropped here —
    // the relay path is what this test pins down).
    let f_control = ControlPlane::new(
        forwarder.media_streams.clone(),
        call,
        CallKey::new(key_bytes),
        &forwarder_id,
    );
    let f_shared = PumpShared::new();
    let (f_decode_tx, _f_decode_rx) = tokio::sync::mpsc::channel(64);
    let (f_events_tx, mut f_events_rx) = tokio::sync::mpsc::channel(64);
    let f_tap = forwarder.calls.video_tap(call).await;
    let f_pump = tokio::spawn({
        let key = CallKey::new(key_bytes);
        let shared = Arc::clone(&f_shared);
        async move {
            run_video_pump(
                f_tap,
                &key,
                shared,
                f_decode_tx,
                f_events_tx,
                Duration::from_secs(1),
            )
            .await
        }
    });
    let _f_events = tokio::spawn({
        let f_shared = Arc::clone(&f_shared);
        let f_control = Arc::clone(&f_control);
        let streams = forwarder.media_streams.clone();
        async move {
            while let Some(event) = f_events_rx.recv().await {
                if let PumpEvent::Control { from, message } = event {
                    match message {
                        ControlMessage::ForwarderAssign { viewers } => {
                            let fanout = VideoFanout::new_relay(streams.clone(), call);
                            fanout.set_peers(viewers);
                            f_shared.set_relay(Some(RelayTarget {
                                sharer: from,
                                fanout,
                            }));
                            let _ = f_control.send(from, &ControlMessage::ForwarderAck).await;
                        }
                        ControlMessage::ForwarderRevoke => f_shared.set_relay(None),
                        _ => {}
                    }
                }
            }
        }
    });

    // Viewer: pump + decoder + feedback to the sharer.
    let v_control = ControlPlane::new(
        viewer.media_streams.clone(),
        call,
        CallKey::new(key_bytes),
        &viewer_id,
    );
    let v_shared = PumpShared::new();
    v_shared.set_origins([(video_ssrc_of(&a_id), a_id)].into_iter().collect());
    let (v_decode_tx, v_decode_rx) = tokio::sync::mpsc::channel(64);
    let (v_events_tx, mut v_events_rx) = tokio::sync::mpsc::channel(64);
    let v_tap = viewer.calls.video_tap(call).await;
    let _v_pump = tokio::spawn({
        let key = CallKey::new(key_bytes);
        let shared = Arc::clone(&v_shared);
        async move {
            run_video_pump(
                v_tap,
                &key,
                shared,
                v_decode_tx,
                v_events_tx,
                Duration::from_millis(500),
            )
            .await
        }
    });
    let _v_events = tokio::spawn({
        let v_control = Arc::clone(&v_control);
        async move {
            while let Some(event) = v_events_rx.recv().await {
                if let PumpEvent::FeedbackDue { origin, report } = event {
                    let _ = v_control
                        .send(origin, &ControlMessage::Feedback(report))
                        .await;
                }
            }
        }
    });
    let pictures: Pictures = Arc::new(StdMutex::new(Vec::new()));
    let _v_recv = tokio::spawn({
        let key = CallKey::new(key_bytes);
        let mut sink = SharedSink {
            pictures: Arc::clone(&pictures),
        };
        async move {
            run_video_receiver(v_decode_rx, &key, || Box::new(FakeDecoder), &mut sink, None).await
        }
    });

    // ---- sharer targets the forwarder: ONE outbound lane ----
    let a_fanout = VideoFanout::new(sharer.media_streams.clone(), call);
    a_fanout.set_peers(vec![forwarder_id]);
    assert_eq!(a_fanout.peer_count(), 1, "one lane regardless of viewers");
    assert!(
        a_control
            .send(
                forwarder_id,
                &ControlMessage::ForwarderAssign {
                    viewers: vec![viewer_id],
                },
            )
            .await,
        "assignment must reach the forwarder"
    );
    wait_for(|| *a_acked.lock().unwrap(), "the forwarder's ack").await;

    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(8);
    let _a_sender = tokio::spawn({
        let fanout = Arc::clone(&a_fanout);
        let key = CallKey::new(key_bytes);
        let ssrc = video_ssrc_of(&a_id);
        async move {
            run_video_sender(
                fanout,
                &key,
                ssrc,
                10,
                VideoSenderControl::new(1_500_000),
                frame_rx,
                Box::new(FakeEncoder { frames_seen: 0 }),
            )
            .await
        }
    });
    for step in 0..5u8 {
        frame_tx.send(test_frame(step)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The viewer rendered the sharer's pixels byte-for-byte — carried by
    // the forwarder, attributed to the sharer.
    wait_for(
        || pictures.lock().unwrap().len() >= 5,
        "5 pictures at the viewer via the forwarder",
    )
    .await;
    {
        let got = pictures.lock().unwrap();
        for (step, (from, rgba)) in got.iter().take(5).enumerate() {
            assert_eq!(*from, a_id, "attributed to the sharer, not the relay");
            assert_eq!(
                *rgba,
                test_frame(step as u8).bgra,
                "picture {step} must survive seal → relay → open byte-for-byte"
            );
        }
    }
    assert_eq!(
        v_shared.via_of(&a_id),
        Some(forwarder_id),
        "the viewer knows who carried the stream"
    );
    assert_eq!(
        a_fanout.peer_count(),
        1,
        "the sharer never grew a second lane"
    );

    // Feedback closed the loop: the viewer's report reached the sharer.
    wait_for(
        || {
            a_feedback
                .lock()
                .unwrap()
                .iter()
                .any(|(from, received, _)| *from == viewer_id && *received > 0)
        },
        "viewer feedback at the sharer",
    )
    .await;

    // ---- the forwarder dies mid-share ----
    forwarder.shutdown();
    f_pump.abort();
    // The sharer's watchdog path: the forwarder is unreachable, so
    // re-election excludes it. One survivor is too few candidates — the
    // strategy refuses and the sharer falls back to DIRECT fan-out, the
    // M16 behavior, and the share does not end.
    let re_elected = sharer
        .calls
        .reconcile_forwarder(call, &[forwarder_id])
        .await
        .unwrap();
    assert_eq!(re_elected, None, "one candidate is too few — direct");
    a_fanout.set_peers(vec![viewer_id]);

    let before = pictures.lock().unwrap().len();
    for step in 5..10u8 {
        frame_tx.send(test_frame(step)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for(
        || pictures.lock().unwrap().len() > before,
        "pictures resuming after the forwarder died",
    )
    .await;
    // Direct frames clear the via marker: the viewer sees the truth.
    wait_for(|| v_shared.via_of(&a_id).is_none(), "via marker cleared").await;

    drop(frame_tx);
    sharer.calls.hang_up_all(call).await.unwrap();
    sharer.shutdown();
    peer_b.shutdown();
    peer_c.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
