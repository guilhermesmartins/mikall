//! Integration: screen-share video frames between two real nodes, riding
//! the unidirectional media-stream protocol — while voice frames ride the
//! old request-response path in the same call, interleaved. Proves the
//! transport end to end (seal → stream → demux → open) without hardware:
//! deterministic fake codecs carry the pixels byte-for-byte.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mikall_domain::calls::CallPhase;
use mikall_domain::shared::IdentityId;
use mikall_media::engine::{
    frames_from_source, run_receiver, run_sender, AudioSource, CollectSink, PcmCodec,
    SenderControl, SineSource,
};
use mikall_media::frame::CallKey;
use mikall_media::video::{
    run_video_receiver, run_video_sender, video_ssrc_of, CapturedFrame, DecodedPicture,
    EncodedPicture, VideoDecoder, VideoEncoder, VideoError, VideoFanout, VideoSink,
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
        // Tests drive media pipelines by hand; never open devices.
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

/// Deterministic fake codec: "encoding" prefixes a keyframe marker and
/// the dimensions, "decoding" strips them — pixels survive byte-exact.
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

struct CollectPictures {
    pictures: Vec<(IdentityId, DecodedPicture)>,
}

impl VideoSink for CollectPictures {
    fn present(&mut self, from: IdentityId, picture: DecodedPicture) {
        self.pictures.push((from, picture));
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

#[tokio::test(flavor = "multi_thread")]
async fn video_rides_streams_while_voice_rides_the_old_path() {
    let base = std::env::temp_dir().join(format!("mikall-video-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let alice = start(test_config(&base.join("a"))).await.unwrap();
    let bob = start(test_config(&base.join("b"))).await.unwrap();
    connect(&alice, &bob).await;

    // A rings B; B accepts; both Active. (Exercised in depth by the voice
    // test — here it only sets the stage.)
    let call = alice.calls.start_call(vec![bob.identity.local_id()]).await;
    for _ in 0..100 {
        if bob.calls.accept_incoming(call).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for _ in 0..100 {
        let a = alice.calls.snapshot(call).await;
        if a.is_ok_and(|s| matches!(s.phase, CallPhase::Active) && s.participants.len() == 2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let key_bytes = alice.calls.media_key(call).await.unwrap();
    assert_eq!(Some(key_bytes), bob.calls.media_key(call).await);

    // B taps both kinds. A shares: video over the stream transport.
    let video_tap = bob.calls.video_tap(call).await;
    let audio_tap = bob.calls.media_tap(call).await;

    let video_receiver = tokio::spawn({
        let key = CallKey::new(key_bytes);
        async move {
            let mut sink = CollectPictures {
                pictures: Vec::new(),
            };
            let stats = run_video_receiver(
                video_tap,
                &key,
                || Box::new(FakeDecoder),
                &mut sink,
                Some(5),
            )
            .await;
            (stats, sink.pictures)
        }
    });
    let audio_receiver = tokio::spawn({
        let key = CallKey::new(key_bytes);
        async move {
            let mut sink = CollectSink::default();
            let stats =
                run_receiver(audio_tap, &key, || Box::new(PcmCodec), &mut sink, Some(5)).await;
            (stats, sink.samples)
        }
    });

    // Interleave: video through per-viewer stream lanes, voice through
    // the old request-response transport — same call, same key.
    let fanout = VideoFanout::new(Arc::clone(&alice.media_streams), call);
    fanout.set_peers(vec![bob.identity.local_id()]);
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(8);
    let video_sender = tokio::spawn({
        let fanout = Arc::clone(&fanout);
        let key = CallKey::new(key_bytes);
        let ssrc = video_ssrc_of(&alice.identity.local_id());
        async move {
            run_video_sender(
                fanout,
                &key,
                ssrc,
                10,
                frame_rx,
                Box::new(FakeEncoder { frames_seen: 0 }),
            )
            .await
        }
    });
    let audio_control = SenderControl::new(vec![bob.identity.local_id()]);
    let audio_frames = frames_from_source(Box::new(SineSource::new(5)), None);
    let audio_key = CallKey::new(key_bytes);
    let audio_sender = run_sender(
        alice.media.clone(),
        call,
        &audio_key,
        1,
        audio_control,
        audio_frames,
        Box::new(PcmCodec),
    );
    for step in 0..5u8 {
        frame_tx.send(test_frame(step)).await.unwrap();
        // Paced like a real capture source (a burst faster than the lane
        // can open its stream would rightly be dropped as a stale run).
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(frame_tx);
    let sent_audio = audio_sender.await;
    assert_eq!(sent_audio, 5);
    let video_totals = tokio::time::timeout(Duration::from_secs(10), video_sender)
        .await
        .expect("video sender timed out")
        .unwrap();
    assert_eq!(video_totals.encoded, 5);
    assert_eq!(video_totals.keyframes, 1);

    // The viewer got every picture, byte-for-byte, in order — and not a
    // single audio frame leaked into the video lane (or vice versa).
    let (video_stats, pictures) = tokio::time::timeout(Duration::from_secs(15), video_receiver)
        .await
        .expect("video receiver timed out")
        .unwrap();
    assert_eq!(video_stats.presented, 5);
    assert_eq!(video_stats.rejected, 0);
    assert_eq!(video_stats.discarded_pre_keyframe, 0);
    assert_eq!(pictures.len(), 5);
    for (step, (from, picture)) in pictures.iter().enumerate() {
        assert_eq!(*from, alice.identity.local_id());
        let expected = test_frame(step as u8);
        assert_eq!((picture.width, picture.height), (16, 8));
        assert_eq!(
            picture.rgba, expected.bgra,
            "picture {step} must survive encode → seal → stream → open → decode byte-for-byte"
        );
    }

    let (audio_stats, samples) = tokio::time::timeout(Duration::from_secs(15), audio_receiver)
        .await
        .expect("audio receiver timed out")
        .unwrap();
    assert_eq!(audio_stats.rejected, 0);
    assert_eq!(audio_stats.played + audio_stats.concealed, 5);
    let expected_pcm: Vec<i16> = {
        let mut source = SineSource::new(5);
        let mut all = Vec::new();
        while let Some(frame) = source.next_frame() {
            all.extend_from_slice(&frame);
        }
        all
    };
    assert_eq!(samples, expected_pcm, "voice must be untouched by video");

    alice.calls.hang_up_all(call).await.unwrap();
    alice.shutdown();
    bob.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
