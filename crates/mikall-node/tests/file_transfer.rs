//! Integration: a real file travels between two nodes over the blob
//! protocol — offer as a signed direct envelope, verified chunk fetching,
//! reassembly into the receiver's downloads directory.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::events::AppEvent;
use mikall_domain::transfer::{TransferPhase, CHUNK_SIZE};
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
    }
}

async fn connect(a: &NodeHandle, b: &NodeHandle) {
    let addrs = a.net.listen_addrs().await;
    let target = format!("{}/p2p/{}", addrs[0], a.net.local_peer_id);
    b.net.dial(target.parse().unwrap()).await.unwrap();
    // Let the connection establish before sending request-response traffic.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn file_offer_fetch_verify_assemble() {
    let base = std::env::temp_dir().join(format!("mikall-ft-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let sender = start(test_config(&base.join("a"))).await.unwrap();
    let receiver = start(test_config(&base.join("b"))).await.unwrap();
    connect(&sender, &receiver).await;

    // Sender imports a 2.5-chunk file and offers it.
    let src = base.join("album mix.flac");
    let data: Vec<u8> = (0..(CHUNK_SIZE * 2 + 5000))
        .map(|i| (i % 249) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();

    let mut receiver_events = receiver.subscribe_events();
    sender
        .transfer
        .offer_path(receiver.identity.local_id(), &src)
        .await
        .unwrap();

    // Receiver sees the offer event and accepts.
    let offer_id = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), receiver_events.recv())
            .await
            .expect("no offer arrived")
            .unwrap();
        if let AppEvent::Domain(DomainEvent::Transfer(
            mikall_domain::transfer::TransferEvent::FileOffered {
                transfer,
                name,
                size,
            },
        )) = event
        {
            assert_eq!(name.as_str(), "album mix.flac");
            assert_eq!(size, data.len() as u64);
            break transfer;
        }
    };
    receiver.transfer.accept(offer_id).await.unwrap();

    // Wait for completion + on-disk assembly.
    let mut saved_path = None;
    for _ in 0..200 {
        if let Ok(progress) = receiver.transfer.progress(offer_id).await {
            if progress.phase == TransferPhase::Complete {
                // drain events for the saved path
                while let Ok(event) = receiver_events.try_recv() {
                    if let AppEvent::TransferSaved { path, .. } = event {
                        saved_path = Some(path);
                    }
                }
                if saved_path.is_some() {
                    break;
                }
            }
            if let TransferPhase::Failed { reason } = &progress.phase {
                panic!("transfer failed: {reason}");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let saved = saved_path.expect("transfer never completed/saved");
    assert_eq!(std::fs::read(&saved).unwrap(), data, "byte-for-byte copy");

    sender.shutdown();
    receiver.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
