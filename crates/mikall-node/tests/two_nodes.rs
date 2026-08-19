//! Integration: two real in-process nodes (full libp2p swarms over
//! loopback TCP, mDNS disabled, explicit dial) chat in a channel, exchange
//! a DM, and a restarted node keeps its history.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use mikall_domain::messaging::{ChannelName, Nickname};
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

async fn wait_for<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn connect(a: &NodeHandle, b: &NodeHandle) {
    let addrs = a.net.listen_addrs().await;
    assert!(!addrs.is_empty(), "node A has no listen address");
    let target = format!("{}/p2p/{}", addrs[0], a.net.local_peer_id);
    b.net.dial(target.parse().unwrap()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn channel_chat_dm_and_persistence() {
    let base = std::env::temp_dir().join(format!("mikall-e2e-{}", std::process::id()));
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    let _ = std::fs::remove_dir_all(&base);

    let node_a = start(test_config(&dir_a)).await.unwrap();
    let node_b = start(test_config(&dir_b)).await.unwrap();
    node_a
        .identity
        .set_nickname(Nickname::parse("miku").unwrap())
        .await;
    node_b
        .identity
        .set_nickname(Nickname::parse("rin").unwrap())
        .await;

    connect(&node_a, &node_b).await;

    let stage = ChannelName::parse("#stage").unwrap();
    let chan_a = node_a.chat.join_channel(stage.clone()).await.unwrap();
    // B looks the channel up via the DHT record A stored.
    wait_for("directory lookup of #stage from B", || async {
        node_b.chat.join_channel(stage.clone()).await.is_ok()
    })
    .await;

    // Wait for the gossip meshes to include each other.
    wait_for("gossip mesh formation", || async {
        node_a.net.mesh_peer_count(chan_a).await > 0 && node_b.net.mesh_peer_count(chan_a).await > 0
    })
    .await;

    node_a
        .chat
        .post_message(&stage, "sekai de ichiban ohime-sama")
        .await
        .unwrap();

    wait_for("message arrival at B", || async {
        node_b
            .chat
            .history(&stage)
            .await
            .map(|h| {
                h.iter()
                    .any(|m| m.body.as_str() == "sekai de ichiban ohime-sama")
            })
            .unwrap_or(false)
    })
    .await;

    // Direct message, routed by identity-derived PeerId.
    node_b
        .dm
        .send_dm(node_a.identity.local_id(), "kon kon")
        .await
        .unwrap();
    wait_for("dm arrival at A", || async {
        node_a
            .dm
            .history(node_b.identity.local_id())
            .await
            .map(|h| h.iter().any(|m| m.body.as_str() == "kon kon"))
            .unwrap_or(false)
    })
    .await;

    // A restarted node rehydrates channel history from redb.
    node_b.shutdown();
    drop(node_b);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let node_b2 = start(test_config(&dir_b)).await.unwrap();
    node_b2.chat.join_channel(stage.clone()).await.unwrap();
    let history = node_b2.chat.history(&stage).await.unwrap();
    assert!(
        history
            .iter()
            .any(|m| m.body.as_str() == "sekai de ichiban ohime-sama"),
        "restarted node lost its history: {history:?}"
    );

    let _ = std::fs::remove_dir_all(&base);
}
