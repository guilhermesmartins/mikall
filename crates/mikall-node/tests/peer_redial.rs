//! Integration: a remembered peer is redialed automatically at boot. Node
//! A dials B once (the manual first-time act) and remembers B's address;
//! after a restart A reconnects to B with no dial call — the peer cache
//! makes connecting automatic from the second boot on, which is the whole
//! point when mDNS multicast is blocked.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::ports::PeerAddr;
use mikall_domain::messaging::{ChannelName, Nickname};
use mikall_net::NetConfig;
use mikall_node::{start, NodeConfig};

fn test_config(dir: &Path) -> NodeConfig {
    NodeConfig {
        data_dir: dir.to_path_buf(),
        net: NetConfig {
            listen: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            enable_mdns: false,
            ..NetConfig::default()
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

#[tokio::test(flavor = "multi_thread")]
async fn remembered_peer_reconnects_after_restart() {
    let base = std::env::temp_dir().join(format!("mikall-redial-{}", std::process::id()));
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    let _ = std::fs::remove_dir_all(&base);

    // B stays up for the whole test.
    let node_b = start(test_config(&dir_b)).await.unwrap();
    node_b
        .identity
        .set_nickname(Nickname::parse("rin").unwrap())
        .await;
    let b_addrs = node_b.net.listen_addrs().await;
    assert!(!b_addrs.is_empty(), "node B has no listen address");
    let b_addr = format!("{}/p2p/{}", b_addrs[0], node_b.net.local_peer_id);
    let b_peer_addr = PeerAddr::parse(&b_addr).unwrap();

    // First boot of A: the one manual dial, then remember B — exactly what
    // the settings UI does on a successful dial.
    let node_a = start(test_config(&dir_a)).await.unwrap();
    node_a.net.dial(b_addr.parse().unwrap()).await.unwrap();
    node_a.identity.remember_peer(b_peer_addr.clone()).await;
    assert_eq!(
        node_a.identity.known_peers().await,
        vec![b_peer_addr.clone()]
    );

    // B founds a channel that a reconnected A can only find via B's DHT.
    let stage = ChannelName::parse("#stage").unwrap();
    let chan = node_b.chat.join_channel(stage.clone()).await.unwrap();

    // Restart A. From here on nothing dials manually.
    node_a.shutdown();
    drop(node_a);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let node_a2 = start(test_config(&dir_a)).await.unwrap();

    // The remembered address survived the restart...
    assert_eq!(node_a2.identity.known_peers().await, vec![b_peer_addr]);

    // ...and the boot redial reconnects on its own: the connection to B
    // shows up, A finds B's channel through the DHT, and the gossip mesh
    // forms — all without a single dial call after the restart.
    wait_for("automatic reconnection to B", || async {
        node_a2
            .net
            .connected_peers()
            .await
            .contains(&node_b.net.local_peer_id)
    })
    .await;
    wait_for("directory lookup of #stage from restarted A", || async {
        node_a2.chat.join_channel(stage.clone()).await.is_ok()
    })
    .await;
    wait_for("gossip mesh formation after redial", || async {
        node_a2.net.mesh_peer_count(chan).await > 0 && node_b.net.mesh_peer_count(chan).await > 0
    })
    .await;

    let _ = std::fs::remove_dir_all(&base);
}
