//! Integration: the profile nickname persists across a node restart. A
//! fresh boot has no nickname (the GUI onboards); after `set_nickname` a
//! reboot at the same data directory loads it back — and the rename still
//! publishes `IdentityEvent::NicknameChanged` for live frontends.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::events::AppEvent;
use mikall_domain::identity::IdentityEvent;
use mikall_domain::messaging::Nickname;
use mikall_domain::DomainEvent;
use mikall_net::NetConfig;
use mikall_node::{start, NodeConfig};

fn test_config(dir: &Path) -> NodeConfig {
    NodeConfig {
        data_dir: dir.to_path_buf(),
        net: NetConfig {
            listen: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            enable_mdns: false,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nickname_survives_restart() {
    let dir = std::env::temp_dir().join(format!("mikall-nick-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // First boot: no nickname yet — this is the state that onboards.
    let node = start(test_config(&dir)).await.unwrap();
    assert_eq!(node.identity.nickname().await, None);

    let mut events = node.subscribe_events();
    node.identity
        .set_nickname(Nickname::parse("miku").unwrap())
        .await;

    // The rename still reaches live frontends as a domain event.
    let mut saw_nickname_changed = false;
    while let Ok(event) = events.try_recv() {
        if let AppEvent::Domain(DomainEvent::Identity(IdentityEvent::NicknameChanged {
            nickname,
        })) = event
        {
            assert_eq!(nickname.as_str(), "miku");
            saw_nickname_changed = true;
        }
    }
    assert!(saw_nickname_changed, "set_nickname must publish its event");

    // Restart: release the key file and database lock, boot again.
    node.shutdown();
    drop(node);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rebooted = start(test_config(&dir)).await.unwrap();
    assert_eq!(
        rebooted.identity.nickname().await,
        Some(Nickname::parse("miku").unwrap()),
        "restarted node lost its nickname"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
