//! Integration: IRC-gateway settings and the local blocklist persist across
//! a node restart. A fresh boot answers with the defaults (gateway off,
//! port 6667, nobody blocked); after `set_irc_config` and `block`, a reboot
//! at the same data directory loads both back — and a `set_nickname` in
//! between doesn't clobber them (read-modify-write discipline).

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::ports::{GatewayPort, IrcGatewayConfig};
use mikall_domain::messaging::Nickname;
use mikall_domain::shared::IdentityId;
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
async fn irc_settings_and_blocklist_survive_restart() {
    let dir = std::env::temp_dir().join(format!("mikall-irc-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // First boot: never configured — the gateway defaults to off on 6667.
    let node = start(test_config(&dir)).await.unwrap();
    let defaults = node.identity.irc_config().await;
    assert!(!defaults.enabled);
    assert_eq!(defaults.port.get(), 6667);
    assert_eq!(defaults.password, None);
    assert!(node.identity.blocked_contacts().await.is_empty());

    // The user turns the gateway on, sets a port and password, blocks a
    // peer, and renames — each write must survive the others.
    node.identity
        .set_irc_config(IrcGatewayConfig {
            enabled: true,
            port: GatewayPort::new(6668).unwrap(),
            password: Some("negi".to_owned()),
        })
        .await;
    let troll = IdentityId::from_bytes([9; 32]);
    node.identity.block(troll).await;
    node.identity
        .set_nickname(Nickname::parse("miku").unwrap())
        .await;

    // Restart: release the key file and database lock, boot again.
    node.shutdown();
    drop(node);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rebooted = start(test_config(&dir)).await.unwrap();
    let config = rebooted.identity.irc_config().await;
    assert!(config.enabled, "restarted node lost the gateway toggle");
    assert_eq!(config.port.get(), 6668, "restarted node lost the port");
    assert_eq!(
        config.password.as_deref(),
        Some("negi"),
        "restarted node lost the gateway password"
    );
    assert!(
        rebooted.identity.is_blocked(&troll).await,
        "restarted node lost the blocklist"
    );
    assert_eq!(
        rebooted.identity.nickname().await,
        Some(Nickname::parse("miku").unwrap()),
        "gateway/blocklist writes clobbered the nickname"
    );

    // Unblock persists too: forgetting the contact must survive a reboot.
    rebooted.identity.unblock(troll).await;
    assert!(!rebooted.identity.is_blocked(&troll).await);
    rebooted.shutdown();
    drop(rebooted);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let third = start(test_config(&dir)).await.unwrap();
    assert!(
        !third.identity.is_blocked(&troll).await,
        "unblock did not persist"
    );
    assert!(third.identity.blocked_contacts().await.is_empty());
    third.shutdown();

    let _ = std::fs::remove_dir_all(&dir);
}
