//! Integration: the NAT-traversal machinery over loopback. A real NAT box
//! cannot be simulated honestly on one machine, so this exercises the
//! machinery rather than the miracle: R donates relay capacity, A is told
//! to behave as NATed (`ReachabilityMode::AssumePrivate` — loopback would
//! defeat an honest probe verdict) and everything downstream of that
//! decision is the production path — autonat probes reach verdicts, A
//! automatically reserves a circuit slot on R and publishes a
//! dialable-from-anywhere `/p2p-circuit` address in `listen_addrs()`, B
//! connects to A through R, real traffic crosses the relayed connection,
//! and DCUtR upgrades it to a direct one.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::time::Duration;

use mikall_app::ports::PeerAddr;
use mikall_net::{NetConfig, ProbeConfig, Reachability, ReachabilityMode};
use mikall_node::{start, NodeConfig};

fn test_config(dir: &Path, relay_service: bool, reachability: ReachabilityMode) -> NodeConfig {
    NodeConfig {
        data_dir: dir.to_path_buf(),
        net: NetConfig {
            // QUIC listeners matter here: outbound QUIC dials reuse the
            // listening endpoint, so the address a peer observes for us
            // IS our listen address — which is what makes the DCUtR
            // hole-punch dial land on loopback.
            listen: vec![
                "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
                "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
            ],
            enable_mdns: false,
            relay_service,
            reachability,
            probe: ProbeConfig {
                boot_delay: Duration::from_millis(500),
                retry_interval: Duration::from_secs(1),
                refresh_interval: Duration::from_secs(30),
                // Loopback addresses are not global; without this no probe
                // would ever run in a test swarm.
                only_global_ips: false,
            },
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
async fn relayed_connect_then_dcutr_upgrade() {
    // Probe outcomes, reservation grants, and hole-punch results are all
    // traced by mikall-net; `RUST_LOG=mikall_net=info` makes this test
    // narrate the whole traversal. Quiet by default.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let base = std::env::temp_dir().join(format!("mikall-nat-{}", std::process::id()));
    let dir_r = base.join("r");
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    let _ = std::fs::remove_dir_all(&base);

    // R: publicly reachable, donates relay capacity (the default).
    let node_r = start(test_config(&dir_r, true, ReachabilityMode::Probe))
        .await
        .unwrap();
    // A: the "NATed" node — no relay service of its own, assumes private.
    let node_a = start(test_config(&dir_a, false, ReachabilityMode::AssumePrivate))
        .await
        .unwrap();
    // B: a remote peer that wants to reach A.
    let node_b = start(test_config(&dir_b, false, ReachabilityMode::Probe))
        .await
        .unwrap();

    // Everyone meets R first — the one advertised address every P2P
    // network needs. Dial over QUIC so observed addresses equal listen
    // addresses (endpoint reuse).
    let r_addrs = node_r.net.listen_addrs().await;
    let r_quic = r_addrs
        .iter()
        .find(|a| a.to_string().contains("/quic-v1"))
        .unwrap();
    let r_target = format!("{r_quic}/p2p/{}", node_r.net.local_peer_id);
    node_a.net.dial(r_target.parse().unwrap()).await.unwrap();
    node_b.net.dial(r_target.parse().unwrap()).await.unwrap();

    // autonat probes reach verdicts: on loopback every node is dialable
    // back, so all three probe out as Public. (A still *behaves* private
    // because its config says so — that split is exactly what lets this
    // test drive the private-path machinery.)
    wait_for("autonat probe verdicts on all three nodes", || async {
        node_r.net.reachability().await == Reachability::Public
            && node_a.net.reachability().await == Reachability::Public
            && node_b.net.reachability().await == Reachability::Public
    })
    .await;

    // A obtained a reservation on R automatically and now publishes a
    // dialable-from-anywhere circuit address in the same listen_addrs()
    // list the settings → network pane renders — zero new UI.
    wait_for("A's /p2p-circuit listen address", || async {
        node_a
            .net
            .listen_addrs()
            .await
            .iter()
            .any(|a| a.to_string().contains("/p2p-circuit"))
    })
    .await;
    let circuit = node_a
        .net
        .listen_addrs()
        .await
        .into_iter()
        .find(|a| a.to_string().contains("/p2p-circuit"))
        .unwrap();
    let circuit_str = circuit.to_string();
    assert!(
        circuit_str.contains(&format!(
            "/p2p/{}/p2p-circuit/p2p/{}",
            node_r.net.local_peer_id, node_a.net.local_peer_id
        )),
        "circuit address must route through R and terminate at A: {circuit_str}"
    );
    // The remembered-peers store (boot redial, M13) accepts it unchanged.
    PeerAddr::parse(&circuit_str).unwrap();

    // B dials the circuit address — exactly what a user who pasted it
    // from A's settings pane would do — and connects through R.
    node_b.net.dial(circuit).await.unwrap();
    wait_for("B connected to A through the relay", || async {
        node_b
            .net
            .connected_peers()
            .await
            .contains(&node_a.net.local_peer_id)
    })
    .await;

    // The relayed connection carries real traffic.
    node_b
        .dm
        .send_dm(node_a.identity.local_id(), "kita yo, chuukei-goshi")
        .await
        .unwrap();
    wait_for("dm arrival at A", || async {
        node_a
            .dm
            .history(node_b.identity.local_id())
            .await
            .map(|h| {
                h.iter()
                    .any(|m| m.body.as_str() == "kita yo, chuukei-goshi")
            })
            .unwrap_or(false)
    })
    .await;

    // DCUtR upgrades the relayed connection: a connection to A appears at
    // B whose remote address has no /p2p-circuit hop in it.
    wait_for("dcutr upgrade to a direct connection", || async {
        node_b
            .net
            .peer_connections(node_a.net.local_peer_id)
            .await
            .iter()
            .any(|a| !a.to_string().contains("/p2p-circuit"))
    })
    .await;

    let _ = std::fs::remove_dir_all(&base);
}
