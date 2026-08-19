//! Cucumber suite for the IRC gateway: every scenario boots real nodes
//! (libp2p swarms on loopback) and drives the gateway over a real TCP
//! socket — the gateway is exercised as a true driving adapter.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cucumber::{given, then, when, World};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use mikall_domain::messaging::ChannelName;
use mikall_irc::{GatewayServices, IrcBindAddr, IrcConfig};
use mikall_net::NetConfig;
use mikall_node::{start, NodeConfig, NodeHandle};

static SCENARIO: AtomicUsize = AtomicUsize::new(0);

fn fresh_dir(tag: &str) -> PathBuf {
    let n = SCENARIO.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("mikall-ircbdd-{}-{n}-{tag}", std::process::id()))
}

fn node_config(dir: PathBuf) -> NodeConfig {
    NodeConfig {
        data_dir: dir,
        net: NetConfig {
            listen: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            enable_mdns: false,
        },
        // Tests drive media pipelines by hand; never open real devices.
        call_audio: false,
    }
}

fn gateway_services(node: &NodeHandle) -> GatewayServices {
    GatewayServices {
        identity: node.identity.clone(),
        chat: node.chat.clone(),
        dm: node.dm.clone(),
        presence: node.presence.clone(),
        bus: node.bus.clone(),
    }
}

struct IrcClient {
    writer: OwnedWriteHalf,
    lines: Lines<BufReader<OwnedReadHalf>>,
    log: Vec<String>,
}

impl std::fmt::Debug for IrcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrcClient")
            .field("log_lines", &self.log.len())
            .finish()
    }
}

impl IrcClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect gateway");
        let (read, writer) = stream.into_split();
        IrcClient {
            writer,
            lines: BufReader::new(read).lines(),
            log: Vec::new(),
        }
    }

    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .await
            .expect("write to gateway");
    }

    /// Wait (10 s) until a received line satisfies `pred`; panics with the
    /// full log otherwise.
    async fn expect(&mut self, what: &str, pred: impl Fn(&str) -> bool) -> String {
        if let Some(hit) = self.log.iter().find(|l| pred(l)) {
            return hit.clone();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let next = tokio::time::timeout_at(deadline, self.lines.next_line()).await;
            match next {
                Ok(Ok(Some(line))) => {
                    self.log.push(line.clone());
                    if pred(&line) {
                        return line;
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) => {
                    panic!("connection closed waiting for {what}; log: {:#?}", self.log)
                }
                Err(_) => panic!("timeout waiting for {what}; log: {:#?}", self.log),
            }
        }
    }
}

fn is_numeric(line: &str, code: &str) -> bool {
    line.split_whitespace().nth(1) == Some(code)
}

#[derive(cucumber::World)]
#[world(init = Self::fresh)]
struct IrcWorld {
    nodes: Vec<NodeHandle>,
    gateway_addr: Option<SocketAddr>,
    client: Option<IrcClient>,
}

impl std::fmt::Debug for IrcWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrcWorld")
            .field("nodes", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

impl IrcWorld {
    fn fresh() -> Self {
        IrcWorld {
            nodes: Vec::new(),
            gateway_addr: None,
            client: None,
        }
    }

    async fn boot_gateway_node(&mut self, password: Option<String>) {
        let node = start(node_config(fresh_dir("gw"))).await.expect("node");
        let config = IrcConfig {
            bind: IrcBindAddr::localhost(0),
            password,
        };
        let (addr, _task) = mikall_irc::serve(gateway_services(&node), config)
            .await
            .expect("gateway");
        self.nodes.push(node);
        self.gateway_addr = Some(addr);
    }

    fn client(&mut self) -> &mut IrcClient {
        self.client.as_mut().expect("no IRC client connected")
    }

    async fn connect_client(&mut self) {
        let addr = self.gateway_addr.expect("gateway not started");
        self.client = Some(IrcClient::connect(addr).await);
    }
}

// ---------------------------------------------------------------- givens --

#[given(expr = "a node with the IRC gateway listening")]
async fn gateway_listening(world: &mut IrcWorld) {
    world.boot_gateway_node(None).await;
}

#[given(expr = "a node with the IRC gateway requiring password {string}")]
async fn gateway_with_password(world: &mut IrcWorld, password: String) {
    world.boot_gateway_node(Some(password)).await;
}

#[given(expr = "an IRC client registered as {string}")]
async fn client_registered(world: &mut IrcWorld, nick: String) {
    world.connect_client().await;
    let client = world.client();
    client.send(&format!("NICK {nick}")).await;
    client.send(&format!("USER {nick} 0 * :{nick}")).await;
    client
        .expect("welcome numeric 001", |l| is_numeric(l, "001"))
        .await;
}

#[given(expr = "two connected nodes where the second runs an IRC gateway")]
async fn two_nodes_second_gateway(world: &mut IrcWorld) {
    let node_a = start(node_config(fresh_dir("a"))).await.expect("node a");
    world.nodes.push(node_a);
    world.boot_gateway_node(None).await;

    let a_addrs = world.nodes[0].net.listen_addrs().await;
    let target = format!("{}/p2p/{}", a_addrs[0], world.nodes[0].net.local_peer_id);
    world.nodes[1]
        .net
        .dial(target.parse().unwrap())
        .await
        .expect("dial");
}

#[given(expr = "the client joined {string} and the first node joined {string}")]
async fn both_sides_joined(world: &mut IrcWorld, chan_irc: String, chan_p2p: String) {
    world.client().send(&format!("JOIN {chan_irc}")).await;
    world
        .client()
        .expect("end of NAMES", |l| is_numeric(l, "366"))
        .await;
    let name = ChannelName::parse(&chan_p2p).unwrap();
    let chan_id = world.nodes[0]
        .chat
        .join_channel(name)
        .await
        .expect("first node join");
    // Wait for the gossip meshes to link both nodes.
    for _ in 0..300 {
        if world.nodes[0].net.mesh_peer_count(chan_id).await > 0
            && world.nodes[1].net.mesh_peer_count(chan_id).await > 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("gossip mesh never formed");
}

// ----------------------------------------------------------------- whens --

#[when(expr = "an IRC client connects and registers as {string}")]
async fn connect_and_register(world: &mut IrcWorld, nick: String) {
    world.connect_client().await;
    let client = world.client();
    client.send(&format!("NICK {nick}")).await;
    client.send(&format!("USER {nick} 0 * :{nick}")).await;
}

#[when(expr = "an IRC client connects and sends nickname {string}")]
async fn connect_bad_nick(world: &mut IrcWorld, nick: String) {
    world.connect_client().await;
    let client = world.client();
    client.send(&format!("NICK {nick}")).await;
    client.send("USER u 0 * :u").await;
}

#[when(expr = "an IRC client connects with password {string} and registers as {string}")]
async fn connect_with_password(world: &mut IrcWorld, password: String, nick: String) {
    world.connect_client().await;
    let client = world.client();
    client.send(&format!("PASS {password}")).await;
    client.send(&format!("NICK {nick}")).await;
    client.send(&format!("USER {nick} 0 * :{nick}")).await;
}

#[when(expr = "the client sends {string}")]
async fn client_sends(world: &mut IrcWorld, line: String) {
    world.client().send(&line).await;
}

#[when(expr = "the first node posts {string} in {string}")]
async fn first_node_posts(world: &mut IrcWorld, text: String, chan: String) {
    let name = ChannelName::parse(&chan).unwrap();
    world.nodes[0]
        .chat
        .post_message(&name, &text)
        .await
        .expect("post");
}

// ----------------------------------------------------------------- thens --

#[then(expr = "the client receives numeric {int}")]
async fn receives_numeric(world: &mut IrcWorld, code: u32) {
    let code = format!("{code:03}");
    world
        .client()
        .expect(&format!("numeric {code}"), |l| is_numeric(l, &code))
        .await;
}

#[then(expr = "the client receives numeric {int} containing {string}")]
async fn receives_numeric_containing(world: &mut IrcWorld, code: u32, needle: String) {
    let code = format!("{code:03}");
    world
        .client()
        .expect(&format!("numeric {code} with {needle}"), |l| {
            is_numeric(l, &code) && l.contains(&needle)
        })
        .await;
}

#[then(expr = "the client receives a JOIN for {string}")]
async fn receives_join(world: &mut IrcWorld, chan: String) {
    world
        .client()
        .expect("JOIN echo", |l| l.contains(" JOIN ") && l.contains(&chan))
        .await;
}

#[then(expr = "the node's channel {string} contains {string}")]
async fn node_channel_contains(world: &mut IrcWorld, chan: String, text: String) {
    let name = ChannelName::parse(&chan).unwrap();
    // The gateway node is the last node booted.
    let node = world.nodes.last().unwrap();
    for _ in 0..100 {
        if let Ok(history) = node.chat.history(&name).await {
            if history.iter().any(|m| m.body.as_str() == text) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("message {text:?} never reached channel {chan}");
}

#[then(expr = "the client receives a PRIVMSG in {string} saying {string}")]
async fn receives_privmsg(world: &mut IrcWorld, chan: String, text: String) {
    world
        .client()
        .expect("PRIVMSG delivery", |l| {
            l.contains(&format!("PRIVMSG {chan} :{text}"))
        })
        .await;
}

#[tokio::main]
async fn main() {
    IrcWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit("tests/features")
        .await;
}
