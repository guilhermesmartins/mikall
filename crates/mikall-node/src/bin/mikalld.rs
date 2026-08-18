//! `mikalld` — the headless mikall node with a line-based REPL.
//!
//! Usage: `mikalld [data-dir]` (default `~/.mikall` or `$MIKALL_DATA`).
//!
//! Commands:
//!   /nick <name>          set your nickname
//!   /join #channel        join (or found) a channel
//!   /part #channel        leave a channel
//!   /say #channel <text>  post a message
//!   /dm <hex-id> <text>   direct-message an identity (64-char hex key)
//!   /topic #chan <text>   set the topic
//!   /members #channel     list members
//!   /history #channel     print channel history
//!   /list                 list channels known to the directory
//!   /away [message]       set or clear away
//!   /whoami               print identity + fingerprint
//!   /addr                 print listen addresses
//!   /dial <multiaddr>     dial a peer explicitly
//!   /quit                 exit

use std::path::PathBuf;

use tokio::io::{AsyncBufReadExt, BufReader};

use mikall_app::events::AppEvent;
use mikall_domain::messaging::{ChannelName, Nickname};
use mikall_domain::shared::IdentityId;
use mikall_node::{start, NodeConfig, NodeHandle};

fn parse_identity(hex: &str) -> Option<IdentityId> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(IdentityId::from_bytes(bytes))
}

fn identity_hex(id: &IdentityId) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

async fn print_events(node: NodeHandle) {
    let mut events = node.subscribe_events();
    loop {
        match events.recv().await {
            Ok(AppEvent::ChannelMessage {
                channel,
                author,
                author_nick,
                body,
                ..
            }) => {
                let name = author_nick
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| author.to_string());
                println!("[{channel}] <{name}> {body}");
            }
            Ok(AppEvent::DirectMessage {
                from,
                from_nick,
                body,
                ..
            }) => {
                let name = from_nick
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| from.to_string());
                println!("[dm] <{name}> {body}");
            }
            Ok(AppEvent::Domain(event)) => {
                tracing::debug!("domain event: {event:?}");
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn handle_line(node: &NodeHandle, line: &str) -> anyhow::Result<bool> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(true);
    }
    let (command, rest) = line.split_once(' ').unwrap_or((line, ""));
    match command {
        "/quit" => return Ok(false),
        "/whoami" => {
            println!("identity:    {}", identity_hex(&node.identity.local_id()));
            println!("fingerprint: {}", node.identity.fingerprint());
        }
        "/addr" => {
            for addr in node.net.listen_addrs().await {
                println!("{addr}/p2p/{}", node.net.local_peer_id);
            }
        }
        "/dial" => match rest.parse() {
            Ok(addr) => match node.net.dial(addr).await {
                Ok(()) => println!("dialing {rest}"),
                Err(e) => println!("dial failed: {e}"),
            },
            Err(e) => println!("bad multiaddr: {e}"),
        },
        "/nick" => match Nickname::parse(rest) {
            Ok(nick) => {
                node.identity.set_nickname(nick).await;
                println!("nickname set");
            }
            Err(e) => println!("invalid nickname: {e}"),
        },
        "/join" => match ChannelName::parse(rest) {
            Ok(name) => match node.chat.join_channel(name).await {
                Ok(_) => println!("joined {rest}"),
                Err(e) => println!("join failed: {e}"),
            },
            Err(e) => println!("invalid channel name: {e}"),
        },
        "/part" => match ChannelName::parse(rest) {
            Ok(name) => match node.chat.leave_channel(&name).await {
                Ok(()) => println!("left {rest}"),
                Err(e) => println!("part failed: {e}"),
            },
            Err(e) => println!("invalid channel name: {e}"),
        },
        "/say" => {
            let (chan, text) = rest.split_once(' ').unwrap_or((rest, ""));
            match ChannelName::parse(chan) {
                Ok(name) => match node.chat.post_message(&name, text).await {
                    Ok(_) => {}
                    Err(e) => println!("send failed: {e}"),
                },
                Err(e) => println!("invalid channel name: {e}"),
            }
        }
        "/topic" => {
            let (chan, text) = rest.split_once(' ').unwrap_or((rest, ""));
            match ChannelName::parse(chan) {
                Ok(name) => match node.chat.set_topic(&name, text).await {
                    Ok(()) => println!("topic set"),
                    Err(e) => println!("topic failed: {e}"),
                },
                Err(e) => println!("invalid channel name: {e}"),
            }
        }
        "/dm" => {
            let (target, text) = rest.split_once(' ').unwrap_or((rest, ""));
            match parse_identity(target) {
                Some(to) => match node.dm.send_dm(to, text).await {
                    Ok(_) => {}
                    Err(e) => println!("dm failed: {e}"),
                },
                None => println!("expected a 64-char hex identity"),
            }
        }
        "/members" => match ChannelName::parse(rest) {
            Ok(name) => match node.chat.members(&name).await {
                Ok(members) => {
                    for member in members {
                        let nick = member
                            .nick
                            .map(|n| n.as_str().to_owned())
                            .unwrap_or_else(|| member.id.to_string());
                        println!("{nick} ({:?})", member.role);
                    }
                }
                Err(e) => println!("members failed: {e}"),
            },
            Err(e) => println!("invalid channel name: {e}"),
        },
        "/history" => match ChannelName::parse(rest) {
            Ok(name) => match node.chat.history(&name).await {
                Ok(history) => {
                    for message in history {
                        let nick = message
                            .author_nick
                            .map(|n| n.as_str().to_owned())
                            .unwrap_or_else(|| message.author.to_string());
                        println!("<{nick}> {}", message.body);
                    }
                }
                Err(e) => println!("history failed: {e}"),
            },
            Err(e) => println!("invalid channel name: {e}"),
        },
        "/list" => match node.chat.list_channels().await {
            Ok(records) => {
                for record in records {
                    println!("{}", record.name);
                }
            }
            Err(e) => println!("list failed: {e}"),
        },
        "/away" => {
            let message = if rest.is_empty() { None } else { Some(rest) };
            match node.presence.set_away(message).await {
                Ok(()) => println!("away updated"),
                Err(e) => println!("away failed: {e}"),
            }
        }
        other => println!("unknown command: {other}"),
    }
    Ok(true)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,libp2p=warn".into()),
        )
        .init();

    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| std::env::var("MIKALL_DATA").ok().map(PathBuf::from))
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(".mikall"))
        })
        .unwrap_or_else(|| PathBuf::from("./mikall-data"));

    println!(
        "mikalld — decentralized node (data: {})",
        data_dir.display()
    );
    let node = start(NodeConfig::at(data_dir)).await?;
    println!("identity:    {}", identity_hex(&node.identity.local_id()));
    println!("fingerprint: {}", node.identity.fingerprint());
    println!("type /join #channel to begin; /quit to exit");

    // Optional loopback IRC gateway: MIKALL_IRC=<port> [MIKALL_IRC_PASS=..]
    if let Ok(port) = std::env::var("MIKALL_IRC") {
        match port.parse::<u16>() {
            Ok(port) => {
                let services = mikall_irc::GatewayServices {
                    identity: node.identity.clone(),
                    chat: node.chat.clone(),
                    dm: node.dm.clone(),
                    presence: node.presence.clone(),
                    bus: node.bus.clone(),
                };
                let config = mikall_irc::IrcConfig {
                    bind: mikall_irc::IrcBindAddr::localhost(port),
                    password: std::env::var("MIKALL_IRC_PASS").ok(),
                };
                match mikall_irc::serve(services, config).await {
                    Ok((addr, _task)) => println!("IRC gateway on {addr} (loopback only)"),
                    Err(e) => println!("IRC gateway failed to start: {e}"),
                }
            }
            Err(_) => println!("MIKALL_IRC must be a port number"),
        }
    }

    tokio::spawn(print_events(node.clone()));

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        if !handle_line(&node, &line).await? {
            break;
        }
    }
    Ok(())
}
