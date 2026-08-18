//! The IRC gateway: every mikall node can speak real RFC 1459/2812 on
//! loopback, so WeeChat/irssi drive the same `NodeHandle` the GUI does.
//!
//! Security posture (see docs/security.md §9): E2E encryption terminates at
//! the node and this gateway re-emits plaintext, so it binds loopback
//! **only** — [`IrcBindAddr`] cannot represent anything else — supports an
//! optional `PASS`, and announces at connect that local processes may see
//! the traffic.

pub mod proto;

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use mikall_app::events::AppEvent;
use mikall_app::events::EventBus;
use mikall_app::services::ChatError;
use mikall_app::services::{ChatService, DmService, IdentityService, PresenceService};
use mikall_domain::identity::{IdentityEvent, TrustLevel};
use mikall_domain::messaging::{ChannelError, ChannelName, MessagingEvent, Nickname, Role};
use mikall_domain::shared::IdentityId;
use mikall_domain::DomainEvent;

use proto::{parse_line, server_time, IrcMessage};

/// The slice of the application the gateway drives. Built by the
/// composition root from the same services every other frontend uses.
#[derive(Clone)]
pub struct GatewayServices {
    pub identity: Arc<IdentityService>,
    pub chat: Arc<ChatService>,
    pub dm: Arc<DmService>,
    pub presence: Arc<PresenceService>,
    pub bus: EventBus,
}

impl std::fmt::Debug for GatewayServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayServices").finish_non_exhaustive()
    }
}

const SERVER_NAME: &str = "mikall.local";

/// A bind address that is loopback by construction — the plaintext gateway
/// physically cannot listen on an external interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IrcBindAddr(SocketAddr);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the IRC gateway may only bind a loopback address")]
pub struct NotLoopback;

impl IrcBindAddr {
    pub fn new(addr: SocketAddr) -> Result<Self, NotLoopback> {
        if addr.ip().is_loopback() {
            Ok(IrcBindAddr(addr))
        } else {
            Err(NotLoopback)
        }
    }

    /// `127.0.0.1:<port>`.
    pub fn localhost(port: u16) -> Self {
        IrcBindAddr(SocketAddr::new(IpAddr::from([127, 0, 0, 1]), port))
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct IrcConfig {
    pub bind: IrcBindAddr,
    /// Recommended on multi-user machines: other local users can reach
    /// loopback ports.
    pub password: Option<String>,
}

impl Default for IrcConfig {
    fn default() -> Self {
        IrcConfig {
            bind: IrcBindAddr::localhost(6667),
            password: None,
        }
    }
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

/// Start the gateway. Returns the actual bound address (useful with port 0)
/// and the accept-loop task.
pub async fn serve(
    node: GatewayServices,
    config: IrcConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(config.bind.socket_addr()).await?;
    let local = listener.local_addr()?;
    let password = Arc::new(config.password);
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let node = node.clone();
                    let password = Arc::clone(&password);
                    tokio::spawn(async move {
                        if let Err(err) = Session::run(node, stream, password).await {
                            tracing::debug!("irc session ended: {err}");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!("irc accept failed: {err}");
                    break;
                }
            }
        }
    });
    Ok((local, task))
}

fn short_id(id: &IdentityId) -> String {
    id.to_string()
}

fn prefix(nick: &str, id: &IdentityId) -> String {
    format!("{nick}!{}@mikall", short_id(id))
}

fn role_sigil(role: Role) -> &'static str {
    match role {
        Role::Founder => "~",
        Role::Op => "@",
        Role::Member => "",
    }
}

fn identity_hex(id: &IdentityId) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_identity_hex(hex: &str) -> Option<IdentityId> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(IdentityId::from_bytes(bytes))
}

struct Session {
    node: GatewayServices,
    out: mpsc::UnboundedSender<String>,
    me: IdentityId,
    nick: Option<String>,
    got_user: bool,
    got_pass: Option<String>,
    password: Arc<Option<String>>,
    registered: bool,
    cap_negotiating: bool,
    caps: HashSet<String>,
    joined: HashSet<ChannelName>,
}

impl Session {
    async fn run(
        node: GatewayServices,
        stream: TcpStream,
        password: Arc<Option<String>>,
    ) -> std::io::Result<()> {
        let (read_half, mut write_half) = stream.into_split();
        let (out, mut out_rx) = mpsc::unbounded_channel::<String>();
        let writer = tokio::spawn(async move {
            while let Some(line) = out_rx.recv().await {
                if write_half.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if write_half.write_all(b"\r\n").await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        });

        let me = node.identity.local_id();
        let mut events = node.bus.subscribe();
        let mut session = Session {
            node,
            out,
            me,
            nick: None,
            got_user: false,
            got_pass: None,
            password,
            registered: false,
            cap_negotiating: false,
            caps: HashSet::new(),
            joined: HashSet::new(),
        };

        let mut lines = BufReader::new(read_half).lines();
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    match line? {
                        Some(line) => {
                            if !session.handle_line(&line).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                event = events.recv() => {
                    match event {
                        Ok(event) => session.handle_event(event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        writer.abort();
        Ok(())
    }

    fn send(&self, line: String) {
        let _ = self.out.send(line);
    }

    fn numeric(&self, code: &str, rest: &str) {
        let nick = self.nick.as_deref().unwrap_or("*");
        self.send(format!(":{SERVER_NAME} {code} {nick} {rest}"));
    }

    fn my_nick(&self) -> String {
        self.nick.clone().unwrap_or_else(|| short_id(&self.me))
    }

    async fn handle_line(&mut self, raw: &str) -> bool {
        let Some(message) = parse_line(raw) else {
            return true;
        };
        match message.command.as_str() {
            "PASS" => {
                self.got_pass = message.params.first().cloned();
                true
            }
            "CAP" => {
                self.handle_cap(&message);
                true
            }
            "NICK" => {
                self.handle_nick(&message).await;
                self.try_register().await;
                true
            }
            "USER" => {
                self.got_user = true;
                self.try_register().await;
                true
            }
            "PING" => {
                let token = message.params.first().cloned().unwrap_or_default();
                self.send(format!(":{SERVER_NAME} PONG {SERVER_NAME} :{token}"));
                true
            }
            "QUIT" => false,
            _ if !self.registered => {
                self.numeric("451", ":You have not registered");
                true
            }
            "JOIN" => {
                self.handle_join(&message).await;
                true
            }
            "PART" => {
                self.handle_part(&message).await;
                true
            }
            "PRIVMSG" | "NOTICE" => {
                self.handle_privmsg(&message).await;
                true
            }
            "TOPIC" => {
                self.handle_topic(&message).await;
                true
            }
            "NAMES" => {
                if let Some(target) = message.params.first() {
                    if let Ok(name) = ChannelName::parse(target) {
                        self.send_names(&name).await;
                    }
                }
                true
            }
            "WHO" => {
                self.handle_who(&message).await;
                true
            }
            "LIST" => {
                self.handle_list().await;
                true
            }
            "WHOIS" => {
                self.handle_whois(&message).await;
                true
            }
            "AWAY" => {
                self.handle_away(&message).await;
                true
            }
            "MODE" => {
                self.handle_mode(&message).await;
                true
            }
            "MOTD" => {
                self.send_motd();
                true
            }
            other => {
                self.numeric("421", &format!("{other} :Unknown command"));
                true
            }
        }
    }

    fn handle_cap(&mut self, message: &IrcMessage) {
        let sub = message
            .params
            .first()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();
        match sub.as_str() {
            "LS" => {
                self.cap_negotiating = !self.registered;
                self.send(format!(":{SERVER_NAME} CAP * LS :message-tags server-time"));
            }
            "REQ" => {
                let requested = message.params.get(1).cloned().unwrap_or_default();
                let mut acked = Vec::new();
                for cap in requested.split_whitespace() {
                    if cap == "server-time" || cap == "message-tags" {
                        self.caps.insert(cap.to_owned());
                        acked.push(cap);
                    }
                }
                self.send(format!(":{SERVER_NAME} CAP * ACK :{}", acked.join(" ")));
            }
            "END" => {
                self.cap_negotiating = false;
            }
            _ => {}
        }
    }

    async fn handle_nick(&mut self, message: &IrcMessage) {
        let Some(raw) = message.params.first() else {
            self.numeric("431", ":No nickname given");
            return;
        };
        match Nickname::parse(raw) {
            Ok(nick) => {
                let old = self.my_nick();
                self.node.identity.set_nickname(nick.clone()).await;
                self.nick = Some(nick.as_str().to_owned());
                if self.registered {
                    self.send(format!(
                        ":{}!{}@mikall NICK :{nick}",
                        old,
                        short_id(&self.me)
                    ));
                }
            }
            Err(_) => {
                self.numeric("432", &format!("{raw} :Erroneous nickname"));
            }
        }
    }

    async fn try_register(&mut self) {
        if self.registered || self.cap_negotiating || self.nick.is_none() || !self.got_user {
            return;
        }
        if let Some(required) = self.password.as_ref() {
            let supplied = self.got_pass.as_deref().unwrap_or("");
            if !constant_time_eq(required, supplied) {
                self.numeric("464", ":Password incorrect");
                self.send("ERROR :Access denied".to_owned());
                return;
            }
        }
        self.registered = true;
        let nick = self.my_nick();
        self.numeric("001", &format!(":Welcome to the mikall network {nick}"));
        self.numeric(
            "002",
            &format!(":Your host is {SERVER_NAME}, running mikall 0.1"),
        );
        self.numeric("003", ":This server was created just for you");
        self.numeric("004", &format!("{SERVER_NAME} mikall-0.1 o ont"));
        self.numeric(
            "005",
            "CASEMAPPING=ascii CHANTYPES=# NICKLEN=16 TOPICLEN=390 NETWORK=mikall :are supported by this server",
        );
        self.send_motd();
        self.send(format!(
            ":{SERVER_NAME} NOTICE {nick} :E2E encryption ends at this node; this gateway is plaintext on loopback. Other local processes may connect - set irc.password on shared machines."
        ));
    }

    fn send_motd(&self) {
        self.numeric("375", &format!(":- {SERVER_NAME} Message of the day -"));
        self.numeric(
            "372",
            ":- mikall: serverless chat. Channels live on peers, not servers.",
        );
        self.numeric(
            "372",
            ":- Identity is a keypair; verify fingerprints with /WHOIS.",
        );
        self.numeric("376", ":End of /MOTD command");
    }

    async fn handle_join(&mut self, message: &IrcMessage) {
        let Some(targets) = message.params.first() else {
            self.numeric("461", "JOIN :Not enough parameters");
            return;
        };
        for target in targets.split(',') {
            match ChannelName::parse(target) {
                Err(_) => {
                    self.numeric("403", &format!("{target} :No such channel"));
                }
                Ok(name) => match self.node.chat.join_channel(name.clone()).await {
                    Err(err) => {
                        self.numeric("403", &format!("{target} :Cannot join: {err}"));
                    }
                    Ok(_) => {
                        self.joined.insert(name.clone());
                        let nick = self.my_nick();
                        self.send(format!(
                            ":{} JOIN {}",
                            prefix(&nick, &self.me),
                            name.as_str()
                        ));
                        self.send_topic_numeric(&name).await;
                        self.send_names(&name).await;
                        if self.caps.contains("server-time") {
                            self.replay_history(&name).await;
                        }
                    }
                },
            }
        }
    }

    async fn send_topic_numeric(&self, name: &ChannelName) {
        match self.node.chat.topic(name).await {
            Ok(topic) if !topic.is_empty() => {
                self.numeric("332", &format!("{} :{}", name.as_str(), topic.as_str()));
            }
            _ => {
                self.numeric("331", &format!("{} :No topic is set", name.as_str()));
            }
        }
    }

    async fn send_names(&self, name: &ChannelName) {
        if let Ok(members) = self.node.chat.members(name).await {
            let list: Vec<String> = members
                .iter()
                .map(|m| {
                    let nick = m
                        .nick
                        .as_ref()
                        .map(|n| n.as_str().to_owned())
                        .unwrap_or_else(|| short_id(&m.id));
                    format!("{}{}", role_sigil(m.role), nick)
                })
                .collect();
            self.numeric("353", &format!("= {} :{}", name.as_str(), list.join(" ")));
        }
        self.numeric("366", &format!("{} :End of /NAMES list", name.as_str()));
    }

    async fn replay_history(&self, name: &ChannelName) {
        let Ok(history) = self.node.chat.history(name).await else {
            return;
        };
        let start = history.len().saturating_sub(50);
        for message in &history[start..] {
            let nick = message
                .author_nick
                .as_ref()
                .map(|n| n.as_str().to_owned())
                .unwrap_or_else(|| short_id(&message.author));
            for line in message.body.irc_lines() {
                self.send(format!(
                    "@time={} :{} PRIVMSG {} :{}",
                    server_time(message.ts_hint_ms),
                    prefix(&nick, &message.author),
                    name.as_str(),
                    line.as_str()
                ));
            }
        }
    }

    async fn handle_part(&mut self, message: &IrcMessage) {
        let Some(target) = message.params.first() else {
            self.numeric("461", "PART :Not enough parameters");
            return;
        };
        let Ok(name) = ChannelName::parse(target) else {
            self.numeric("403", &format!("{target} :No such channel"));
            return;
        };
        if !self.joined.remove(&name) {
            self.numeric("442", &format!("{target} :You're not on that channel"));
            return;
        }
        let nick = self.my_nick();
        let _ = self.node.chat.leave_channel(&name).await;
        self.send(format!(
            ":{} PART {}",
            prefix(&nick, &self.me),
            name.as_str()
        ));
    }

    async fn resolve_target(&self, target: &str) -> Option<IdentityId> {
        if let Some(id) = parse_identity_hex(target) {
            return Some(id);
        }
        self.node.identity.identity_by_nickname(target).await
    }

    async fn handle_privmsg(&mut self, message: &IrcMessage) {
        let notice = message.command == "NOTICE";
        let (Some(target), Some(text)) = (message.params.first(), message.params.get(1)) else {
            if !notice {
                self.numeric("411", ":No recipient given (PRIVMSG)");
            }
            return;
        };
        let text = text
            .strip_prefix("\u{1}ACTION ")
            .map(|action| format!("/me {}", action.trim_end_matches('\u{1}')))
            .unwrap_or_else(|| text.clone());
        if target.starts_with('#') {
            let Ok(name) = ChannelName::parse(target) else {
                if !notice {
                    self.numeric("403", &format!("{target} :No such channel"));
                }
                return;
            };
            if !self.joined.contains(&name) {
                if !notice {
                    self.numeric("442", &format!("{target} :You're not on that channel"));
                }
                return;
            }
            match self.node.chat.post_message(&name, &text).await {
                Ok(_) => {}
                Err(ChatError::Body(_)) => {
                    if !notice {
                        self.numeric("412", ":No text to send");
                    }
                }
                Err(err) => {
                    if !notice {
                        self.numeric("404", &format!("{target} :Cannot send: {err}"));
                    }
                }
            }
        } else {
            let Some(to) = self.resolve_target(target).await else {
                if !notice {
                    self.numeric("401", &format!("{target} :No such nick"));
                }
                return;
            };
            if self.node.dm.send_dm(to, &text).await.is_err() && !notice {
                self.numeric("401", &format!("{target} :Cannot reach peer"));
            }
        }
    }

    async fn handle_topic(&mut self, message: &IrcMessage) {
        let Some(target) = message.params.first() else {
            self.numeric("461", "TOPIC :Not enough parameters");
            return;
        };
        let Ok(name) = ChannelName::parse(target) else {
            self.numeric("403", &format!("{target} :No such channel"));
            return;
        };
        match message.params.get(1) {
            None => self.send_topic_numeric(&name).await,
            Some(new_topic) => match self.node.chat.set_topic(&name, new_topic).await {
                Ok(()) => {
                    let nick = self.my_nick();
                    self.send(format!(
                        ":{} TOPIC {} :{new_topic}",
                        prefix(&nick, &self.me),
                        name.as_str()
                    ));
                }
                Err(ChatError::Channel(ChannelError::NotAnOperator)) => {
                    self.numeric("482", &format!("{target} :You're not channel operator"));
                }
                Err(err) => {
                    self.numeric("403", &format!("{target} :Cannot set topic: {err}"));
                }
            },
        }
    }

    async fn handle_who(&self, message: &IrcMessage) {
        let Some(target) = message.params.first() else {
            self.numeric("315", "* :End of /WHO list");
            return;
        };
        if let Ok(name) = ChannelName::parse(target) {
            if let Ok(members) = self.node.chat.members(&name).await {
                for member in members {
                    let nick = member
                        .nick
                        .as_ref()
                        .map(|n| n.as_str().to_owned())
                        .unwrap_or_else(|| short_id(&member.id));
                    self.numeric(
                        "352",
                        &format!(
                            "{} {} mikall {SERVER_NAME} {} H :0 {}",
                            name.as_str(),
                            short_id(&member.id),
                            nick,
                            identity_hex(&member.id)
                        ),
                    );
                }
            }
        }
        self.numeric("315", &format!("{target} :End of /WHO list"));
    }

    async fn handle_list(&self) {
        self.numeric("321", "Channel :Users Name");
        if let Ok(records) = self.node.chat.list_channels().await {
            for record in records {
                self.numeric("322", &format!("{} 0 :", record.name.as_str()));
            }
        }
        self.numeric("323", ":End of /LIST");
    }

    async fn handle_whois(&self, message: &IrcMessage) {
        let Some(target) = message.params.first() else {
            self.numeric("431", ":No nickname given");
            return;
        };
        let id = if target.eq_ignore_ascii_case(&self.my_nick()) {
            Some(self.me)
        } else {
            self.resolve_target(target).await
        };
        let Some(id) = id else {
            self.numeric("401", &format!("{target} :No such nick"));
            return;
        };
        self.numeric(
            "311",
            &format!("{target} {} mikall * :{}", short_id(&id), identity_hex(&id)),
        );
        let fingerprint = if id == self.me {
            Some(self.node.identity.fingerprint())
        } else {
            self.node.identity.fingerprint_of_contact(&id).await
        };
        if let Some(fp) = fingerprint {
            let trust = match self.node.identity.trust_of(&id).await {
                _ if id == self.me => "self",
                Some(TrustLevel::Verified { .. }) => "verified",
                Some(TrustLevel::TofuPinned { .. }) => "tofu-pinned",
                Some(TrustLevel::Blocked) => "blocked",
                Some(TrustLevel::Unverified) | None => "UNVERIFIED",
            };
            self.numeric("320", &format!("{target} :fingerprint {fp} ({trust})"));
        }
        self.numeric("318", &format!("{target} :End of /WHOIS list"));
    }

    async fn handle_away(&self, message: &IrcMessage) {
        let away = message.params.first().filter(|m| !m.is_empty());
        match self.node.presence.set_away(away.map(String::as_str)).await {
            Ok(()) => {
                if away.is_some() {
                    self.numeric("306", ":You have been marked as being away");
                } else {
                    self.numeric("305", ":You are no longer marked as being away");
                }
            }
            Err(err) => self.numeric("461", &format!("AWAY :{err}")),
        }
    }

    async fn handle_mode(&self, message: &IrcMessage) {
        let Some(target) = message.params.first() else {
            self.numeric("461", "MODE :Not enough parameters");
            return;
        };
        let Ok(name) = ChannelName::parse(target) else {
            return; // user modes unsupported; stay silent
        };
        match (message.params.get(1), message.params.get(2)) {
            (None, _) => {
                self.numeric("324", &format!("{} +nt", name.as_str()));
            }
            (Some(mode), Some(arg)) if mode == "+o" || mode == "-o" => {
                let Some(who) = self.resolve_target(arg).await else {
                    self.numeric("401", &format!("{arg} :No such nick"));
                    return;
                };
                let role = if mode == "+o" { Role::Op } else { Role::Member };
                match self.node.chat.set_role(&name, who, role).await {
                    Ok(()) => {
                        let nick = self.my_nick();
                        self.send(format!(
                            ":{} MODE {} {mode} {arg}",
                            prefix(&nick, &self.me),
                            name.as_str()
                        ));
                    }
                    Err(ChatError::Channel(ChannelError::NotFounder)) => {
                        self.numeric("482", &format!("{target} :Only the founder can change ops"));
                    }
                    Err(err) => {
                        self.numeric("482", &format!("{target} :{err}"));
                    }
                }
            }
            (Some(mode), Some(arg)) if mode == "+b" => {
                if let Some(who) = self.resolve_target(arg).await {
                    self.node.identity.block(who).await;
                }
                self.send(format!(
                    ":{SERVER_NAME} NOTICE {} :decentralized network: bans are local blocklist entries on this node only",
                    self.my_nick()
                ));
            }
            _ => {}
        }
    }

    fn time_tag(&self, ts_ms: u64) -> String {
        if self.caps.contains("server-time") {
            format!("@time={} ", server_time(ts_ms))
        } else {
            String::new()
        }
    }

    async fn handle_event(&mut self, event: AppEvent) {
        if !self.registered {
            return;
        }
        match event {
            AppEvent::ChannelMessage {
                channel,
                author,
                author_nick,
                body,
                ts_hint_ms,
                ..
            } => {
                if author == self.me || !self.joined.contains(&channel) {
                    return;
                }
                let nick = author_nick
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| short_id(&author));
                for line in body.irc_lines() {
                    self.send(format!(
                        "{}:{} PRIVMSG {} :{}",
                        self.time_tag(ts_hint_ms),
                        prefix(&nick, &author),
                        channel.as_str(),
                        line.as_str()
                    ));
                }
            }
            AppEvent::DirectMessage {
                from,
                from_nick,
                body,
                ts_hint_ms,
                ..
            } => {
                if from == self.me {
                    return;
                }
                let nick = from_nick
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| short_id(&from));
                let target = self.my_nick();
                for line in body.irc_lines() {
                    self.send(format!(
                        "{}:{} PRIVMSG {} :{}",
                        self.time_tag(ts_hint_ms),
                        prefix(&nick, &from),
                        target,
                        line.as_str()
                    ));
                }
            }
            AppEvent::Domain(DomainEvent::Messaging(event)) => {
                self.handle_messaging_event(event).await;
            }
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::ContactKeyChanged {
                id,
                ..
            })) => {
                self.send(format!(
                    ":{SERVER_NAME} NOTICE {} :WARNING: the key for {} CHANGED. Verify the new fingerprint out of band before trusting messages.",
                    self.my_nick(),
                    short_id(&id)
                ));
            }
            AppEvent::TransferSaved { path, .. } => {
                self.send(format!(
                    ":{SERVER_NAME} NOTICE {} :file transfer complete, saved to {path}",
                    self.my_nick()
                ));
            }
            AppEvent::Domain(_) => {}
        }
    }

    async fn handle_messaging_event(&mut self, event: MessagingEvent) {
        match event {
            MessagingEvent::ChannelJoined { channel, who } if who != self.me => {
                if let Some(name) = self.node.chat.name_of(channel).await {
                    if self.joined.contains(&name) {
                        let nick = self
                            .node
                            .identity
                            .nickname_of(&who)
                            .await
                            .map(|n| n.as_str().to_owned())
                            .unwrap_or_else(|| short_id(&who));
                        self.send(format!(":{} JOIN {}", prefix(&nick, &who), name.as_str()));
                    }
                }
            }
            MessagingEvent::ChannelLeft { channel, who } if who != self.me => {
                if let Some(name) = self.node.chat.name_of(channel).await {
                    if self.joined.contains(&name) {
                        let nick = self
                            .node
                            .identity
                            .nickname_of(&who)
                            .await
                            .map(|n| n.as_str().to_owned())
                            .unwrap_or_else(|| short_id(&who));
                        self.send(format!(":{} PART {}", prefix(&nick, &who), name.as_str()));
                    }
                }
            }
            MessagingEvent::TopicChanged { channel, by, topic } if by != self.me => {
                if let Some(name) = self.node.chat.name_of(channel).await {
                    if self.joined.contains(&name) {
                        let nick = self
                            .node
                            .identity
                            .nickname_of(&by)
                            .await
                            .map(|n| n.as_str().to_owned())
                            .unwrap_or_else(|| short_id(&by));
                        self.send(format!(
                            ":{} TOPIC {} :{}",
                            prefix(&nick, &by),
                            name.as_str(),
                            topic.as_str()
                        ));
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn non_loopback_bind_is_unrepresentable() {
        assert_eq!(
            IrcBindAddr::new("0.0.0.0:6667".parse().unwrap()),
            Err(NotLoopback)
        );
        assert_eq!(
            IrcBindAddr::new("192.168.1.10:6667".parse().unwrap()),
            Err(NotLoopback)
        );
        assert!(IrcBindAddr::new("127.0.0.1:6667".parse().unwrap()).is_ok());
        assert!(IrcBindAddr::new("[::1]:6667".parse().unwrap()).is_ok());
    }

    #[test]
    fn constant_time_eq_basics() {
        assert!(constant_time_eq("negi", "negi"));
        assert!(!constant_time_eq("negi", "leek"));
        assert!(!constant_time_eq("negi", "negi2"));
        assert!(!constant_time_eq("", "x"));
        assert!(constant_time_eq("", ""));
    }
}
