//! The mikall GUI: a Discord-like three-pane iced application driving the
//! same `NodeHandle` as the IRC gateway. This crate is a driving adapter —
//! it never touches net/store/crypto directly.
//!
//! Visual language follows docs/design-brief.md: miku_teal tokens, monospace
//! for everything trust- or IRC-flavored, pink author names, and honesty copy
//! stated plainly.

mod theme;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use iced::widget::{
    button, column, container, row, scrollable, text, text_input, tooltip, Column, Row, Space,
};
use iced::{Element, Fill, Font, Subscription, Task};

use mikall_app::events::AppEvent;
use mikall_app::services::{CallSnapshot, MemberView, RenderedMessage};
use mikall_domain::calls::{CallEvent, CallId, CallPhase, CallRoster, EndReason, MediaState};
use mikall_domain::identity::IdentityEvent;
use mikall_domain::messaging::{ChannelName, MessagingEvent, Nickname, Role};
use mikall_domain::presence::PresenceState;
use mikall_domain::shared::IdentityId;
use mikall_domain::DomainEvent;
use mikall_node::{start, NodeConfig, NodeHandle};

const MONO: Font = Font::MONOSPACE;
const MONO_BOLD: Font = Font {
    weight: iced::font::Weight::Bold,
    ..Font::MONOSPACE
};
const SEMIBOLD: Font = Font {
    weight: iced::font::Weight::Semibold,
    ..Font::DEFAULT
};
const ITALIC: Font = Font {
    style: iced::font::Style::Italic,
    ..Font::DEFAULT
};

fn data_dir() -> PathBuf {
    std::env::var("MIKALL_DATA")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|home| PathBuf::from(home).join(".mikall"))
        })
        .unwrap_or_else(|| PathBuf::from("./mikall-data"))
}

/// Advisory wall-clock hint (ordering is causal): hh:mm, UTC.
fn ts_hint(ms: u64) -> String {
    let day_secs = (ms / 1000) % 86_400;
    format!("{:02}:{:02}", day_secs / 3600, (day_secs % 3600) / 60)
}

fn shorten(raw: &str, max: usize) -> String {
    if raw.chars().count() > max {
        format!("{}…", raw.chars().take(max).collect::<String>())
    } else {
        raw.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Channel(ChannelName),
    Dm(IdentityId),
}

impl Target {
    fn key(&self) -> String {
        match self {
            Target::Channel(name) => name.as_str().to_owned(),
            Target::Dm(id) => format!("dm:{id}"),
        }
    }
}

#[derive(Debug)]
enum Screen {
    Booting,
    Onboarding { nick: String },
    Main,
    Failed(String),
}

/// The single alarm slot at the top of the shell. Key changes are the
/// loudest thing in the app; saved-file notices share the slot at a
/// lower severity.
#[derive(Debug, Clone)]
enum Alarm {
    Danger { title: &'static str, body: String },
    Info { body: String },
}

/// The call surface: one call shown at a time, offers that arrive while it
/// is up wait in `Mikall::call_backlog`. `snapshot` is the service's
/// read-model, refetched on every call event — the UI never mutates call
/// state locally, so it can never disagree with the aggregate. `None` means
/// the first fetch is still in flight (nothing is rendered yet).
#[derive(Debug)]
struct CallUi {
    id: CallId,
    snapshot: Option<CallSnapshot>,
}

#[derive(Debug, Clone)]
enum Msg {
    Booted(Result<NodeHandle, String>),
    NickLoaded(Option<String>),
    ChannelsLoaded(Vec<ChannelName>),
    IrcGateway(Result<String, String>),
    NickInput(String),
    ConfirmNick,
    CopyFingerprint,
    Event(AppEvent),
    JoinInput(String),
    JoinSubmit,
    Joined(Result<ChannelName, String>),
    Select(Target),
    HistoryLoaded(String, Vec<RenderedMessage>),
    MembersLoaded(Vec<MemberView>),
    PresenceLoaded(Vec<(IdentityId, PresenceState)>),
    TopicLoaded(String),
    ComposerInput(String),
    Send,
    Sent(Result<(), String>),
    ToggleRoster,
    DismissAlarm,
    CallStart,
    CallStarted(CallId),
    CallAccept(CallId),
    CallDecline(CallId),
    CallHangUp(CallId),
    CallDone(Result<(), String>),
    CallSnapshot(Result<CallSnapshot, String>),
    CallDismiss,
    RingPulse,
    Noop,
}

struct Mikall {
    node: Option<NodeHandle>,
    screen: Screen,
    fingerprint: String,
    nickname: Option<String>,
    channels: Vec<ChannelName>,
    dms: Vec<(IdentityId, Option<Nickname>)>,
    active: Option<Target>,
    messages: Vec<RenderedMessage>,
    members: Vec<MemberView>,
    presence: BTreeMap<IdentityId, PresenceState>,
    topic: String,
    unread: HashMap<String, usize>,
    composer: String,
    join_input: String,
    show_roster: bool,
    alarm: Option<Alarm>,
    call: Option<CallUi>,
    /// Incoming offers that rang while another call surface was up; the
    /// next one is shown when the current call ends or is dismissed.
    call_backlog: Vec<CallId>,
    /// Alternates while ringing to drive the strip's gentle edge pulse.
    ring_glow: bool,
}

impl Mikall {
    fn boot() -> (Self, Task<Msg>) {
        let app = Mikall {
            node: None,
            screen: Screen::Booting,
            fingerprint: String::new(),
            nickname: None,
            channels: Vec::new(),
            dms: Vec::new(),
            active: None,
            messages: Vec::new(),
            members: Vec::new(),
            presence: BTreeMap::new(),
            topic: String::new(),
            unread: HashMap::new(),
            composer: String::new(),
            join_input: String::new(),
            show_roster: true,
            alarm: None,
            call: None,
            call_backlog: Vec::new(),
            ring_glow: false,
        };
        let task = Task::perform(
            async {
                start(NodeConfig::at(data_dir()))
                    .await
                    .map_err(|e| e.to_string())
            },
            Msg::Booted,
        );
        (app, task)
    }

    fn node(&self) -> Option<NodeHandle> {
        self.node.clone()
    }

    fn me(&self) -> Option<IdentityId> {
        self.node.as_ref().map(|n| n.identity.local_id())
    }

    fn reload_active(&self) -> Task<Msg> {
        let (Some(node), Some(target)) = (self.node(), self.active.clone()) else {
            return Task::none();
        };
        let key = target.key();
        match target {
            Target::Channel(name) => {
                let history = {
                    let node = node.clone();
                    let name = name.clone();
                    let key = key.clone();
                    Task::perform(
                        async move { node.chat.history(&name).await.unwrap_or_default() },
                        move |messages| Msg::HistoryLoaded(key.clone(), messages),
                    )
                };
                let members = {
                    let node = node.clone();
                    let name = name.clone();
                    Task::perform(
                        async move { node.chat.members(&name).await.unwrap_or_default() },
                        Msg::MembersLoaded,
                    )
                };
                let topic = Task::perform(
                    async move {
                        node.chat
                            .topic(&name)
                            .await
                            .map(|t| t.as_str().to_owned())
                            .unwrap_or_default()
                    },
                    Msg::TopicLoaded,
                );
                Task::batch([history, members, topic])
            }
            Target::Dm(id) => Task::perform(
                async move { node.dm.history(id).await.unwrap_or_default() },
                move |messages| Msg::HistoryLoaded(key.clone(), messages),
            ),
        }
    }

    fn load_channels(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        Task::perform(
            async move { node.chat.joined_channels().await },
            Msg::ChannelsLoaded,
        )
    }

    /// Serve RFC 1459 on loopback when MIKALL_IRC=<port> is set — the same
    /// gateway `mikalld` offers, so WeeChat sits beside this window.
    fn start_irc_gateway(node: &NodeHandle) -> Task<Msg> {
        let port = match std::env::var("MIKALL_IRC") {
            Ok(raw) => match raw.parse::<u16>() {
                Ok(port) => port,
                Err(_) => {
                    return Task::done(Msg::IrcGateway(Err(
                        "MIKALL_IRC must be a port number".to_owned()
                    )))
                }
            },
            Err(_) => return Task::none(),
        };
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
        Task::perform(
            async move {
                mikall_irc::serve(services, config)
                    .await
                    .map(|(addr, _task)| addr.to_string())
                    .map_err(|e| e.to_string())
            },
            Msg::IrcGateway,
        )
    }

    fn load_presence(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        let ids: Vec<IdentityId> = self.members.iter().map(|m| m.id).collect();
        if ids.is_empty() {
            return Task::none();
        }
        Task::perform(
            async move {
                let mut states = Vec::with_capacity(ids.len());
                for id in ids {
                    states.push((id, node.presence.state_of(&id).await));
                }
                states
            },
            Msg::PresenceLoaded,
        )
    }

    /// A call surface is up and not yet ended — the topic-bar call button
    /// stays disabled and new offers queue behind it.
    fn call_in_progress(&self) -> bool {
        self.call.as_ref().is_some_and(|call| {
            call.snapshot
                .as_ref()
                .is_none_or(|s| !matches!(s.phase, CallPhase::Ended { .. }))
        })
    }

    fn call_is_ringing(&self) -> bool {
        self.call
            .as_ref()
            .and_then(|call| call.snapshot.as_ref())
            .is_some_and(|s| matches!(s.phase, CallPhase::Ringing { .. }))
    }

    /// Refetch the service's read-model of this call.
    fn refresh_call(&self, id: CallId) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        Task::perform(
            async move { node.calls.snapshot(id).await.map_err(|e| e.to_string()) },
            Msg::CallSnapshot,
        )
    }

    /// Surface the oldest queued incoming offer, if the slot is free.
    fn show_next_offer(&mut self) -> Task<Msg> {
        if self.call.is_some() || self.call_backlog.is_empty() {
            return Task::none();
        }
        let id = self.call_backlog.remove(0);
        self.call = Some(CallUi { id, snapshot: None });
        self.refresh_call(id)
    }

    /// Nick if any surface knows one (active-channel roster, DM list, our
    /// own profile), else the shortened identity hex. The bool means
    /// "render monospace" — hex is identity material.
    fn peer_label(&self, id: IdentityId) -> (String, bool) {
        if self.me() == Some(id) {
            if let Some(nick) = &self.nickname {
                return (nick.clone(), false);
            }
        }
        if let Some(nick) = self
            .members
            .iter()
            .find(|m| m.id == id)
            .and_then(|m| m.nick.as_ref())
        {
            return (nick.as_str().to_owned(), false);
        }
        if let Some(nick) = self
            .dms
            .iter()
            .find(|(other, _)| *other == id)
            .and_then(|(_, nick)| nick.as_ref())
        {
            return (nick.as_str().to_owned(), false);
        }
        (shorten(&id.to_string(), 9), true)
    }

    fn call_event_id(event: &CallEvent) -> CallId {
        match event {
            CallEvent::CallOffered { call, .. }
            | CallEvent::CallAccepted { call, .. }
            | CallEvent::CallDeclined { call, .. }
            | CallEvent::ParticipantJoined { call, .. }
            | CallEvent::ParticipantLeft { call, .. }
            | CallEvent::ScreenShareStarted { call, .. }
            | CallEvent::ScreenShareStopped { call, .. }
            | CallEvent::CallEnded { call, .. } => *call,
        }
    }

    fn update(&mut self, message: Msg) -> Task<Msg> {
        match message {
            Msg::Booted(Ok(node)) => {
                self.fingerprint = node.identity.fingerprint().display_groups();
                let identity = node.identity.clone();
                let gateway = Self::start_irc_gateway(&node);
                self.node = Some(node);
                let nick = Task::perform(
                    async move { identity.nickname().await.map(|n| n.as_str().to_owned()) },
                    Msg::NickLoaded,
                );
                Task::batch([nick, self.load_channels(), gateway])
            }
            Msg::Booted(Err(error)) => {
                self.screen = Screen::Failed(error);
                Task::none()
            }
            Msg::NickLoaded(nick) => {
                self.screen = if nick.is_some() {
                    Screen::Main
                } else {
                    Screen::Onboarding {
                        nick: String::new(),
                    }
                };
                self.nickname = nick;
                Task::none()
            }
            Msg::ChannelsLoaded(list) => {
                for name in list {
                    if !self.channels.contains(&name) {
                        self.channels.push(name);
                    }
                }
                if self.active.is_none() {
                    if let Some(first) = self.channels.first().cloned() {
                        return self.update(Msg::Select(Target::Channel(first)));
                    }
                }
                Task::none()
            }
            Msg::IrcGateway(Ok(addr)) => {
                self.alarm = Some(Alarm::Info {
                    body: format!("irc gateway on {addr} — plaintext, loopback only"),
                });
                Task::none()
            }
            Msg::IrcGateway(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "IRC GATEWAY",
                    body: error,
                });
                Task::none()
            }
            Msg::NickInput(value) => {
                if let Screen::Onboarding { nick } = &mut self.screen {
                    *nick = value;
                }
                Task::none()
            }
            Msg::ConfirmNick => {
                let Screen::Onboarding { nick } = &self.screen else {
                    return Task::none();
                };
                let Ok(nickname) = Nickname::parse(nick) else {
                    return Task::none(); // the field shows validity live
                };
                let Some(node) = self.node() else {
                    return Task::none();
                };
                self.nickname = Some(nickname.as_str().to_owned());
                self.screen = Screen::Main;
                Task::perform(
                    async move {
                        node.identity.set_nickname(nickname).await;
                    },
                    |_| Msg::Noop,
                )
            }
            Msg::CopyFingerprint => iced::clipboard::write(self.fingerprint.clone()),
            Msg::Event(event) => self.handle_event(event),
            Msg::JoinInput(value) => {
                self.join_input = value;
                Task::none()
            }
            Msg::JoinSubmit => {
                let raw = self.join_input.trim().to_owned();
                let Some(node) = self.node() else {
                    return Task::none();
                };
                let raw = if raw.starts_with('#') {
                    raw
                } else {
                    format!("#{raw}")
                };
                self.join_input.clear();
                Task::perform(
                    async move {
                        let name = ChannelName::parse(&raw).map_err(|e| e.to_string())?;
                        node.chat
                            .join_channel(name.clone())
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok(name)
                    },
                    Msg::Joined,
                )
            }
            Msg::Joined(Ok(name)) => {
                if !self.channels.contains(&name) {
                    self.channels.push(name.clone());
                }
                self.active = Some(Target::Channel(name));
                self.reload_active()
            }
            Msg::Joined(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "JOIN FAILED",
                    body: error,
                });
                Task::none()
            }
            Msg::Select(target) => {
                self.unread.remove(&target.key());
                self.active = Some(target);
                self.messages.clear();
                self.members.clear();
                self.presence.clear();
                self.topic.clear();
                self.reload_active()
            }
            Msg::HistoryLoaded(key, messages) => {
                if self.active.as_ref().map(Target::key) == Some(key) {
                    self.messages = messages;
                }
                Task::none()
            }
            Msg::MembersLoaded(members) => {
                self.members = members;
                self.load_presence()
            }
            Msg::PresenceLoaded(states) => {
                self.presence = states.into_iter().collect();
                Task::none()
            }
            Msg::TopicLoaded(topic) => {
                self.topic = topic;
                Task::none()
            }
            Msg::ComposerInput(value) => {
                self.composer = value;
                Task::none()
            }
            Msg::Send => {
                let body = self.composer.trim().to_owned();
                if body.is_empty() {
                    return Task::none();
                }
                let (Some(node), Some(target)) = (self.node(), self.active.clone()) else {
                    return Task::none();
                };
                self.composer.clear();
                Task::perform(
                    async move {
                        match target {
                            Target::Channel(name) => node
                                .chat
                                .post_message(&name, &body)
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string()),
                            Target::Dm(id) => node
                                .dm
                                .send_dm(id, &body)
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string()),
                        }
                    },
                    Msg::Sent,
                )
            }
            Msg::Sent(Ok(())) => self.reload_active(),
            Msg::Sent(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "SEND FAILED",
                    body: error,
                });
                Task::none()
            }
            Msg::ToggleRoster => {
                self.show_roster = !self.show_roster;
                Task::none()
            }
            Msg::DismissAlarm => {
                self.alarm = None;
                Task::none()
            }
            Msg::CallStart => {
                let (Some(node), Some(target)) = (self.node(), self.active.clone()) else {
                    return Task::none();
                };
                if self.call_in_progress() {
                    return Task::none();
                }
                let me = self.me();
                let peers: Vec<IdentityId> = match target {
                    Target::Dm(id) => vec![id],
                    Target::Channel(_) => self
                        .members
                        .iter()
                        .map(|m| m.id)
                        .filter(|id| Some(*id) != me)
                        .collect(),
                };
                if peers.is_empty() {
                    return Task::none();
                }
                Task::perform(
                    async move { node.calls.start_call(peers).await },
                    Msg::CallStarted,
                )
            }
            Msg::CallStarted(id) => {
                self.call = Some(CallUi { id, snapshot: None });
                self.refresh_call(id)
            }
            Msg::CallAccept(id) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.calls
                            .accept_incoming(id)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    Msg::CallDone,
                )
            }
            Msg::CallDecline(id) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.calls
                            .decline_incoming(id)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    Msg::CallDone,
                )
            }
            Msg::CallHangUp(id) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move { node.calls.hang_up_all(id).await.map_err(|e| e.to_string()) },
                    Msg::CallDone,
                )
            }
            // Success is silent: the call events land on the bus and the
            // snapshot refresh moves the surface.
            Msg::CallDone(Ok(())) => Task::none(),
            Msg::CallDone(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "CALL",
                    body: error,
                });
                Task::none()
            }
            Msg::CallSnapshot(Ok(snapshot)) => {
                let me = self.me();
                let Some(call) = &mut self.call else {
                    return Task::none();
                };
                if call.id != snapshot.id {
                    return Task::none();
                }
                // An offer that ended before it was ever rendered isn't
                // worth interrupting for — skip to the next queued one.
                let unseen_expired_offer = call.snapshot.is_none()
                    && matches!(snapshot.phase, CallPhase::Ended { .. })
                    && Some(snapshot.initiator) != me;
                if unseen_expired_offer {
                    self.call = None;
                    return self.show_next_offer();
                }
                call.snapshot = Some(snapshot);
                Task::none()
            }
            Msg::CallSnapshot(Err(_)) => {
                // Unknown call: the service has nothing real to show.
                self.call = None;
                self.show_next_offer()
            }
            Msg::CallDismiss => {
                self.call = None;
                self.show_next_offer()
            }
            Msg::RingPulse => {
                self.ring_glow = !self.ring_glow;
                Task::none()
            }
            Msg::Noop => Task::none(),
        }
    }

    fn handle_event(&mut self, event: AppEvent) -> Task<Msg> {
        match event {
            AppEvent::ChannelMessage { channel, .. } => {
                let is_active = self.active == Some(Target::Channel(channel.clone()));
                if is_active {
                    self.reload_active()
                } else {
                    *self.unread.entry(channel.as_str().to_owned()).or_insert(0) += 1;
                    Task::none()
                }
            }
            AppEvent::DirectMessage {
                from, from_nick, ..
            } => {
                if let Some(me) = self.me() {
                    if from != me && !self.dms.iter().any(|(id, _)| *id == from) {
                        self.dms.push((from, from_nick));
                    }
                }
                let is_active = self.active == Some(Target::Dm(from));
                if is_active {
                    self.reload_active()
                } else {
                    let key = Target::Dm(from).key();
                    *self.unread.entry(key).or_insert(0) += 1;
                    Task::none()
                }
            }
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::NicknameChanged {
                nickname,
            })) => {
                // A rename from any frontend (this GUI, IRC NICK) lands here.
                self.nickname = Some(nickname.as_str().to_owned());
                self.reload_active()
            }
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::ContactKeyChanged {
                id,
                new_fingerprint,
                ..
            })) => {
                self.alarm = Some(Alarm::Danger {
                    title: "KEY CHANGED",
                    body: format!(
                        "for {id}: new fingerprint {new_fingerprint} — verify out of band before trusting."
                    ),
                });
                Task::none()
            }
            AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::ChannelJoined { .. })) => {
                // A join from any frontend (this GUI, IRC) lands in the sidebar.
                Task::batch([self.load_channels(), self.reload_active()])
            }
            AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::TopicChanged { .. }))
            | AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::ChannelLeft { .. })) => {
                self.reload_active()
            }
            AppEvent::Domain(DomainEvent::Presence(_)) => self.load_presence(),
            AppEvent::Domain(DomainEvent::Calls(event)) => self.handle_call_event(event),
            AppEvent::TransferSaved { path, .. } => {
                self.alarm = Some(Alarm::Info {
                    body: format!("file saved to {path}"),
                });
                Task::none()
            }
            AppEvent::Domain(_) => Task::none(),
        }
    }

    /// Call events never carry UI state — every one triggers a snapshot
    /// refetch, so the surface always mirrors the aggregate.
    fn handle_call_event(&mut self, event: CallEvent) -> Task<Msg> {
        if let CallEvent::CallOffered { call, by, .. } = &event {
            let incoming = Some(*by) != self.me();
            if incoming && self.call.as_ref().map(|c| c.id) != Some(*call) {
                if self.call_in_progress() {
                    // Gentle interruption only: one call surface at a time.
                    if !self.call_backlog.contains(call) {
                        self.call_backlog.push(*call);
                    }
                    return Task::none();
                }
                self.call = Some(CallUi {
                    id: *call,
                    snapshot: None,
                });
                return self.refresh_call(*call);
            }
        }
        let id = Self::call_event_id(&event);
        if self.call.as_ref().map(|c| c.id) == Some(id) {
            return self.refresh_call(id);
        }
        // A queued offer that died before being shown leaves the queue.
        if matches!(
            event,
            CallEvent::CallDeclined { .. } | CallEvent::CallEnded { .. }
        ) {
            self.call_backlog.retain(|queued| *queued != id);
        }
        Task::none()
    }

    fn subscription(&self) -> Subscription<Msg> {
        let Some(node) = &self.node else {
            return Subscription::none();
        };
        let bus = node.bus.clone();
        let events = Subscription::run_with_id(
            "mikall-events",
            futures::stream::unfold(bus.subscribe(), |mut rx| async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => return Some((Msg::Event(event), rx)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }),
        );
        if self.call_is_ringing() {
            // The pulse only ticks while something actually rings.
            Subscription::batch([
                events,
                iced::time::every(Duration::from_millis(700)).map(|_| Msg::RingPulse),
            ])
        } else {
            events
        }
    }

    fn view(&self) -> Element<'_, Msg> {
        match &self.screen {
            Screen::Booting => container(
                text("starting node…")
                    .size(15)
                    .font(MONO)
                    .color(theme::MUTED),
            )
            .center_x(Fill)
            .center_y(Fill)
            .into(),
            Screen::Failed(error) => container(
                column![
                    text("node failed to start").size(22).color(theme::DANGER),
                    text(error.clone()).size(13).font(MONO).color(theme::MUTED),
                ]
                .spacing(10)
                .align_x(iced::Center),
            )
            .center_x(Fill)
            .center_y(Fill)
            .into(),
            Screen::Onboarding { nick } => self.view_onboarding(nick),
            Screen::Main => self.view_main(),
        }
    }

    fn view_onboarding<'a>(&'a self, nick: &'a str) -> Element<'a, Msg> {
        let valid = Nickname::parse(nick).is_ok();
        let hint = if nick.is_empty() {
            text("a label, not an identity — your fingerprint is who you are").color(theme::MUTED)
        } else if valid {
            text("looks good ✔").color(theme::TEAL)
        } else {
            text("letters first; letters/digits/[]\\`_^{|}- after; max 16").color(theme::DANGER)
        };

        let groups: Vec<&str> = self.fingerprint.split('-').collect();
        let (fp_top, fp_bottom) = if groups.len() == 8 {
            (groups[..4].join("-"), groups[4..].join("-"))
        } else {
            (self.fingerprint.clone(), String::new())
        };

        let identity_card = container(
            column![
                row![
                    text("IDENTITY · ED25519")
                        .size(10)
                        .font(MONO)
                        .color(theme::TEAL)
                        .width(Fill),
                    button(text("copy").size(11).font(MONO))
                        .style(theme::ghost)
                        .padding([3, 10])
                        .on_press(Msg::CopyFingerprint),
                ]
                .align_y(iced::Center),
                Space::with_height(12),
                text(fp_top).size(20).font(MONO),
                text(fp_bottom).size(20).font(MONO),
                Space::with_height(10),
                text("share it out of band so friends can verify it's you")
                    .size(12)
                    .color(theme::MUTED),
            ]
            .spacing(2),
        )
        .style(theme::identity_card)
        .padding([20, 24])
        .width(460);

        container(
            column![
                text("mikall").size(38).color(theme::TEAL),
                text("serverless chat — your identity is a keypair on this device")
                    .size(14)
                    .color(theme::MUTED),
                Space::with_height(30),
                identity_card,
                Space::with_height(26),
                column![
                    text("NICKNAME").size(10).font(MONO).color(theme::MUTED),
                    text_input("nickname", nick)
                        .on_input(Msg::NickInput)
                        .on_submit(Msg::ConfirmNick)
                        .style(theme::input)
                        .font(MONO)
                        .padding(11)
                        .size(14),
                    hint.size(12).font(MONO),
                ]
                .spacing(7)
                .width(460),
                Space::with_height(24),
                button(text("enter the stage").size(15).font(SEMIBOLD))
                    .style(theme::primary)
                    .padding([11, 30])
                    .on_press_maybe(valid.then_some(Msg::ConfirmNick)),
                Space::with_height(26),
                text("lose the key file, lose the identity — there is no recovery")
                    .size(11)
                    .font(MONO)
                    .color(theme::MUTED),
            ]
            .align_x(iced::Center),
        )
        .center_x(Fill)
        .center_y(Fill)
        .into()
    }

    fn view_main(&self) -> Element<'_, Msg> {
        let mut root = Column::new();
        if let Some(alarm) = &self.alarm {
            root = root.push(self.view_alarm(alarm));
        }
        // The call surface docks below the alarm slot, above the panes:
        // a gentle interruption, never a modal takeover. A future screen-
        // share viewer pane can grow below this strip without touching
        // the three-pane body.
        if let Some(surface) = self.view_call() {
            root = root.push(surface);
        }
        let body = row![self.view_sidebar(), self.view_chat()]
            .push_maybe((self.show_roster && self.active.is_some()).then(|| self.view_roster()));
        root.push(body.height(Fill)).into()
    }

    fn view_call(&self) -> Option<Element<'_, Msg>> {
        let call = self.call.as_ref()?;
        let snapshot = call.snapshot.as_ref()?;
        let id = call.id;
        let outgoing = Some(snapshot.initiator) == self.me();
        Some(match &snapshot.phase {
            CallPhase::Ringing { offered_to } if outgoing => {
                self.view_call_ringing_out(id, offered_to)
            }
            CallPhase::Ringing { .. } => self.view_call_offer(id, snapshot.initiator),
            CallPhase::Connecting => self.view_call_connecting(id),
            CallPhase::Active => self.view_call_active(id, snapshot),
            CallPhase::Ended { reason } => Self::view_call_ended(*reason),
        })
    }

    /// Outgoing ring: callee label + "ringing…" + cancel.
    fn view_call_ringing_out(&self, id: CallId, offered_to: &[IdentityId]) -> Element<'_, Msg> {
        let callees = offered_to
            .iter()
            .map(|peer| self.peer_label(*peer).0)
            .collect::<Vec<_>>()
            .join(", ");
        container(
            row![
                text("RINGING").size(11).font(MONO_BOLD).color(theme::TEAL),
                text(format!("calling {callees} — ringing…"))
                    .size(13)
                    .width(Fill),
                button(text("cancel").size(12).font(MONO))
                    .style(theme::danger_outline)
                    .padding([4, 14])
                    .on_press(Msg::CallHangUp(id)),
            ]
            .spacing(12)
            .align_y(iced::Center),
        )
        .style(theme::call_ringing(self.ring_glow))
        .padding([8, 14])
        .width(Fill)
        .into()
    }

    /// Incoming offer: caller label + accept / decline.
    fn view_call_offer(&self, id: CallId, caller: IdentityId) -> Element<'_, Msg> {
        let (label, mono) = self.peer_label(caller);
        let caller_text = if mono {
            text(label).size(13).font(MONO)
        } else {
            text(label).size(13).font(SEMIBOLD)
        };
        container(
            row![
                text("INCOMING CALL")
                    .size(11)
                    .font(MONO_BOLD)
                    .color(theme::TEAL),
                caller_text,
                text("is calling — voice")
                    .size(13)
                    .color(theme::MUTED)
                    .width(Fill),
                button(text("accept").size(12).font(SEMIBOLD))
                    .style(theme::primary)
                    .padding([4, 14])
                    .on_press(Msg::CallAccept(id)),
                button(text("decline").size(12).font(MONO))
                    .style(theme::danger_outline)
                    .padding([4, 14])
                    .on_press(Msg::CallDecline(id)),
            ]
            .spacing(12)
            .align_y(iced::Center),
        )
        .style(theme::call_ringing(self.ring_glow))
        .padding([8, 14])
        .width(Fill)
        .into()
    }

    /// Brief transitional state while media transports come up.
    fn view_call_connecting(&self, id: CallId) -> Element<'_, Msg> {
        container(
            row![
                text("CONNECTING")
                    .size(11)
                    .font(MONO_BOLD)
                    .color(theme::TEAL),
                text("setting up media…")
                    .size(13)
                    .color(theme::MUTED)
                    .width(Fill),
                Self::honesty_label(),
                button(text("hang up").size(12).font(MONO))
                    .style(theme::danger_outline)
                    .padding([4, 14])
                    .on_press(Msg::CallHangUp(id)),
            ]
            .spacing(12)
            .align_y(iced::Center),
        )
        .style(theme::call_panel)
        .padding([8, 14])
        .width(Fill)
        .into()
    }

    /// Active call: participant tiles (max 8, rows of 4) + controls.
    /// Mute/deafen toggles are absent on purpose: no use-case service
    /// exposes them yet, and a control that does nothing would lie.
    fn view_call_active(&self, id: CallId, snapshot: &CallSnapshot) -> Element<'_, Msg> {
        let header = row![
            text("VOICE CALL")
                .size(11)
                .font(MONO_BOLD)
                .color(theme::TEAL),
            text(format!(
                "mesh {}/{}",
                snapshot.participants.len(),
                CallRoster::MAX_PARTICIPANTS
            ))
            .size(10)
            .font(MONO)
            .color(theme::MUTED)
            .width(Fill),
            Self::honesty_label(),
        ]
        .spacing(12)
        .align_y(iced::Center);

        let mut grid = Column::new().spacing(8).align_x(iced::Center);
        for tiles in snapshot.participants.chunks(4) {
            let mut line = Row::new().spacing(8);
            for (who, media) in tiles {
                line = line.push(self.view_call_tile(*who, *media));
            }
            grid = grid.push(line);
        }

        let controls = row![button(text("hang up").size(12).font(SEMIBOLD))
            .style(theme::danger)
            .padding([6, 20])
            .on_press(Msg::CallHangUp(id))]
        .spacing(10);

        container(
            column![
                header,
                container(grid).center_x(Fill),
                container(controls).center_x(Fill),
            ]
            .spacing(10),
        )
        .style(theme::call_panel)
        .padding([12, 18])
        .width(Fill)
        .into()
    }

    fn view_call_tile(&self, who: IdentityId, media: MediaState) -> Element<'_, Msg> {
        let is_me = self.me() == Some(who);
        let (label, mono) = self.peer_label(who);
        let name = if mono {
            text(label).size(13).font(MONO)
        } else {
            text(label).size(13).font(SEMIBOLD)
        };
        let mut head = Row::new().spacing(6).align_y(iced::Center).push(name);
        if is_me {
            head = head.push(text("you").size(9).font(MONO).color(theme::MUTED));
        }
        let mut tile = Column::new().spacing(5).align_x(iced::Center).push(head);
        // Real per-participant media state from the aggregate. The
        // sharing_screen flag is deliberately unrendered here — the
        // screen-share viewer is the next milestone.
        let mut chips = Row::new().spacing(4);
        let flagged = media.mic_muted || media.deafened;
        if media.mic_muted {
            chips = chips.push(Self::media_chip("mic off"));
        }
        if media.deafened {
            chips = chips.push(Self::media_chip("deafened"));
        }
        if flagged {
            tile = tile.push(chips);
        }
        container(tile)
            .style(theme::call_tile)
            .padding([10, 14])
            .width(150)
            .align_x(iced::Center)
            .into()
    }

    fn media_chip(label: &str) -> Element<'_, Msg> {
        container(text(label).size(9).font(MONO))
            .style(theme::chip_danger)
            .padding([2, 6])
            .into()
    }

    /// Brief §3.3: the persistent honesty label of every connected state.
    fn honesty_label() -> Element<'static, Msg> {
        text("direct connection — participants can see your IP")
            .size(10)
            .font(MONO)
            .color(theme::MUTED)
            .into()
    }

    fn view_call_ended(reason: EndReason) -> Element<'static, Msg> {
        let reason = match reason {
            EndReason::HungUp => "hung up",
            EndReason::Declined => "declined",
            EndReason::Failed => "failed",
            EndReason::LastParticipantLeft => "last participant left",
        };
        container(
            row![
                text("CALL ENDED")
                    .size(11)
                    .font(MONO_BOLD)
                    .color(theme::MUTED),
                text(reason).size(13).width(Fill),
                button(text("dismiss").size(11).font(MONO))
                    .style(theme::ghost)
                    .padding([3, 10])
                    .on_press(Msg::CallDismiss),
            ]
            .spacing(12)
            .align_y(iced::Center),
        )
        .style(theme::call_panel)
        .padding([8, 14])
        .width(Fill)
        .into()
    }

    fn view_alarm<'a>(&'a self, alarm: &'a Alarm) -> Element<'a, Msg> {
        let dismiss = button(text("dismiss").size(11).font(MONO))
            .style(theme::ghost)
            .padding([3, 10])
            .on_press(Msg::DismissAlarm);
        match alarm {
            Alarm::Danger { title, body } => container(
                row![
                    text(*title).size(11).font(MONO_BOLD).color(theme::DANGER),
                    text(body.as_str()).size(13).width(Fill),
                    dismiss,
                ]
                .spacing(12)
                .align_y(iced::Center),
            )
            .style(theme::alarm_danger)
            .padding([8, 14])
            .width(Fill)
            .into(),
            Alarm::Info { body } => container(
                row![
                    text(body.as_str()).size(13).color(theme::TEXT).width(Fill),
                    dismiss,
                ]
                .spacing(12)
                .align_y(iced::Center),
            )
            .style(theme::alarm_info)
            .padding([8, 14])
            .width(Fill)
            .into(),
        }
    }

    fn section_label<'a>(label: &'a str) -> Element<'a, Msg> {
        container(text(label).size(10).font(MONO).color(theme::MUTED))
            .padding([0, 6])
            .into()
    }

    fn nav_button<'a>(
        label: String,
        mono: bool,
        unread: usize,
        active: bool,
        msg: Msg,
    ) -> Element<'a, Msg> {
        let name = if mono {
            text(label).size(13).font(MONO)
        } else {
            text(label).size(13)
        };
        let mut inner = Row::new()
            .spacing(6)
            .align_y(iced::Center)
            .push(name.width(Fill));
        if unread > 0 {
            inner = inner.push(
                container(text(unread.to_string()).size(10).font(MONO_BOLD))
                    .style(theme::badge)
                    .padding([1, 7]),
            );
        }
        button(inner)
            .width(Fill)
            .padding([4, 8])
            .style(theme::nav_item(active, unread > 0))
            .on_press(msg)
            .into()
    }

    fn dm_label(id: &IdentityId, nick: Option<&Nickname>) -> (String, bool) {
        match nick {
            Some(n) => (n.as_str().to_owned(), false),
            None => (shorten(&id.to_string(), 9), true),
        }
    }

    fn view_sidebar(&self) -> Element<'_, Msg> {
        let fp_short: String = {
            let groups: Vec<&str> = self.fingerprint.split('-').take(4).collect();
            format!("{}-…", groups.join("-"))
        };

        let mut list = Column::new().spacing(2);
        list = list.push(Self::section_label("CHANNELS"));
        for name in &self.channels {
            let key = name.as_str().to_owned();
            let unread = self.unread.get(&key).copied().unwrap_or(0);
            let active = self.active == Some(Target::Channel(name.clone()));
            list = list.push(Self::nav_button(
                key,
                true,
                unread,
                active,
                Msg::Select(Target::Channel(name.clone())),
            ));
        }
        if !self.dms.is_empty() {
            list = list.push(Space::with_height(12));
            list = list.push(Self::section_label("DIRECT MESSAGES"));
            for (id, nick) in &self.dms {
                let (label, is_hex) = Self::dm_label(id, nick.as_ref());
                let key = Target::Dm(*id).key();
                let unread = self.unread.get(&key).copied().unwrap_or(0);
                let active = self.active == Some(Target::Dm(*id));
                list = list.push(Self::nav_button(
                    label,
                    is_hex,
                    unread,
                    active,
                    Msg::Select(Target::Dm(*id)),
                ));
            }
        }

        let join = column![
            text_input("join #channel", &self.join_input)
                .on_input(Msg::JoinInput)
                .on_submit(Msg::JoinSubmit)
                .style(theme::input)
                .font(MONO)
                .padding(8)
                .size(12),
            text("join — or found, if unclaimed")
                .size(9)
                .font(MONO)
                .color(theme::MUTED),
        ]
        .spacing(4);

        let self_area = container(
            row![
                text("●").size(9).color(theme::TEAL),
                text(self.nickname.as_deref().unwrap_or("—"))
                    .size(13)
                    .font(SEMIBOLD)
                    .width(Fill),
            ]
            .spacing(8)
            .align_y(iced::Center),
        )
        .style(theme::panel_raised)
        .padding([9, 14])
        .width(Fill);

        container(column![
            container(
                column![
                    text("mikall").size(18).color(theme::TEAL),
                    text(fp_short).size(9).font(MONO).color(theme::MUTED),
                ]
                .spacing(2),
            )
            .padding([12, 12]),
            container(Space::new(Fill, 1)).style(theme::hairline),
            container(scrollable(list).height(Fill))
                .padding([10, 6])
                .height(Fill),
            container(join).padding([0, 10]),
            Space::with_height(10),
            self_area,
        ])
        .width(220)
        .height(Fill)
        .style(theme::panel)
        .into()
    }

    fn role_sigil(role: Role) -> &'static str {
        match role {
            Role::Founder => "~",
            Role::Op => "@",
            Role::Member => "",
        }
    }

    fn author_label(&self, message: &RenderedMessage) -> String {
        message
            .author_nick
            .as_ref()
            .map(|n| n.as_str().to_owned())
            .unwrap_or_else(|| shorten(&message.author.to_string(), 9))
    }

    fn view_chat(&self) -> Element<'_, Msg> {
        let Some(active) = &self.active else {
            return container(
                column![
                    text("welcome — join a channel to start").size(18),
                    text("channels are IRC-style #names — joining a name no one holds founds it")
                        .size(13)
                        .color(theme::MUTED),
                ]
                .spacing(8)
                .align_x(iced::Center),
            )
            .center_x(Fill)
            .center_y(Fill)
            .into();
        };

        let (title, placeholder) = match active {
            Target::Channel(name) => (
                name.as_str().to_owned(),
                format!("message {} — enter to send · max 4096 bytes", name.as_str()),
            ),
            Target::Dm(id) => {
                let label = self
                    .dms
                    .iter()
                    .find(|(other, _)| other == id)
                    .and_then(|(_, nick)| nick.as_ref())
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| shorten(&id.to_string(), 9));
                (
                    format!("dm — {label}"),
                    "message — enter to send · max 4096 bytes".to_owned(),
                )
            }
        };

        // Ring the DM partner, or every member of the channel (the mesh
        // itself is capped at 8 by the domain as people actually join).
        let me = self.me();
        let can_call = !self.call_in_progress()
            && match active {
                Target::Dm(_) => true,
                Target::Channel(_) => self.members.iter().any(|m| Some(m.id) != me),
            };

        let topic_bar = container(
            row![
                text(title).size(15).font(MONO_BOLD).color(theme::TEAL),
                text(&self.topic).size(13).color(theme::MUTED).width(Fill),
                button(text("call").size(11).font(MONO))
                    .style(theme::ghost)
                    .padding([3, 10])
                    .on_press_maybe(can_call.then_some(Msg::CallStart)),
                button(
                    text(if self.show_roster {
                        "roster »"
                    } else {
                        "« roster"
                    })
                    .size(11)
                    .font(MONO)
                )
                .style(theme::ghost)
                .padding([3, 10])
                .on_press(Msg::ToggleRoster),
            ]
            .spacing(14)
            .align_y(iced::Center),
        )
        .padding([11, 18]);

        let roles: HashMap<IdentityId, Role> =
            self.members.iter().map(|m| (m.id, m.role)).collect();

        let mut timeline = Column::new().spacing(16).padding([12, 18]);
        let mut i = 0;
        while i < self.messages.len() {
            let first = &self.messages[i];
            let author = first.author;
            let label = self.author_label(first);
            let sigil = roles
                .get(&author)
                .copied()
                .map(Self::role_sigil)
                .unwrap_or("");

            let mut head = Row::new().spacing(8).align_y(iced::Center);
            if !sigil.is_empty() {
                head = head.push(text(sigil).size(13).font(MONO).color(theme::MUTED));
            }
            head = head
                .push(
                    text(label.clone())
                        .size(14)
                        .font(SEMIBOLD)
                        .color(theme::PINK),
                )
                .push(
                    text(ts_hint(first.ts_hint_ms))
                        .size(10)
                        .font(MONO)
                        .color(theme::MUTED),
                );

            let mut group = Column::new().spacing(3).push(head);
            while i < self.messages.len() && self.messages[i].author == author {
                let body = self.messages[i].body.as_str();
                let line: Element<'_, Msg> = match body.strip_prefix("/me ") {
                    Some(action) => text(format!("* {label} {action}"))
                        .size(14)
                        .font(ITALIC)
                        .color(theme::MUTED)
                        .into(),
                    None => text(body.to_owned()).size(14).into(),
                };
                group = group.push(line);
                i += 1;
            }
            timeline = timeline.push(group);
        }

        let composer = container(
            text_input(&placeholder, &self.composer)
                .on_input(Msg::ComposerInput)
                .on_submit(Msg::Send)
                .style(theme::composer)
                .padding([10, 14])
                .size(14),
        )
        .padding([10, 18]);

        container(column![
            topic_bar,
            container(Space::new(Fill, 1)).style(theme::hairline),
            scrollable(timeline)
                .anchor_bottom()
                .height(Fill)
                .width(Fill),
            composer,
        ])
        .height(Fill)
        .width(Fill)
        .into()
    }

    fn view_roster(&self) -> Element<'_, Msg> {
        let me = self.me();
        let mut list = Column::new().spacing(3);
        list = list.push(
            text(format!("MEMBERS — {}", self.members.len()))
                .size(10)
                .font(MONO)
                .color(theme::MUTED),
        );
        list = list.push(Space::with_height(4));

        for member in &self.members {
            let state = self.presence.get(&member.id);
            let is_me = me == Some(member.id);
            let (dot, dot_color, name_color) = match state {
                Some(PresenceState::Online) => ("●", theme::TEAL, theme::TEXT),
                Some(PresenceState::Away { .. }) => ("●", theme::AMBER, theme::MUTED),
                // This node is running, so its own identity is online by
                // definition — the roster only observes peers' beacons.
                Some(PresenceState::Offline { .. }) | None if is_me => {
                    ("●", theme::TEAL, theme::TEXT)
                }
                Some(PresenceState::Offline { .. }) | None => ("○", theme::MUTED, theme::MUTED),
            };
            let sigil = Self::role_sigil(member.role);
            let nick = member
                .nick
                .as_ref()
                .map(|n| n.as_str().to_owned())
                .unwrap_or_else(|| shorten(&member.id.to_string(), 9));

            let mut entry = Row::new().spacing(7).align_y(iced::Center);
            entry = entry.push(text(dot).size(9).color(dot_color));
            if !sigil.is_empty() {
                entry = entry.push(text(sigil).size(12).font(MONO).color(theme::TEAL));
            }
            entry = entry.push(text(nick).size(13).color(name_color).width(Fill));
            if me == Some(member.id) {
                entry = entry.push(text("you").size(10).font(MONO).color(theme::MUTED));
            }

            let entry: Element<'_, Msg> = match state {
                Some(PresenceState::Away {
                    message: Some(away),
                }) => tooltip(
                    entry,
                    container(text(away.as_str().to_owned()).size(11))
                        .style(theme::card)
                        .padding([4, 8]),
                    tooltip::Position::Left,
                )
                .into(),
                _ => entry.into(),
            };
            list = list.push(entry);
        }

        container(scrollable(list).height(Fill))
            .padding(14)
            .width(180)
            .height(Fill)
            .style(theme::panel)
            .into()
    }
}

fn main() -> iced::Result {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,libp2p=warn,wgpu=warn".into()),
        )
        .init();
    iced::application("mikall", Mikall::update, Mikall::view)
        .subscription(Mikall::subscription)
        .theme(|_| theme::miku_teal())
        .run_with(Mikall::boot)
}
