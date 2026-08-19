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
use std::sync::Arc;
use std::time::Duration;

use iced::widget::{
    button, column, container, row, scrollable, text, text_input, tooltip, Column, Row, Space,
};
use iced::{Element, Fill, Font, Subscription, Task};

use mikall_app::events::AppEvent;
use mikall_app::ports::PeerAddr;
use mikall_app::ports::{GatewayPort, IrcGatewayConfig};
use mikall_app::services::{CallSnapshot, MemberView, RenderedMessage};
use mikall_domain::calls::{CallEvent, CallId, CallPhase, CallRoster, EndReason, MediaState};
use mikall_domain::identity::IdentityEvent;
use mikall_domain::messaging::{ChannelName, MessagingEvent, Nickname, Role};
use mikall_domain::presence::PresenceState;
use mikall_domain::shared::IdentityId;
use mikall_domain::DomainEvent;
use mikall_node::{start, MediaAlert, NodeConfig, NodeHandle};

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

/// Copy the identity key file to `~/Downloads/mikall-identity.key`,
/// keeping owner-only permissions. Key custody stays in mikall-crypto —
/// this is a byte-for-byte file copy of the backup the user asked for.
fn export_key_file(source: &std::path::Path) -> Result<String, String> {
    let home = std::env::var("HOME").map_err(|_| "no HOME directory to export into".to_owned())?;
    let dest_dir = PathBuf::from(home).join("Downloads");
    std::fs::create_dir_all(&dest_dir).map_err(|e| e.to_string())?;
    let dest = dest_dir.join("mikall-identity.key");
    std::fs::copy(source, &dest).map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
    }
    Ok(dest.display().to_string())
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
    /// Which remote sharer's viewer pane is expanded. Pure local view
    /// state — collapsing it never touches the call. It auto-opens when a
    /// remote `ScreenShareStarted` lands and is reconciled against every
    /// snapshot, so it can never point at someone the aggregate says
    /// stopped sharing (or at ourselves).
    watching: Option<IdentityId>,
}

/// The abortable accept-loop of a running gateway. `mikall_irc::serve`
/// hands back a `JoinHandle`; keeping it (instead of M7's `_task` drop) is
/// what makes the settings toggle able to stop the gateway live. Wrapped in
/// `Arc<Mutex<Option<…>>>` so it can ride inside a `Msg` (which must be
/// `Clone`) and be taken exactly once.
#[derive(Debug, Clone)]
struct GatewayTask(Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>);

impl GatewayTask {
    fn new(task: tokio::task::JoinHandle<()>) -> Self {
        GatewayTask(Arc::new(std::sync::Mutex::new(Some(task))))
    }

    /// Abort the accept loop: the listener drops and the port closes.
    /// Already-connected IRC sessions run on their own tasks and keep
    /// their connections until they disconnect.
    fn abort(&self) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(task) = guard.take() {
                task.abort();
            }
        }
    }
}

/// A gateway that came up: where it listens, whether the environment forced
/// it (`MIKALL_IRC` wins over the persisted setting), and its abort handle.
#[derive(Debug, Clone)]
struct GatewayUp {
    addr: String,
    env_forced: bool,
    task: GatewayTask,
}

/// Runtime state of the loopback IRC gateway, distinct from the *persisted*
/// choice (`Mikall::irc_cfg`): env override or a start failure can make the
/// two disagree, and the settings screen shows the runtime truth.
#[derive(Debug)]
enum GatewayState {
    Stopped,
    Starting { env_forced: bool },
    Running(GatewayUp),
}

/// Local state of the settings surface: the gateway inputs being edited and
/// the two-step key-export confirmation.
#[derive(Debug)]
struct SettingsUi {
    port_input: String,
    pass_input: String,
    export_armed: bool,
}

#[derive(Debug, Clone)]
enum Msg {
    Booted(Result<NodeHandle, String>),
    NickLoaded(Option<String>),
    ChannelsLoaded(Vec<ChannelName>),
    IrcConfigLoaded(IrcGatewayConfig),
    IrcGateway(Result<GatewayUp, String>),
    IrcToggle,
    IrcPortInput(String),
    IrcPassInput(String),
    IrcSave,
    OpenSettings,
    CloseSettings,
    ListenAddrsLoaded(Vec<String>),
    DialInput(String),
    DialSubmit,
    Dialed(Result<String, String>),
    KnownPeersLoaded(Vec<PeerAddr>),
    ForgetPeer(PeerAddr),
    CopyText(String),
    BlockedLoaded(Vec<(IdentityId, Option<Nickname>)>),
    Unblock(IdentityId),
    CopyKeyPath,
    ExportKeyArm,
    ExportKeyCancel,
    ExportKeyConfirm,
    KeyExported(Result<String, String>),
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
    /// Ring one more peer into the ongoing call (mid-call invite).
    CallInvite(CallId, IdentityId),
    /// Toggle our own screen share (true = start, false = stop).
    ShareScreen(CallId, bool),
    /// Toggle our mic (true = mute). Real: the media engine sends silence
    /// while muted, and the button renders from the aggregate's state.
    ToggleMute(CallId, bool),
    /// Voice-device trouble from the node (mic denied, no output device).
    MediaTrouble(MediaAlert),
    /// Expand the viewer pane for a remote sharer (local view only).
    Watch(IdentityId),
    /// Collapse the viewer back to tiles (local view only — the sharer
    /// keeps sharing).
    StopWatching,
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
    /// Our libp2p listen addresses — shown in settings → network so a peer
    /// can dial us out of band when mDNS discovery is unavailable.
    listen_addrs: Vec<String>,
    dial_input: String,
    /// Peers persisted for automatic redial at boot (settings → network).
    known_peers: Vec<PeerAddr>,
    call: Option<CallUi>,
    /// Incoming offers that rang while another call surface was up; the
    /// next one is shown when the current call ends or is dismissed.
    call_backlog: Vec<CallId>,
    /// Alternates while ringing to drive the strip's gentle edge pulse.
    ring_glow: bool,
    /// The settings surface, when open. It replaces the three panes inside
    /// `view_main`, so the alarm slot and call strip stay visible above it.
    settings: Option<SettingsUi>,
    /// Runtime state of the loopback IRC gateway.
    gateway: GatewayState,
    /// The persisted gateway choice, hydrated at boot and kept in step
    /// with every save — the source for the settings inputs.
    irc_cfg: IrcGatewayConfig,
    /// Blocked identities for the settings blocklist (refreshed on open
    /// and on `ContactBlocked`).
    blocked: Vec<(IdentityId, Option<Nickname>)>,
    /// Where the identity key file lives — custody stays in mikall-crypto;
    /// the settings screen only names (and offers to copy) the file.
    key_path: PathBuf,
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
            listen_addrs: Vec::new(),
            dial_input: String::new(),
            known_peers: Vec::new(),
            call: None,
            call_backlog: Vec::new(),
            ring_glow: false,
            settings: None,
            gateway: GatewayState::Stopped,
            irc_cfg: IrcGatewayConfig::default(),
            blocked: Vec::new(),
            key_path: data_dir().join("identity.key"),
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

    fn load_listen_addrs(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        Task::perform(
            async move {
                node.net
                    .listen_addrs()
                    .await
                    .into_iter()
                    .map(|a| a.to_string())
                    .collect()
            },
            Msg::ListenAddrsLoaded,
        )
    }

    fn load_known_peers(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        Task::perform(
            async move { node.identity.known_peers().await },
            Msg::KnownPeersLoaded,
        )
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

    /// Serve RFC 1459 on loopback — the same gateway `mikalld` offers, so
    /// WeeChat sits beside this window. The returned task's `JoinHandle`
    /// rides back inside [`GatewayUp`] so the settings toggle can stop it.
    fn start_gateway(
        &mut self,
        port: u16,
        password: Option<String>,
        env_forced: bool,
    ) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        self.gateway = GatewayState::Starting { env_forced };
        let services = mikall_irc::GatewayServices {
            identity: node.identity.clone(),
            chat: node.chat.clone(),
            dm: node.dm.clone(),
            presence: node.presence.clone(),
            bus: node.bus.clone(),
        };
        let config = mikall_irc::IrcConfig {
            bind: mikall_irc::IrcBindAddr::localhost(port),
            password,
        };
        Task::perform(
            async move {
                mikall_irc::serve(services, config)
                    .await
                    .map(|(addr, task)| GatewayUp {
                        addr: addr.to_string(),
                        env_forced,
                        task: GatewayTask::new(task),
                    })
                    .map_err(|e| e.to_string())
            },
            Msg::IrcGateway,
        )
    }

    /// Persist the current gateway choice through the identity service —
    /// the single settings writer, so a save keeps every other field.
    fn persist_irc_cfg(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        let config = self.irc_cfg.clone();
        Task::perform(
            async move { node.identity.set_irc_config(config).await },
            |_| Msg::Noop,
        )
    }

    /// The gateway inputs of an open settings surface, parsed: a valid
    /// user-space port and the password (empty means none).
    fn settings_gateway_input(&self) -> Option<(GatewayPort, Option<String>)> {
        let settings = self.settings.as_ref()?;
        let port = settings
            .port_input
            .trim()
            .parse::<u16>()
            .ok()
            .and_then(|raw| GatewayPort::new(raw).ok())?;
        let password = Some(settings.pass_input.clone()).filter(|p| !p.is_empty());
        Some((port, password))
    }

    /// Refresh the settings blocklist from the identity service.
    fn load_blocked(&self) -> Task<Msg> {
        let Some(node) = self.node() else {
            return Task::none();
        };
        Task::perform(
            async move { node.identity.blocked_contacts().await },
            Msg::BlockedLoaded,
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
        self.call = Some(CallUi {
            id,
            snapshot: None,
            watching: None,
        });
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
            | CallEvent::CallOpened { call, .. }
            | CallEvent::CallInvited { call, .. }
            | CallEvent::CallAccepted { call, .. }
            | CallEvent::CallDeclined { call, .. }
            | CallEvent::ParticipantJoined { call, .. }
            | CallEvent::ParticipantLeft { call, .. }
            | CallEvent::ScreenShareStarted { call, .. }
            | CallEvent::ScreenShareStopped { call, .. }
            | CallEvent::MicMuted { call, .. }
            | CallEvent::MicUnmuted { call, .. }
            | CallEvent::CallEnded { call, .. } => *call,
        }
    }

    fn update(&mut self, message: Msg) -> Task<Msg> {
        match message {
            Msg::Booted(Ok(node)) => {
                self.fingerprint = node.identity.fingerprint().display_groups();
                let identity = node.identity.clone();
                let identity_cfg = node.identity.clone();
                self.node = Some(node);
                let nick = Task::perform(
                    async move { identity.nickname().await.map(|n| n.as_str().to_owned()) },
                    Msg::NickLoaded,
                );
                // The persisted gateway choice decides whether the gateway
                // starts; `Msg::IrcConfigLoaded` applies the env override.
                let irc = Task::perform(
                    async move { identity_cfg.irc_config().await },
                    Msg::IrcConfigLoaded,
                );
                Task::batch([nick, self.load_channels(), irc, self.load_listen_addrs()])
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
            Msg::IrcConfigLoaded(config) => {
                self.irc_cfg = config;
                // If the settings surface opened before the hydrated config
                // arrived (a boot-time race), refresh its inputs so they
                // show the persisted values, not the defaults.
                if let Some(settings) = &mut self.settings {
                    settings.port_input = self.irc_cfg.port.get().to_string();
                    settings.pass_input = self.irc_cfg.password.clone().unwrap_or_default();
                }
                // Env override wins: MIKALL_IRC forces the gateway on for
                // this session, whatever the persisted choice says.
                match std::env::var("MIKALL_IRC") {
                    Ok(raw) => match raw.parse::<u16>() {
                        Ok(port) => {
                            self.start_gateway(port, std::env::var("MIKALL_IRC_PASS").ok(), true)
                        }
                        Err(_) => Task::done(Msg::IrcGateway(Err(
                            "MIKALL_IRC must be a port number".to_owned(),
                        ))),
                    },
                    Err(_) if self.irc_cfg.enabled => {
                        let port = self.irc_cfg.port.get();
                        let password = self.irc_cfg.password.clone();
                        self.start_gateway(port, password, false)
                    }
                    Err(_) => Task::none(),
                }
            }
            Msg::IrcGateway(Ok(up)) => {
                self.alarm = Some(Alarm::Info {
                    body: if up.env_forced {
                        format!(
                            "irc gateway on {} — plaintext, loopback only · forced by MIKALL_IRC",
                            up.addr
                        )
                    } else {
                        format!("irc gateway on {} — plaintext, loopback only", up.addr)
                    },
                });
                self.gateway = GatewayState::Running(up);
                Task::none()
            }
            Msg::IrcGateway(Err(error)) => {
                self.gateway = GatewayState::Stopped;
                self.alarm = Some(Alarm::Danger {
                    title: "IRC GATEWAY",
                    body: error,
                });
                Task::none()
            }
            Msg::IrcToggle => match &self.gateway {
                GatewayState::Running(up) if !up.env_forced => {
                    // Stop live *and* persist the choice — a toggle that
                    // forgot itself on restart would be a bug, not a quirk.
                    up.task.abort();
                    self.gateway = GatewayState::Stopped;
                    self.irc_cfg.enabled = false;
                    self.persist_irc_cfg()
                }
                GatewayState::Stopped => {
                    let Some((port, password)) = self.settings_gateway_input() else {
                        return Task::none();
                    };
                    self.irc_cfg = IrcGatewayConfig {
                        enabled: true,
                        port,
                        password: password.clone(),
                    };
                    let start = self.start_gateway(port.get(), password, false);
                    Task::batch([self.persist_irc_cfg(), start])
                }
                GatewayState::Running(_) | GatewayState::Starting { .. } => Task::none(),
            },
            Msg::IrcPortInput(value) => {
                if let Some(settings) = &mut self.settings {
                    settings.port_input = value;
                }
                Task::none()
            }
            Msg::IrcPassInput(value) => {
                if let Some(settings) = &mut self.settings {
                    settings.pass_input = value;
                }
                Task::none()
            }
            Msg::IrcSave => {
                let Some((port, password)) = self.settings_gateway_input() else {
                    return Task::none();
                };
                self.irc_cfg.port = port;
                self.irc_cfg.password = password.clone();
                let persist = self.persist_irc_cfg();
                match &self.gateway {
                    // Saving while the gateway runs applies live: restart
                    // on the new port/password.
                    GatewayState::Running(up) if !up.env_forced => {
                        up.task.abort();
                        let start = self.start_gateway(port.get(), password, false);
                        Task::batch([persist, start])
                    }
                    GatewayState::Running(_)
                    | GatewayState::Starting { .. }
                    | GatewayState::Stopped => {
                        self.alarm = Some(Alarm::Info {
                            body: "irc gateway settings saved".to_owned(),
                        });
                        persist
                    }
                }
            }
            Msg::OpenSettings => {
                self.settings = Some(SettingsUi {
                    port_input: self.irc_cfg.port.get().to_string(),
                    pass_input: self.irc_cfg.password.clone().unwrap_or_default(),
                    export_armed: false,
                });
                Task::batch([
                    self.load_blocked(),
                    self.load_listen_addrs(),
                    self.load_known_peers(),
                ])
            }
            Msg::CloseSettings => {
                self.settings = None;
                Task::none()
            }
            Msg::ListenAddrsLoaded(addrs) => {
                self.listen_addrs = addrs;
                Task::none()
            }
            Msg::DialInput(value) => {
                self.dial_input = value;
                Task::none()
            }
            Msg::DialSubmit => {
                let raw = self.dial_input.trim().to_owned();
                let Some(node) = self.node() else {
                    return Task::none();
                };
                if raw.is_empty() {
                    return Task::none();
                }
                self.dial_input.clear();
                Task::perform(
                    async move {
                        let addr = raw.parse().map_err(|e| format!("{e}"))?;
                        node.net
                            .dial(addr)
                            .await
                            .map(|()| raw)
                            .map_err(|e| e.to_string())
                    },
                    Msg::Dialed,
                )
            }
            Msg::Dialed(Ok(addr)) => {
                self.alarm = Some(Alarm::Info {
                    body: format!("connected — dialed {addr} (remembered for next boot)"),
                });
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        if let Ok(peer) = PeerAddr::parse(&addr) {
                            node.identity.remember_peer(peer).await;
                        }
                        node.identity.known_peers().await
                    },
                    Msg::KnownPeersLoaded,
                )
            }
            Msg::Dialed(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "DIAL FAILED",
                    body: error,
                });
                Task::none()
            }
            Msg::KnownPeersLoaded(peers) => {
                self.known_peers = peers;
                Task::none()
            }
            Msg::ForgetPeer(peer) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.identity.forget_peer(&peer).await;
                        node.identity.known_peers().await
                    },
                    Msg::KnownPeersLoaded,
                )
            }
            Msg::CopyText(value) => iced::clipboard::write(value),
            Msg::BlockedLoaded(blocked) => {
                self.blocked = blocked;
                Task::none()
            }
            Msg::Unblock(id) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.identity.unblock(id).await;
                        node.identity.blocked_contacts().await
                    },
                    Msg::BlockedLoaded,
                )
            }
            Msg::CopyKeyPath => iced::clipboard::write(self.key_path.display().to_string()),
            Msg::ExportKeyArm => {
                if let Some(settings) = &mut self.settings {
                    settings.export_armed = true;
                }
                Task::none()
            }
            Msg::ExportKeyCancel => {
                if let Some(settings) = &mut self.settings {
                    settings.export_armed = false;
                }
                Task::none()
            }
            Msg::ExportKeyConfirm => {
                if let Some(settings) = &mut self.settings {
                    settings.export_armed = false;
                }
                let source = self.key_path.clone();
                Task::perform(async move { export_key_file(&source) }, Msg::KeyExported)
            }
            Msg::KeyExported(Ok(path)) => {
                self.alarm = Some(Alarm::Info {
                    body: format!(
                        "key file copied to {path} — move it somewhere offline, then delete the copy"
                    ),
                });
                Task::none()
            }
            Msg::KeyExported(Err(error)) => {
                self.alarm = Some(Alarm::Danger {
                    title: "KEY EXPORT",
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
                // Nobody else around? An empty offer list opens a solo
                // stage — active with just us, ready to ring people in.
                let peers: Vec<IdentityId> = match target {
                    Target::Dm(id) => vec![id],
                    Target::Channel(_) => self
                        .members
                        .iter()
                        .map(|m| m.id)
                        .filter(|id| Some(*id) != me)
                        .collect(),
                };
                Task::perform(
                    async move { node.calls.start_call(peers).await },
                    Msg::CallStarted,
                )
            }
            Msg::CallStarted(id) => {
                self.call = Some(CallUi {
                    id,
                    snapshot: None,
                    watching: None,
                });
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
            Msg::CallInvite(id, who) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.calls
                            .invite(id, vec![who])
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
            Msg::ShareScreen(id, active) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.calls
                            .share_screen(id, active)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    Msg::CallDone,
                )
            }
            Msg::ToggleMute(id, muted) => {
                let Some(node) = self.node() else {
                    return Task::none();
                };
                Task::perform(
                    async move {
                        node.calls
                            .set_muted(id, muted)
                            .await
                            .map_err(|e| e.to_string())
                    },
                    Msg::CallDone,
                )
            }
            Msg::MediaTrouble(alert) => {
                // Honest, not fatal: the call carries on in whatever
                // direction still works; the alarm says which one broke.
                let (title, body) = match alert {
                    MediaAlert::MicUnavailable { detail, .. } => (
                        "MIC",
                        format!(
                            "mic unavailable — {detail}. The call continues receive-only \
                             (peers cannot hear you). On macOS: System Settings → Privacy \
                             & Security → Microphone."
                        ),
                    ),
                    MediaAlert::SpeakerUnavailable { detail, .. } => (
                        "AUDIO OUT",
                        format!(
                            "speaker unavailable — {detail}. Peers still hear you; \
                             you cannot hear them."
                        ),
                    ),
                };
                self.alarm = Some(Alarm::Danger { title, body });
                Task::none()
            }
            Msg::Watch(who) => {
                // Never watch ourselves — our own share has no viewer.
                if Some(who) != self.me() {
                    if let Some(call) = &mut self.call {
                        call.watching = Some(who);
                    }
                }
                Task::none()
            }
            Msg::StopWatching => {
                if let Some(call) = &mut self.call {
                    call.watching = None;
                }
                Task::none()
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
                // The viewer may only point at a remote participant the
                // aggregate says is sharing — anyone who stopped or left
                // collapses it back to tiles.
                if let Some(peer) = call.watching {
                    let still_sharing = snapshot
                        .participants
                        .iter()
                        .any(|(who, media)| *who == peer && media.sharing_screen);
                    if !still_sharing {
                        call.watching = None;
                    }
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
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::ContactBlocked { .. })) => {
                // A block from any frontend (IRC MODE +b) lands in the
                // settings blocklist while it is open.
                if self.settings.is_some() {
                    self.load_blocked()
                } else {
                    Task::none()
                }
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
    /// refetch, so the surface always mirrors the aggregate. The one
    /// exception is `CallUi::watching`, which is local view preference:
    /// share events open/close the viewer pane here.
    fn handle_call_event(&mut self, event: CallEvent) -> Task<Msg> {
        let me = self.me();
        if let CallEvent::CallOffered { call, by, .. } = &event {
            let incoming = Some(*by) != me;
            if self.call.as_ref().map(|c| c.id) != Some(*call) {
                if self.call_in_progress() {
                    // Gentle interruption only: one call surface at a time.
                    if incoming && !self.call_backlog.contains(call) {
                        self.call_backlog.push(*call);
                    }
                    return Task::none();
                }
                // Incoming rings and calls this node started from *any*
                // frontend (this GUI via `Msg::CallStarted`, mikalld, a
                // future one) both surface here — the GUI always shows
                // its node's calls.
                self.call = Some(CallUi {
                    id: *call,
                    snapshot: None,
                    watching: None,
                });
                return self.refresh_call(*call);
            }
        }
        if let Some(current) = &mut self.call {
            match &event {
                // A remote share just started: open the viewer (brief
                // §3.3 — "their tile expands or a viewer pane opens").
                CallEvent::ScreenShareStarted { call, who }
                    if *call == current.id && Some(*who) != me =>
                {
                    current.watching = Some(*who);
                }
                CallEvent::ScreenShareStopped { call, who }
                    if *call == current.id && current.watching == Some(*who) =>
                {
                    current.watching = None;
                }
                _ => {}
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
        let alerts = Subscription::run_with_id(
            "mikall-media-alerts",
            futures::stream::unfold(node.subscribe_media_alerts(), |mut rx| async move {
                loop {
                    match rx.recv().await {
                        Ok(alert) => return Some((Msg::MediaTrouble(alert), rx)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }),
        );
        let mut subs = vec![events, alerts];
        if self.call_is_ringing() {
            // The pulse only ticks while something actually rings.
            subs.push(iced::time::every(Duration::from_millis(700)).map(|_| Msg::RingPulse));
        }
        if self.settings.is_some() {
            // Esc closes the settings surface (keyboard-friendly, §6).
            subs.push(iced::keyboard::on_key_press(|key, _modifiers| match key {
                iced::keyboard::Key::Named(iced::keyboard::key::Named::Escape) => {
                    Some(Msg::CloseSettings)
                }
                _ => None,
            }));
        }
        Subscription::batch(subs)
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
        // a gentle interruption, never a modal takeover. The screen-share
        // viewer grows inside the active-call view of this strip, so the
        // three-pane body is never touched.
        if let Some(surface) = self.view_call() {
            root = root.push(surface);
        }
        // The settings surface replaces the three panes but docks below the
        // alarm slot and call strip, so a key-change alarm or a ringing
        // call is never hidden by it.
        if let Some(settings) = &self.settings {
            return root.push(self.view_settings(settings)).into();
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
    /// The mic toggle is real as of M15: it drives `CallService::set_muted`,
    /// the media engine sends silence while muted, and the button renders
    /// from the aggregate's state — never a local bool. Deafen is still
    /// absent on purpose: nothing implements it yet, and a control that
    /// does nothing would lie. The share-screen toggle drives
    /// `CallService::share_screen` the same way.
    fn view_call_active(&self, id: CallId, snapshot: &CallSnapshot) -> Element<'_, Msg> {
        let me = self.me();
        let i_am_sharing = snapshot
            .participants
            .iter()
            .any(|(who, media)| Some(*who) == me && media.sharing_screen);
        let i_am_muted = snapshot
            .participants
            .iter()
            .any(|(who, media)| Some(*who) == me && media.mic_muted);
        // The expanded viewer only ever shows a remote participant the
        // aggregate says is sharing right now (we never watch ourselves).
        let watching = self
            .call
            .as_ref()
            .and_then(|call| call.watching)
            .filter(|peer| {
                Some(*peer) != me
                    && snapshot
                        .participants
                        .iter()
                        .any(|(who, media)| who == peer && media.sharing_screen)
            });

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

        let share_toggle = if i_am_sharing {
            button(text("stop sharing").size(12).font(MONO))
                .style(theme::teal_outline)
                .padding([6, 14])
                .on_press(Msg::ShareScreen(id, false))
        } else {
            button(text("share screen").size(12).font(MONO))
                .style(theme::ghost)
                .padding([6, 14])
                .on_press(Msg::ShareScreen(id, true))
        };
        // Real state, real switch: while muted the engine ships silence
        // frames, so the red button always tells the truth.
        let mute_toggle = if i_am_muted {
            button(text("mic off — unmute").size(12).font(MONO))
                .style(theme::danger_outline)
                .padding([6, 14])
                .on_press(Msg::ToggleMute(id, false))
        } else {
            button(text("mute mic").size(12).font(MONO))
                .style(theme::ghost)
                .padding([6, 14])
                .on_press(Msg::ToggleMute(id, true))
        };
        let controls = row![
            mute_toggle,
            share_toggle,
            button(text("hang up").size(12).font(SEMIBOLD))
                .style(theme::danger)
                .padding([6, 20])
                .on_press(Msg::CallHangUp(id)),
        ]
        .spacing(10);

        let mut body = Column::new().spacing(10).push(header);
        match watching {
            // Watching a remote share: the viewer pane replaces the tile
            // grid; "stop watching" brings the tiles back.
            Some(peer) => body = body.push(self.view_share_viewer(peer)),
            None => body = body.push(container(grid).center_x(Fill)),
        }
        // The stage is a room: whoever is here (channel members, or the DM
        // partner) but not in the call can be rung in from below. Alone on
        // an open stage is a legal, honest state — say so plainly.
        let candidates = self.invite_candidates(snapshot);
        if snapshot.participants.len() == 1 {
            let line = if candidates.is_empty() && snapshot.invited.is_empty() {
                "you're on stage alone — no one else is here to ring yet"
            } else {
                "you're on stage alone — ring someone below, or wait"
            };
            body = body
                .push(container(text(line).size(11).font(MONO).color(theme::MUTED)).center_x(Fill));
        }
        if !candidates.is_empty() || !snapshot.invited.is_empty() {
            body = body.push(self.view_ring_list(id, snapshot, &candidates));
        }
        if i_am_sharing {
            // Our own share never gets a self-viewer — just the plain
            // state, said honestly (§5): the share is real signaling, but
            // nothing captures pixels yet.
            body = body.push(
                container(
                    text("you are sharing your screen — signaled to peers, no pixels captured yet")
                        .size(10)
                        .font(MONO)
                        .color(theme::TEAL),
                )
                .center_x(Fill),
            );
        }
        body = body.push(container(controls).center_x(Fill));

        container(body)
            .style(theme::call_panel)
            .padding([12, 18])
            .width(Fill)
            .into()
    }

    /// Brief §3.3: when someone shares, a viewer pane opens — a 16:9 area
    /// with the sharer's nick and "stop watching". Screen-share *signaling*
    /// is shipped; no decodable video reaches this process yet, so the
    /// stage renders the honest placeholder instead of a fake image.
    fn view_share_viewer(&self, peer: IdentityId) -> Element<'_, Msg> {
        let (label, mono) = self.peer_label(peer);
        let name = if mono {
            text(label).size(13).font(MONO)
        } else {
            text(label).size(13).font(SEMIBOLD)
        };
        let header = row![
            text("SCREEN SHARE")
                .size(11)
                .font(MONO_BOLD)
                .color(theme::TEAL),
            name,
            text("is sharing their screen")
                .size(12)
                .color(theme::MUTED)
                .width(Fill),
            button(text("stop watching").size(11).font(MONO))
                .style(theme::ghost)
                .padding([3, 10])
                .on_press(Msg::StopWatching),
        ]
        .spacing(10)
        .align_y(iced::Center);
        let stage = container(
            text("share signaled — video frames land with the hardware-media milestone")
                .size(11)
                .font(MONO)
                .color(theme::MUTED),
        )
        .style(theme::share_stage)
        .center_x(480)
        .center_y(270);
        column![header, container(stage).center_x(Fill)]
            .spacing(8)
            .width(Fill)
            .into()
    }

    /// Channel members (or the DM partner) who are neither in the call nor
    /// already rung — the ring buttons of the active-call view.
    fn invite_candidates(&self, snapshot: &CallSnapshot) -> Vec<IdentityId> {
        let me = self.me();
        let absent = |id: &IdentityId| {
            Some(*id) != me
                && !snapshot.participants.iter().any(|(who, _)| who == id)
                && !snapshot.invited.contains(id)
        };
        match &self.active {
            Some(Target::Channel(_)) => self.members.iter().map(|m| m.id).filter(absent).collect(),
            Some(Target::Dm(peer)) => [*peer].into_iter().filter(absent).collect(),
            None => Vec::new(),
        }
    }

    /// The ring/invite strip of an active call: absent peers each get a
    /// "ring" button; peers already rung show as ringing until they answer
    /// or decline — never a second ring button.
    fn view_ring_list(
        &self,
        id: CallId,
        snapshot: &CallSnapshot,
        candidates: &[IdentityId],
    ) -> Element<'_, Msg> {
        let entries: Vec<(IdentityId, bool)> = snapshot
            .invited
            .iter()
            .map(|peer| (*peer, true))
            .chain(candidates.iter().map(|peer| (*peer, false)))
            .collect();
        let mut list = Column::new().spacing(6).align_x(iced::Center);
        for chunk in entries.chunks(4) {
            let mut line = Row::new().spacing(8).align_y(iced::Center);
            for (peer, pending) in chunk {
                let (label, mono) = self.peer_label(*peer);
                let name = if mono {
                    text(label).size(12).font(MONO)
                } else {
                    text(label).size(12).font(SEMIBOLD)
                };
                let entry: Element<'_, Msg> = if *pending {
                    row![
                        name,
                        text("ringing…").size(10).font(MONO).color(theme::TEAL),
                    ]
                    .spacing(8)
                    .align_y(iced::Center)
                    .into()
                } else {
                    row![
                        name,
                        button(text("ring").size(10).font(MONO))
                            .style(theme::ghost)
                            .padding([2, 10])
                            .on_press(Msg::CallInvite(id, *peer)),
                    ]
                    .spacing(8)
                    .align_y(iced::Center)
                    .into()
                };
                line = line.push(container(entry).style(theme::call_tile).padding([6, 12]));
            }
            list = list.push(line);
        }
        column![
            container(
                text("not in the call")
                    .size(10)
                    .font(MONO)
                    .color(theme::MUTED)
            )
            .center_x(Fill),
            list,
        ]
        .spacing(6)
        .align_x(iced::Center)
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
        // Real per-participant media state from the aggregate. Sharing is
        // teal — a feature, not a fault like a muted mic.
        let mut chips = Row::new().spacing(4);
        let flagged = media.mic_muted || media.deafened || media.sharing_screen;
        if media.mic_muted {
            chips = chips.push(Self::media_chip("mic off", theme::chip_danger));
        }
        if media.deafened {
            chips = chips.push(Self::media_chip("deafened", theme::chip_danger));
        }
        if media.sharing_screen {
            chips = chips.push(Self::media_chip("sharing", theme::chip_teal));
        }
        if flagged {
            tile = tile.push(chips);
        }
        // A collapsed remote share reopens from the sharer's tile.
        let watching = self.call.as_ref().and_then(|call| call.watching);
        if media.sharing_screen && !is_me && watching != Some(who) {
            tile = tile.push(
                button(text("watch").size(10).font(MONO))
                    .style(theme::ghost)
                    .padding([2, 10])
                    .on_press(Msg::Watch(who)),
            );
        }
        container(tile)
            .style(theme::call_tile)
            .padding([10, 14])
            .width(150)
            .align_x(iced::Center)
            .into()
    }

    fn media_chip(label: &str, style: fn(&iced::Theme) -> container::Style) -> Element<'_, Msg> {
        container(text(label).size(9).font(MONO))
            .style(style)
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
                button(text("settings").size(10).font(MONO))
                    .style(theme::ghost)
                    .padding([2, 8])
                    .on_press(Msg::OpenSettings),
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
                    Space::with_height(4),
                    text("alone on the network? settings → network shows your address and dials a peer")
                        .size(11)
                        .font(MONO)
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
        // Alone is fine too: calling with nobody else here opens a solo
        // stage to ring people into as they arrive.
        let can_call = !self.call_in_progress();

        let call_button = button(text("call").size(11).font(MONO))
            .style(theme::ghost)
            .padding([3, 10])
            .on_press_maybe(can_call.then_some(Msg::CallStart));
        // A dead control with no explanation is a lie of omission: when the
        // button can't work, hovering says why.
        let call_button: Element<'_, Msg> = if can_call {
            call_button.into()
        } else {
            tooltip(
                call_button,
                container(text("a call is already up — hang up first").size(11))
                    .style(theme::card)
                    .padding([4, 8]),
                tooltip::Position::Bottom,
            )
            .into()
        };

        let topic_bar = container(
            row![
                text(title).size(15).font(MONO_BOLD).color(theme::TEAL),
                text(&self.topic).size(13).color(theme::MUTED).width(Fill),
                call_button,
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

    /// The settings surface (brief §3.5 — modest, one screen): identity &
    /// key backup, the IRC gateway, the privacy block, and the blocklist.
    fn view_settings<'a>(&'a self, settings: &'a SettingsUi) -> Element<'a, Msg> {
        let header = row![
            text("settings").size(20).color(theme::TEAL).width(Fill),
            button(text("back · esc").size(11).font(MONO))
                .style(theme::ghost)
                .padding([4, 12])
                .on_press(Msg::CloseSettings),
        ]
        .align_y(iced::Center);

        let content = column![
            header,
            self.view_settings_identity(settings),
            self.view_settings_gateway(settings),
            self.view_settings_network(),
            Self::view_settings_privacy(),
            self.view_settings_blocklist(),
        ]
        .spacing(14)
        .max_width(660);

        scrollable(container(content).padding([18, 24]).center_x(Fill))
            .height(Fill)
            .width(Fill)
            .into()
    }

    fn view_settings_identity<'a>(&'a self, settings: &'a SettingsUi) -> Element<'a, Msg> {
        let groups: Vec<&str> = self.fingerprint.split('-').collect();
        let (fp_top, fp_bottom) = if groups.len() == 8 {
            (groups[..4].join("-"), groups[4..].join("-"))
        } else {
            (self.fingerprint.clone(), String::new())
        };

        let export: Element<'_, Msg> = if settings.export_armed {
            column![
                text("this file IS your identity — anyone holding it can be you. export a copy to ~/Downloads?")
                    .size(12)
                    .color(theme::DANGER),
                row![
                    button(text("yes, export the key file").size(12).font(MONO))
                        .style(theme::danger_outline)
                        .padding([5, 14])
                        .on_press(Msg::ExportKeyConfirm),
                    button(text("cancel").size(12).font(MONO))
                        .style(theme::ghost)
                        .padding([5, 14])
                        .on_press(Msg::ExportKeyCancel),
                ]
                .spacing(10),
            ]
            .spacing(8)
            .into()
        } else {
            button(text("export key file…").size(12).font(MONO))
                .style(theme::danger_outline)
                .padding([5, 14])
                .on_press(Msg::ExportKeyArm)
                .into()
        };

        container(
            column![
                row![
                    text("IDENTITY")
                        .size(10)
                        .font(MONO)
                        .color(theme::TEAL)
                        .width(Fill),
                    button(text("copy fingerprint").size(11).font(MONO))
                        .style(theme::ghost)
                        .padding([3, 10])
                        .on_press(Msg::CopyFingerprint),
                ]
                .align_y(iced::Center),
                Space::with_height(8),
                text(fp_top).size(16).font(MONO),
                text(fp_bottom).size(16).font(MONO),
                Space::with_height(4),
                text("nicknames are labels — this fingerprint is who you are")
                    .size(11)
                    .color(theme::MUTED),
                Space::with_height(12),
                container(Space::new(Fill, 1)).style(theme::hairline),
                Space::with_height(12),
                text("KEY BACKUP").size(10).font(MONO).color(theme::MUTED),
                Space::with_height(6),
                row![
                    text(self.key_path.display().to_string())
                        .size(11)
                        .font(MONO)
                        .color(theme::MUTED)
                        .width(Fill),
                    button(text("copy path").size(11).font(MONO))
                        .style(theme::ghost)
                        .padding([3, 10])
                        .on_press(Msg::CopyKeyPath),
                ]
                .spacing(10)
                .align_y(iced::Center),
                Space::with_height(6),
                text("lose the key file, lose the identity — there is no recovery")
                    .size(11)
                    .font(MONO)
                    .color(theme::MUTED),
                Space::with_height(10),
                export,
            ]
            .spacing(2),
        )
        .style(theme::card)
        .padding([16, 20])
        .width(Fill)
        .into()
    }

    fn view_settings_gateway<'a>(&'a self, settings: &'a SettingsUi) -> Element<'a, Msg> {
        let mut status = Row::new().spacing(8).align_y(iced::Center).push(
            text("IRC GATEWAY")
                .size(10)
                .font(MONO)
                .color(theme::TEAL)
                .width(Fill),
        );
        match &self.gateway {
            GatewayState::Running(up) => {
                status = status.push(
                    container(text(format!("running on {}", up.addr)).size(10).font(MONO))
                        .style(theme::chip_teal)
                        .padding([2, 8]),
                );
                if up.env_forced {
                    status = status.push(text("env-forced").size(10).font(MONO).color(theme::PINK));
                }
            }
            GatewayState::Starting { env_forced } => {
                status = status.push(text("starting…").size(10).font(MONO).color(theme::MUTED));
                if *env_forced {
                    status = status.push(text("env-forced").size(10).font(MONO).color(theme::PINK));
                }
            }
            GatewayState::Stopped => {
                status = status.push(text("stopped").size(10).font(MONO).color(theme::MUTED));
            }
        }

        let port_valid = self.settings_gateway_input().is_some();
        let port_hint = if port_valid {
            text("ok").size(10).font(MONO).color(theme::TEAL)
        } else {
            text("a port, 1024..=65535")
                .size(10)
                .font(MONO)
                .color(theme::DANGER)
        };
        let port_field = column![
            text("PORT").size(10).font(MONO).color(theme::MUTED),
            text_input("6667", &settings.port_input)
                .on_input(Msg::IrcPortInput)
                .style(theme::input)
                .font(MONO)
                .padding(8)
                .size(13)
                .width(110),
            port_hint,
        ]
        .spacing(5);
        let pass_field = column![
            text("PASSWORD — OPTIONAL")
                .size(10)
                .font(MONO)
                .color(theme::MUTED),
            text_input("no password", &settings.pass_input)
                .on_input(Msg::IrcPassInput)
                .secure(true)
                .style(theme::input)
                .font(MONO)
                .padding(8)
                .size(13),
            text("the gateway is plaintext on loopback — set a password on shared machines")
                .size(10)
                .font(MONO)
                .color(theme::MUTED),
        ]
        .spacing(5)
        .width(Fill);

        let toggle: Element<'_, Msg> = match &self.gateway {
            GatewayState::Running(up) if !up.env_forced => {
                button(text("stop gateway").size(12).font(MONO))
                    .style(theme::danger_outline)
                    .padding([5, 14])
                    .on_press(Msg::IrcToggle)
                    .into()
            }
            GatewayState::Running(_) => {
                text("forced on by MIKALL_IRC — unset it to control the gateway here")
                    .size(11)
                    .font(MONO)
                    .color(theme::PINK)
                    .into()
            }
            GatewayState::Starting { .. } => button(text("starting…").size(12).font(MONO))
                .style(theme::ghost)
                .padding([5, 14])
                .into(),
            GatewayState::Stopped => button(text("start gateway").size(12).font(MONO))
                .style(theme::teal_outline)
                .padding([5, 14])
                .on_press_maybe(port_valid.then_some(Msg::IrcToggle))
                .into(),
        };
        let save = button(text("save").size(12).font(SEMIBOLD))
            .style(theme::primary)
            .padding([5, 18])
            .on_press_maybe(port_valid.then_some(Msg::IrcSave));

        container(column![
            status,
            Space::with_height(10),
            row![port_field, pass_field].spacing(14),
            Space::with_height(10),
            row![toggle, save].spacing(10).align_y(iced::Center),
            Space::with_height(8),
            text(
                "binds 127.0.0.1 only, by construction — saving while running restarts the gateway"
            )
            .size(10)
            .font(MONO)
            .color(theme::MUTED),
        ])
        .style(theme::card)
        .padding([16, 20])
        .width(Fill)
        .into()
    }

    /// Brief §3.5: the static honesty block, stated plainly (§5 voice).
    /// Settings → network: our dialable addresses and a manual dial input.
    /// On a serverless network this IS the connectivity story when mDNS
    /// discovery is unavailable (e.g. macOS local-network permission).
    fn view_settings_network(&self) -> Element<'_, Msg> {
        let mut addrs = Column::new().spacing(3);
        if self.listen_addrs.is_empty() {
            addrs = addrs.push(
                text("no listen addresses yet — the node may still be starting")
                    .size(11)
                    .font(MONO)
                    .color(theme::MUTED),
            );
        }
        for addr in &self.listen_addrs {
            addrs = addrs.push(
                row![
                    text(addr.clone()).size(11).font(MONO).width(Fill),
                    button(text("copy").size(10).font(MONO))
                        .style(theme::ghost)
                        .padding([2, 8])
                        .on_press(Msg::CopyText(addr.clone())),
                ]
                .spacing(8)
                .align_y(iced::Center),
            );
        }

        let dial = row![
            text_input("/ip4/…/udp/…/quic-v1 — a peer's address", &self.dial_input)
                .on_input(Msg::DialInput)
                .on_submit(Msg::DialSubmit)
                .style(theme::input)
                .font(MONO)
                .padding(8)
                .size(12),
            button(text("connect").size(11).font(MONO))
                .style(theme::ghost)
                .padding([8, 14])
                .on_press(Msg::DialSubmit),
        ]
        .spacing(8)
        .align_y(iced::Center);

        container(
            column![
                text("NETWORK").size(10).font(MONO).color(theme::TEAL),
                Space::with_height(6),
                text("your addresses — share one out of band; a friend dials it to reach you")
                    .size(12)
                    .color(theme::MUTED),
                addrs,
                Space::with_height(10),
                text("connect to a peer").size(12).color(theme::MUTED),
                dial,
                text(
                    "lan discovery uses mdns — on macos, allow local network for this app. \
                     once connected, join the same #channel on both nodes."
                )
                .size(11)
                .font(MONO)
                .color(theme::MUTED),
                Space::with_height(10),
                text("remembered peers — redialed automatically at boot")
                    .size(12)
                    .color(theme::MUTED),
                self.view_known_peers(),
            ]
            .spacing(6),
        )
        .style(theme::card)
        .padding([16, 20])
        .width(Fill)
        .into()
    }

    fn view_known_peers(&self) -> Element<'_, Msg> {
        if self.known_peers.is_empty() {
            return text("none yet — a successful connection is remembered here")
                .size(11)
                .font(MONO)
                .color(theme::MUTED)
                .into();
        }
        let mut list = Column::new().spacing(3);
        for peer in &self.known_peers {
            list = list.push(
                row![
                    text(peer.as_str().to_owned())
                        .size(11)
                        .font(MONO)
                        .width(Fill),
                    button(text("forget").size(10).font(MONO))
                        .style(theme::ghost)
                        .padding([2, 8])
                        .on_press(Msg::ForgetPeer(peer.clone())),
                ]
                .spacing(8)
                .align_y(iced::Center),
            );
        }
        list.into()
    }

    fn view_settings_privacy() -> Element<'static, Msg> {
        container(
            column![
                text("PRIVACY — PLAINLY")
                    .size(10)
                    .font(MONO)
                    .color(theme::TEAL),
                Space::with_height(8),
                text("your IP is visible to the peers you connect with — direct connections are the point")
                    .size(12),
                text("bans and blocks are local — there is no global moderator on a decentralized network")
                    .size(12),
                text("messages live on peers, not servers — deleting here does not recall them")
                    .size(12),
                text("no account recovery — the key file above is the only credential there is")
                    .size(12),
                Space::with_height(8),
                text("the full posture: docs/security.md")
                    .size(10)
                    .font(MONO)
                    .color(theme::MUTED),
            ]
            .spacing(4),
        )
        .style(theme::card)
        .padding([16, 20])
        .width(Fill)
        .into()
    }

    fn view_settings_blocklist(&self) -> Element<'_, Msg> {
        let mut col = Column::new()
            .spacing(6)
            .push(text("BLOCKLIST").size(10).font(MONO).color(theme::TEAL))
            .push(Space::with_height(4));
        if self.blocked.is_empty() {
            col = col.push(
                text("no one is blocked — a block hides a peer's messages on this node only")
                    .size(12)
                    .color(theme::MUTED),
            );
        } else {
            for (id, nick) in &self.blocked {
                let mut entry = Row::new().spacing(10).align_y(iced::Center);
                if let Some(nick) = nick {
                    entry = entry.push(text(nick.as_str().to_owned()).size(13).font(SEMIBOLD));
                }
                entry = entry.push(
                    text(shorten(&id.to_string(), 16))
                        .size(12)
                        .font(MONO)
                        .color(theme::MUTED)
                        .width(Fill),
                );
                entry = entry.push(
                    button(text("unblock").size(11).font(MONO))
                        .style(theme::ghost)
                        .padding([3, 10])
                        .on_press(Msg::Unblock(*id)),
                );
                col = col.push(entry);
            }
            col = col.push(Space::with_height(4));
            col = col.push(
                text("unblocking forgets the peer — they re-pin like any stranger on next sight")
                    .size(10)
                    .font(MONO)
                    .color(theme::MUTED),
            );
        }
        container(col)
            .style(theme::card)
            .padding([16, 20])
            .width(Fill)
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
