//! The mikall GUI: a Discord-like three-pane iced application driving the
//! same `NodeHandle` as the IRC gateway. This crate is a driving adapter —
//! it never touches net/store/crypto directly.

mod theme;

use std::collections::HashMap;
use std::path::PathBuf;

use iced::widget::{button, column, container, row, scrollable, text, text_input, Column, Space};
use iced::{Element, Fill, Subscription, Task};

use mikall_app::events::AppEvent;
use mikall_app::services::{MemberView, RenderedMessage};
use mikall_domain::identity::IdentityEvent;
use mikall_domain::messaging::{ChannelName, MessagingEvent, Nickname};
use mikall_domain::shared::IdentityId;
use mikall_domain::DomainEvent;
use mikall_node::{start, NodeConfig, NodeHandle};

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

#[derive(Debug, Clone)]
enum Msg {
    Booted(Result<NodeHandle, String>),
    HasNickname(bool),
    NickInput(String),
    ConfirmNick,
    Event(AppEvent),
    JoinInput(String),
    JoinSubmit,
    Joined(Result<ChannelName, String>),
    Select(Target),
    HistoryLoaded(String, Vec<RenderedMessage>),
    MembersLoaded(Vec<MemberView>),
    TopicLoaded(String),
    ComposerInput(String),
    Send,
    Sent(Result<(), String>),
    ToggleRoster,
    DismissAlarm,
    Noop,
}

struct Mikall {
    node: Option<NodeHandle>,
    screen: Screen,
    fingerprint: String,
    channels: Vec<ChannelName>,
    dms: Vec<(IdentityId, Option<Nickname>)>,
    active: Option<Target>,
    messages: Vec<RenderedMessage>,
    members: Vec<MemberView>,
    topic: String,
    unread: HashMap<String, usize>,
    composer: String,
    join_input: String,
    show_roster: bool,
    alarm: Option<String>,
}

impl Mikall {
    fn boot() -> (Self, Task<Msg>) {
        let app = Mikall {
            node: None,
            screen: Screen::Booting,
            fingerprint: String::new(),
            channels: Vec::new(),
            dms: Vec::new(),
            active: None,
            messages: Vec::new(),
            members: Vec::new(),
            topic: String::new(),
            unread: HashMap::new(),
            composer: String::new(),
            join_input: String::new(),
            show_roster: true,
            alarm: None,
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

    fn update(&mut self, message: Msg) -> Task<Msg> {
        match message {
            Msg::Booted(Ok(node)) => {
                self.fingerprint = node.identity.fingerprint().display_groups();
                let identity = node.identity.clone();
                self.node = Some(node);
                Task::perform(
                    async move { identity.nickname().await.is_some() },
                    Msg::HasNickname,
                )
            }
            Msg::Booted(Err(error)) => {
                self.screen = Screen::Failed(error);
                Task::none()
            }
            Msg::HasNickname(has) => {
                self.screen = if has {
                    Screen::Main
                } else {
                    Screen::Onboarding {
                        nick: String::new(),
                    }
                };
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
                self.screen = Screen::Main;
                Task::perform(
                    async move {
                        node.identity.set_nickname(nickname).await;
                    },
                    |_| Msg::Noop,
                )
            }
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
                self.alarm = Some(format!("join failed: {error}"));
                Task::none()
            }
            Msg::Select(target) => {
                self.unread.remove(&target.key());
                self.active = Some(target);
                self.messages.clear();
                self.members.clear();
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
                self.alarm = Some(format!("send failed: {error}"));
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
                if let Some(me) = self.node.as_ref().map(|n| n.identity.local_id()) {
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
            AppEvent::Domain(DomainEvent::Identity(IdentityEvent::ContactKeyChanged {
                id,
                new_fingerprint,
                ..
            })) => {
                self.alarm = Some(format!(
                    "KEY CHANGED for {id}: new fingerprint {new_fingerprint}. Verify out of band before trusting."
                ));
                Task::none()
            }
            AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::TopicChanged { .. }))
            | AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::ChannelJoined { .. }))
            | AppEvent::Domain(DomainEvent::Messaging(MessagingEvent::ChannelLeft { .. })) => {
                self.reload_active()
            }
            AppEvent::TransferSaved { path, .. } => {
                self.alarm = Some(format!("file saved to {path}"));
                Task::none()
            }
            AppEvent::Domain(_) => Task::none(),
        }
    }

    fn subscription(&self) -> Subscription<Msg> {
        let Some(node) = &self.node else {
            return Subscription::none();
        };
        let bus = node.bus.clone();
        Subscription::run_with_id(
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
        )
    }

    fn view(&self) -> Element<'_, Msg> {
        match &self.screen {
            Screen::Booting => container(text("starting node…").size(20))
                .center_x(Fill)
                .center_y(Fill)
                .into(),
            Screen::Failed(error) => container(
                column![
                    text("node failed to start").size(24).color(theme::DANGER),
                    text(error.clone()).size(14),
                ]
                .spacing(12),
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
            text("pick a nickname (a label, not an identity)").color(theme::MUTED)
        } else if valid {
            text("looks good ✔").color(theme::TEAL)
        } else {
            text("letters first; letters/digits/[]\\`_^{|}- after; max 16").color(theme::DANGER)
        };
        let confirm = if valid {
            button(text("enter the stage")).on_press(Msg::ConfirmNick)
        } else {
            button(text("enter the stage"))
        };
        container(
            column![
                text("mikall").size(42).color(theme::TEAL),
                text("serverless chat — your identity is a keypair on this device")
                    .size(14)
                    .color(theme::MUTED),
                Space::with_height(12),
                text(format!("your fingerprint: {}", self.fingerprint)).size(13),
                text("share it out of band so friends can verify you")
                    .size(12)
                    .color(theme::MUTED),
                Space::with_height(16),
                text_input("nickname", nick)
                    .on_input(Msg::NickInput)
                    .on_submit(Msg::ConfirmNick)
                    .padding(10)
                    .width(280),
                hint.size(12),
                Space::with_height(10),
                confirm,
            ]
            .spacing(6)
            .align_x(iced::Center),
        )
        .center_x(Fill)
        .center_y(Fill)
        .into()
    }

    fn view_main(&self) -> Element<'_, Msg> {
        let mut root = Column::new();
        if let Some(alarm) = &self.alarm {
            root = root.push(
                container(
                    row![
                        text(alarm.clone()).size(13).color(theme::BG).width(Fill),
                        button(text("dismiss").size(12)).on_press(Msg::DismissAlarm),
                    ]
                    .spacing(8)
                    .align_y(iced::Center),
                )
                .padding(8)
                .style(|_| container::Style {
                    background: Some(theme::PINK.into()),
                    ..container::Style::default()
                })
                .width(Fill),
            );
        }
        let body = row![self.view_sidebar(), self.view_chat()]
            .push_maybe(self.show_roster.then(|| self.view_roster()));
        root.push(body.height(Fill)).into()
    }

    fn view_sidebar(&self) -> Element<'_, Msg> {
        let mut list = Column::new().spacing(4);
        list = list.push(text("channels").size(12).color(theme::MUTED));
        for name in &self.channels {
            let key = name.as_str().to_owned();
            let unread = self.unread.get(&key).copied().unwrap_or(0);
            let label = if unread > 0 {
                format!("{key}  ({unread})")
            } else {
                key.clone()
            };
            let active = self.active == Some(Target::Channel(name.clone()));
            let color = if active { theme::TEAL } else { theme::TEXT };
            list = list.push(
                button(text(label).size(14).color(color))
                    .style(button::text)
                    .on_press(Msg::Select(Target::Channel(name.clone()))),
            );
        }
        if !self.dms.is_empty() {
            list = list.push(Space::with_height(8));
            list = list.push(text("direct messages").size(12).color(theme::MUTED));
            for (id, nick) in &self.dms {
                let label = nick
                    .as_ref()
                    .map(|n| n.as_str().to_owned())
                    .unwrap_or_else(|| id.to_string());
                let key = Target::Dm(*id).key();
                let unread = self.unread.get(&key).copied().unwrap_or(0);
                let label = if unread > 0 {
                    format!("{label}  ({unread})")
                } else {
                    label
                };
                list = list.push(
                    button(text(label).size(14))
                        .style(button::text)
                        .on_press(Msg::Select(Target::Dm(*id))),
                );
            }
        }

        container(
            column![
                text("mikall").size(22).color(theme::TEAL),
                text(&self.fingerprint).size(9).color(theme::MUTED),
                Space::with_height(10),
                scrollable(list).height(Fill),
                text_input("join #channel", &self.join_input)
                    .on_input(Msg::JoinInput)
                    .on_submit(Msg::JoinSubmit)
                    .padding(8)
                    .size(13),
            ]
            .spacing(6),
        )
        .padding(12)
        .width(220)
        .height(Fill)
        .style(|_| container::Style {
            background: Some(theme::SURFACE.into()),
            ..container::Style::default()
        })
        .into()
    }

    fn view_chat(&self) -> Element<'_, Msg> {
        let title = match &self.active {
            Some(Target::Channel(name)) => name.as_str().to_owned(),
            Some(Target::Dm(id)) => format!("dm with {id}"),
            None => "welcome — join a channel to start".to_owned(),
        };
        let topic_bar = row![
            text(title).size(16).color(theme::TEAL),
            Space::with_width(16),
            text(&self.topic).size(13).color(theme::MUTED).width(Fill),
            button(
                text(if self.show_roster {
                    "» roster"
                } else {
                    "« roster"
                })
                .size(12)
            )
            .style(button::text)
            .on_press(Msg::ToggleRoster),
        ]
        .align_y(iced::Center);

        let mut messages = Column::new().spacing(6).padding(8);
        for message in &self.messages {
            let nick = message
                .author_nick
                .as_ref()
                .map(|n| n.as_str().to_owned())
                .unwrap_or_else(|| message.author.to_string());
            messages = messages.push(
                column![
                    text(nick).size(13).color(theme::PINK),
                    text(message.body.as_str().to_owned()).size(14),
                ]
                .spacing(2),
            );
        }

        let composer = text_input("message…", &self.composer)
            .on_input(Msg::ComposerInput)
            .on_submit(Msg::Send)
            .padding(10)
            .size(14);

        container(
            column![
                container(topic_bar).padding(10),
                scrollable(messages)
                    .anchor_bottom()
                    .height(Fill)
                    .width(Fill),
                container(composer).padding(10),
            ]
            .spacing(0),
        )
        .height(Fill)
        .width(Fill)
        .into()
    }

    fn view_roster(&self) -> Element<'_, Msg> {
        let mut list = Column::new().spacing(4);
        list = list.push(text("members").size(12).color(theme::MUTED));
        for member in &self.members {
            let sigil = match member.role {
                mikall_domain::messaging::Role::Founder => "~",
                mikall_domain::messaging::Role::Op => "@",
                mikall_domain::messaging::Role::Member => "",
            };
            let nick = member
                .nick
                .as_ref()
                .map(|n| n.as_str().to_owned())
                .unwrap_or_else(|| member.id.to_string());
            list = list.push(text(format!("{sigil}{nick}")).size(13));
        }
        container(scrollable(list).height(Fill))
            .padding(12)
            .width(180)
            .height(Fill)
            .style(|_| container::Style {
                background: Some(theme::SURFACE.into()),
                ..container::Style::default()
            })
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
