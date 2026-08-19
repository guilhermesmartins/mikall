//! `miku_teal`: the original teal dark theme. No Crypton assets — an
//! original palette inspired by a certain shade of teal (#39C5BB).
//!
//! The seven base tokens are the brand (docs/design-brief.md §2); everything
//! else here is a derived tint or a widget style built from them.

use iced::border;
use iced::theme::Palette;
use iced::widget::{button, container, text_input};
use iced::{Border, Color, Theme};

pub const BG: Color = Color::from_rgb(0.055, 0.106, 0.118); // #0E1B1E
pub const SURFACE: Color = Color::from_rgb(0.086, 0.20, 0.227); // #16333A
pub const TEAL: Color = Color::from_rgb(0.224, 0.773, 0.733); // #39C5BB
pub const PINK: Color = Color::from_rgb(1.0, 0.631, 0.788); // #FFA1C9
pub const TEXT: Color = Color::from_rgb(0.910, 0.965, 0.961); // #E8F6F5
pub const MUTED: Color = Color::from_rgb(0.55, 0.65, 0.64); // #8CA6A3
pub const DANGER: Color = Color::from_rgb(1.0, 0.361, 0.541); // #FF5C8A

// Derived tints/shades (brief §2 allows extending the base tokens).
pub const SURFACE_2: Color = Color::from_rgb(0.106, 0.243, 0.275); // raised surface
pub const AMBER: Color = Color::from_rgb(0.910, 0.761, 0.478); // away presence only
pub const TEAL_DIM: Color = Color { a: 0.12, ..TEAL };
pub const HAIRLINE: Color = Color { a: 0.14, ..TEAL };
pub const DANGER_DIM: Color = Color { a: 0.12, ..DANGER };
/// Dark ink for text sitting on a teal or pink fill.
pub const INK_ON_TEAL: Color = Color::from_rgb(0.024, 0.129, 0.122);
pub const INK_ON_PINK: Color = Color::from_rgb(0.227, 0.071, 0.125);

pub fn miku_teal() -> Theme {
    Theme::custom(
        "miku_teal".to_owned(),
        Palette {
            background: BG,
            text: TEXT,
            primary: TEAL,
            success: TEAL,
            danger: DANGER,
        },
    )
}

fn filled(color: Color) -> container::Style {
    container::Style {
        background: Some(color.into()),
        ..container::Style::default()
    }
}

/// A side panel: sidebar, roster.
pub fn panel(_: &Theme) -> container::Style {
    filled(SURFACE)
}

/// The raised strip at the bottom of the sidebar (self area).
pub fn panel_raised(_: &Theme) -> container::Style {
    filled(SURFACE_2)
}

/// A 1px teal-tinted separator line.
pub fn hairline(_: &Theme) -> container::Style {
    filled(HAIRLINE)
}

/// A card on the surface color with a hairline border: offer cards, blocks.
pub fn card(_: &Theme) -> container::Style {
    container::Style {
        background: Some(SURFACE.into()),
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(8),
        },
        ..container::Style::default()
    }
}

/// The onboarding identity card: raised, teal-edged.
pub fn identity_card(_: &Theme) -> container::Style {
    container::Style {
        background: Some(SURFACE_2.into()),
        border: Border {
            color: Color { a: 0.4, ..TEAL },
            width: 1.0,
            radius: border::radius(12),
        },
        ..container::Style::default()
    }
}

/// The alarm slot at severity danger (key changes, failed sends/joins).
pub fn alarm_danger(_: &Theme) -> container::Style {
    container::Style {
        background: Some(DANGER_DIM.into()),
        border: Border {
            color: Color { a: 0.4, ..DANGER },
            width: 1.0,
            radius: border::radius(0),
        },
        ..container::Style::default()
    }
}

/// The alarm slot at severity info ("file saved to …").
pub fn alarm_info(_: &Theme) -> container::Style {
    container::Style {
        background: Some(TEAL_DIM.into()),
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(0),
        },
        ..container::Style::default()
    }
}

/// The docked call strip while a call is ringing (either direction): a
/// teal wash whose edge pulses gently between two alphas (brief §2 motion).
pub fn call_ringing(glow: bool) -> impl Fn(&Theme) -> container::Style {
    move |_| container::Style {
        background: Some(TEAL_DIM.into()),
        border: Border {
            color: Color {
                a: if glow { 0.55 } else { 0.22 },
                ..TEAL
            },
            width: 1.0,
            radius: border::radius(0),
        },
        ..container::Style::default()
    }
}

/// The docked call strip while connecting, active, or ended.
pub fn call_panel(_: &Theme) -> container::Style {
    container::Style {
        background: Some(SURFACE.into()),
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(0),
        },
        ..container::Style::default()
    }
}

/// A participant tile in the call grid (max 8 — the domain's mesh cap).
pub fn call_tile(_: &Theme) -> container::Style {
    container::Style {
        background: Some(SURFACE_2.into()),
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(8),
        },
        ..container::Style::default()
    }
}

/// Media-state chip on a call tile ("mic off", "deafened").
pub fn chip_danger(_: &Theme) -> container::Style {
    container::Style {
        background: Some(DANGER_DIM.into()),
        text_color: Some(DANGER),
        border: Border {
            color: Color { a: 0.35, ..DANGER },
            width: 1.0,
            radius: border::radius(4),
        },
        ..container::Style::default()
    }
}

/// Media-state chip for the "sharing" state — teal, not danger: a screen
/// share is a feature in progress, not a fault.
pub fn chip_teal(_: &Theme) -> container::Style {
    container::Style {
        background: Some(TEAL_DIM.into()),
        text_color: Some(TEAL),
        border: Border {
            color: Color { a: 0.35, ..TEAL },
            width: 1.0,
            radius: border::radius(4),
        },
        ..container::Style::default()
    }
}

/// The screen-share stage: a near-black 16:9 well where decoded video will
/// land; until then it hosts the honest placeholder copy.
pub fn share_stage(_: &Theme) -> container::Style {
    container::Style {
        background: Some(Color::from_rgb(0.031, 0.067, 0.075).into()),
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(8),
        },
        ..container::Style::default()
    }
}

/// Destructive fill: hang up.
pub fn danger(_: &Theme, status: button::Status) -> button::Style {
    let fill = match status {
        button::Status::Hovered | button::Status::Pressed => Color::from_rgb(1.0, 0.45, 0.62),
        button::Status::Active | button::Status::Disabled => DANGER,
    };
    button::Style {
        background: Some(fill.into()),
        text_color: INK_ON_PINK,
        border: Border {
            radius: border::radius(6),
            ..Border::default()
        },
        ..button::Style::default()
    }
}

/// Destructive outline: decline an offer, cancel an outgoing ring.
pub fn danger_outline(_: &Theme, status: button::Status) -> button::Style {
    let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
    button::Style {
        background: hovered.then(|| DANGER_DIM.into()),
        text_color: DANGER,
        border: Border {
            color: Color { a: 0.5, ..DANGER },
            width: 1.0,
            radius: border::radius(6),
        },
        ..button::Style::default()
    }
}

/// Engaged non-destructive toggle outline ("stop sharing") — the teal
/// sibling of `danger_outline`.
pub fn teal_outline(_: &Theme, status: button::Status) -> button::Style {
    let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
    button::Style {
        background: hovered.then(|| TEAL_DIM.into()),
        text_color: TEAL,
        border: Border {
            color: Color { a: 0.5, ..TEAL },
            width: 1.0,
            radius: border::radius(6),
        },
        ..button::Style::default()
    }
}

/// The pink unread-count pill.
pub fn badge(_: &Theme) -> container::Style {
    container::Style {
        background: Some(PINK.into()),
        text_color: Some(INK_ON_PINK),
        border: Border {
            radius: border::radius(9),
            ..Border::default()
        },
        ..container::Style::default()
    }
}

/// Sidebar item: `active` gets the teal-dim fill; `emphasized` (unread)
/// reads at full text color while idle items stay muted.
pub fn nav_item(
    active: bool,
    emphasized: bool,
) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |_, status| {
        let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
        button::Style {
            background: if active {
                Some(TEAL_DIM.into())
            } else if hovered {
                Some(Color { a: 0.06, ..TEAL }.into())
            } else {
                None
            },
            text_color: if active {
                TEAL
            } else if emphasized || hovered {
                TEXT
            } else {
                MUTED
            },
            border: Border {
                radius: border::radius(5),
                ..Border::default()
            },
            ..button::Style::default()
        }
    }
}

/// Primary CTA: teal fill, dark ink. Disabled falls back to a ghost.
pub fn primary(_: &Theme, status: button::Status) -> button::Style {
    match status {
        button::Status::Disabled => button::Style {
            background: None,
            text_color: MUTED,
            border: Border {
                color: HAIRLINE,
                width: 1.0,
                radius: border::radius(6),
            },
            ..button::Style::default()
        },
        button::Status::Hovered | button::Status::Pressed => button::Style {
            background: Some(Color::from_rgb(0.29, 0.84, 0.80).into()),
            text_color: INK_ON_TEAL,
            border: Border {
                radius: border::radius(6),
                ..Border::default()
            },
            ..button::Style::default()
        },
        button::Status::Active => button::Style {
            background: Some(TEAL.into()),
            text_color: INK_ON_TEAL,
            border: Border {
                radius: border::radius(6),
                ..Border::default()
            },
            ..button::Style::default()
        },
    }
}

/// Quiet bordered button: roster toggle, dismiss, reject.
pub fn ghost(_: &Theme, status: button::Status) -> button::Style {
    let hovered = matches!(status, button::Status::Hovered | button::Status::Pressed);
    button::Style {
        background: hovered.then(|| Color { a: 0.06, ..TEAL }.into()),
        text_color: TEXT,
        border: Border {
            color: HAIRLINE,
            width: 1.0,
            radius: border::radius(6),
        },
        ..button::Style::default()
    }
}

/// Inputs: bg-colored well with a hairline edge that turns teal on focus.
pub fn input(_: &Theme, status: text_input::Status) -> text_input::Style {
    let focused = matches!(status, text_input::Status::Focused);
    text_input::Style {
        background: BG.into(),
        border: Border {
            color: if focused { TEAL } else { HAIRLINE },
            width: 1.0,
            radius: border::radius(6),
        },
        icon: MUTED,
        placeholder: MUTED,
        value: TEXT,
        selection: TEAL_DIM,
    }
}

/// The composer: same grammar as `input` but sits on `SURFACE`.
pub fn composer(_: &Theme, status: text_input::Status) -> text_input::Style {
    let focused = matches!(status, text_input::Status::Focused);
    text_input::Style {
        background: SURFACE.into(),
        border: Border {
            color: if focused { TEAL } else { HAIRLINE },
            width: 1.0,
            radius: border::radius(8),
        },
        icon: MUTED,
        placeholder: MUTED,
        value: TEXT,
        selection: TEAL_DIM,
    }
}
