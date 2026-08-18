//! `miku_teal`: the original teal dark theme. No Crypton assets — an
//! original palette inspired by a certain shade of teal (#39C5BB).

use iced::theme::Palette;
use iced::{Color, Theme};

pub const BG: Color = Color::from_rgb(0.055, 0.106, 0.118); // #0E1B1E
pub const SURFACE: Color = Color::from_rgb(0.086, 0.20, 0.227); // #16333A
pub const TEAL: Color = Color::from_rgb(0.224, 0.773, 0.733); // #39C5BB
pub const PINK: Color = Color::from_rgb(1.0, 0.631, 0.788); // #FFA1C9
pub const TEXT: Color = Color::from_rgb(0.910, 0.965, 0.961); // #E8F6F5
pub const MUTED: Color = Color::from_rgb(0.55, 0.65, 0.64);
pub const DANGER: Color = Color::from_rgb(1.0, 0.361, 0.541); // #FF5C8A

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
