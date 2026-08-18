//! Parse-don't-validate value objects of the messaging context.

use core::fmt;

/// IRC nickname specials per RFC 2812: `[ ] \ ` _ ^ { | }`.
const fn is_special(c: char) -> bool {
    matches!(c, '[' | ']' | '\\' | '`' | '_' | '^' | '{' | '|' | '}')
}

/// A nickname following RFC 2812 grammar, 1..=16 chars: first char a letter
/// or special, rest letters/digits/specials/`-`. Nicknames are labels, not
/// identities — collisions are legal on a serverless network and are
/// disambiguated by key fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nickname(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NicknameError {
    #[error("nickname must not be empty")]
    Empty,
    #[error("nickname exceeds 16 characters")]
    TooLong,
    #[error("nickname must start with a letter or one of []\\`_^{{|}}")]
    BadFirstChar,
    #[error("nickname contains an invalid character")]
    BadChar,
}

impl Nickname {
    pub fn parse(raw: &str) -> Result<Self, NicknameError> {
        let mut chars = raw.chars();
        let first = chars.next().ok_or(NicknameError::Empty)?;
        if raw.chars().count() > 16 {
            return Err(NicknameError::TooLong);
        }
        if !(first.is_ascii_alphabetic() || is_special(first)) {
            return Err(NicknameError::BadFirstChar);
        }
        if !chars.all(|c| c.is_ascii_alphanumeric() || is_special(c) || c == '-') {
            return Err(NicknameError::BadChar);
        }
        Ok(Nickname(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Nickname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A channel name: starts with `#`, 2..=50 bytes, ASCII printable without
/// space, comma, colon, or control characters. Stored in canonical ASCII
/// lowercase so `#Stage` and `#stage` are one channel everywhere.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChannelNameError {
    #[error("channel name must start with '#'")]
    MissingHash,
    #[error("channel name must be 2..=50 bytes")]
    BadLength,
    #[error("channel name contains an invalid character")]
    BadChar,
}

impl ChannelName {
    pub fn parse(raw: &str) -> Result<Self, ChannelNameError> {
        if !raw.starts_with('#') {
            return Err(ChannelNameError::MissingHash);
        }
        if raw.len() < 2 || raw.len() > 50 {
            return Err(ChannelNameError::BadLength);
        }
        let body = &raw[1..];
        let ok = body
            .chars()
            .all(|c| c.is_ascii_graphic() && !matches!(c, ',' | ':' | '#'));
        if !ok {
            return Err(ChannelNameError::BadChar);
        }
        Ok(ChannelName(raw.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A chat message body: 1..=4096 bytes of UTF-8. `\n` and `\t` are the only
/// permitted control characters (no NUL, no CR — CR/LF injection into the IRC
/// gateway is impossible by construction).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageBody(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MessageBodyError {
    #[error("message must not be empty")]
    Empty,
    #[error("message exceeds 4096 bytes")]
    TooLong,
    #[error("message contains a forbidden control character")]
    ControlCharacter,
}

impl MessageBody {
    pub const MAX_BYTES: usize = 4096;

    pub fn parse(raw: &str) -> Result<Self, MessageBodyError> {
        if raw.is_empty() {
            return Err(MessageBodyError::Empty);
        }
        if raw.len() > Self::MAX_BYTES {
            return Err(MessageBodyError::TooLong);
        }
        if raw
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t')
        {
            return Err(MessageBodyError::ControlCharacter);
        }
        Ok(MessageBody(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Project the body into lines an IRC gateway may emit verbatim: split on
    /// `\n`, then chunk at UTF-8 boundaries to at most
    /// [`IrcSafeLine::MAX_BYTES`] bytes. Empty lines are dropped (IRC cannot
    /// carry an empty PRIVMSG).
    pub fn irc_lines(&self) -> Vec<IrcSafeLine> {
        self.0.split('\n').flat_map(IrcSafeLine::chunk).collect()
    }
}

impl fmt::Display for MessageBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single line safe to embed in an IRC message: non-empty, at most 400
/// bytes (leaving headroom inside IRC's 512-byte frame), no CR/LF/NUL.
/// Constructible only via [`MessageBody::irc_lines`] / [`IrcSafeLine::chunk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrcSafeLine(String);

impl IrcSafeLine {
    pub const MAX_BYTES: usize = 400;

    fn chunk(line: &str) -> Vec<IrcSafeLine> {
        let mut out = Vec::new();
        let mut rest = line;
        while !rest.is_empty() {
            let mut cut = rest.len().min(Self::MAX_BYTES);
            while !rest.is_char_boundary(cut) {
                cut -= 1;
            }
            out.push(IrcSafeLine(rest[..cut].to_owned()));
            rest = &rest[cut..];
        }
        out
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A channel topic: at most 390 bytes, single line. Empty means "no topic".
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Topic(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("topic exceeds 390 bytes")]
    TooLong,
    #[error("topic must be a single line without control characters")]
    ControlCharacter,
}

impl Topic {
    pub fn parse(raw: &str) -> Result<Self, TopicError> {
        if raw.len() > 390 {
            return Err(TopicError::TooLong);
        }
        if raw.chars().any(char::is_control) {
            return Err(TopicError::ControlCharacter);
        }
        Ok(Topic(raw.to_owned()))
    }

    pub fn none() -> Self {
        Topic(String::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use proptest::prelude::*;

    #[test]
    fn nickname_accepts_rfc2812_forms() {
        for ok in ["miku", "Miku-01", "[negi]", "`tail", "a"] {
            assert!(Nickname::parse(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn nickname_rejects_bad_forms() {
        assert_eq!(Nickname::parse("9bad"), Err(NicknameError::BadFirstChar));
        assert_eq!(Nickname::parse(""), Err(NicknameError::Empty));
        assert_eq!(
            Nickname::parse("seventeen-chars-x"),
            Err(NicknameError::TooLong)
        );
        assert_eq!(Nickname::parse("mi ku"), Err(NicknameError::BadChar));
        assert_eq!(Nickname::parse("mi,ku"), Err(NicknameError::BadChar));
    }

    #[test]
    fn channel_name_is_canonically_lowercase() {
        assert_eq!(ChannelName::parse("#Stage").unwrap().as_str(), "#stage");
        assert_eq!(
            ChannelName::parse("stage"),
            Err(ChannelNameError::MissingHash)
        );
        assert_eq!(ChannelName::parse("#"), Err(ChannelNameError::BadLength));
        assert_eq!(
            ChannelName::parse("#has space"),
            Err(ChannelNameError::BadChar)
        );
        assert_eq!(ChannelName::parse("#a,b"), Err(ChannelNameError::BadChar));
    }

    #[test]
    fn body_rejects_cr_and_nul() {
        assert!(MessageBody::parse("hi\r\nJOIN #evil").is_err());
        assert!(MessageBody::parse("hi\0").is_err());
        assert!(MessageBody::parse("line one\nline two").is_ok());
    }

    #[test]
    fn irc_lines_split_and_chunk() {
        let body = MessageBody::parse(&format!("{}\nshort", "x".repeat(900))).unwrap();
        let lines = body.irc_lines();
        assert_eq!(lines.len(), 4); // 400 + 400 + 100 + "short"
        assert!(lines
            .iter()
            .all(|l| l.as_str().len() <= IrcSafeLine::MAX_BYTES));
        assert_eq!(lines[3].as_str(), "short");
    }

    #[test]
    fn irc_lines_drop_empty_lines() {
        let body = MessageBody::parse("a\n\nb").unwrap();
        let lines = body.irc_lines();
        assert_eq!(lines.len(), 2);
    }

    proptest! {
        #[test]
        fn nickname_roundtrip(s in "[a-zA-Z\\[\\]\\\\`_^{|}][a-zA-Z0-9\\[\\]\\\\`_^{|}-]{0,15}") {
            let nick = Nickname::parse(&s).unwrap();
            prop_assert_eq!(Nickname::parse(nick.as_str()).unwrap(), nick);
        }

        #[test]
        fn body_never_panics(s in ".{0,5000}") {
            let _ = MessageBody::parse(&s);
        }

        #[test]
        fn accepted_bodies_produce_safe_irc_lines(s in "[a-zA-Z0-9 \\n]{1,2000}") {
            if let Ok(body) = MessageBody::parse(&s) {
                for line in body.irc_lines() {
                    prop_assert!(!line.as_str().is_empty());
                    prop_assert!(line.as_str().len() <= IrcSafeLine::MAX_BYTES);
                    prop_assert!(!line.as_str().contains(['\r', '\n', '\0']));
                }
            }
        }

        #[test]
        fn channel_name_parse_is_idempotent(s in "#[a-zA-Z0-9_.-]{1,49}") {
            let name = ChannelName::parse(&s).unwrap();
            prop_assert_eq!(ChannelName::parse(name.as_str()).unwrap(), name);
        }
    }
}
