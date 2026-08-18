//! Minimal RFC 1459/2812 message framing: parse `[@tags] [:prefix] COMMAND
//! params [:trailing]`, serialize replies. No dependency — the grammar is
//! ~40 lines and hand-rolling it keeps the whole surface auditable.

/// A parsed inbound IRC message (tags and prefix are accepted and ignored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrcMessage {
    pub command: String,
    pub params: Vec<String>,
}

/// Parse one CRLF-stripped IRC line. Returns `None` for empty lines.
pub fn parse_line(line: &str) -> Option<IrcMessage> {
    let mut rest = line.trim_end_matches(['\r', '\n']);
    if rest.is_empty() {
        return None;
    }
    // tags
    if let Some(after) = rest.strip_prefix('@') {
        rest = after.split_once(' ')?.1;
    }
    // prefix
    if let Some(after) = rest.strip_prefix(':') {
        rest = after.split_once(' ')?.1;
    }
    let rest = rest.trim_start();
    let (command_part, params_part) = match rest.split_once(' ') {
        Some((c, p)) => (c, Some(p)),
        None => (rest, None),
    };
    if command_part.is_empty() {
        return None;
    }
    let mut params = Vec::new();
    if let Some(mut p) = params_part {
        loop {
            p = p.trim_start();
            if p.is_empty() {
                break;
            }
            if let Some(trailing) = p.strip_prefix(':') {
                params.push(trailing.to_owned());
                break;
            }
            match p.split_once(' ') {
                Some((param, next)) => {
                    params.push(param.to_owned());
                    p = next;
                }
                None => {
                    params.push(p.to_owned());
                    break;
                }
            }
        }
    }
    Some(IrcMessage {
        command: command_part.to_ascii_uppercase(),
        params,
    })
}

/// Format the unix-ms timestamp as an IRCv3 `server-time` tag value
/// (`2011-10-19T16:40:51.620Z`).
pub fn server_time(ts_ms: u64) -> String {
    let secs = (ts_ms / 1000) as i64;
    let millis = ts_ms % 1000;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (hour, minute, second) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parses_basic_commands() {
        assert_eq!(
            parse_line("NICK miku\r\n").unwrap(),
            IrcMessage {
                command: "NICK".into(),
                params: vec!["miku".into()]
            }
        );
        assert_eq!(
            parse_line("PRIVMSG #stage :hello  world").unwrap(),
            IrcMessage {
                command: "PRIVMSG".into(),
                params: vec!["#stage".into(), "hello  world".into()]
            }
        );
        assert_eq!(
            parse_line("USER m 0 * :Miku Chan").unwrap().params,
            vec!["m", "0", "*", "Miku Chan"]
        );
    }

    #[test]
    fn ignores_tags_and_prefix() {
        let msg = parse_line("@time=x;id=y :nick!u@h PRIVMSG #a :hi").unwrap();
        assert_eq!(msg.command, "PRIVMSG");
        assert_eq!(msg.params, vec!["#a", "hi"]);
    }

    #[test]
    fn command_is_uppercased() {
        assert_eq!(parse_line("join #a").unwrap().command, "JOIN");
    }

    #[test]
    fn empty_lines_are_none() {
        assert!(parse_line("").is_none());
        assert!(parse_line("\r\n").is_none());
    }

    #[test]
    fn server_time_formats_known_instant() {
        // 2011-10-19T16:40:51.620Z
        assert_eq!(server_time(1_319_042_451_620), "2011-10-19T16:40:51.620Z");
        assert_eq!(server_time(0), "1970-01-01T00:00:00.000Z");
    }
}
