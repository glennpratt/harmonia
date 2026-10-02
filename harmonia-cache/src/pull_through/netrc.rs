//! Minimal netrc parser, enough for the credentials Nix itself reads from
//! `netrc-file` (e.g. FlakeHub).

use harmonia_utils_base_encoding::{Base, encode_for_base};

#[derive(Debug, Default, PartialEq, Eq)]
struct Entry {
    login: String,
    password: String,
}

#[derive(Debug, Default)]
pub(crate) struct Netrc {
    machines: Vec<(String, Entry)>,
    default: Option<Entry>,
}

impl Netrc {
    pub(crate) fn parse(s: &str) -> Netrc {
        let mut netrc = Netrc::default();
        // The entry being filled in: `Some(host)` for a machine, `None` for
        // `default`.
        let mut current: Option<(Option<String>, Entry)> = None;
        let finish = |netrc: &mut Netrc, current: Option<(Option<String>, Entry)>| match current {
            Some((Some(host), entry)) => netrc.machines.push((host, entry)),
            Some((None, entry)) => netrc.default = Some(entry),
            None => {}
        };

        let mut lines = s.lines();
        while let Some(line) = lines.next() {
            let mut tokens = line.split_whitespace();
            while let Some(token) = tokens.next() {
                match token {
                    "machine" => {
                        finish(&mut netrc, current.take());
                        let host = tokens.next().unwrap_or_default().to_owned();
                        current = Some((Some(host), Entry::default()));
                    }
                    "default" => {
                        finish(&mut netrc, current.take());
                        current = Some((None, Entry::default()));
                    }
                    "login" => {
                        if let (Some((_, e)), Some(v)) = (current.as_mut(), tokens.next()) {
                            e.login = v.to_owned();
                        }
                    }
                    "password" => {
                        if let (Some((_, e)), Some(v)) = (current.as_mut(), tokens.next()) {
                            e.password = v.to_owned();
                        }
                    }
                    "account" => {
                        tokens.next();
                    }
                    "macdef" => {
                        // A macro body runs to the next empty line.
                        finish(&mut netrc, current.take());
                        for body in lines.by_ref() {
                            if body.trim().is_empty() {
                                break;
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }
        finish(&mut netrc, current);
        netrc
    }

    /// The `Authorization` header value for `host`, if the file has
    /// credentials for it (or a `default` entry).
    pub(crate) fn authorization(&self, host: &str) -> Option<String> {
        let entry = self
            .machines
            .iter()
            .find(|(m, _)| m.eq_ignore_ascii_case(host))
            .map(|(_, e)| e)
            .or(self.default.as_ref())?;
        let raw = format!("{}:{}", entry.login, entry.password);
        let mut out = vec![0; Base::Base64.input_len(raw.len())];
        encode_for_base(Base::Base64)(raw.as_bytes(), &mut out);
        Some(format!(
            "Basic {}",
            String::from_utf8(out).expect("base64 is ascii")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machines_and_default() {
        let n = Netrc::parse(
            "machine cache.flakehub.com login flakehub password s3cret\n\
             machine api.flakehub.com\n  login other\n  account x\n  password pw2\n\
             default login anon password guest\n",
        );
        // "flakehub:s3cret"
        assert_eq!(
            n.authorization("cache.flakehub.com").as_deref(),
            Some("Basic Zmxha2VodWI6czNjcmV0")
        );
        assert_eq!(
            n.authorization("CACHE.FLAKEHUB.COM"),
            n.authorization("cache.flakehub.com")
        );
        // "other:pw2"
        assert_eq!(
            n.authorization("api.flakehub.com").as_deref(),
            Some("Basic b3RoZXI6cHcy")
        );
        // "anon:guest"
        assert_eq!(
            n.authorization("elsewhere").as_deref(),
            Some("Basic YW5vbjpndWVzdA==")
        );
    }

    #[test]
    fn no_match_without_default() {
        let n = Netrc::parse("machine a login u password p\n");
        assert!(n.authorization("b").is_none());
        assert!(Netrc::parse("").authorization("a").is_none());
    }

    #[test]
    fn macdef_is_skipped() {
        let n = Netrc::parse(
            "machine a login u password p\n\
             macdef init\nmachine evil login x password y\n\n\
             machine b login v password q\n",
        );
        assert!(n.authorization("a").is_some());
        assert!(n.authorization("evil").is_none());
        // "v:q"
        assert_eq!(n.authorization("b").as_deref(), Some("Basic djpx"));
    }
}
