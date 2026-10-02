use crate::error::{CacheError, ConfigError, Result};
use crate::store::Store;
use harmonia_store_path::StoreDir;
use harmonia_utils_signature::SecretKey;
use serde::Deserialize;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn default_bind() -> String {
    "[::]:5000".into()
}

fn default_workers() -> usize {
    4
}

fn default_connection_rate() -> usize {
    256
}

fn default_max_connections() -> usize {
    2048
}

fn default_priority() -> usize {
    30
}

fn default_enable_compression() -> bool {
    true
}

/// zstd parameters applied to on-the-fly NAR encoding when the client sends
/// `Accept-Encoding: zstd`. Defaults are tuned for a substitution cache:
/// level 1 with long-distance matching beats the libzstd default (level 3)
/// on both ratio and throughput for typical NARs, and the window cap keeps
/// per-stream decoder memory bounded under parallel `nix copy`.
#[derive(Clone, Copy, Debug, Deserialize)]
pub(crate) struct ZstdConfig {
    #[serde(default = "ZstdConfig::default_level")]
    pub(crate) level: i32,
    #[serde(default = "ZstdConfig::default_long_distance")]
    pub(crate) long_distance_matching: bool,
    /// log2 of the match window (non-NAR responses are clamped to 23 per
    /// RFC 9659). 0 = auto: with LDM, cap at 25 (32 MiB) so
    /// decoder memory stays bounded; without LDM, use the level default so
    /// the encoder doesn't allocate a large window it can't fill.
    #[serde(default)]
    pub(crate) window_log: u32,
    /// Per-worker cap on concurrent LDM encoders for large bodies (>= 4 MiB).
    /// An LDM encoder grows to ~35 MiB; overflow falls back to no-LDM
    /// (~0.75 MiB, lower ratio). 0 = unbounded. Ignored if LDM is disabled.
    #[serde(default = "ZstdConfig::default_max_ldm_encoders_per_worker")]
    pub(crate) max_ldm_encoders_per_worker: usize,
}

impl ZstdConfig {
    fn default_level() -> i32 {
        1
    }
    fn default_long_distance() -> bool {
        true
    }
    fn default_max_ldm_encoders_per_worker() -> usize {
        16
    }
}

impl Default for ZstdConfig {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            long_distance_matching: Self::default_long_distance(),
            window_log: 0,
            max_ldm_encoders_per_worker: Self::default_max_ldm_encoders_per_worker(),
        }
    }
}

/// Parse a duration such as `"500ms"`, `"30s"`, `"5m"` or `"1h"`. A bare
/// number is taken as seconds.
pub(crate) fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration '{s}': expected e.g. \"30s\" or \"5m\""))?;
    let secs = |mul: u64| {
        n.checked_mul(mul)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration '{s}' is too large"))
    };
    match unit.trim() {
        "ms" => Ok(Duration::from_millis(n)),
        "" | "s" => secs(1),
        "m" => secs(60),
        "h" => secs(60 * 60),
        other => Err(format!(
            "invalid duration unit '{other}' in '{s}': expected ms, s, m or h"
        )),
    }
}

/// Deserialize a duration from either a string (`"5m"`) or integer seconds.
fn deserialize_duration<'de, D>(d: D) -> std::result::Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Secs(u64),
        Str(String),
    }
    match Raw::deserialize(d)? {
        Raw::Secs(n) => Ok(Duration::from_secs(n)),
        Raw::Str(s) => parse_duration(&s).map_err(serde::de::Error::custom),
    }
}

/// On a narinfo miss, have the local nix-daemon substitute the path from its
/// own substituters, then serve it. See `pull_through.rs`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PullThroughConfig {
    #[serde(default)]
    pub(crate) enable: bool,
    /// Binary caches queried to resolve a hash part to a full store path. The
    /// daemon's own substituters do the downloading, so these should match.
    #[serde(default)]
    pub(crate) upstreams: Vec<String>,
    /// netrc file with credentials for authenticated upstreams.
    #[serde(default)]
    pub(crate) netrc_file: Option<PathBuf>,
    #[serde(default = "PullThroughConfig::default_daemon_socket")]
    pub(crate) daemon_socket: PathBuf,
    /// How long a hash that no upstream has (or that failed to substitute)
    /// is answered with 404 without asking again.
    #[serde(
        default = "PullThroughConfig::default_negative_ttl",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) negative_ttl: Duration,
    /// Cap on concurrent `EnsurePath` calls to the daemon.
    #[serde(default = "PullThroughConfig::default_max_concurrent")]
    pub(crate) max_concurrent: usize,
    /// Pulled paths stay GC-rooted for between one and two of these, which
    /// must cover the gap between a client's narinfo and NAR requests.
    #[serde(
        default = "PullThroughConfig::default_temp_root_ttl",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) temp_root_ttl: Duration,
    /// How long a narinfo request waits for a substitution before answering
    /// 404. The substitution carries on, so a later request finds the path.
    #[serde(
        default = "PullThroughConfig::default_request_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) request_timeout: Duration,
    /// Timeout for each upstream narinfo lookup.
    #[serde(
        default = "PullThroughConfig::default_upstream_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) upstream_timeout: Duration,
}

impl PullThroughConfig {
    fn default_daemon_socket() -> PathBuf {
        PathBuf::from("/nix/var/nix/daemon-socket/socket")
    }
    fn default_negative_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }
    fn default_max_concurrent() -> usize {
        16
    }
    fn default_temp_root_ttl() -> Duration {
        Duration::from_secs(10 * 60)
    }
    fn default_request_timeout() -> Duration {
        Duration::from_secs(60)
    }
    fn default_upstream_timeout() -> Duration {
        Duration::from_secs(10)
    }

    fn validate(&self) -> Result<()> {
        if !self.enable {
            return Ok(());
        }
        let invalid = |reason: String| -> CacheError { ConfigError::Invalid { reason }.into() };
        if self.upstreams.is_empty() {
            return Err(invalid(
                "pull_through.upstreams must not be empty when pull_through is enabled".into(),
            ));
        }
        for upstream in &self.upstreams {
            let url = url::Url::parse(upstream)
                .map_err(|e| invalid(format!("pull_through upstream '{upstream}': {e}")))?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(invalid(format!(
                    "pull_through upstream '{upstream}' must be an http(s) URL"
                )));
            }
        }
        if self.max_concurrent == 0 {
            return Err(invalid(
                "pull_through.max_concurrent must be greater than 0".into(),
            ));
        }
        if self.temp_root_ttl.is_zero() || self.request_timeout.is_zero() {
            return Err(invalid(
                "pull_through.temp_root_ttl and request_timeout must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

impl Default for PullThroughConfig {
    fn default() -> Self {
        toml::from_str("").expect("empty pull_through config deserializes with serde defaults")
    }
}

fn default_virtual_store() -> PathBuf {
    PathBuf::from("/nix/store")
}

/// Derive the location of `db.sqlite` from the on-disk store directory.
///
/// Nix lays out a store root as `<root>/store` and `<root>/var/nix/db/db.sqlite`,
/// so for both the default `/nix/store` and chroot stores we can find the
/// database by replacing the trailing `store` component.
fn derive_db_path(real_store: &Path) -> Option<PathBuf> {
    let root = real_store.parent()?;
    Some(root.join("var/nix/db/db.sqlite"))
}

// TODO(conni2461): users to restrict access
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default = "default_bind")]
    pub(crate) bind: String,
    #[serde(default = "default_workers")]
    pub(crate) workers: usize,
    #[serde(default = "default_connection_rate")]
    pub(crate) max_connection_rate: usize,
    /// Per-worker cap on concurrent connections. Together with the LDM
    /// semaphore this bounds compression memory. 0 keeps the actix default.
    #[serde(default = "default_max_connections")]
    pub(crate) max_connections: usize,
    #[serde(default = "default_priority")]
    pub(crate) priority: usize,

    #[serde(default = "default_virtual_store")]
    pub(crate) virtual_nix_store: PathBuf,

    #[serde(default)]
    pub(crate) real_nix_store: Option<PathBuf>,

    #[serde(default = "default_enable_compression")]
    pub(crate) enable_compression: bool,

    #[serde(default)]
    pub(crate) zstd: ZstdConfig,

    #[serde(default)]
    pub(crate) sign_key_path: Option<String>,
    #[serde(default)]
    pub(crate) sign_key_paths: Vec<PathBuf>,
    #[serde(default)]
    pub(crate) tls_cert_path: Option<String>,
    #[serde(default)]
    pub(crate) tls_key_path: Option<String>,

    /// Path to the nix SQLite database. Derived from the store layout when unset.
    #[serde(default)]
    pub(crate) nix_db_path: Option<PathBuf>,

    #[serde(default)]
    pub(crate) pull_through: PullThroughConfig,

    #[serde(skip, default)]
    pub(crate) secret_keys: Vec<SecretKey>,
    #[serde(skip)]
    pub(crate) store: Store,
}

impl Default for Config {
    fn default() -> Self {
        toml::from_str("").expect("empty config deserializes with serde defaults")
    }
}

impl Config {
    pub(crate) fn load(settings_file: &Path) -> Result<Config> {
        let contents = read_to_string(settings_file).map_err(|e| ConfigError::ReadFile {
            path: settings_file.display().to_string(),
            source: e,
        })?;
        toml::from_str(&contents).map_err(|e| CacheError::from(ConfigError::from(e)))
    }
}

pub(crate) fn load() -> Result<Config> {
    let mut settings = match std::env::var("CONFIG_FILE") {
        Err(_) => {
            if Path::new("settings.toml").exists() {
                Config::load(Path::new("settings.toml"))?
            } else {
                Config::default()
            }
        }
        Ok(settings_file) => Config::load(Path::new(&settings_file))?,
    };

    if settings.workers == 0 {
        return Err(ConfigError::Invalid {
            reason: "workers must be greater than 0".to_string(),
        }
        .into());
    }

    settings.pull_through.validate()?;

    if let Some(sign_key_path) = &settings.sign_key_path {
        tracing::warn!(
            "The sign_key_path configuration option is deprecated. Use sign_key_paths instead."
        );
        settings.sign_key_paths.push(PathBuf::from(sign_key_path));
    }
    if let Ok(sign_key_path) = std::env::var("SIGN_KEY_PATH") {
        tracing::warn!(
            "The SIGN_KEY_PATH environment variable is deprecated. Use SIGN_KEY_PATHS instead."
        );
        settings.sign_key_paths.push(PathBuf::from(sign_key_path));
    }
    if let Ok(sign_key_paths) = std::env::var("SIGN_KEY_PATHS") {
        for sign_key_path in sign_key_paths.split_whitespace() {
            settings.sign_key_paths.push(PathBuf::from(sign_key_path));
        }
    }
    for sign_key_path in &settings.sign_key_paths {
        crate::tls::warn_insecure_permissions(sign_key_path);
        let key_content =
            read_to_string(sign_key_path).map_err(|e| ConfigError::InvalidSigningKey {
                reason: format!(
                    "Couldn't read secret key from '{}': {}",
                    sign_key_path.display(),
                    e
                ),
            })?;
        let key: SecretKey =
            key_content
                .trim()
                .parse()
                .map_err(|e| ConfigError::InvalidSigningKey {
                    reason: format!(
                        "Couldn't parse secret key from '{}': {}",
                        sign_key_path.display(),
                        e
                    ),
                })?;
        settings.secret_keys.push(key);
    }
    let virtual_store_str = std::env::var("NIX_STORE_DIR").unwrap_or_else(|_| {
        settings
            .virtual_nix_store
            .to_str()
            .unwrap_or("/nix/store")
            .to_owned()
    });
    let store_dir = StoreDir::new(&virtual_store_str).map_err(|_| ConfigError::Invalid {
        reason: format!("invalid store dir: {virtual_store_str}"),
    })?;
    let real_store_path = settings
        .real_nix_store
        .clone()
        .unwrap_or_else(|| AsRef::<Path>::as_ref(&store_dir).to_path_buf());
    let db_path = settings
        .nix_db_path
        .clone()
        .or_else(|| derive_db_path(&real_store_path))
        .ok_or_else(|| ConfigError::Invalid {
            reason: format!(
                "could not derive nix_db_path from store dir {}; set nix_db_path explicitly",
                real_store_path.display()
            ),
        })?;
    if !db_path.exists() {
        return Err(ConfigError::Invalid {
            reason: format!(
                "nix database {} not found; set nix_db_path to the store's db.sqlite",
                db_path.display()
            ),
        }
        .into());
    }
    settings.store = Store::new(store_dir, settings.real_nix_store.clone(), db_path);
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_uses_serde_defaults() {
        let c = Config::default();
        assert_eq!(c.workers, default_workers());
        assert_eq!(c.bind, default_bind());
        assert_eq!(c.priority, default_priority());
        assert!(!c.pull_through.enable);
    }

    #[test]
    fn parse_durations() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert!(parse_duration("").is_err());
        assert!(parse_duration("5d").is_err());
        assert!(parse_duration("m").is_err());
        assert!(parse_duration("99999999999999999999h").is_err());
    }

    #[test]
    fn pull_through_config() {
        let c: Config = toml::from_str(
            r#"
            [pull_through]
            enable = true
            upstreams = ["https://cache.nixos.org", "http://upstream"]
            netrc_file = "/etc/nix/netrc"
            daemon_socket = "/run/nix.sock"
            negative_ttl = "1m"
            max_concurrent = 4
            temp_root_ttl = 90
            request_timeout = "2m"
            upstream_timeout = "500ms"
            "#,
        )
        .unwrap();
        let pt = &c.pull_through;
        assert!(pt.enable);
        assert_eq!(pt.upstreams.len(), 2);
        assert_eq!(pt.netrc_file.as_deref(), Some(Path::new("/etc/nix/netrc")));
        assert_eq!(pt.daemon_socket, Path::new("/run/nix.sock"));
        assert_eq!(pt.negative_ttl, Duration::from_secs(60));
        assert_eq!(pt.max_concurrent, 4);
        assert_eq!(pt.temp_root_ttl, Duration::from_secs(90));
        assert_eq!(pt.request_timeout, Duration::from_secs(120));
        assert_eq!(pt.upstream_timeout, Duration::from_millis(500));
        pt.validate().unwrap();
    }

    #[test]
    fn pull_through_defaults() {
        let c: Config =
            toml::from_str("[pull_through]\nenable = true\nupstreams = [\"http://u\"]").unwrap();
        let pt = &c.pull_through;
        assert_eq!(
            pt.daemon_socket,
            Path::new("/nix/var/nix/daemon-socket/socket")
        );
        assert_eq!(pt.negative_ttl, Duration::from_secs(300));
        assert_eq!(pt.max_concurrent, 16);
        assert_eq!(pt.temp_root_ttl, Duration::from_secs(600));
    }

    #[test]
    fn pull_through_validation() {
        let parse = |s: &str| toml::from_str::<Config>(s).map(|c| c.pull_through);
        // Disabled needs nothing else.
        parse("[pull_through]").unwrap().validate().unwrap();
        for bad in [
            "[pull_through]\nenable = true",
            "[pull_through]\nenable = true\nupstreams = [\"file:///x\"]",
            "[pull_through]\nenable = true\nupstreams = [\"not a url\"]",
            "[pull_through]\nenable = true\nupstreams = [\"http://u\"]\nmax_concurrent = 0",
        ] {
            assert!(parse(bad).unwrap().validate().is_err(), "{bad}");
        }
        assert!(parse("[pull_through]\nunknown = 1").is_err());
        assert!(parse("[pull_through]\nnegative_ttl = \"5 parsecs\"").is_err());
    }
}
