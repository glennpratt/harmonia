//! Pull-through substitution: on a narinfo miss, have the host's nix-daemon
//! substitute the path into the local store, then serve it.
//!
//! Going through the daemon (rather than caching upstream NARs ourselves)
//! means pulled paths are ordinary store paths pruned by the normal GC, the
//! daemon's own substituters, trusted keys and credentials apply, and the
//! upstream signatures land in the store database next to our own.
//!
//! This lets anyone who can reach harmonia make the host download anything
//! the upstreams have, so it is off by default and meant for trusted networks.

mod daemon;
mod netrc;
mod upstream;

use crate::config::PullThroughConfig;
use crate::error::{ConfigError, Result};
use crate::prometheus::PullThroughMetrics;
use harmonia_store_path::{StoreDir, StorePath, StorePathHash};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, watch};

type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug)]
pub(crate) enum Resolved {
    Found(StorePath),
    NotFound,
    Error(String),
}

/// Maps a hash part to the full store path an upstream has for it.
pub(crate) trait Resolve: Send + Sync {
    fn resolve<'a>(&'a self, hash: &'a StorePathHash) -> LocalBoxFuture<'a, Resolved>;
}

/// Makes a store path valid in the local store and keeps it GC-rooted for a
/// while.
pub(crate) trait Substitute: Send + Sync {
    fn substitute<'a>(
        &'a self,
        path: &'a StorePath,
    ) -> LocalBoxFuture<'a, std::result::Result<(), String>>;
}

/// What a narinfo request should do after asking for a pull.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The path is now valid locally; query the store again.
    Pulled,
    /// No upstream has it (or we asked recently and none did).
    NotFound,
    /// Resolving or substituting failed.
    Failed,
    /// Still substituting after `request_timeout`. It carries on in the
    /// background, so a retry may find it.
    Pending,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Pulled => "pulled",
            Outcome::NotFound => "not_found",
            Outcome::Failed => "failed",
            Outcome::Pending => "timeout",
        }
    }
}

/// Hashes recently found to be unavailable. Clients probe narinfos for many
/// paths that exist nowhere (every build input, say); without this each of
/// those 404s would cost an upstream round trip.
struct NegativeCache {
    ttl: Duration,
    entries: HashMap<StorePathHash, Instant>,
}

impl NegativeCache {
    /// Bounds memory if clients probe huge numbers of distinct hashes.
    const MAX_ENTRIES: usize = 100_000;

    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: HashMap::new(),
        }
    }

    fn contains(&mut self, hash: &StorePathHash, now: Instant) -> bool {
        match self.entries.get(hash) {
            Some(expires) if *expires > now => true,
            Some(_) => {
                self.entries.remove(hash);
                false
            }
            None => false,
        }
    }

    fn insert(&mut self, hash: StorePathHash, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }
        if self.entries.len() >= Self::MAX_ENTRIES {
            self.entries.retain(|_, expires| *expires > now);
            if self.entries.len() >= Self::MAX_ENTRIES {
                self.entries.clear();
            }
        }
        self.entries.insert(hash, now + self.ttl);
    }
}

pub(crate) struct PullThrough {
    resolver: Box<dyn Resolve>,
    substituter: Arc<dyn Substitute>,
    request_timeout: Duration,
    negative: Mutex<NegativeCache>,
    /// One entry per hash being pulled; later requests wait on the same
    /// result. A `watch` rather than a shared future because actix workers
    /// are separate single-threaded runtimes.
    inflight: Mutex<HashMap<StorePathHash, watch::Receiver<Option<Outcome>>>>,
    substitutions: Semaphore,
    metrics: Option<PullThroughMetrics>,
}

impl PullThrough {
    /// Build from config and start the temp-root rotation. Must be called
    /// from within the actix system.
    pub(crate) fn start(
        cfg: &PullThroughConfig,
        store_dir: &StoreDir,
        metrics: Option<PullThroughMetrics>,
    ) -> Result<Arc<Self>> {
        let netrc = match &cfg.netrc_file {
            Some(path) => {
                crate::tls::warn_insecure_permissions(path);
                let text = std::fs::read_to_string(path).map_err(|e| ConfigError::ReadFile {
                    path: path.display().to_string(),
                    source: e,
                })?;
                Some(netrc::Netrc::parse(&text))
            }
            None => None,
        };
        let resolver = upstream::HttpResolver::new(
            &cfg.upstreams,
            netrc.as_ref(),
            store_dir.clone(),
            cfg.upstream_timeout,
        );
        let substituter = Arc::new(daemon::DaemonSubstituter::new(
            cfg.daemon_socket.clone(),
            store_dir.clone(),
        ));
        substituter.spawn_rotation(cfg.temp_root_ttl);
        tracing::info!(
            "pull-through enabled: upstreams {:?}, daemon {}",
            cfg.upstreams,
            cfg.daemon_socket.display()
        );
        Ok(Arc::new(Self::new(
            Box::new(resolver),
            substituter,
            cfg,
            metrics,
        )))
    }

    fn new(
        resolver: Box<dyn Resolve>,
        substituter: Arc<dyn Substitute>,
        cfg: &PullThroughConfig,
        metrics: Option<PullThroughMetrics>,
    ) -> Self {
        Self {
            resolver,
            substituter,
            request_timeout: cfg.request_timeout,
            negative: Mutex::new(NegativeCache::new(cfg.negative_ttl)),
            inflight: Mutex::new(HashMap::new()),
            substitutions: Semaphore::new(cfg.max_concurrent),
            metrics,
        }
    }

    /// Try to make `hash` valid in the local store. Must be called from an
    /// actix worker (the pull runs as a local task there).
    pub(crate) async fn ensure(self: &Arc<Self>, hash: StorePathHash) -> Outcome {
        if self
            .negative
            .lock()
            .expect("negative cache poisoned")
            .contains(&hash, Instant::now())
        {
            self.count("negative_cached");
            return Outcome::NotFound;
        }

        let mut rx = {
            let mut inflight = self.inflight.lock().expect("inflight map poisoned");
            match inflight.get(&hash) {
                Some(rx) => rx.clone(),
                None => {
                    let (tx, rx) = watch::channel(None);
                    inflight.insert(hash, rx.clone());
                    // Spawned, so the pull finishes even if this request
                    // times out or the client goes away.
                    actix_web::rt::spawn(self.clone().pull(hash, tx));
                    rx
                }
            }
        };

        let outcome =
            match tokio::time::timeout(self.request_timeout, rx.wait_for(Option::is_some)).await {
                Ok(Ok(outcome)) => outcome.expect("waited for Some"),
                // The pull task went away without reporting (panicked).
                Ok(Err(_)) => Outcome::Failed,
                Err(_) => Outcome::Pending,
            };
        self.count(outcome.label());
        outcome
    }

    async fn pull(self: Arc<Self>, hash: StorePathHash, tx: watch::Sender<Option<Outcome>>) {
        let outcome = self.resolve_and_substitute(&hash).await;
        if outcome != Outcome::Pulled {
            self.negative
                .lock()
                .expect("negative cache poisoned")
                .insert(hash, Instant::now());
        }
        // Publish before removing, so a request arriving in between still
        // sees the result instead of starting another pull.
        tx.send_replace(Some(outcome));
        self.inflight
            .lock()
            .expect("inflight map poisoned")
            .remove(&hash);
    }

    async fn resolve_and_substitute(&self, hash: &StorePathHash) -> Outcome {
        let path = match self.resolver.resolve(hash).await {
            Resolved::Found(path) => path,
            Resolved::NotFound => {
                self.count_upstream("not_found");
                return Outcome::NotFound;
            }
            Resolved::Error(e) => {
                self.count_upstream("error");
                tracing::warn!("pull-through: resolving {hash} failed: {e}");
                return Outcome::Failed;
            }
        };
        self.count_upstream("found");

        let _permit = self
            .substitutions
            .acquire()
            .await
            .expect("semaphore is never closed");
        let start = Instant::now();
        let result = self.substituter.substitute(&path).await;
        if let Some(m) = &self.metrics {
            m.substitute_duration.observe(start.elapsed().as_secs_f64());
        }
        match result {
            Ok(()) => {
                tracing::info!(
                    "pull-through: substituted {path} in {:.1?}",
                    start.elapsed()
                );
                Outcome::Pulled
            }
            Err(e) => {
                // Upstream said it has the path, so the likely cause is the
                // daemon's substituters (or trusted keys) not matching
                // `pull_through.upstreams`.
                tracing::warn!(
                    "pull-through: an upstream has {path} but the daemon failed to substitute it \
                     (do the daemon's substituters and trusted-public-keys match \
                     pull_through.upstreams?): {e}"
                );
                Outcome::Failed
            }
        }
    }

    fn count(&self, result: &str) {
        if let Some(m) = &self.metrics {
            m.requests.with_label_values(&[result]).inc();
        }
    }

    fn count_upstream(&self, result: &str) {
        if let Some(m) = &self.metrics {
            m.upstream_lookups.with_label_values(&[result]).inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const HASH: &str = "7rjj86a15146cq1d3qy068lml7n8ykzm";

    fn hash() -> StorePathHash {
        HASH.parse().unwrap()
    }

    fn path() -> StorePath {
        format!("{HASH}-hello").parse().unwrap()
    }

    #[derive(Clone, Copy)]
    enum Answer {
        Found,
        NotFound,
        Error,
    }

    struct FakeResolver {
        answer: Answer,
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl Resolve for FakeResolver {
        fn resolve<'a>(&'a self, _: &'a StorePathHash) -> LocalBoxFuture<'a, Resolved> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
                match self.answer {
                    Answer::Found => Resolved::Found(path()),
                    Answer::NotFound => Resolved::NotFound,
                    Answer::Error => Resolved::Error("boom".into()),
                }
            })
        }
    }

    struct FakeSubstituter {
        ok: bool,
        calls: Arc<AtomicUsize>,
        delay: Duration,
        running: AtomicUsize,
        max_running: AtomicUsize,
    }

    impl Substitute for FakeSubstituter {
        fn substitute<'a>(
            &'a self,
            _: &'a StorePath,
        ) -> LocalBoxFuture<'a, std::result::Result<(), String>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_running.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
                self.running.fetch_sub(1, Ordering::SeqCst);
                if self.ok { Ok(()) } else { Err("nope".into()) }
            })
        }
    }

    struct Harness {
        pt: Arc<PullThrough>,
        resolves: Arc<AtomicUsize>,
        substitutes: Arc<AtomicUsize>,
        substituter: Arc<FakeSubstituter>,
    }

    fn harness(answer: Answer, ok: bool, cfg: PullThroughConfig, delay: Duration) -> Harness {
        let resolves = Arc::new(AtomicUsize::new(0));
        let substitutes = Arc::new(AtomicUsize::new(0));
        let substituter = Arc::new(FakeSubstituter {
            ok,
            calls: substitutes.clone(),
            delay,
            running: AtomicUsize::new(0),
            max_running: AtomicUsize::new(0),
        });
        let pt = Arc::new(PullThrough::new(
            Box::new(FakeResolver {
                answer,
                calls: resolves.clone(),
                delay,
            }),
            substituter.clone(),
            &cfg,
            None,
        ));
        Harness {
            pt,
            resolves,
            substitutes,
            substituter,
        }
    }

    #[actix_web::test]
    async fn single_flight() {
        let h = harness(
            Answer::Found,
            true,
            PullThroughConfig::default(),
            Duration::from_millis(50),
        );
        let outcomes = futures_util::future::join_all((0..20).map(|_| h.pt.ensure(hash()))).await;
        assert!(outcomes.iter().all(|o| *o == Outcome::Pulled));
        assert_eq!(h.resolves.load(Ordering::SeqCst), 1);
        assert_eq!(h.substitutes.load(Ordering::SeqCst), 1);
        assert!(h.pt.inflight.lock().unwrap().is_empty());

        // A pulled path isn't negatively cached; a later miss (say, after a
        // GC) pulls again.
        assert_eq!(h.pt.ensure(hash()).await, Outcome::Pulled);
        assert_eq!(h.resolves.load(Ordering::SeqCst), 2);
    }

    #[actix_web::test]
    async fn negative_cache() {
        let cfg = PullThroughConfig {
            negative_ttl: Duration::from_millis(200),
            ..Default::default()
        };
        let h = harness(Answer::NotFound, true, cfg, Duration::ZERO);
        for _ in 0..5 {
            assert_eq!(h.pt.ensure(hash()).await, Outcome::NotFound);
        }
        assert_eq!(h.resolves.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(h.pt.ensure(hash()).await, Outcome::NotFound);
        assert_eq!(h.resolves.load(Ordering::SeqCst), 2);
        assert_eq!(h.substitutes.load(Ordering::SeqCst), 0);
    }

    #[actix_web::test]
    async fn failures_are_negatively_cached() {
        for (answer, ok) in [(Answer::Error, true), (Answer::Found, false)] {
            let h = harness(answer, ok, PullThroughConfig::default(), Duration::ZERO);
            assert_eq!(h.pt.ensure(hash()).await, Outcome::Failed);
            assert_eq!(h.pt.ensure(hash()).await, Outcome::NotFound);
            assert_eq!(h.resolves.load(Ordering::SeqCst), 1);
        }
    }

    #[actix_web::test]
    async fn timeout_leaves_pull_running() {
        let cfg = PullThroughConfig {
            request_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        let h = harness(Answer::Found, true, cfg, Duration::from_millis(100));
        assert_eq!(h.pt.ensure(hash()).await, Outcome::Pending);
        // A retry joins the pull still in flight instead of starting another.
        assert_eq!(h.pt.ensure(hash()).await, Outcome::Pending);
        // Resolve and substitute each take 100ms.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(h.resolves.load(Ordering::SeqCst), 1);
        assert_eq!(h.substitutes.load(Ordering::SeqCst), 1);
        assert!(h.pt.inflight.lock().unwrap().is_empty());
    }

    #[actix_web::test]
    async fn concurrency_limit() {
        let cfg = PullThroughConfig {
            max_concurrent: 2,
            ..Default::default()
        };
        let h = harness(Answer::Found, true, cfg, Duration::from_millis(30));
        // Distinct hashes, so single-flight doesn't merge them. The fake
        // resolver ignores the hash, which is fine here.
        let hashes: Vec<StorePathHash> = (0..6u8)
            .map(|i| StorePathHash::new([i; StorePathHash::len()]))
            .collect();
        let outcomes =
            futures_util::future::join_all(hashes.iter().map(|h2| h.pt.ensure(*h2))).await;
        assert!(outcomes.iter().all(|o| *o == Outcome::Pulled));
        assert_eq!(h.substitutes.load(Ordering::SeqCst), 6);
        assert_eq!(h.substituter.max_running.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn negative_cache_is_bounded() {
        let mut c = NegativeCache::new(Duration::from_secs(60));
        let now = Instant::now();
        for i in 0..NegativeCache::MAX_ENTRIES + 10 {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            c.insert(StorePathHash::new(bytes), now);
        }
        assert!(c.entries.len() <= NegativeCache::MAX_ENTRIES);

        let mut off = NegativeCache::new(Duration::ZERO);
        off.insert(hash(), now);
        assert!(!off.contains(&hash(), now));
    }
}
