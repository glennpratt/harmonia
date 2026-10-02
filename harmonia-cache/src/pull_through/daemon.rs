//! Having the local nix-daemon substitute a path, and keeping it alive until
//! it has been served.

use super::{LocalBoxFuture, Substitute};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_store_remote::{DaemonClient, DaemonResult, DaemonStore};
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

type Client = DaemonClient<OwnedReadHalf, OwnedWriteHalf>;

/// Temp roots last as long as the daemon connection that added them. Pulled
/// paths are rooted on `current`; every `temp_root_ttl` it becomes `previous`
/// and the old `previous` is closed, so each root lives for between one and
/// two ttls. Two connections regardless of how many paths are pulled, rather
/// than one idle daemon process per path.
#[derive(Default)]
struct Roots {
    current: Option<Client>,
    previous: Option<Client>,
}

pub(crate) struct DaemonSubstituter {
    socket: PathBuf,
    store_dir: StoreDir,
    roots: Mutex<Roots>,
}

impl DaemonSubstituter {
    pub(crate) fn new(socket: PathBuf, store_dir: StoreDir) -> Self {
        Self {
            socket,
            store_dir,
            roots: Mutex::new(Roots::default()),
        }
    }

    async fn connect(&self) -> DaemonResult<Client> {
        DaemonClient::builder()
            .set_store_dir(&self.store_dir)
            .connect_unix(&self.socket)
            .await
    }

    /// Release the roots of the older generation. Called every
    /// `temp_root_ttl` by a background task.
    pub(crate) async fn rotate(&self) {
        let mut roots = self.roots.lock().await;
        roots.previous = roots.current.take();
    }

    /// Spawn the task that calls `rotate` every `ttl`. Must be called from
    /// within the actix system.
    pub(crate) fn spawn_rotation(self: &std::sync::Arc<Self>, ttl: Duration) {
        let this = self.clone();
        actix_web::rt::spawn(async move {
            let mut tick = tokio::time::interval(ttl);
            tick.tick().await;
            loop {
                tick.tick().await;
                this.rotate().await;
            }
        });
    }

    async fn root(&self, path: &StorePath) -> DaemonResult<()> {
        let mut roots = self.roots.lock().await;
        // Retry once on a fresh connection, in case the daemon dropped ours.
        for attempt in 0..2 {
            if roots.current.is_none() {
                roots.current = Some(self.connect().await?);
            }
            let client = roots.current.as_mut().expect("just connected");
            match client.add_temp_root(path).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt == 0 => {
                    tracing::debug!("temp-root connection failed, reconnecting: {e}");
                    roots.current = None;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns on the second attempt")
    }

    async fn substitute_inner(&self, path: &StorePath) -> DaemonResult<()> {
        // A dedicated connection, so a slow substitution doesn't block the
        // root connection. `EnsurePath` also roots the path on this
        // connection; it stays open until the long-lived root is in place.
        let mut client = self.connect().await?;
        client.add_temp_root(path).await?;
        client.ensure_path(path).await?;
        self.root(path).await
    }
}

impl Substitute for DaemonSubstituter {
    fn substitute<'a>(
        &'a self,
        path: &'a StorePath,
    ) -> LocalBoxFuture<'a, std::result::Result<(), String>> {
        Box::pin(async move { self.substitute_inner(path).await.map_err(|e| e.to_string()) })
    }
}
