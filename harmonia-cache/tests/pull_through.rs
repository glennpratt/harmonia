// End-to-end pull-through against a real nix-daemon.
//
// "upstream" is a harmonia serving store A. The harmonia under test serves
// store B, whose nix-daemon substitutes from upstream. Both stores share one
// logical store dir (A keeps its files elsewhere via `real=`) so store paths
// are identical.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

mod common;

use common::{
    CanonicalTempDir, ProcessGuard, Result, init_local_store, pick_unused_port,
    start_harmonia_cache,
};

const SIGNING_KEY: &str = include_str!("../../tests/cache.sk");
const PUBLIC_KEY: &str = include_str!("../../tests/cache.pk");

// Long enough that the GC straight after a pull can't outlive it, short
// enough to wait out both generations.
const TEMP_ROOT_TTL_SECS: u64 = 3;

fn curl(url: &str) -> Result<(u16, Vec<u8>)> {
    let out = Command::new("curl")
        .args([
            "--silent",
            "--max-time",
            "60",
            "--write-out",
            "\n%{http_code}",
        ])
        .arg(url)
        .output()?;
    let mut body = out.stdout;
    let split = body.iter().rposition(|b| *b == b'\n').ok_or("no status")?;
    let status = std::str::from_utf8(&body[split + 1..])?.parse()?;
    body.truncate(split);
    Ok((status, body))
}

fn nix(args: &[&str], envs: &[(&str, &Path)]) -> Result<String> {
    let out = Command::new("nix")
        .args(["--extra-experimental-features", "nix-command"])
        .args(args)
        .envs(envs.iter().copied())
        .env_remove("NIX_REMOTE")
        .output()?;
    if !out.status.success() {
        return Err(format!(
            "nix {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_owned())
}

fn spawn_daemon(root: &Path, store: &Path, state: &Path, conf: &str) -> Result<(Child, PathBuf)> {
    let conf_dir = root.join("b-etc");
    std::fs::create_dir_all(&conf_dir)?;
    std::fs::create_dir_all(state.join("log"))?;
    std::fs::write(conf_dir.join("nix.conf"), conf)?;
    let socket = root.join("b.sock");
    let child = Command::new("nix-daemon")
        .env("NIX_STORE_DIR", store)
        .env("NIX_STATE_DIR", state)
        .env("NIX_LOG_DIR", state.join("log"))
        .env("NIX_CONF_DIR", &conf_dir)
        .env("NIX_DAEMON_SOCKET_PATH", &socket)
        .env("XDG_CACHE_HOME", root.join("b-cache"))
        .env_remove("NIX_REMOTE")
        .env_remove("NIX_CONFIG")
        .stdin(Stdio::null())
        .spawn()?;
    for _ in 0..300 {
        if socket.exists() {
            return Ok((child, socket));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("timed out waiting for nix-daemon socket".into())
}

fn hash_part(store_path: &str) -> &str {
    let base = store_path.rsplit('/').next().unwrap();
    &base[..32]
}

#[tokio::test]
async fn pull_through_substitutes_via_daemon() -> Result<()> {
    let tmp = CanonicalTempDir::new()?;
    let root = tmp.path();
    let store_dir = root.join("store");

    // Upstream store A: logical dir `store_dir`, files under `a-real`.
    let a_real = root.join("a-real");
    let a_state = root.join("a-state");
    let a_uri = format!(
        "local?store={}&real={}&state={}",
        store_dir.display(),
        a_real.display(),
        a_state.display()
    );
    let content = root.join("pulled-file");
    std::fs::write(&content, "pulled through harmonia\n")?;
    let target = nix(
        &[
            "store",
            "add-file",
            "--store",
            &a_uri,
            content.to_str().unwrap(),
        ],
        &[],
    )?;
    let target_hash = hash_part(&target).to_owned();

    let a_port = pick_unused_port().ok_or("no port")?;
    let key_file = root.join("cache.sk");
    std::fs::write(&key_file, SIGNING_KEY.trim())?;
    let _upstream = start_harmonia_cache(
        &format!(
            "bind = \"127.0.0.1:{a_port}\"\n\
             virtual_nix_store = \"{}\"\n\
             real_nix_store = \"{}\"\n\
             nix_db_path = \"{}\"\n\
             sign_key_paths = [\"{}\"]\n",
            store_dir.display(),
            a_real.display(),
            a_state.join("db/db.sqlite").display(),
            key_file.display(),
        ),
        a_port,
    )
    .await?;
    let upstream_url = format!("http://127.0.0.1:{a_port}");
    let (status, _) = curl(&format!("{upstream_url}/{target_hash}.narinfo"))?;
    assert_eq!(status, 200, "upstream serves the target");

    // Store B and its daemon, substituting from upstream.
    let b_state = root.join("b-state");
    init_local_store(&store_dir, &b_state)?;
    let (daemon, socket) = spawn_daemon(
        root,
        &store_dir,
        &b_state,
        &format!(
            "substituters = {upstream_url}\n\
             trusted-public-keys = {}\n\
             require-sigs = true\n\
             experimental-features = nix-command\n",
            PUBLIC_KEY.trim()
        ),
    )?;
    let _daemon = ProcessGuard::new(daemon);

    let b_port = pick_unused_port().ok_or("no port")?;
    let _cache = start_harmonia_cache(
        &format!(
            "bind = \"127.0.0.1:{b_port}\"\n\
             virtual_nix_store = \"{store}\"\n\
             real_nix_store = \"{store}\"\n\
             nix_db_path = \"{db}\"\n\
             [pull_through]\n\
             enable = true\n\
             upstreams = [\"{upstream_url}\"]\n\
             daemon_socket = \"{socket}\"\n\
             negative_ttl = \"1h\"\n\
             temp_root_ttl = \"{TEMP_ROOT_TTL_SECS}s\"\n",
            store = store_dir.display(),
            db = b_state.join("db/db.sqlite").display(),
            socket = socket.display(),
        ),
        b_port,
    )
    .await?;
    let base = format!("http://127.0.0.1:{b_port}");
    let target_on_disk = PathBuf::from(&target);
    assert!(!target_on_disk.exists(), "B starts without the target");

    // A miss is pulled through the daemon and served.
    let (status, body) = curl(&format!("{base}/{target_hash}.narinfo"))?;
    let narinfo = String::from_utf8(body)?;
    assert_eq!(status, 200, "narinfo: {narinfo}");
    assert!(
        narinfo.contains(&format!("StorePath: {target}")),
        "{narinfo}"
    );
    assert!(
        narinfo.contains("Sig: cache.example.com-1:"),
        "upstream signature preserved: {narinfo}"
    );
    assert!(target_on_disk.exists(), "target now valid in B");

    // GC between the narinfo and the NAR request: the temp root keeps it.
    let daemon_store = format!("unix://{}?store={}", socket.display(), store_dir.display());
    nix(&["store", "gc", "--store", &daemon_store], &[])?;
    assert!(target_on_disk.exists(), "temp root survived GC");
    let url = narinfo
        .lines()
        .find_map(|l| l.strip_prefix("URL: "))
        .ok_or("narinfo has no URL")?;
    let (status, nar) = curl(&format!("{base}/{url}"))?;
    assert_eq!(status, 200);
    assert!(
        nar.windows(23).any(|w| w == b"pulled through harmonia"),
        "NAR has the file contents"
    );

    // An unknown hash: 404, and one upstream lookup per negative_ttl.
    let unknown = "00000000000000000000000000000000";
    for _ in 0..3 {
        let (status, _) = curl(&format!("{base}/{unknown}.narinfo"))?;
        assert_eq!(status, 404);
    }
    let (_, metrics) = curl(&format!("{base}/metrics"))?;
    let metrics = String::from_utf8(metrics)?;
    for line in [
        "harmonia_pull_through_upstream_lookups_total{result=\"not_found\"} 1",
        "harmonia_pull_through_upstream_lookups_total{result=\"found\"} 1",
        "harmonia_pull_through_requests_total{result=\"negative_cached\"} 2",
        "harmonia_pull_through_requests_total{result=\"pulled\"} 1",
    ] {
        assert!(metrics.contains(line), "missing {line:?} in\n{metrics}");
    }

    // Once both root generations have rotated out, a normal GC prunes the
    // pulled path like any other, and the next request pulls it again.
    tokio::time::sleep(Duration::from_secs(TEMP_ROOT_TTL_SECS * 2 + 1)).await;
    nix(&["store", "gc", "--store", &daemon_store], &[])?;
    assert!(!target_on_disk.exists(), "GC removed the pulled path");
    let (status, _) = curl(&format!("{base}/{target_hash}.narinfo"))?;
    assert_eq!(status, 200);
    assert!(target_on_disk.exists(), "pulled again after GC");

    Ok(())
}
