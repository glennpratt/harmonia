# Pull-through substitution: working notes

Status: notes only, nothing implemented. Branch `pull-through` in
glennpratt/harmonia, off upstream `main` at acf86e8.

## The use case

[knix](https://github.com/glennpratt/knix) runs local Kubernetes clusters (kind) whose
nodes have their own Nix stores and substitute from harmonia serving the
**host's** store (its "bridge" mode). Anything built on the host reaches the
nodes with no push step. Measured: 2.0 s for a 45-path / 254 MiB closure into
an empty store. That's close to static zstd files (1.25 s) and far ahead of
nix-serve (17 s).

The gap is paths the host doesn't have. A node falls back to fetching them
from cache.nixos.org itself, so they live only in the node's store and are
downloaded again every time a cluster is recreated. The wanted behaviour:

> On a narinfo miss, harmonia has the host's nix-daemon **substitute** the path
> into the host store, then serves it.

Why through the daemon, rather than harmonia caching upstream NARs itself:

- **One cache, one GC.** Pulled paths are ordinary store paths. The host's
  normal `nix-collect-garbage` prunes them, with no separate cache directory to
  manage (ncps and Attic keep their own storage).
- **The host's substituters and credentials apply.** The daemon substitutes
  with its own config: substituters, trusted keys, netrc and FlakeHub auth
  (Determinate). harmonia never needs those credentials to download.
- **Signatures are preserved.** The substituted path's upstream signatures land
  in the store database, and `build_narinfo` already emits them next to
  harmonia's own.

## What harmonia does today

- `harmonia-cache/src/narinfo.rs`, `get`: decodes the hash part, then
  `settings.store.query_path_info_by_hash_part(..)` and `some_or_404!`. A miss
  is a plain 404; **this is the hook point.**
- `harmonia-cache/src/store.rs`: `Store` opens the nix SQLite database
  **read-only**, one handle per actix worker thread (`thread_local!`). A path
  the daemon adds later is visible on the next query (WAL readers see new
  commits), so nothing needs reopening.
- `harmonia-cache/src/nar.rs`: NARs are streamed from the real store path.
  Once a path is valid it just works.
- `harmonia-cache/src/config.rs`: `Config` has `#[serde(deny_unknown_fields)]`,
  so new options must be declared there.
- `harmonia-store-remote/src/client.rs`: a nix-daemon protocol client
  (`DaemonClientBuilder::connect_unix` / `connect_daemon`) that already
  implements `ensure_path` (`Operation::EnsurePath`, which substitutes if
  needed) and `add_temp_root` (`Operation::AddTempRoot`).
- `harmonia-store-nar-info`: narinfo parsing and formatting, reusable for
  reading upstream narinfos.

## What's missing

1. **Resolving a hash part to a full store path via upstreams.** A client asks
   for `/<hash>.narinfo`; `EnsurePath` needs the full `StorePath`. The daemon
   can't do this for us: `QueryPathFromHashPart` only looks at the local
   database, and `QuerySubstitutablePathInfos` wants full paths. So harmonia
   must `GET <upstream>/<hash>.narinfo` itself and read `StorePath:`.
   - Needs an **HTTP client**, which `harmonia-cache` doesn't depend on today.
     Candidates: `awc` (actix's client, same runtime), `reqwest`, or a thin
     `hyper` client. Plus rustls, already a dependency.
   - Needs **netrc** support for authenticated upstreams, e.g. FlakeHub
     (`netrc_file`, and whether to follow nix.conf's `netrc-file`).
   - `nix store path-from-hash-part --store <upstream> <hash>` does exactly
     this and resolved a cache.nixos.org path in about 70 ms. Useful for a
     prototype, but upstream would want it native.
2. **Asking the daemon to substitute.** Add `harmonia-store-remote` as a
   dependency of `harmonia-cache`, connect to the daemon socket and call
   `ensure_path`. harmonia usually runs as an untrusted user; untrusted
   clients may still substitute from the daemon's configured substituters,
   with signatures checked, so no `trusted-users` change is needed.
3. **Keeping the path alive until it's served.** The narinfo and the NAR are
   separate requests, and a GC in between would delete an unrooted path.
   `add_temp_root` pins it for the lifetime of the daemon connection. That
   suggests a long-lived connection (or small pool) that holds temp roots for
   recently pulled paths, recycled every N minutes to release them. Temp
   roots are per connection, so this works even when harmonia runs in a
   container with the socket mounted in, where an indirect GC root would
   point at a path the host can't see.
4. **Config** (sketch):
   ```toml
   [pull_through]
   enable = true
   # Only used to resolve hash parts to store paths. The daemon's own
   # substituters do the downloading, so these should match them.
   upstreams = ["https://cache.nixos.org", "https://cache.flakehub.com"]
   netrc_file = "/nix/var/determinate/netrc"
   daemon_socket = "/nix/var/nix/daemon-socket/socket"
   negative_ttl = "5m"
   max_concurrent = 16
   temp_root_ttl = "10m"
   ```
5. **Behaviour under load.**
   - **Single-flight per hash.** Concurrent requests for one path share one
     resolve plus `EnsurePath`.
   - **Negative cache.** Clients query narinfo for many paths that exist
     nowhere, e.g. every build dependency. Without it, each 404 becomes an
     upstream round trip.
   - **A concurrency limit** on in-flight substitutions.
   - **Timeouts.** A large `EnsurePath` can take a while, so decide between
     holding the request and returning 404 with a hint (Nix will retry or
     fall back to its other substituters).
6. **Metrics.** Pull-through hits, misses and failures, and substitution
   duration. There's already a `prometheus.rs`.

## Questions upstream will ask

- **Abuse.** Pull-through lets anyone who can reach harmonia make the host
  download anything an upstream has. It must be off by default, with explicit
  upstreams, and probably documented as for trusted networks only.
- **Push/pull confusion** ([NixOS/nix#15249](https://github.com/NixOS/nix/issues/15249)):
  a pull-through cache can't tell an existence probe before a push from a
  pull. harmonia is read-only, so it's mostly moot; say so.
- **Upstream list vs. the daemon's substituters.** Can they drift? Reading the
  daemon's configured substituters through the protocol isn't possible, so
  they're configured twice. Worth a clear error when `EnsurePath` fails for a
  path an upstream claimed to have.
- **Scope.** Only paths with a narinfo upstream: substitution, never builds.

Prior art: niks3 added a pull-through read proxy
([Qumulo/niks3#1](https://github.com/Qumulo/niks3/pull/1)), which caches into
its own storage rather than through a daemon.

## Build artifacts

- `nix build .#harmonia` (also `packages.default`) gives
  `bin/harmonia-cache`, which is what knix runs.
- Rust iteration: `nix develop`, then `cargo test -p harmonia-cache` (and
  `-p harmonia-store-remote` for client changes).
- Flake checks (x86_64-linux): `basic`, `chroot-store`, `gc`,
  `ca-derivations`, `bench-closure`, `clippy`, `treefmt`, `tests`. NixOS VM
  tests need `/dev/kvm`; the WSL dev host has it.

## Test plan

1. **Unit tests** in `harmonia-cache`:
   - upstream narinfo → `StorePath` resolution, with a local HTTP fixture
   - the negative cache
   - single-flight (N concurrent misses → 1 resolve)
   - config parsing
2. **A NixOS test, `nix/tests/pull-through.nix`**, modelled on
   `nix/tests/basic.nix` (VM tests have no internet, so the upstream is a VM
   too):
   - `upstream`: serves a signed `file://` binary cache (e.g. `nix copy
     --to file://...` of `pkgs.hello` plus a path the harmonia node doesn't
     have) over nginx.
   - `harmonia`: its daemon has `substituters = [ "http://upstream" ]` and
     trusts that key; harmonia has `pull_through.upstreams = [
     "http://upstream" ]`; its store **lacks** the target path.
   - `client01`: `substituters = [ "http://harmonia:5000" ]`.
   - Assert: `nix copy --from http://harmonia:5000 <target>` succeeds; the
     path is now valid on `harmonia`; the narinfo carries the upstream
     signature; an unknown hash returns 404 and makes exactly one upstream
     request per `negative_ttl`.
   - A GC race: run `nix-collect-garbage` on `harmonia` between the narinfo
     and the NAR request, and assert the NAR is still served (temp roots).
3. **Integration in knix.** knix needs a small change first: `knix-cache-up`
   takes its harmonia from nixpkgs; add an override (e.g. `KNIX_HARMONIA` =
   a harmonia store path, such as `nix build ~/Code/github.com/glennpratt/harmonia#harmonia --print-out-paths`)
   and pass the `[pull_through]` config through, with `upstreams` from
   `nix config show substituters`. Then:
   - measure a cluster recreate (knix e2e) before and after: node fetches
     from cache.nixos.org should drop to zero on the second cluster;
   - confirm FlakeHub-authenticated paths resolve (netrc) and substitute
     (daemon);
   - confirm host `nix-collect-garbage` removes pulled paths and the next
     request pulls them again.

## Suggested order

1. Prototype in knix first, as a ~150-line proxy in front of stock harmonia
   using `nix store path-from-hash-part` and `nix-store --realise`, to get
   real latency and concurrency numbers.
2. Open an upstream issue with the use case, those numbers and this design,
   before writing the Rust.
3. Implement here: resolve (HTTP + netrc), `EnsurePath` plus temp roots,
   single-flight and the negative cache, config, and the NixOS test.
