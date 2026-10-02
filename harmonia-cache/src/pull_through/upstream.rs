//! Resolving a hash part to a full store path by asking upstream caches.
//!
//! The daemon can't do this: `QueryPathFromHashPart` only consults the local
//! database and `QuerySubstitutablePathInfos` wants full paths. So we fetch
//! `<upstream>/<hash>.narinfo` ourselves and read its `StorePath:` line.

use super::netrc::Netrc;
use super::{LocalBoxFuture, Resolve, Resolved};
use harmonia_store_path::{StoreDir, StorePath, StorePathHash};
use std::cell::OnceCell;
use std::time::Duration;

struct Upstream {
    base: String,
    authorization: Option<String>,
}

pub(crate) struct HttpResolver {
    upstreams: Vec<Upstream>,
    store_dir: StoreDir,
    timeout: Duration,
}

// awc::Client is !Send, so each actix worker thread builds its own. Keeping
// one per thread (rather than per request) reuses upstream connections.
thread_local! {
    static CLIENT: OnceCell<awc::Client> = const { OnceCell::new() };
}

impl HttpResolver {
    /// `upstreams` must already be validated as http(s) URLs.
    pub(crate) fn new(
        upstreams: &[String],
        netrc: Option<&Netrc>,
        store_dir: StoreDir,
        timeout: Duration,
    ) -> Self {
        let upstreams = upstreams
            .iter()
            .map(|u| {
                let host = url::Url::parse(u)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_owned));
                Upstream {
                    base: u.trim_end_matches('/').to_owned(),
                    authorization: netrc.zip(host).and_then(|(n, h)| n.authorization(&h)),
                }
            })
            .collect();
        Self {
            upstreams,
            store_dir,
            timeout,
        }
    }

    fn client(&self) -> awc::Client {
        CLIENT.with(|c| {
            c.get_or_init(|| {
                awc::Client::builder()
                    .timeout(self.timeout)
                    .add_default_header((
                        "User-Agent",
                        concat!("harmonia/", env!("CARGO_PKG_VERSION")),
                    ))
                    .finish()
            })
            .clone()
        })
    }

    async fn query(&self, upstream: &Upstream, hash: &StorePathHash) -> Resolved {
        let url = format!("{}/{hash}.narinfo", upstream.base);
        let mut req = self.client().get(&url);
        if let Some(auth) = &upstream.authorization {
            req = req.insert_header(("Authorization", auth.as_str()));
        }
        let mut res = match req.send().await {
            Ok(res) => res,
            Err(e) => return Resolved::Error(format!("GET {url}: {e}")),
        };
        let status = res.status();
        // Nix treats 403 like 404 for narinfo lookups (S3 answers 403 for
        // missing keys without list permission).
        if status == awc::http::StatusCode::NOT_FOUND || status == awc::http::StatusCode::FORBIDDEN
        {
            return Resolved::NotFound;
        }
        if !status.is_success() {
            return Resolved::Error(format!("GET {url}: HTTP {status}"));
        }
        let body = match res.body().limit(1 << 20).await {
            Ok(body) => body,
            Err(e) => return Resolved::Error(format!("GET {url}: {e}")),
        };
        match parse_store_path(&self.store_dir, &body, hash) {
            Ok(path) => Resolved::Found(path),
            Err(e) => Resolved::Error(format!("{url}: {e}")),
        }
    }
}

impl Resolve for HttpResolver {
    fn resolve<'a>(&'a self, hash: &'a StorePathHash) -> LocalBoxFuture<'a, Resolved> {
        Box::pin(async move {
            let mut errors = Vec::new();
            for upstream in &self.upstreams {
                match self.query(upstream, hash).await {
                    found @ Resolved::Found(_) => return found,
                    Resolved::NotFound => {}
                    Resolved::Error(e) => errors.push(e),
                }
            }
            if errors.is_empty() {
                Resolved::NotFound
            } else {
                Resolved::Error(errors.join("; "))
            }
        })
    }
}

/// Extract the `StorePath:` of a narinfo and check it's the path we asked for.
pub(crate) fn parse_store_path(
    store_dir: &StoreDir,
    narinfo: &[u8],
    hash: &StorePathHash,
) -> Result<StorePath, String> {
    let text = std::str::from_utf8(narinfo).map_err(|_| "narinfo is not UTF-8".to_owned())?;
    let value = text
        .lines()
        .find_map(|l| l.strip_prefix("StorePath:"))
        .ok_or_else(|| "narinfo has no StorePath".to_owned())?
        .trim();
    let path: StorePath = store_dir
        .parse(value)
        .map_err(|e| format!("bad StorePath '{value}': {e}"))?;
    // A misbehaving upstream must not be able to make us pull (and then serve
    // under this hash) some other path.
    if path.hash() != hash {
        return Err(format!("StorePath '{value}' does not match hash {hash}"));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const HASH: &str = "7rjj86a15146cq1d3qy068lml7n8ykzm";
    const PATH: &str = "/nix/store/7rjj86a15146cq1d3qy068lml7n8ykzm-hello-2.12.2";

    fn hash() -> StorePathHash {
        HASH.parse().unwrap()
    }

    fn narinfo(path: &str) -> String {
        format!(
            "StorePath: {path}\nURL: nar/x.nar.xz\nCompression: xz\nNarHash: sha256:abc\nNarSize: 1\n"
        )
    }

    #[test]
    fn parses_store_path() {
        let dir = StoreDir::default();
        let p = parse_store_path(&dir, narinfo(PATH).as_bytes(), &hash()).unwrap();
        assert_eq!(
            p.to_string(),
            "7rjj86a15146cq1d3qy068lml7n8ykzm-hello-2.12.2"
        );
    }

    #[test]
    fn rejects_mismatch_and_garbage() {
        let dir = StoreDir::default();
        let other = "/nix/store/00000000000000000000000000000000-evil";
        assert!(parse_store_path(&dir, narinfo(other).as_bytes(), &hash()).is_err());
        assert!(parse_store_path(&dir, b"URL: x\n", &hash()).is_err());
        assert!(parse_store_path(&dir, b"\xff\xfe", &hash()).is_err());
        // Different store dir.
        let foreign = format!("/gnu/store/{HASH}-hello");
        assert!(parse_store_path(&dir, narinfo(&foreign).as_bytes(), &hash()).is_err());
    }

    /// A local HTTP fixture: `routes` maps request paths to (status, body).
    /// Returns its base URL, a hit counter and the last Authorization header.
    async fn fixture(
        routes: Vec<(&'static str, u16, String)>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Option<String>>>,
    ) {
        use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
        let hits = Arc::new(AtomicUsize::new(0));
        let auth = Arc::new(std::sync::Mutex::new(None));
        let routes = Arc::new(routes);
        let (h, a) = (hits.clone(), auth.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = HttpServer::new(move || {
            let (routes, h, a) = (routes.clone(), h.clone(), a.clone());
            App::new().default_service(web::to(move |req: HttpRequest| {
                let (routes, h, a) = (routes.clone(), h.clone(), a.clone());
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    *a.lock().unwrap() = req
                        .headers()
                        .get("Authorization")
                        .map(|v| v.to_str().unwrap().to_owned());
                    match routes.iter().find(|(p, _, _)| *p == req.path()) {
                        Some((_, status, body)) => HttpResponse::build(
                            actix_web::http::StatusCode::from_u16(*status).unwrap(),
                        )
                        .body(body.clone()),
                        None => HttpResponse::NotFound().finish(),
                    }
                }
            }))
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        actix_web::rt::spawn(server);
        (format!("http://{addr}"), hits, auth)
    }

    fn resolver(upstreams: &[String], netrc: Option<&Netrc>) -> HttpResolver {
        HttpResolver::new(
            upstreams,
            netrc,
            StoreDir::default(),
            Duration::from_secs(5),
        )
    }

    #[actix_web::test]
    async fn resolves_from_first_upstream_that_has_it() {
        let route = Box::leak(format!("/{HASH}.narinfo").into_boxed_str());
        let (miss, miss_hits, _) = fixture(vec![]).await;
        let (hit, hit_hits, auth) = fixture(vec![(route, 200, narinfo(PATH))]).await;
        let netrc = Netrc::parse("machine 127.0.0.1 login u password p\n");
        let r = resolver(&[miss, format!("{hit}/")], Some(&netrc));
        match r.resolve(&hash()).await {
            Resolved::Found(p) => assert_eq!(p.hash(), &hash()),
            other => panic!("expected Found, got {other:?}"),
        }
        assert_eq!(miss_hits.load(Ordering::SeqCst), 1);
        assert_eq!(hit_hits.load(Ordering::SeqCst), 1);
        assert_eq!(auth.lock().unwrap().as_deref(), Some("Basic dTpw"));
    }

    #[actix_web::test]
    async fn not_found_vs_error() {
        let route = Box::leak(format!("/{HASH}.narinfo").into_boxed_str());
        let (miss, _, _) = fixture(vec![(route, 403, String::new())]).await;
        let (broken, _, _) = fixture(vec![(route, 500, String::new())]).await;
        let r = resolver(std::slice::from_ref(&miss), None);
        assert!(matches!(r.resolve(&hash()).await, Resolved::NotFound));
        let r = resolver(&[miss, broken], None);
        assert!(matches!(r.resolve(&hash()).await, Resolved::Error(_)));
        // Nothing listening.
        let r = resolver(&["http://127.0.0.1:1".to_owned()], None);
        assert!(matches!(r.resolve(&hash()).await, Resolved::Error(_)));
    }

    #[actix_web::test]
    async fn wrong_path_is_an_error() {
        let route = Box::leak(format!("/{HASH}.narinfo").into_boxed_str());
        let evil = narinfo("/nix/store/00000000000000000000000000000000-evil");
        let (base, _, _) = fixture(vec![(route, 200, evil)]).await;
        let r = resolver(&[base], None);
        assert!(matches!(r.resolve(&hash()).await, Resolved::Error(_)));
    }
}
