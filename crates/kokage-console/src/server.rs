use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use kokage_supervisor::SupervisorSnapshot;
use tokio::sync::watch;

use crate::{ConsoleHandle, LifecycleSource, StatsSource, ws};

const INDEX_HTML: &str = include_str!("../assets/index.html");

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) snapshots: watch::Receiver<SupervisorSnapshot>,
    pub(crate) lifecycle: LifecycleSource,
    pub(crate) stats: StatsSource,
}

#[derive(Clone)]
struct SecurityState {
    allowed_hosts: Arc<HashSet<String>>,
    access_token: Option<Arc<str>>,
    session: Option<Session>,
}

#[derive(Clone)]
struct Session {
    cookie_name: Arc<str>,
    value: Arc<str>,
}

fn insert_authority(hosts: &mut HashSet<String>, authority: impl Into<String>) {
    let authority = authority.into().to_ascii_lowercase();
    if let Some(host) = authority
        .strip_suffix(":80")
        .or_else(|| authority.strip_suffix(":443"))
    {
        hosts.insert(host.to_owned());
    }
    hosts.insert(authority);
}

fn request_authority(request: &Request<Body>) -> Option<&str> {
    request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(axum::http::uri::Authority::as_str)
        })
}

fn host_allowed(request: &Request<Body>, state: &SecurityState) -> bool {
    request_authority(request)
        .is_some_and(|host| state.allowed_hosts.contains(&host.to_ascii_lowercase()))
}

fn token_from_query(request: &Request<Body>) -> Option<&str> {
    request.uri().query()?.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == "token").then_some(value)
    })
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .and_then(|(scheme, token)| scheme.eq_ignore_ascii_case("Bearer").then_some(token))
}

fn cookie_value<'a>(headers: &'a HeaderMap, cookie_name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;
                (name == cookie_name).then_some(value)
            })
        })
}

fn new_session(port: u16) -> std::io::Result<Session> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(Session {
        cookie_name: Arc::from(format!("kokage_console_session_{port}")),
        value: Arc::from(value),
    })
}

fn token_matches(candidate: Option<&str>, expected: &str) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };
    let length = candidate.len().max(expected.len());
    let difference = (0..length).fold(candidate.len() ^ expected.len(), |difference, index| {
        difference
            | usize::from(
                candidate.as_bytes().get(index).copied().unwrap_or(0)
                    ^ expected.as_bytes().get(index).copied().unwrap_or(0),
            )
    });
    difference == 0
}

async fn security(
    axum::extract::State(state): axum::extract::State<SecurityState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !host_allowed(&request, &state) {
        return (StatusCode::FORBIDDEN, "host not allowed").into_response();
    }

    let Some(expected) = state.access_token.as_deref() else {
        return next.run(request).await;
    };

    if request.method() == Method::GET
        && request.uri().path() == "/"
        && token_matches(token_from_query(&request), expected)
    {
        let mut response = (StatusCode::SEE_OTHER, [(header::LOCATION, "/")]).into_response();
        let Some(session) = state.session.as_ref() else {
            return next.run(request).await;
        };
        let cookie = format!(
            "{}={}; HttpOnly; SameSite=Lax; Path=/",
            session.cookie_name, session.value
        );
        let Ok(cookie) = HeaderValue::from_str(&cookie) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        response.headers_mut().insert(header::SET_COOKIE, cookie);
        response.headers_mut().insert(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        );
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }

    let bearer_matches = token_matches(bearer_token(request.headers()), expected);
    let session_matches = state.session.as_ref().is_some_and(|session| {
        token_matches(
            cookie_value(request.headers(), &session.cookie_name),
            &session.value,
        )
    });
    if !bearer_matches && !session_matches {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "valid bearer token required",
        )
            .into_response();
    }

    next.run(request).await
}

fn router(state: AppState, security_state: SecurityState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/ws", get(ws::handler))
        .with_state(state)
        .layer(middleware::from_fn_with_state(security_state, security))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn bind_app(
    snapshots: watch::Receiver<SupervisorSnapshot>,
    lifecycle: LifecycleSource,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
) -> std::io::Result<(tokio::net::TcpListener, Router, SocketAddr)> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let local_addr = listener.local_addr()?;
    if local_addr.ip().is_unspecified() && allowed_hosts.is_empty() {
        tracing::warn!(
            %local_addr,
            "wildcard console bind requires allowed_host for normal client access"
        );
    }
    let mut hosts = HashSet::new();
    for host in allowed_hosts {
        insert_authority(&mut hosts, host);
    }
    insert_authority(&mut hosts, local_addr.to_string());
    if local_addr.ip().is_loopback() {
        insert_authority(&mut hosts, format!("localhost:{}", local_addr.port()));
    }
    let session = access_token
        .as_ref()
        .map(|_| new_session(local_addr.port()))
        .transpose()?;
    let app = router(
        AppState {
            snapshots,
            lifecycle,
            stats,
        },
        SecurityState {
            allowed_hosts: Arc::new(hosts),
            access_token: access_token.map(Arc::from),
            session,
        },
    );
    Ok((listener, app, local_addr))
}

async fn shutdown_signal(mut shutdown_rx: watch::Receiver<bool>) {
    let _ = shutdown_rx.wait_for(|&shutdown| shutdown).await;
}

pub(crate) async fn spawn(
    snapshots: watch::Receiver<SupervisorSnapshot>,
    lifecycle: LifecycleSource,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
) -> std::io::Result<ConsoleHandle> {
    let (listener, app, local_addr) = bind_app(
        snapshots,
        lifecycle,
        stats,
        bind,
        access_token,
        allowed_hosts,
    )
    .await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tracing::info!(%local_addr, "kokage-console listening");

    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(shutdown_rx))
            .await
        {
            tracing::warn!(%error, "kokage-console server stopped with error");
        }
    });

    Ok(ConsoleHandle {
        shutdown_tx,
        local_addr,
    })
}

pub(crate) async fn run(
    snapshots: watch::Receiver<SupervisorSnapshot>,
    lifecycle: LifecycleSource,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
) -> std::io::Result<()> {
    let (listener, app, local_addr) = bind_app(
        snapshots,
        lifecycle,
        stats,
        bind,
        access_token,
        allowed_hosts,
    )
    .await?;

    tracing::info!(%local_addr, "kokage-console listening");

    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_authority_falls_back_to_http2_uri_authority() {
        let request = Request::builder()
            .uri("http://console.example:9100/")
            .body(Body::empty())
            .expect("failed to build request");
        assert_eq!(request_authority(&request), Some("console.example:9100"));
    }
}
