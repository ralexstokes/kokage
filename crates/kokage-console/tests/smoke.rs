use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use kokage::{
    Actor, ActorSpec, Context, DynamicTree, ExitResult, RunningTree, TaskSpec, observe::ActorStats,
};
use kokage_console::{ConsoleBuilder, ConsoleError, ConsoleHandle};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message,
        client::IntoClientRequest,
        http::{HeaderValue, StatusCode, header},
    },
};

type TestWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone)]
struct IdleActor;

impl Actor for IdleActor {
    type Msg = ();

    async fn handle(&mut self, _message: (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn actor_stats() -> Vec<ActorStats> {
    vec![
        serde_json::from_value(json!({
            "actor_id": "worker",
            "scope_path": [],
            "lineage": 0,
            "messages_received": 11,
            "messages_accepted": 10,
            "messages_conflated": 3,
            "sends_rejected": 1,
            "outstanding_offloads": 0,
            "mailbox_depth": 3,
            "mailbox_capacity": 32,
        }))
        .expect("actor stats fixture is valid"),
    ]
}

async fn spawn_console_with_stats(
    stats: impl Fn() -> Vec<ActorStats> + Send + Sync + 'static,
) -> (ConsoleHandle, RunningTree, RunningTree) {
    let snapshots = DynamicTree::new()
        .spawn()
        .expect("test snapshot tree spawns");
    let snapshots_handle = snapshots.scope();
    snapshots_handle
        .add_task(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("test snapshot child is added");
    let lifecycle = DynamicTree::new()
        .spawn()
        .expect("test lifecycle tree spawns");
    let lifecycle_source = lifecycle.scope();
    let handle = ConsoleBuilder::new()
        .snapshots(snapshots_handle.subscribe_snapshots())
        .lifecycle(move || lifecycle_source.watch_lifecycle())
        .actor_stats(stats)
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await
        .expect("failed to spawn console");

    (handle, snapshots, lifecycle)
}

async fn spawn_console() -> (ConsoleHandle, RunningTree, RunningTree) {
    spawn_console_with_stats(actor_stats).await
}

async fn connect(addr: SocketAddr) -> TestWebSocket {
    connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("failed to connect websocket")
        .0
}

async fn http_get(addr: SocketAddr, host: &str, path: &str, extra_headers: &str) -> String {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("failed to connect to console");
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n{extra_headers}Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("failed to send HTTP request");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("failed to read HTTP response");
    String::from_utf8(response).expect("HTTP response was not UTF-8")
}

async fn read_json(socket: &mut TestWebSocket) -> Value {
    let message = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("timed out waiting for websocket frame")
        .expect("websocket closed before the expected frame")
        .expect("failed to read websocket frame");
    let Message::Text(text) = message else {
        panic!("expected a text websocket frame, got {message:?}");
    };

    serde_json::from_str(&text).expect("websocket frame was not valid JSON")
}

async fn read_non_stats_json(socket: &mut TestWebSocket) -> Value {
    loop {
        let frame = read_json(socket).await;
        if frame["type"] != "actor_stats" {
            return frame;
        }
    }
}

async fn read_handshake(socket: &mut TestWebSocket) {
    let snapshot = read_json(socket).await;
    assert_eq!(snapshot["type"], "snapshot");
    let stats = read_json(socket).await;
    assert_eq!(stats["type"], "actor_stats");
}

#[tokio::test]
async fn index_serves_dashboard() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let response = http_get(
        handle.local_addr(),
        &format!("localhost:{}", handle.local_addr().port()),
        "/",
        "",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"));
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response did not contain a header/body separator");
    assert!(headers.to_ascii_lowercase().contains("text/html"));
    assert!(body.contains("kokage console"));
}

#[tokio::test]
async fn rejects_unrecognized_host() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let response = http_get(handle.local_addr(), "attacker.example", "/", "").await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
}

#[tokio::test]
async fn rejects_cross_origin_websocket() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let mut request = format!("ws://{}/ws", handle.local_addr())
        .into_client_request()
        .expect("failed to build websocket request");
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_static("https://attacker.example"),
    );

    let error = connect_async(request)
        .await
        .expect_err("cross-origin websocket unexpectedly connected");
    let WebSocketError::Http(response) = error else {
        panic!("expected HTTP rejection, got {error}");
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn accepts_matching_browser_websocket_origin() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let mut request = format!("ws://{}/ws", handle.local_addr())
        .into_client_request()
        .expect("failed to build websocket request");
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_str(&format!("http://{}", handle.local_addr()))
            .expect("local address produced invalid Origin"),
    );

    let (mut socket, _) = connect_async(request)
        .await
        .expect("matching-origin websocket was rejected");
    read_handshake(&mut socket).await;
}

#[tokio::test]
async fn token_bootstrap_sets_cookie_and_authorization_is_accepted() {
    let snapshots = DynamicTree::new()
        .spawn()
        .expect("test snapshot tree spawns");
    let lifecycle = DynamicTree::new()
        .spawn()
        .expect("test lifecycle tree spawns");
    let snapshots_handle = snapshots.scope();
    let lifecycle_source = lifecycle.scope();
    let handle = ConsoleBuilder::new()
        .snapshots(snapshots_handle.subscribe_snapshots())
        .lifecycle(move || lifecycle_source.watch_lifecycle())
        .access_token("test-token")
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await
        .expect("failed to spawn token-protected console");
    let host = handle.local_addr().to_string();

    let unauthorized = http_get(handle.local_addr(), &host, "/", "").await;
    assert!(unauthorized.starts_with("HTTP/1.1 401"), "{unauthorized}");
    assert!(
        unauthorized
            .to_ascii_lowercase()
            .contains("www-authenticate: bearer")
    );

    let wrong_token = http_get(handle.local_addr(), &host, "/?token=wrong", "").await;
    assert!(wrong_token.starts_with("HTTP/1.1 401"), "{wrong_token}");
    assert!(!wrong_token.to_ascii_lowercase().contains("set-cookie:"));

    let bootstrap = http_get(handle.local_addr(), &host, "/?token=test-token", "").await;
    assert!(bootstrap.starts_with("HTTP/1.1 303"), "{bootstrap}");
    let cookie = bootstrap
        .lines()
        .find_map(|line| line.strip_prefix("set-cookie: "))
        .and_then(|value| value.split_once(';').map(|(cookie, _)| cookie))
        .expect("bootstrap response did not set a session cookie");
    assert!(cookie.starts_with("kokage_console_session_"));
    assert!(!cookie.ends_with("=test-token"));
    assert!(bootstrap.to_ascii_lowercase().contains("samesite=lax"));
    assert!(bootstrap.to_ascii_lowercase().contains("location: /"));

    let cookie_authorized = http_get(
        handle.local_addr(),
        &host,
        "/",
        &format!("Cookie: {cookie}\r\n"),
    )
    .await;
    assert!(
        cookie_authorized.starts_with("HTTP/1.1 200"),
        "{cookie_authorized}"
    );

    let authorized = http_get(
        handle.local_addr(),
        &host,
        "/",
        "Authorization: bearer test-token\r\n",
    )
    .await;
    assert!(authorized.starts_with("HTTP/1.1 200"), "{authorized}");

    let ws_query_request = format!("ws://{}/ws?token=test-token", handle.local_addr())
        .into_client_request()
        .expect("failed to build websocket request");
    let error = connect_async(ws_query_request)
        .await
        .expect_err("websocket query token unexpectedly authenticated");
    let WebSocketError::Http(response) = error else {
        panic!("expected HTTP rejection, got {error}");
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let mut ws_cookie_request = format!("ws://{}/ws", handle.local_addr())
        .into_client_request()
        .expect("failed to build websocket request");
    ws_cookie_request.headers_mut().insert(
        header::COOKIE,
        HeaderValue::from_str(cookie).expect("session cookie was not a valid header value"),
    );
    let (mut socket, _) = connect_async(ws_cookie_request)
        .await
        .expect("session-authenticated websocket was rejected");
    read_handshake(&mut socket).await;
}

#[tokio::test]
async fn explicit_host_allowlist_accepts_external_and_default_port_forms() {
    let snapshots = DynamicTree::new()
        .spawn()
        .expect("test snapshot tree spawns");
    let lifecycle = DynamicTree::new()
        .spawn()
        .expect("test lifecycle tree spawns");
    let snapshots_handle = snapshots.scope();
    let lifecycle_source = lifecycle.scope();
    let handle = ConsoleBuilder::new()
        .snapshots(snapshots_handle.subscribe_snapshots())
        .lifecycle(move || lifecycle_source.watch_lifecycle())
        .allowed_host("console.example:80")
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await
        .expect("failed to spawn allowlisted console");

    let response = http_get(handle.local_addr(), "console.example", "/", "").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

#[tokio::test]
async fn non_loopback_bind_requires_token() {
    let snapshots = DynamicTree::new();
    let snapshot_rx = snapshots.scope().subscribe_snapshots();
    let lifecycle = DynamicTree::new();
    let lifecycle_handle = lifecycle.scope();
    let error = ConsoleBuilder::new()
        .snapshots(snapshot_rx)
        .lifecycle(move || lifecycle_handle.watch_lifecycle())
        .bind(([0, 0, 0, 0], 9100))
        .spawn()
        .await
        .err()
        .expect("non-loopback bind must require a token");
    assert!(matches!(error, ConsoleError::AccessTokenRequired));
}

#[tokio::test]
async fn builder_reports_missing_observability_sources() {
    let missing_snapshots = ConsoleBuilder::new()
        .spawn()
        .await
        .err()
        .expect("snapshots must be required");
    assert!(matches!(missing_snapshots, ConsoleError::MissingSnapshots));

    let snapshots = DynamicTree::new();
    let snapshot_rx = snapshots.scope().subscribe_snapshots();
    let missing_lifecycle = ConsoleBuilder::new()
        .snapshots(snapshot_rx)
        .spawn()
        .await
        .err()
        .expect("lifecycle source must be required");
    assert!(matches!(missing_lifecycle, ConsoleError::MissingLifecycle));
}

#[tokio::test]
async fn builder_rejects_invalid_access_tokens() {
    for token in ["", "contains a space", "not-ascii-é"] {
        let error = ConsoleBuilder::new()
            .access_token(token)
            .spawn()
            .await
            .err()
            .expect("invalid token must be rejected");
        assert!(matches!(error, ConsoleError::InvalidAccessToken));
    }
}

#[tokio::test]
async fn ws_sends_snapshot_then_stats_on_connect() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let mut socket = connect(handle.local_addr()).await;

    let snapshot = read_json(&mut socket).await;
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["data"]["children"][0]["id"], "worker");

    let stats = read_json(&mut socket).await;
    assert_eq!(stats["type"], "actor_stats");
    assert_eq!(
        stats["data"],
        json!([{
            "actor_id": "worker",
            "scope_path": [],
            "lineage": 0,
            "messages_received": 11,
            "messages_accepted": 10,
            "messages_conflated": 3,
            "sends_rejected": 1,
            "outstanding_offloads": 0,
            "mailbox_depth": 3,
            "mailbox_capacity": 32,
        }])
    );
}

#[tokio::test]
async fn ws_skips_unchanged_stats() {
    let stats = Arc::new(Mutex::new(actor_stats()));
    let stats_source = Arc::clone(&stats);
    let (handle, _snapshot_tx, _event_tx) = spawn_console_with_stats(move || {
        stats_source
            .lock()
            .expect("actor stats mutex poisoned")
            .clone()
    })
    .await;
    let mut socket = connect(handle.local_addr()).await;
    read_handshake(&mut socket).await;

    let unchanged = timeout(Duration::from_millis(2500), socket.next()).await;
    assert!(
        unchanged.is_err(),
        "received an unexpected frame for unchanged actor stats: {unchanged:?}"
    );

    stats
        .lock()
        .expect("actor stats mutex poisoned")
        .first_mut()
        .expect("actor stats fixture was empty")
        .mailbox_depth += 1;

    let frame = read_json(&mut socket).await;
    assert_eq!(frame["type"], "actor_stats");
    assert_eq!(frame["data"][0]["mailbox_depth"], 4);
}

#[tokio::test]
async fn ws_streams_snapshot_updates() {
    let (handle, snapshots, _event_tx) = spawn_console().await;
    let mut socket = connect(handle.local_addr()).await;
    read_handshake(&mut socket).await;

    snapshots
        .scope()
        .add_task(TaskSpec::new("updated", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("failed to add snapshot child");

    let frame = loop {
        let frame = read_non_stats_json(&mut socket).await;
        if frame["type"] == "snapshot"
            && frame["data"]["children"]
                .as_array()
                .is_some_and(|children| children.iter().any(|child| child["id"] == "updated"))
        {
            break frame;
        }
    };
    assert_eq!(frame["type"], "snapshot");
    assert!(
        frame["data"]["children"]
            .as_array()
            .expect("snapshot children are an array")
            .iter()
            .any(|child| child["id"] == json!("updated"))
    );
}

#[tokio::test]
async fn ws_streams_events() {
    let (handle, _snapshot_tx, lifecycle) = spawn_console().await;
    let mut socket = connect(handle.local_addr()).await;
    read_handshake(&mut socket).await;

    lifecycle
        .scope()
        .add_task(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("failed to add lifecycle child");

    let _added = read_non_stats_json(&mut socket).await;
    let frame = read_non_stats_json(&mut socket).await;
    assert_eq!(frame["type"], "event");
    assert_eq!(frame["data"]["scope_path"], json!([]));
    assert_eq!(frame["data"]["kind"]["ChildStarted"]["child_id"], "worker");
    assert_eq!(frame["data"]["kind"]["ChildStarted"]["generation"], 0);
}

#[tokio::test]
async fn dynamic_tree_wires_public_observability() {
    let runtime = DynamicTree::new()
        .spawn()
        .expect("failed to spawn empty runtime");
    let console = ConsoleBuilder::for_runtime(&runtime.scope())
        .bind(([127, 0, 0, 1], 0))
        .spawn()
        .await
        .expect("failed to spawn console");
    let mut socket = connect(console.local_addr()).await;

    let snapshot = read_json(&mut socket).await;
    assert_eq!(snapshot["type"], "snapshot");
    let stats = read_json(&mut socket).await;
    assert_eq!(stats, json!({ "type": "actor_stats", "data": [] }));

    runtime
        .scope()
        .add_task(TaskSpec::new("worker", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("failed to add runtime child");

    runtime
        .scope()
        .add_actor(ActorSpec::new("tracked", || IdleActor))
        .await
        .expect("failed to add runtime actor");

    let mut saw_event = false;
    let mut saw_actor_stats = false;
    while !saw_event || !saw_actor_stats {
        let frame = read_json(&mut socket).await;
        match frame["type"].as_str() {
            Some("event") => saw_event = true,
            Some("actor_stats")
                if frame["data"]
                    .as_array()
                    .is_some_and(|stats| !stats.is_empty()) =>
            {
                assert_eq!(frame["data"][0]["actor_id"], "tracked");
                saw_actor_stats = true;
            }
            _ => {}
        }
    }

    console.shutdown();
    runtime
        .shutdown_and_wait()
        .await
        .expect("failed to stop runtime");
}

#[tokio::test]
async fn shutdown_stops_server() {
    let (handle, _snapshot_tx, _event_tx) = spawn_console().await;
    let addr = handle.local_addr();
    handle
        .shutdown_and_wait()
        .await
        .expect("console server exits cleanly");

    timeout(Duration::from_secs(2), async {
        while let Ok(stream) = TcpStream::connect(addr).await {
            drop(stream);
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("console still accepted TCP connections after shutdown");
}

#[tokio::test]
async fn dropping_the_handle_detaches_without_stopping_the_server() {
    let (handle, snapshots, lifecycle) = spawn_console().await;
    let addr = handle.local_addr();
    drop(handle);

    let response = timeout(
        Duration::from_secs(2),
        http_get(addr, &addr.to_string(), "/", ""),
    )
    .await
    .expect("detached console remains reachable");
    assert!(response.starts_with("HTTP/1.1 200"));

    snapshots.shutdown();
    lifecycle.shutdown();
}
