use axum::{
    extract::{State, WebSocketUpgrade, ws::Message},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use kokage::observe::{LifecycleEvent, ScopedActorStats, SupervisorSnapshot};
use tokio::time::{self, Duration};

use crate::server::AppState;

type WebSocket = axum::extract::ws::WebSocket;

pub(crate) async fn handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Response {
    if !origin_matches_host(&headers) {
        return (StatusCode::FORBIDDEN, "websocket origin not allowed").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

fn origin_matches_host(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        // Non-browser WebSocket clients generally omit Origin and cannot mount
        // a browser-based cross-site WebSocket attack.
        return true;
    };
    let (Ok(origin), Some(host)) = (
        origin.to_str(),
        headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok()),
    ) else {
        return false;
    };
    let Ok(uri) = origin.parse::<axum::http::Uri>() else {
        return false;
    };
    matches!(uri.scheme_str(), Some("http" | "https"))
        && uri
            .authority()
            .is_some_and(|authority| authority.as_str().eq_ignore_ascii_case(host))
}

fn snapshot_message(snapshot: SupervisorSnapshot) -> Message {
    Message::Text(
        serde_json::json!({ "type": "snapshot", "data": snapshot })
            .to_string()
            .into(),
    )
}

fn event_message(event: LifecycleEvent) -> Message {
    Message::Text(
        serde_json::json!({ "type": "event", "data": event })
            .to_string()
            .into(),
    )
}

fn stats_message(stats: &[ScopedActorStats]) -> Message {
    Message::Text(
        serde_json::json!({ "type": "actor_stats", "data": stats })
            .to_string()
            .into(),
    )
}

async fn send_snapshot(socket: &mut WebSocket, snapshot: SupervisorSnapshot) -> bool {
    socket.send(snapshot_message(snapshot)).await.is_ok()
}

async fn send_event(socket: &mut WebSocket, event: LifecycleEvent) -> bool {
    socket.send(event_message(event)).await.is_ok()
}

async fn send_stats(
    socket: &mut WebSocket,
    state: &AppState,
    last_sent: &mut Vec<ScopedActorStats>,
) -> bool {
    let stats = (state.stats)();
    if stats == *last_sent {
        return true;
    }

    if socket.send(stats_message(&stats)).await.is_err() {
        return false;
    }
    *last_sent = stats;
    true
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut snapshots = state.snapshots.clone();
    let mut lifecycle = (state.lifecycle)();

    // Send current snapshot immediately on connect.
    if !send_snapshot(&mut socket, snapshots.latest()).await {
        return;
    }
    let mut last_sent_stats = (state.stats)();
    if socket.send(stats_message(&last_sent_stats)).await.is_err() {
        return;
    }

    let mut stats_tick = time::interval(Duration::from_secs(1));
    stats_tick.tick().await;

    loop {
        tokio::select! {
            result = snapshots.changed() => {
                let Ok(snapshot) = result else {
                    break;
                };
                if !send_snapshot(&mut socket, snapshot).await {
                    break;
                }
            }
            event = lifecycle.next() => {
                match event {
                    Some(event) => {
                        if !send_event(&mut socket, event).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = stats_tick.tick() => {
                if !send_stats(&mut socket, &state, &mut last_sent_stats).await {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}
