//! Web-based dashboard for visualizing live `tokio-otp` supervisor state.
//!
//! `tokio-otp-console` hosts an axum web server with WebSocket streaming and
//! an embedded single-file HTML/JS/CSS frontend. It renders supervision trees,
//! child states, events, and summary stats in real time.
//!
//! # Usage
//!
//! ```no_run
//! use tokio_otp::{Runtime, prelude::*};
//! use tokio_otp_console::Console;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let handle = Runtime::builder().build()?.spawn();
//! let console = Console::for_runtime(&handle)
//!     .build()
//!     .spawn()
//!     .await
//!     .expect("failed to start console");
//!
//! println!("Console at http://{}", console.local_addr());
//! # handle.shutdown_and_wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Security
//!
//! The token-free default is restricted to loopback. The server validates
//! every request's `Host` and rejects browser WebSocket connections whose
//! `Origin` does not match that host. Non-loopback binds require an access
//! token. Console snapshots and events are operationally sensitive: child
//! identifiers and failed-exit strings may contain application details.

mod server;
mod ws;

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::{broadcast, watch};
use tokio_otp::{ActorStats, RuntimeHandle};
use tokio_supervisor::{SupervisorEvent, SupervisorSnapshot};

/// Display-oriented snapshot of one actor's message and mailbox statistics.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ActorStatsView {
    pub actor_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supervisor_path: Option<Vec<SupervisorPathSegmentView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub membership_epoch: Option<u64>,
    pub messages_received: u64,
    pub messages_accepted: u64,
    pub messages_conflated: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_bytes_accepted: Option<u64>,
    pub sends_rejected: u64,
    pub outstanding_offloads: u64,
    pub mailbox_depth: usize,
    pub mailbox_capacity: usize,
}

/// Identity of one nested supervisor containing an actor stats sample.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SupervisorPathSegmentView {
    pub id: String,
    pub membership_epoch: u64,
    pub generation: u64,
}

impl From<ActorStats> for ActorStatsView {
    fn from(stats: ActorStats) -> Self {
        Self {
            actor_id: stats.actor_id,
            supervisor_path: stats.supervisor_path.map(|path| {
                path.into_iter()
                    .map(|segment| SupervisorPathSegmentView {
                        id: segment.id,
                        membership_epoch: segment.membership_epoch,
                        generation: segment.generation,
                    })
                    .collect()
            }),
            membership_epoch: stats.membership_epoch,
            messages_received: stats.messages_received,
            messages_accepted: stats.messages_accepted,
            messages_conflated: stats.messages_conflated,
            message_bytes_accepted: stats.message_bytes_accepted,
            sends_rejected: stats.sends_rejected,
            outstanding_offloads: stats.outstanding_offloads,
            mailbox_depth: stats.mailbox_depth,
            mailbox_capacity: stats.mailbox_capacity,
        }
    }
}

type StatsSource = Arc<dyn Fn() -> Vec<ActorStatsView> + Send + Sync>;
type EventSource = Arc<dyn Fn() -> broadcast::Receiver<SupervisorEvent> + Send + Sync>;

/// Builder for configuring a [`Console`] server.
pub struct ConsoleBuilder {
    snapshots: Option<watch::Receiver<SupervisorSnapshot>>,
    events: Option<EventSource>,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
}

impl ConsoleBuilder {
    fn new() -> Self {
        Self {
            snapshots: None,
            events: None,
            stats: Arc::new(Vec::new),
            bind: ([127, 0, 0, 1], 9100).into(),
            access_token: None,
            allowed_hosts: Vec::new(),
        }
    }

    /// Sets the snapshot watch receiver.
    pub fn snapshots(mut self, rx: watch::Receiver<SupervisorSnapshot>) -> Self {
        self.snapshots = Some(rx);
        self
    }

    /// Sets the event broadcast sender. Each WebSocket connection will call
    /// `subscribe()` to get its own receiver.
    pub fn events(mut self, tx: broadcast::Sender<SupervisorEvent>) -> Self {
        self.events = Some(Arc::new(move || tx.subscribe()));
        self
    }

    fn event_source(
        mut self,
        source: impl Fn() -> broadcast::Receiver<SupervisorEvent> + Send + Sync + 'static,
    ) -> Self {
        self.events = Some(Arc::new(source));
        self
    }

    /// Sets the pull source sampled for per-actor stats.
    pub fn actor_stats(
        mut self,
        source: impl Fn() -> Vec<ActorStatsView> + Send + Sync + 'static,
    ) -> Self {
        self.stats = Arc::new(source);
        self
    }

    /// Sets the bind address. Defaults to `127.0.0.1:9100`.
    pub fn bind(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.bind = addr.into();
        self
    }

    /// Requires this bearer token for HTTP and WebSocket access.
    ///
    /// A token is required when binding to a non-loopback address. Browser
    /// users can establish an HTTP-only session cookie by opening
    /// `http://HOST/?token=TOKEN`; the token is removed from the URL by an
    /// immediate redirect. Tokens must contain only URL-safe ASCII characters.
    pub fn access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(token.into());
        self
    }

    /// Allows an additional HTTP `Host` authority (for example,
    /// `console.example.test:9100`).
    ///
    /// The listener address is always allowed. Loopback listeners also allow
    /// `localhost` on the listener port. Add the externally visible authority
    /// when serving through a hostname or reverse proxy. A wildcard bind
    /// (`0.0.0.0` or `[::]`) rejects normal client hosts until at least one
    /// externally visible authority is added here.
    pub fn allowed_host(mut self, authority: impl Into<String>) -> Self {
        self.allowed_hosts.push(authority.into());
        self
    }

    /// Validates the builder and returns a [`Console`].
    ///
    /// # Panics
    ///
    /// Panics if `snapshots` or `events` have not been set, a non-loopback bind
    /// has no access token, or the access token contains non-URL-safe bytes.
    pub fn build(self) -> Console {
        assert!(
            self.bind.ip().is_loopback() || self.access_token.is_some(),
            "ConsoleBuilder: access_token required for non-loopback binds"
        );
        if let Some(token) = &self.access_token {
            assert!(
                !token.is_empty()
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte)),
                "ConsoleBuilder: access_token must be non-empty URL-safe ASCII"
            );
        }
        Console {
            snapshots: self.snapshots.expect("ConsoleBuilder: snapshots required"),
            events: self.events.expect("ConsoleBuilder: events required"),
            stats: self.stats,
            bind: self.bind,
            access_token: self.access_token,
            allowed_hosts: self.allowed_hosts,
        }
    }
}

/// A configured console server ready to start.
pub struct Console {
    snapshots: watch::Receiver<SupervisorSnapshot>,
    events: EventSource,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
}

impl Console {
    /// Returns a new [`ConsoleBuilder`].
    pub fn builder() -> ConsoleBuilder {
        ConsoleBuilder::new()
    }

    /// Returns a builder pre-wired to a runtime's public observability
    /// surfaces.
    ///
    /// The console remains an application-side observer: it subscribes to
    /// snapshots and events and samples actor stats without adding a console
    /// dependency or feature to `tokio-otp`.
    pub fn for_runtime(handle: &RuntimeHandle) -> ConsoleBuilder {
        let events = handle.supervisor_handle();
        let stats = handle.clone();
        Console::builder()
            .snapshots(handle.subscribe_snapshots())
            .event_source(move || events.subscribe())
            .actor_stats(move || stats.actor_stats().into_iter().map(Into::into).collect())
    }

    /// Binds the listener and spawns the server in the background.
    ///
    /// Returns a [`ConsoleHandle`] that can be used to query the local address
    /// or shut the server down.
    pub async fn spawn(self) -> std::io::Result<ConsoleHandle> {
        server::spawn(
            self.snapshots,
            self.events,
            self.stats,
            self.bind,
            self.access_token,
            self.allowed_hosts,
        )
        .await
    }

    /// Runs the server on the current task until shut down.
    pub async fn run(self) -> std::io::Result<()> {
        server::run(
            self.snapshots,
            self.events,
            self.stats,
            self.bind,
            self.access_token,
            self.allowed_hosts,
        )
        .await
    }
}

/// Handle to a running console server.
pub struct ConsoleHandle {
    shutdown_tx: watch::Sender<bool>,
    local_addr: SocketAddr,
}

impl ConsoleHandle {
    /// Returns the address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signals the server to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}
