//! Web-based dashboard for visualizing live `kokage` supervisor state.
//!
//! `kokage-console` hosts an axum web server with WebSocket streaming and
//! an embedded single-file HTML/JS/CSS frontend. It renders supervision trees,
//! child states, events, and summary stats in real time.
//!
//! # Usage
//!
//! ```no_run
//! use kokage::prelude::*;
//! use kokage_console::ConsoleBuilder;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let runtime = OrderedTree::new().spawn()?;
//! let root = runtime.scope();
//! let console = ConsoleBuilder::for_runtime(&root)
//!     .spawn()
//!     .await
//!     .expect("failed to start console");
//!
//! println!("Console at http://{}", console.local_addr());
//! # runtime.shutdown_and_wait().await?;
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

use std::{io, net::SocketAddr, sync::Arc};

use kokage::{
    ScopeRef,
    observe::{ActorStats, LifecycleWatch, SupervisorSnapshotReceiver},
};
use thiserror::Error;
use tokio::sync::watch;

type StatsSource = Arc<dyn Fn() -> Vec<ActorStats> + Send + Sync>;
type LifecycleSource = Arc<dyn Fn() -> LifecycleWatch + Send + Sync>;

/// Errors returned while validating or starting a console server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsoleError {
    /// No supervisor snapshot receiver was configured.
    #[error("console snapshots are required")]
    MissingSnapshots,
    /// No recursive lifecycle-watch source was configured.
    #[error("console lifecycle source is required")]
    MissingLifecycle,
    /// A non-loopback listener was configured without an access token.
    #[error("an access token is required for non-loopback console binds")]
    AccessTokenRequired,
    /// The access token was empty or contained a byte that is not URL-safe.
    #[error("the console access token must be non-empty URL-safe ASCII")]
    InvalidAccessToken,
    /// The console listener could not be bound, served, or joined.
    #[error("failed to start console server: {0}")]
    Io(#[from] std::io::Error),
}

/// Builder for configuring and spawning a console server.
pub struct ConsoleBuilder {
    snapshots: Option<SupervisorSnapshotReceiver>,
    lifecycle: Option<LifecycleSource>,
    stats: StatsSource,
    bind: SocketAddr,
    access_token: Option<String>,
    allowed_hosts: Vec<String>,
}

impl ConsoleBuilder {
    /// Returns a console builder with a loopback bind on port 9100.
    pub fn new() -> Self {
        Self {
            snapshots: None,
            lifecycle: None,
            stats: Arc::new(Vec::new),
            bind: ([127, 0, 0, 1], 9100).into(),
            access_token: None,
            allowed_hosts: Vec::new(),
        }
    }

    /// Returns a builder pre-wired to a runtime's public observability
    /// surfaces.
    ///
    /// The console remains an application-side observer: it subscribes to
    /// snapshots and lifecycle events and samples actor stats without adding
    /// a console dependency or feature to `kokage`.
    pub fn for_runtime(scope: &ScopeRef) -> Self {
        let lifecycle = scope.clone();
        let stats = scope.clone();
        Self::new()
            .snapshots(scope.subscribe_snapshots())
            .lifecycle(move || lifecycle.watch_lifecycle())
            .actor_stats(move || stats.actor_stats())
    }

    /// Sets the supervisor snapshot receiver.
    pub fn snapshots(mut self, rx: SupervisorSnapshotReceiver) -> Self {
        self.snapshots = Some(rx);
        self
    }

    /// Sets the recursive lifecycle-watch source. The console calls this once
    /// for each WebSocket connection.
    pub fn lifecycle(
        mut self,
        source: impl Fn() -> LifecycleWatch + Send + Sync + 'static,
    ) -> Self {
        self.lifecycle = Some(Arc::new(source));
        self
    }

    /// Sets the pull source sampled for per-actor stats.
    pub fn actor_stats(
        mut self,
        source: impl Fn() -> Vec<ActorStats> + Send + Sync + 'static,
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

    fn validate(&self) -> Result<(), ConsoleError> {
        if !self.bind.ip().is_loopback() && self.access_token.is_none() {
            return Err(ConsoleError::AccessTokenRequired);
        }
        if let Some(token) = &self.access_token {
            let valid = !token.is_empty()
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte));
            if !valid {
                return Err(ConsoleError::InvalidAccessToken);
            }
        }
        if self.snapshots.is_none() {
            return Err(ConsoleError::MissingSnapshots);
        }
        if self.lifecycle.is_none() {
            return Err(ConsoleError::MissingLifecycle);
        }
        Ok(())
    }

    /// Validates the configuration, binds the listener, and spawns the server.
    ///
    /// Returns a [`ConsoleHandle`] that can be used to query the local address
    /// or shut the server down.
    pub async fn spawn(self) -> Result<ConsoleHandle, ConsoleError> {
        self.validate()?;
        Ok(server::spawn(
            self.snapshots.expect("validated snapshot source"),
            self.lifecycle.expect("validated lifecycle source"),
            self.stats,
            self.bind,
            self.access_token,
            self.allowed_hosts,
        )
        .await?)
    }
}

impl Default for ConsoleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a running console server.
///
/// Dropping the handle detaches the server; it does not shut it down. Call
/// [`shutdown`](Self::shutdown) to request graceful shutdown and
/// [`wait`](Self::wait) to observe any serve-time error.
#[must_use = "dropping the console handle detaches the still-running server"]
pub struct ConsoleHandle {
    shutdown_tx: watch::Sender<bool>,
    local_addr: SocketAddr,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
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

    /// Waits for the server task to finish and reports serve-time failures.
    ///
    /// This does not request shutdown. Call [`shutdown`](Self::shutdown) first
    /// when the server should stop now.
    pub async fn wait(self) -> Result<(), ConsoleError> {
        self.task
            .await
            .map_err(|error| ConsoleError::Io(io::Error::other(error)))??;
        Ok(())
    }

    /// Requests graceful shutdown and waits for the server task to finish.
    pub async fn shutdown_and_wait(self) -> Result<(), ConsoleError> {
        self.shutdown();
        self.wait().await
    }
}
