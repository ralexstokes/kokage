use std::time::Duration;

use tokio::sync::mpsc;

use crate::observability::SupervisorObservability;

/// Snapshot of how a child task exited.
///
/// This is a cloneable, displayable view of the exit status; the original error
/// value (if any) is converted to its `Display` string.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum ExitStatusView {
    /// The child returned `Ok(())`.
    Completed,
    /// The child returned an `Err`. The string is the error's `Display` output.
    Failed(String),
    /// The child task panicked.
    Panicked,
    /// The child task was aborted by the supervisor (e.g. after a grace-period
    /// timeout).
    Aborted,
    /// The child's cooperative shutdown grace expired. The supervisor first
    /// offered the child wrapper a tidy-abort accounting beat and then
    /// hard-aborted it if necessary.
    ShutdownTimedOut,
}

/// One segment of a [`SupervisorEvent::Nested`] path, identifying which child
/// supervisor forwarded the event.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct EventPathSegment {
    /// The child id of the nested supervisor that forwarded this event.
    pub id: String,
    /// The generation of that child at the time the event was forwarded.
    pub generation: u64,
}

impl EventPathSegment {
    /// Creates a nested-event path segment.
    pub fn new(id: impl Into<String>, generation: u64) -> Self {
        Self {
            id: id.into(),
            generation,
        }
    }
}

/// Lifecycle event emitted by a supervisor.
///
/// Events are broadcast to all subscribers via
/// [`SupervisorHandle::subscribe`](crate::SupervisorHandle::subscribe).
///
/// # Ordering guarantee
///
/// The supervisor publishes an updated [`SupervisorSnapshot`](crate::SupervisorSnapshot)
/// **before** broadcasting the corresponding event, so event handlers can read
/// already-consistent snapshot state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum SupervisorEvent {
    /// The supervisor has started and all initial children are being spawned.
    SupervisorStarted,
    /// The supervisor is beginning its shutdown sequence.
    SupervisorStopping,
    /// The supervisor has fully stopped. No further events will be emitted.
    ///
    /// Emitted after explicit shutdown or a fatal supervisor error once all
    /// child tasks have been joined.
    SupervisorStopped,
    /// An event forwarded from a nested (child) supervisor. Nested events form
    /// a recursive wrapper; use [`path`](SupervisorEvent::path) or
    /// [`leaf`](SupervisorEvent::leaf) to unwrap.
    #[non_exhaustive]
    Nested {
        /// Child id of the nested supervisor.
        id: String,
        /// Generation of that child.
        generation: u64,
        /// The inner event from the nested supervisor.
        event: Box<SupervisorEvent>,
    },
    /// A child has been spawned and completed its readiness boundary.
    #[non_exhaustive]
    ChildStarted {
        /// Child identifier.
        id: String,
        /// Generation counter for this spawn.
        generation: u64,
    },
    /// The child has been fully removed from the supervisor.
    #[non_exhaustive]
    ChildRemoved {
        /// Child identifier.
        id: String,
    },
    /// A child task exited (cleanly, with an error, by panic, or by abort).
    #[non_exhaustive]
    ChildExited {
        /// Child identifier.
        id: String,
        /// Generation that exited.
        generation: u64,
        /// How the child exited.
        status: ExitStatusView,
    },
    /// A restart for this child has been scheduled after a backoff delay.
    #[non_exhaustive]
    ChildRestartScheduled {
        /// Child identifier.
        id: String,
        /// Generation that exited and will be replaced.
        generation: u64,
        /// How long the supervisor will wait before respawning.
        delay: Duration,
    },
    /// A child has been successfully restarted with a new generation.
    #[non_exhaustive]
    ChildRestarted {
        /// Child identifier.
        id: String,
        /// Generation that exited.
        old_generation: u64,
        /// Generation of the newly spawned replacement.
        new_generation: u64,
    },
    /// The restart intensity limit was exceeded. The supervisor will exit with
    /// [`SupervisorError::RestartIntensityExceeded`](crate::SupervisorError::RestartIntensityExceeded).
    RestartIntensityExceeded,
}

impl SupervisorEvent {
    /// Creates an event forwarded from a nested supervisor child.
    pub fn nested(id: impl Into<String>, generation: u64, event: Self) -> Self {
        Self::Nested {
            id: id.into(),
            generation,
            event: Box::new(event),
        }
    }

    /// Creates an event for a child that completed startup readiness.
    pub fn child_started(id: impl Into<String>, generation: u64) -> Self {
        Self::ChildStarted {
            id: id.into(),
            generation,
        }
    }

    /// Creates an event for a child that was fully removed.
    pub fn child_removed(id: impl Into<String>) -> Self {
        Self::ChildRemoved { id: id.into() }
    }

    /// Creates an event for a child task exit.
    pub fn child_exited(id: impl Into<String>, generation: u64, status: ExitStatusView) -> Self {
        Self::ChildExited {
            id: id.into(),
            generation,
            status,
        }
    }

    /// Creates an event for a delayed child restart.
    pub fn child_restart_scheduled(
        id: impl Into<String>,
        generation: u64,
        delay: Duration,
    ) -> Self {
        Self::ChildRestartScheduled {
            id: id.into(),
            generation,
            delay,
        }
    }

    /// Creates an event for a child that successfully restarted.
    pub fn child_restarted(
        id: impl Into<String>,
        old_generation: u64,
        new_generation: u64,
    ) -> Self {
        Self::ChildRestarted {
            id: id.into(),
            old_generation,
            new_generation,
        }
    }

    /// Returns the nested-supervisor path leading to this event.
    ///
    /// For non-nested events the path is empty. For events wrapped in one or
    /// more [`Nested`](SupervisorEvent::Nested) layers, each layer contributes
    /// one [`EventPathSegment`].
    pub fn path(&self) -> Vec<EventPathSegment> {
        let mut path = Vec::new();
        self.collect_path(&mut path);
        path
    }

    /// Unwraps any [`Nested`](SupervisorEvent::Nested) wrappers and returns
    /// the innermost (leaf) event.
    pub fn leaf(&self) -> &Self {
        match self {
            Self::Nested { event, .. } => event.leaf(),
            event => event,
        }
    }

    fn collect_path(&self, path: &mut Vec<EventPathSegment>) {
        if let Self::Nested {
            id,
            generation,
            event,
        } = self
        {
            path.push(EventPathSegment::new(id.clone(), *generation));
            event.collect_path(path);
        }
    }
}

pub(crate) struct NestedEventNotification {
    pub(crate) parent_key: usize,
    pub(crate) parent_instance: u64,
    pub(crate) id: String,
    pub(crate) generation: u64,
    pub(crate) event: SupervisorEvent,
}

#[derive(Clone)]
pub(crate) struct EventSink {
    notifications: mpsc::Sender<NestedEventNotification>,
    parent_key: usize,
    parent_instance: u64,
    observability: SupervisorObservability,
}

impl EventSink {
    pub(crate) fn new(
        notifications: mpsc::Sender<NestedEventNotification>,
        parent_key: usize,
        parent_instance: u64,
        observability: SupervisorObservability,
    ) -> Self {
        Self {
            notifications,
            parent_key,
            parent_instance,
            observability,
        }
    }

    pub(crate) fn forward(&self, id: String, generation: u64, event: SupervisorEvent) {
        let notification = NestedEventNotification {
            parent_key: self.parent_key,
            parent_instance: self.parent_instance,
            id,
            generation,
            event,
        };
        if let Err(error) = self.notifications.try_send(notification)
            && matches!(error, mpsc::error::TrySendError::Full(_))
        {
            self.observability.emit_nested_event_forwarding_lag(1);
        }
    }
}
