//! The build lease: a supervised child that is deliberately *not* an actor.
//!
//! A remote build farm holds a lease on the queue item it is working, and
//! renews it on a timer. There is no protocol to speak to the renewer — it
//! takes no requests and answers no questions — so wrapping it in an actor
//! would buy a mailbox nothing ever sends to. It goes into the tree as a plain
//! [`ChildSpec`], which is the supervisor's own child shape.
//!
//! The trade is visible in the wiring: because it is not an actor it has no
//! [`ActorRef`](tokio_otp::ActorRef), so the only channel between it and the
//! scheduler is the [`Lease`] both sides hold an `Arc` to. Ordering still comes
//! from the tree — the renewer is declared before the scheduler in an ordered
//! scope and uses [`ChildSpec::wait_for_ready`], so the lease is already held
//! the first time the scheduler looks at it.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio_otp::{BackoffPolicy, ChildContext, ChildSpec, RestartIntensity, RestartPolicy};

/// The child id the renewer takes in its supervisor.
pub const LEASE_CHILD_ID: &str = "lease-renewer";

/// Shared lease state.
#[derive(Debug, Default)]
pub struct Lease {
    held: AtomicBool,
    renewals: AtomicU64,
    acquisitions: AtomicU64,
}

impl Lease {
    /// Creates an unheld lease.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns whether the lease is currently held.
    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }

    /// Returns how many times the renewer has extended the lease.
    pub fn renewals(&self) -> u64 {
        self.renewals.load(Ordering::Acquire)
    }

    /// Returns how many times the lease has been taken, including retakes
    /// after a supervised restart.
    pub fn acquisitions(&self) -> u64 {
        self.acquisitions.load(Ordering::Acquire)
    }

    fn acquire(&self) {
        self.acquisitions.fetch_add(1, Ordering::AcqRel);
        self.held.store(true, Ordering::Release);
    }

    fn release(&self) {
        self.held.store(false, Ordering::Release);
    }
}

/// Builds the lease-renewer child.
///
/// `drop_after` scripts one lost lease: the first incarnation renews that many
/// times and then fails, exactly as a renewer would when the lease service
/// rejects its extension. The supervisor restarts it, the lease is retaken, and
/// the scheduler resumes. Later incarnations run until shutdown.
pub fn renewer(lease: Arc<Lease>, period: Duration, drop_after: u64) -> ChildSpec {
    ChildSpec::new(LEASE_CHILD_ID, move |ctx: ChildContext| {
        let lease = Arc::clone(&lease);
        async move {
            let scripted_loss = ctx.generation() == 0;
            lease.acquire();
            ctx.mark_ready();

            let mut renewed = 0;
            loop {
                tokio::select! {
                    () = ctx.shutdown_token().cancelled() => break,
                    () = tokio::time::sleep(period) => {
                        lease.renewals.fetch_add(1, Ordering::AcqRel);
                        renewed += 1;
                    }
                }
                if scripted_loss && renewed >= drop_after {
                    // Releasing before returning `Err` is what makes this
                    // observable: the scheduler sees an unheld lease during the
                    // restart window and backs off instead of dispatching.
                    lease.release();
                    return Err("lease service rejected the renewal".into());
                }
            }

            lease.release();
            Ok(())
        }
    })
    .wait_for_ready()
    .restart(RestartPolicy::Always)
    // Without a backoff the retake is instantaneous and the outage is
    // invisible. A fixed delay makes the window wide enough for the scheduler
    // to actually observe an unheld lease and back off, which is the behavior
    // worth demonstrating.
    .restart_intensity(
        RestartIntensity::new(5, Duration::from_secs(30))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(60))),
    )
}
