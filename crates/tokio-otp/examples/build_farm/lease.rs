//! A supervised lease-renewal task that is deliberately not an actor.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio_otp::{BackoffPolicy, ChildContext, ChildSpec, RestartIntensity, RestartPolicy};

pub const LEASE_ID: &str = "lease-renewer";

#[derive(Debug, Default)]
pub struct Lease {
    held: AtomicBool,
    acquisitions: AtomicU64,
    renewals: AtomicU64,
}

impl Lease {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }

    pub fn acquisitions(&self) -> u64 {
        self.acquisitions.load(Ordering::Acquire)
    }

    pub fn renewals(&self) -> u64 {
        self.renewals.load(Ordering::Acquire)
    }
}

pub fn renewer(lease: Arc<Lease>, fail_first: bool) -> ChildSpec {
    ChildSpec::new(LEASE_ID, move |ctx: ChildContext| {
        let lease = Arc::clone(&lease);
        async move {
            lease.acquisitions.fetch_add(1, Ordering::AcqRel);
            lease.held.store(true, Ordering::Release);
            ctx.mark_ready();

            let scripted_failure = fail_first && ctx.generation() == 0;
            loop {
                tokio::select! {
                    () = ctx.shutdown_token().cancelled() => break,
                    () = tokio::time::sleep(Duration::from_millis(12)) => {
                        lease.renewals.fetch_add(1, Ordering::AcqRel);
                        if scripted_failure {
                            lease.held.store(false, Ordering::Release);
                            return Err("lease service rejected a renewal".into());
                        }
                    }
                }
            }

            lease.held.store(false, Ordering::Release);
            Ok(())
        }
    })
    .wait_for_ready()
    .restart(RestartPolicy::Always)
    .restart_intensity(
        RestartIntensity::new(4, Duration::from_secs(10))
            .with_backoff(BackoffPolicy::Fixed(Duration::from_millis(60))),
    )
}
