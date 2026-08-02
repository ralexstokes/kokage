//! A readiness-gated, supervised lease-renewal task.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{Backoff, RestartPolicy, TaskSpec};

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

pub fn renewer(lease: Arc<Lease>, fail_first: bool) -> TaskSpec {
    TaskSpec::new("lease-renewer", move |ctx| {
        let lease = Arc::clone(&lease);
        async move {
            lease.acquisitions.fetch_add(1, Ordering::AcqRel);
            lease.held.store(true, Ordering::Release);
            ctx.mark_ready();

            loop {
                tokio::select! {
                    () = ctx.shutdown_token().cancelled() => {
                        lease.held.store(false, Ordering::Release);
                        return Ok(());
                    }
                    () = tokio::time::sleep(Duration::from_millis(15)) => {
                        lease.renewals.fetch_add(1, Ordering::AcqRel);
                        if fail_first && ctx.generation() == 0 {
                            lease.held.store(false, Ordering::Release);
                            return Err(std::io::Error::other(
                                "lease service rejected a renewal",
                            ).into());
                        }
                    }
                }
            }
        }
    })
    .manual_readiness(Duration::from_secs(1))
    .restart(
        RestartPolicy::on_failure()
            .limit(3, Duration::from_secs(5))
            .backoff(Backoff::fixed(Duration::from_millis(60))),
    )
}
