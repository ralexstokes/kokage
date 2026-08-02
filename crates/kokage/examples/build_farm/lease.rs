//! A readiness-gated, supervised lease-renewal task.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kokage::{Backoff, RestartPolicy, TaskSpec};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseState {
    pub held: bool,
    pub outages: u64,
}

#[derive(Debug)]
pub struct Lease {
    state: watch::Sender<LeaseState>,
    acquisitions: AtomicU64,
    renewals: AtomicU64,
}

impl Default for Lease {
    fn default() -> Self {
        let (state, _) = watch::channel(LeaseState::default());
        Self {
            state,
            acquisitions: AtomicU64::new(0),
            renewals: AtomicU64::new(0),
        }
    }
}

impl Lease {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn subscribe(&self) -> watch::Receiver<LeaseState> {
        self.state.subscribe()
    }

    pub fn acquisitions(&self) -> u64 {
        self.acquisitions.load(Ordering::Acquire)
    }

    pub fn renewals(&self) -> u64 {
        self.renewals.load(Ordering::Acquire)
    }

    fn acquire(&self) {
        self.acquisitions.fetch_add(1, Ordering::AcqRel);
        self.state.send_modify(|state| state.held = true);
    }

    fn release(&self) {
        self.state.send_modify(|state| state.held = false);
    }

    fn record_outage(&self) {
        self.state.send_modify(|state| {
            state.held = false;
            state.outages += 1;
        });
    }
}

pub fn renewer(lease: Arc<Lease>, fail_first: bool) -> TaskSpec {
    TaskSpec::new("lease-renewer", move |ctx| {
        let lease = Arc::clone(&lease);
        async move {
            lease.acquire();
            ctx.mark_ready();

            loop {
                tokio::select! {
                    () = ctx.shutdown_token().cancelled() => {
                        lease.release();
                        return Ok(());
                    }
                    () = tokio::time::sleep(Duration::from_millis(15)) => {
                        lease.renewals.fetch_add(1, Ordering::AcqRel);
                        if fail_first && ctx.generation() == 0 {
                            lease.record_outage();
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
