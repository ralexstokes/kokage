use std::{collections::HashMap, time::Duration};

use kokage::{MonitorEvent, Reply};
use tokio::time::Instant;

pub type VenueId = &'static str;
pub type OrderKey = String;
pub const CALL_DEADLINE: Duration = Duration::from_millis(300);

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    pub venue: VenueId,
    pub symbol: &'static str,
    pub bid: i64,
    pub ask: i64,
    pub seq: u64,
    pub enqueued_at: Instant,
}

#[derive(Debug)]
pub enum FeedMsg {
    Tick(MarketSnapshot),
    Crash,
}

pub fn feed_message_size(message: &FeedMsg) -> usize {
    match message {
        FeedMsg::Tick(_) => std::mem::size_of::<MarketSnapshot>(),
        FeedMsg::Crash => 0,
    }
}

#[derive(Debug)]
pub enum GatewayMsg {
    Place {
        key: OrderKey,
        symbol: &'static str,
        qty: i64,
        reply: Reply<PlaceOutcome>,
    },
    Cancel {
        key: OrderKey,
        reply: Reply<CancelOutcome>,
    },
    CancelAll {
        reply: Reply<usize>,
    },
    Query {
        key: OrderKey,
        reply: Reply<QueryOutcome>,
    },
    DeliverFill {
        key: OrderKey,
        qty: i64,
        enqueued_at: Instant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlaceOutcome {
    Accepted { key: OrderKey },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    Cancelled,
    NotFound,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    Found(OrderStatus),
    NotFound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderStatus {
    Accepted,
    Filled,
    Cancelled,
}

#[derive(Debug)]
pub enum ReconcilerMsg {
    Market(MarketSnapshot),
    Feed { venue: VenueId, event: MonitorEvent },
    StaleSweep,
    Status { reply: Reply<ReconcilerStatus> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VenueHealth {
    Fresh,
    Stale,
    Down,
}

#[derive(Clone, Debug)]
pub struct ReconcilerStatus {
    pub venues: HashMap<VenueId, VenueHealth>,
    pub transitions: HashMap<VenueId, Vec<VenueHealth>>,
    pub exit_reasons: HashMap<VenueId, Vec<kokage::ExitReason>>,
}

#[derive(Debug)]
pub enum RouterMsg {
    Submit {
        symbol: &'static str,
        qty: i64,
        venue: VenueId,
        reply: Reply<SubmitResult>,
    },
    /// Internal: the outcome of a pipelined gateway `Place` call, sent back
    /// to the router by the task it spawned for the call (see `router.rs`).
    SubmitResolved {
        key: OrderKey,
        disposition: SubmitDisposition,
        submitted: SubmitResult,
        reply: Reply<SubmitResult>,
    },
    Cancel {
        key: OrderKey,
        reply: Reply<CancelOutcome>,
    },
    CancelResolved {
        outcome: CancelOutcome,
        reply: Reply<CancelOutcome>,
    },
    ReconcileAll {
        reply: Reply<usize>,
    },
    InFlight {
        reply: Reply<usize>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitResult {
    Placed(OrderKey),
    Unknown(OrderKey),
    IntakeClosed,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitDisposition {
    Confirmed,
    Unknown,
    Rejected,
}

#[derive(Debug)]
pub enum LedgerMsg {
    Ack {
        key: OrderKey,
        venue: VenueId,
    },
    Fill {
        key: OrderKey,
        venue: VenueId,
        qty: i64,
        enqueued_at: Instant,
    },
    Cancelled {
        key: OrderKey,
        venue: VenueId,
    },
    Report {
        reply: Reply<LedgerReport>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct LedgerEffects {
    pub acknowledgements: usize,
    pub fills: usize,
    pub cancellations: usize,
}

#[derive(Clone, Debug, Default)]
pub struct LedgerReport {
    pub effects: HashMap<OrderKey, LedgerEffects>,
}

#[derive(Debug)]
pub enum ControlMsg {
    KillSwitch { reply: Reply<()> },
    EmergencyCancelAll { reply: Reply<usize> },
}

#[derive(Debug)]
pub enum HealthMsg {
    RestartsObserved { total: u64 },
    ResetBreaker,
    Tripped { reply: Reply<bool> },
}
