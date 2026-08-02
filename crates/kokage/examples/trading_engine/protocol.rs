use std::{collections::HashMap, time::Duration};

use kokage::{MonitorEvent, Reply};
use tokio::time::Instant;

pub type VenueId = &'static str;
pub type Symbol = &'static str;
pub type OrderKey = String;

pub const VENUE_A: VenueId = "venue-a";
pub const VENUE_B: VenueId = "venue-b";

pub const NORMAL_ORDER: Symbol = "NORMAL";
pub const KEEP_OPEN: Symbol = "KEEP-OPEN";
pub const STALL_FOREVER: Symbol = "STALL-FOREVER";
pub const ACCEPT_NO_ACK: Symbol = "ACCEPT-NO-ACK";
const FEED_CONTROL_KEY: Symbol = "__feed-control__";

pub const GATEWAY_DEADLINE: Duration = Duration::from_millis(400);
pub const OFFLOAD_DEADLINE: Duration = Duration::from_millis(450);
pub const PHASE_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
pub struct MarketTick {
    pub venue: VenueId,
    pub symbol: Symbol,
    pub bid: i64,
    pub ask: i64,
    pub sequence: u64,
    pub enqueued_at: Instant,
}

#[derive(Debug)]
pub enum FeedMsg {
    Tick(MarketTick),
    Crash,
}

pub fn feed_message_size(message: &FeedMsg) -> usize {
    match message {
        FeedMsg::Tick(_) => std::mem::size_of::<MarketTick>(),
        FeedMsg::Crash => 0,
    }
}

pub fn feed_message_key(message: &FeedMsg) -> Symbol {
    match message {
        FeedMsg::Tick(tick) => tick.symbol,
        // Same-symbol latest-wins updates cannot replace unread control
        // traffic; bounded distinct-key eviction remains mailbox policy.
        FeedMsg::Crash => FEED_CONTROL_KEY,
    }
}

#[derive(Debug)]
pub enum GatewayMsg {
    Place {
        key: OrderKey,
        symbol: Symbol,
        quantity: i64,
        reply: Reply<PlaceOutcome>,
    },
    Cancel {
        key: OrderKey,
        reply: Reply<CancelOutcome>,
    },
    Query {
        key: OrderKey,
        reply: Reply<QueryOutcome>,
    },
    CancelAll {
        reply: Reply<usize>,
    },
    DeliverFills,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlaceOutcome {
    pub key: OrderKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    Cancelled,
    NotFound,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    Found(OrderStatus),
    NotFound,
}

#[derive(Debug)]
pub enum LedgerMsg {
    Acknowledged {
        key: OrderKey,
        venue: VenueId,
    },
    Filled {
        key: OrderKey,
        venue: VenueId,
        quantity: i64,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LedgerEffects {
    pub acknowledgements: usize,
    pub fills: usize,
    pub cancellations: usize,
}

#[derive(Clone, Debug, Default)]
pub struct LedgerReport {
    pub effects: HashMap<OrderKey, LedgerEffects>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VenueCondition {
    Fresh,
    Stale,
    Down,
}

#[derive(Debug)]
pub enum ReconcilerMsg {
    Market(MarketTick),
    FeedLifecycle { venue: VenueId, event: MonitorEvent },
    StaleSweep,
    Report { reply: Reply<MarketReport> },
}

#[derive(Clone, Debug)]
pub struct MarketReport {
    pub conditions: HashMap<VenueId, VenueCondition>,
    pub sequences: HashMap<VenueId, Option<u64>>,
    pub transitions: HashMap<VenueId, Vec<VenueCondition>>,
    pub exits: HashMap<VenueId, Vec<kokage::observe::ExitStatus>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitResult {
    Placed(OrderKey),
    Unknown(OrderKey),
    IntakeClosed,
    UnknownVenue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayCallResult {
    Acknowledged,
    Unknown,
}

#[derive(Debug)]
pub enum PlacePurpose {
    Submit(Reply<SubmitResult>),
    Reconcile { sweep: u64 },
}

#[derive(Debug)]
pub enum RouterMsg {
    Submit {
        venue: VenueId,
        symbol: Symbol,
        quantity: i64,
        reply: Reply<SubmitResult>,
    },
    PlaceCompleted {
        key: OrderKey,
        purpose: PlacePurpose,
        result: GatewayCallResult,
    },
    Cancel {
        key: OrderKey,
        reply: Reply<CancelOutcome>,
    },
    CancelCompleted {
        key: OrderKey,
        result: CancelOutcome,
        reply: Reply<CancelOutcome>,
    },
    ReconcileAll {
        reply: Reply<ReconcileReport>,
    },
    QueryCompleted {
        sweep: u64,
        key: OrderKey,
        result: Option<QueryOutcome>,
    },
    Report {
        reply: Reply<RouterReport>,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub examined: usize,
    pub resolved: usize,
    pub busy: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouterReport {
    pub pending: usize,
    pub unknown: usize,
    pub confirmed: usize,
    pub cancelled: usize,
    pub reconciling: bool,
}

#[derive(Debug)]
pub enum ControlMsg {
    TripBreaker,
    EmergencyCancelAll { reply: Reply<usize> },
}

#[derive(Debug)]
pub enum HealthMsg {
    RestartsObserved { total: u64 },
    Report { reply: Reply<HealthReport> },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HealthReport {
    pub observed_total: u64,
    pub restarts_in_window: usize,
    pub tripped: bool,
}
