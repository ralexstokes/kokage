use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use kokage::prelude::*;
use tokio::time::Instant;

use crate::{
    protocol::{
        ACCEPT_NO_ACK, CancelOutcome, FeedMsg, GatewayMsg, KEEP_OPEN, LedgerMsg, OrderKey,
        OrderStatus, PlaceOutcome, QueryOutcome, ReconcilerMsg, STALL_FOREVER, VenueId,
    },
    telemetry::LatencyRecorder,
};

const FEED_PARSE_TIME: Duration = Duration::from_millis(2);
const FILL_DELAY: Duration = Duration::from_millis(30);
const FILL_TIMER: TimerKey = TimerKey::new("deliver-fills");

#[derive(Clone, Copy, Debug)]
struct SimOrder {
    status: OrderStatus,
}

#[derive(Debug, Default)]
struct ExchangeState {
    orders: HashMap<OrderKey, SimOrder>,
    open: HashSet<OrderKey>,
    place_attempts: HashMap<OrderKey, usize>,
    accept_counts: HashMap<OrderKey, usize>,
    feed_sessions: u64,
    gateway_sessions: u64,
}

/// One cloneable, in-memory exchange per venue. Actor restarts reconnect to
/// the same exchange state, which is what makes reconciliation meaningful.
#[derive(Clone, Debug, Default)]
pub struct ExchangeSim(Arc<Mutex<ExchangeState>>);

impl ExchangeSim {
    fn state(&self) -> MutexGuard<'_, ExchangeState> {
        self.0.lock().expect("exchange lock poisoned")
    }

    fn open_feed_session(&self) {
        self.state().feed_sessions += 1;
    }

    fn open_gateway_session(&self) {
        self.state().gateway_sessions += 1;
    }

    pub fn feed_sessions(&self) -> u64 {
        self.state().feed_sessions
    }

    pub fn gateway_sessions(&self) -> u64 {
        self.state().gateway_sessions
    }

    fn note_place_attempt(&self, key: &str) -> usize {
        let mut state = self.state();
        let attempts = state.place_attempts.entry(key.to_owned()).or_default();
        *attempts += 1;
        *attempts
    }

    fn accept(&self, key: &str, quantity: i64) -> bool {
        let mut state = self.state();
        if state.orders.contains_key(key) {
            return false;
        }
        state.orders.insert(
            key.to_owned(),
            SimOrder {
                status: OrderStatus::Open,
            },
        );
        state.open.insert(key.to_owned());
        *state.accept_counts.entry(key.to_owned()).or_default() += 1;
        tracing::debug!(order_key = key, quantity, "exchange accepted order");
        true
    }

    fn fill(&self, key: &str) -> bool {
        let mut state = self.state();
        let Some(order) = state.orders.get_mut(key) else {
            return false;
        };
        if order.status != OrderStatus::Open {
            return false;
        }
        order.status = OrderStatus::Filled;
        state.open.remove(key);
        true
    }

    fn query(&self, key: &str) -> QueryOutcome {
        self.state()
            .orders
            .get(key)
            .map_or(QueryOutcome::NotFound, |order| {
                QueryOutcome::Found(order.status)
            })
    }

    fn cancel(&self, key: &str) -> CancelOutcome {
        let mut state = self.state();
        let Some(order) = state.orders.get_mut(key) else {
            return CancelOutcome::NotFound;
        };
        if order.status != OrderStatus::Open {
            return CancelOutcome::NotFound;
        }
        order.status = OrderStatus::Cancelled;
        state.open.remove(key);
        CancelOutcome::Cancelled
    }

    fn cancel_all(&self) -> Vec<OrderKey> {
        let mut state = self.state();
        let keys = state.open.drain().collect::<Vec<_>>();
        for key in &keys {
            state.orders.get_mut(key).expect("open order exists").status = OrderStatus::Cancelled;
        }
        keys
    }

    pub fn accept_count(&self, key: &str) -> usize {
        self.state()
            .accept_counts
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    pub fn place_attempts(&self, key: &str) -> usize {
        self.state()
            .place_attempts
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    pub fn status(&self, key: &str) -> Option<OrderStatus> {
        self.state().orders.get(key).map(|order| order.status)
    }
}

#[derive(kokage::ActorFactory)]
pub struct VenueFeed {
    pub venue: VenueId,
    pub exchange: ExchangeSim,
    pub reconciler: ActorRef<ReconcilerMsg>,
    pub latency: LatencyRecorder,
}

impl Actor for VenueFeed {
    type Msg = FeedMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.exchange.open_feed_session();
        Ok(())
    }

    async fn handle(&mut self, message: FeedMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        let started = Instant::now();
        match message {
            FeedMsg::Tick(tick) => {
                self.latency.record(
                    "feed.queue",
                    started.saturating_duration_since(tick.enqueued_at),
                );
                // Simulation only: make the producer outrun parsing so
                // latest-wins behavior is visible in actor statistics.
                tokio::time::sleep(FEED_PARSE_TIME).await;
                self.reconciler.send(ReconcilerMsg::Market(tick)).await?;
            }
            FeedMsg::Crash => panic!("scripted feed crash at {}", self.venue),
        }
        self.latency.record("feed.handle", started.elapsed());
        Ok(())
    }
}

struct PendingFill {
    key: OrderKey,
    quantity: i64,
    enqueued_at: Instant,
    due: Instant,
}

#[derive(kokage::ActorFactory)]
pub struct VenueGateway {
    pub venue: VenueId,
    pub exchange: ExchangeSim,
    pub ledger: ActorRef<LedgerMsg>,
    pub latency: LatencyRecorder,
    #[factory(default)]
    stalled_replies: Vec<Reply<PlaceOutcome>>,
    #[factory(default)]
    pending_fills: VecDeque<PendingFill>,
}

impl VenueGateway {
    fn schedule_fill(&mut self, key: OrderKey, quantity: i64, ctx: &mut Context<'_, Self>) {
        let enqueued_at = Instant::now();
        let arm_timer = self.pending_fills.is_empty();
        self.pending_fills.push_back(PendingFill {
            key,
            quantity,
            enqueued_at,
            due: enqueued_at + FILL_DELAY,
        });
        if arm_timer {
            ctx.set_timeout(FILL_TIMER, GatewayMsg::DeliverFills, FILL_DELAY);
        }
    }

    async fn deliver_fills(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        while self
            .pending_fills
            .front()
            .is_some_and(|fill| fill.due <= Instant::now())
        {
            let fill = self.pending_fills.pop_front().expect("front fill exists");
            if self.exchange.fill(&fill.key) {
                self.ledger
                    .send(LedgerMsg::Filled {
                        key: fill.key,
                        venue: self.venue,
                        quantity: fill.quantity,
                        enqueued_at: fill.enqueued_at,
                    })
                    .await?;
            }
        }
        if let Some(next) = self.pending_fills.front() {
            ctx.set_timeout(
                FILL_TIMER,
                GatewayMsg::DeliverFills,
                next.due.saturating_duration_since(Instant::now()),
            );
        }
        Ok(())
    }
}

impl Actor for VenueGateway {
    type Msg = GatewayMsg;

    async fn on_start(&mut self, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.exchange.open_gateway_session();
        Ok(())
    }

    async fn handle(&mut self, message: GatewayMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        let started = Instant::now();
        match message {
            GatewayMsg::Place {
                key,
                symbol,
                quantity,
                reply,
            } => {
                let attempt = self.exchange.note_place_attempt(&key);
                if symbol == STALL_FOREVER && attempt == 1 {
                    // Keeping Reply alive makes the call wait for its own
                    // deadline without blocking this gateway's mailbox.
                    self.stalled_replies.push(reply);
                    self.latency.record("gateway.handle", started.elapsed());
                    return Ok(());
                }

                let inserted = self.exchange.accept(&key, quantity);
                if symbol == ACCEPT_NO_ACK && attempt == 1 {
                    // The exchange effect exists, but neither caller nor
                    // ledger hears about it until reconciliation queries it.
                    self.stalled_replies.push(reply);
                    self.latency.record("gateway.handle", started.elapsed());
                    return Ok(());
                }

                reply.send(PlaceOutcome { key: key.clone() });
                if inserted {
                    self.ledger
                        .send(LedgerMsg::Acknowledged {
                            key: key.clone(),
                            venue: self.venue,
                        })
                        .await?;
                    if symbol != KEEP_OPEN {
                        self.schedule_fill(key, quantity, ctx);
                    }
                }
            }
            GatewayMsg::Cancel { key, reply } => {
                let result = self.exchange.cancel(&key);
                if result == CancelOutcome::Cancelled {
                    self.ledger
                        .send(LedgerMsg::Cancelled {
                            key,
                            venue: self.venue,
                        })
                        .await?;
                }
                reply.send(result);
            }
            GatewayMsg::Query { key, reply } => reply.send(self.exchange.query(&key)),
            GatewayMsg::CancelAll { reply } => {
                let cancelled = self.exchange.cancel_all();
                let count = cancelled.len();
                for key in cancelled {
                    self.ledger
                        .send(LedgerMsg::Cancelled {
                            key,
                            venue: self.venue,
                        })
                        .await?;
                }
                reply.send(count);
            }
            GatewayMsg::DeliverFills => self.deliver_fills(ctx).await?,
        }
        self.latency.record("gateway.handle", started.elapsed());
        Ok(())
    }
}
