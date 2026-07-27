use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;
use tokio_otp::{LiveContext, prelude::*};

use crate::{
    messages::{
        CancelOutcome, FeedMsg, GatewayMsg, LedgerMsg, OrderKey, OrderStatus, PlaceOutcome,
        QueryOutcome, ReconcilerMsg, VenueId,
    },
    telemetry::LatencyRecorder,
};

#[derive(Clone, Debug)]
struct SimOrder {
    status: OrderStatus,
}

#[derive(Debug, Default)]
struct ExchangeState {
    orders: HashMap<OrderKey, SimOrder>,
    accept_counts: HashMap<OrderKey, usize>,
    place_attempts: HashMap<OrderKey, usize>,
    open: HashSet<OrderKey>,
    feed_sessions: HashMap<VenueId, u64>,
    gateway_sessions: HashMap<VenueId, u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ExchangeSim(Arc<Mutex<ExchangeState>>);

impl ExchangeSim {
    pub fn open_feed_session(&self, venue: VenueId) {
        *self
            .0
            .lock()
            .expect("exchange lock poisoned")
            .feed_sessions
            .entry(venue)
            .or_default() += 1;
    }

    pub fn open_gateway_session(&self, venue: VenueId) {
        *self
            .0
            .lock()
            .expect("exchange lock poisoned")
            .gateway_sessions
            .entry(venue)
            .or_default() += 1;
    }

    pub fn feed_sessions(&self, venue: VenueId) -> u64 {
        self.0
            .lock()
            .expect("exchange lock poisoned")
            .feed_sessions
            .get(venue)
            .copied()
            .unwrap_or_default()
    }

    pub fn gateway_sessions(&self, venue: VenueId) -> u64 {
        self.0
            .lock()
            .expect("exchange lock poisoned")
            .gateway_sessions
            .get(venue)
            .copied()
            .unwrap_or_default()
    }

    pub fn note_attempt(&self, key: &str) -> usize {
        let mut state = self.0.lock().expect("exchange lock poisoned");
        let attempts = state.place_attempts.entry(key.to_owned()).or_default();
        *attempts += 1;
        *attempts
    }

    pub fn accept(&self, key: &str, qty: i64) -> bool {
        let mut state = self.0.lock().expect("exchange lock poisoned");
        if state.orders.contains_key(key) {
            return false;
        }
        state.orders.insert(
            key.to_owned(),
            SimOrder {
                status: OrderStatus::Accepted,
            },
        );
        tracing::debug!(order_key = key, qty, "exchange accepted order");
        state.open.insert(key.to_owned());
        *state.accept_counts.entry(key.to_owned()).or_default() += 1;
        true
    }

    pub fn fill(&self, key: &str) -> bool {
        let mut state = self.0.lock().expect("exchange lock poisoned");
        let Some(order) = state.orders.get_mut(key) else {
            return false;
        };
        if order.status != OrderStatus::Accepted {
            return false;
        }
        order.status = OrderStatus::Filled;
        state.open.remove(key);
        true
    }

    pub fn query(&self, key: &str) -> QueryOutcome {
        self.0
            .lock()
            .expect("exchange lock poisoned")
            .orders
            .get(key)
            .map_or(QueryOutcome::NotFound, |order| {
                QueryOutcome::Found(order.status)
            })
    }

    pub fn cancel(&self, key: &str) -> CancelOutcome {
        let mut state = self.0.lock().expect("exchange lock poisoned");
        let Some(order) = state.orders.get_mut(key) else {
            return CancelOutcome::NotFound;
        };
        if order.status != OrderStatus::Accepted {
            return CancelOutcome::NotFound;
        }
        order.status = OrderStatus::Cancelled;
        state.open.remove(key);
        CancelOutcome::Cancelled
    }

    pub fn cancel_all(&self) -> Vec<OrderKey> {
        let mut state = self.0.lock().expect("exchange lock poisoned");
        let keys = state.open.drain().collect::<Vec<_>>();
        for key in &keys {
            if let Some(order) = state.orders.get_mut(key) {
                order.status = OrderStatus::Cancelled;
            }
        }
        keys
    }

    pub fn accept_count(&self, key: &str) -> usize {
        self.0
            .lock()
            .expect("exchange lock poisoned")
            .accept_counts
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    pub fn status(&self, key: &str) -> Option<OrderStatus> {
        self.0
            .lock()
            .expect("exchange lock poisoned")
            .orders
            .get(key)
            .map(|order| order.status)
    }
}

#[derive(tokio_otp::ActorFactory)]
pub struct VenueFeed {
    pub venue: VenueId,
    pub exchange: ExchangeSim,
    pub reconciler: ActorRef<ReconcilerMsg>,
    pub latency: LatencyRecorder,
}

impl Actor for VenueFeed {
    type Msg = FeedMsg;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, FeedMsg>) -> ActorResult {
        self.exchange.open_feed_session(self.venue);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: FeedMsg,
        _ctx: &mut HandleContext<'_, FeedMsg>,
    ) -> ActorResult {
        let started = Instant::now();
        match message {
            FeedMsg::Tick(snapshot) => {
                self.latency.record(
                    "queue.market",
                    started.saturating_duration_since(snapshot.enqueued_at),
                );
                // Simulated parsing makes the saturation phase outpace the
                // handler so both latest-wins mailboxes visibly conflate.
                tokio::time::sleep(Duration::from_millis(2)).await;
                self.reconciler
                    .send(ReconcilerMsg::Market(snapshot))
                    .await?;
            }
            FeedMsg::Crash => panic!("scripted venue failure: {}", self.venue),
        }
        self.latency.record("handler.feed", started.elapsed());
        Ok(Continue)
    }
}

#[derive(tokio_otp::ActorFactory)]
pub struct VenueGateway {
    venue: VenueId,
    exchange: ExchangeSim,
    ledger: ActorRef<LedgerMsg>,
    latency: LatencyRecorder,
    #[factory(default)]
    stalled_replies: Arc<Mutex<Vec<Reply<PlaceOutcome>>>>,
}

impl VenueGateway {
    fn stall(&self, reply: Reply<PlaceOutcome>) {
        self.stalled_replies
            .lock()
            .expect("stalled reply lock poisoned")
            .push(reply);
    }
}

impl Actor for VenueGateway {
    type Msg = GatewayMsg;

    async fn on_start(&mut self, _ctx: &mut StartContext<'_, GatewayMsg>) -> ActorResult {
        // Stalled replies belong to the actor session. A restart drops them,
        // allowing abandoned callers to observe the incarnation boundary.
        self.stalled_replies
            .lock()
            .expect("stalled reply lock poisoned")
            .clear();
        self.exchange.open_gateway_session(self.venue);
        Ok(Continue)
    }

    async fn handle(
        &mut self,
        message: GatewayMsg,
        ctx: &mut HandleContext<'_, GatewayMsg>,
    ) -> ActorResult {
        let started = Instant::now();
        match message {
            GatewayMsg::Place {
                key,
                symbol,
                qty,
                reply,
            } => {
                let attempt = self.exchange.note_attempt(&key);
                // Only the first attempt follows the scripted stall. A
                // reconciliation re-place uses the same idempotency key and
                // must be able to reach the exchange.
                if symbol == "STALL-NOACCEPT" && attempt == 1 {
                    self.stall(reply);
                    return Ok(Continue);
                }

                let inserted = self.exchange.accept(&key, qty);
                if symbol == "ACCEPT-NOACK" && attempt == 1 {
                    self.stall(reply);
                    return Ok(Continue);
                }

                reply.send(PlaceOutcome::Accepted { key: key.clone() });
                if inserted {
                    self.ledger
                        .send(LedgerMsg::Ack {
                            key: key.clone(),
                            venue: self.venue,
                        })
                        .await?;
                    if symbol != "OPEN" {
                        ctx.send_after(
                            GatewayMsg::DeliverFill {
                                key,
                                qty,
                                enqueued_at: Instant::now(),
                            },
                            Duration::from_millis(25),
                        );
                    }
                }
            }
            GatewayMsg::DeliverFill {
                key,
                qty,
                enqueued_at,
            } => {
                if self.exchange.fill(&key) {
                    self.ledger
                        .send(LedgerMsg::Fill {
                            key,
                            venue: self.venue,
                            qty,
                            enqueued_at,
                        })
                        .await?;
                }
            }
            GatewayMsg::Query { key, reply } => reply.send(self.exchange.query(&key)),
            GatewayMsg::Cancel { key, reply } => {
                let outcome = self.exchange.cancel(&key);
                if outcome == CancelOutcome::Cancelled {
                    self.ledger
                        .send(LedgerMsg::Cancelled {
                            key,
                            venue: self.venue,
                        })
                        .await?;
                }
                reply.send(outcome);
            }
            GatewayMsg::CancelAll { reply } => {
                let keys = self.exchange.cancel_all();
                let count = keys.len();
                for key in keys {
                    self.ledger
                        .send(LedgerMsg::Cancelled {
                            key,
                            venue: self.venue,
                        })
                        .await?;
                }
                reply.send(count);
            }
        }
        self.latency.record("handler.gateway", started.elapsed());
        Ok(Continue)
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}
