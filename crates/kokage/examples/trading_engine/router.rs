use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use kokage::{CallError, prelude::*};

use crate::messages::{
    CALL_DEADLINE, CancelOutcome, GatewayMsg, LedgerMsg, OrderKey, PlaceOutcome, QueryOutcome,
    RouterMsg, SubmitDisposition, SubmitResult, VenueId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntentState {
    /// A pipelined `Place` call is still waiting on the gateway.
    Pending,
    /// The venue confirmed the order.
    Confirmed,
    /// The `Place` call timed out; only reconciliation can resolve it.
    Unknown,
}

#[derive(Clone, Debug)]
struct OrderIntent {
    venue: VenueId,
    symbol: &'static str,
    qty: i64,
    state: IntentState,
}

#[derive(Clone)]
pub struct OrderRouter {
    /// Shared across incarnations (like `intake_gate`), not per-incarnation
    /// state: order keys are idempotency keys and correlate pipelined
    /// resolutions, so they must never repeat after a router restart.
    sequence: Arc<AtomicU64>,
    intents: HashMap<OrderKey, OrderIntent>,
    gateways: HashMap<VenueId, ActorRef<GatewayMsg>>,
    ledger: ActorRef<LedgerMsg>,
    pub intake_gate: Arc<AtomicBool>,
}

impl OrderRouter {
    pub fn new(
        gateways: HashMap<VenueId, ActorRef<GatewayMsg>>,
        ledger: ActorRef<LedgerMsg>,
        intake_gate: Arc<AtomicBool>,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        Self {
            sequence,
            intents: HashMap::new(),
            gateways,
            ledger,
            intake_gate,
        }
    }
}

// The router is a fan-out actor, and an actor processes one message at a
// time: awaiting a gateway `call` inside `handle` would stop the mailbox for
// the full round-trip, so one stalled venue would block cancels and queries
// bound for healthy venues for up to CALL_DEADLINE — the actor-model
// equivalent of blocking in a `gen_server` callback. Submit and Cancel
// therefore pipeline the call: `handle` records intent and spawns the
// deadline-bounded call, and the continuation completes the caller's `Reply`
// and the router learns the outcome through an ordinary `SubmitResolved`
// message. The context owns the task and stamps its postback to this router
// incarnation, so stale resolutions are dropped across restart. Order keys
// remain because they correlate concurrent requests in the same incarnation.
// A resolution accepted by the old incarnation's mailbox but not yet handled is lost with it
// (at-most-once delivery); in a real system reconciliation would eventually
// resolve the intent it leaves pending.
impl Actor for OrderRouter {
    type Msg = RouterMsg;

    async fn handle(
        &mut self,
        message: RouterMsg,
        ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            RouterMsg::Submit {
                symbol,
                qty,
                venue,
                reply,
            } => {
                if !self.intake_gate.load(Ordering::Acquire) {
                    reply.send(SubmitResult::IntakeClosed);
                    return Ok(());
                }
                let key = format!("ord-{}", self.sequence.fetch_add(1, Ordering::Relaxed) + 1);
                self.intents.insert(
                    key.clone(),
                    OrderIntent {
                        venue,
                        symbol,
                        qty,
                        state: IntentState::Pending,
                    },
                );
                let gateway = self.gateways.get(venue).expect("known venue").clone();
                let message_key = key.clone();
                ctx.offload(
                    CALL_DEADLINE,
                    async move {
                        let result = gateway
                            .call(CALL_DEADLINE, |reply| GatewayMsg::Place {
                                key: key.clone(),
                                symbol,
                                qty,
                                reply,
                            })
                            .await;
                        let (disposition, submitted) = match result {
                            Ok(PlaceOutcome::Accepted { .. }) => (
                                SubmitDisposition::Confirmed,
                                SubmitResult::Placed(key.clone()),
                            ),
                            Err(CallError::Timeout { .. } | CallError::ReplyDropped { .. }) => (
                                SubmitDisposition::Unknown,
                                SubmitResult::Unknown(key.clone()),
                            ),
                            Err(_) => (SubmitDisposition::Rejected, SubmitResult::Rejected),
                        };
                        (disposition, submitted)
                    },
                    move |result| {
                        let (disposition, submitted) = result.unwrap_or_else(|_| {
                            (
                                SubmitDisposition::Unknown,
                                SubmitResult::Unknown(message_key.clone()),
                            )
                        });
                        RouterMsg::SubmitResolved {
                            key: message_key,
                            disposition,
                            submitted,
                            reply,
                        }
                    },
                );
            }
            RouterMsg::SubmitResolved {
                key,
                disposition,
                submitted,
                reply,
            } => {
                match disposition {
                    SubmitDisposition::Confirmed => {
                        if let Some(intent) = self.intents.get_mut(&key) {
                            intent.state = IntentState::Confirmed;
                        }
                    }
                    SubmitDisposition::Unknown => {
                        if let Some(intent) = self.intents.get_mut(&key) {
                            intent.state = IntentState::Unknown;
                        }
                    }
                    SubmitDisposition::Rejected => {
                        self.intents.remove(&key);
                    }
                }
                // Release the caller only after the state update, preserving
                // same-incarnation FIFO ordering for follow-up requests.
                reply.send(submitted);
            }
            RouterMsg::Cancel { key, reply } => {
                let Some(intent) = self.intents.get(&key) else {
                    reply.send(CancelOutcome::NotFound);
                    return Ok(());
                };
                // The exchange needs no router state after the venue lookup,
                // so the whole call pipelines off the handle loop.
                let gateway = self
                    .gateways
                    .get(intent.venue)
                    .expect("known venue")
                    .clone();
                ctx.offload(
                    CALL_DEADLINE,
                    async move {
                        match gateway
                            .call(CALL_DEADLINE, |reply| GatewayMsg::Cancel { key, reply })
                            .await
                        {
                            Ok(outcome) => outcome,
                            Err(_) => CancelOutcome::Unknown,
                        }
                    },
                    move |result| RouterMsg::CancelResolved {
                        outcome: result.unwrap_or(CancelOutcome::Unknown),
                        reply,
                    },
                );
            }
            RouterMsg::CancelResolved { outcome, reply } => reply.send(outcome),
            RouterMsg::ReconcileAll { reply } => {
                // Deliberately inline and serial, unlike Submit and Cancel:
                // reconciliation is an explicit operator-driven sweep run
                // while intake is quiet, and each query/re-place step mutates
                // intent state. Serializing the router for the sweep is the
                // accepted trade-off here; a router that had to stay live
                // during reconciliation would pipeline these calls too.
                let unknown = self
                    .intents
                    .iter()
                    .filter(|(_, intent)| intent.state == IntentState::Unknown)
                    .map(|(key, intent)| (key.clone(), intent.clone()))
                    .collect::<Vec<_>>();
                let mut resolved = 0;
                for (key, intent) in unknown {
                    let gateway = self.gateways.get(intent.venue).expect("known venue");
                    let query = gateway
                        .call(CALL_DEADLINE, |reply| GatewayMsg::Query {
                            key: key.clone(),
                            reply,
                        })
                        .await;
                    match query {
                        Ok(QueryOutcome::Found(_)) => {
                            self.intents.get_mut(&key).expect("known intent").state =
                                IntentState::Confirmed;
                            self.ledger
                                .send(LedgerMsg::Ack {
                                    key: key.clone(),
                                    venue: intent.venue,
                                })
                                .await?;
                            resolved += 1;
                        }
                        Ok(QueryOutcome::NotFound) => {
                            let placed = gateway
                                .call(CALL_DEADLINE, |reply| GatewayMsg::Place {
                                    key: key.clone(),
                                    symbol: intent.symbol,
                                    qty: intent.qty,
                                    reply,
                                })
                                .await;
                            if matches!(placed, Ok(PlaceOutcome::Accepted { .. })) {
                                self.intents.get_mut(&key).expect("known intent").state =
                                    IntentState::Confirmed;
                                resolved += 1;
                            }
                        }
                        Err(_) => {}
                    }
                }
                reply.send(resolved);
            }
            RouterMsg::InFlight { reply } => reply.send(
                self.intents
                    .values()
                    .filter(|intent| intent.state != IntentState::Confirmed)
                    .count(),
            ),
        }
        Ok(())
    }
}
