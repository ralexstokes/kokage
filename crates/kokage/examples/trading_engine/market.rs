use std::{collections::HashMap, time::Duration};

use kokage::{MonitorEventKind, TimerKey, observe::ExitStatus, prelude::*};
use tokio::time::Instant;

use crate::{
    protocol::{FeedMsg, MarketReport, ReconcilerMsg, VenueCondition, VenueId},
    venue::ExchangeSim,
};

pub const STALE_AFTER: Duration = Duration::from_millis(250);
const STALE_TIMER: TimerKey = TimerKey::new("market-staleness");

#[derive(Clone, Debug)]
struct VenueState {
    condition: VenueCondition,
    last_seen: Option<Instant>,
    sequence: Option<u64>,
    transitions: Vec<VenueCondition>,
}

impl Default for VenueState {
    fn default() -> Self {
        Self {
            condition: VenueCondition::Stale,
            last_seen: None,
            sequence: None,
            transitions: vec![VenueCondition::Stale],
        }
    }
}

#[derive(kokage::ActorFactory)]
pub struct MarketReconciler {
    pub feeds: HashMap<VenueId, ActorRef<FeedMsg>>,
    pub exchanges: Vec<(VenueId, ExchangeSim)>,
    #[factory(default)]
    venues: HashMap<VenueId, VenueState>,
    #[factory(default)]
    exits: HashMap<VenueId, Vec<ExitStatus>>,
}

impl MarketReconciler {
    fn transition(&mut self, venue: VenueId, condition: VenueCondition) {
        let state = self.venues.get_mut(venue).expect("known venue");
        if state.condition != condition {
            state.condition = condition;
            state.transitions.push(condition);
        }
    }

    fn rearm_staleness(&mut self, ctx: &mut Context<'_, Self>) {
        let now = Instant::now();
        let next_deadline = self
            .venues
            .values()
            .filter(|state| state.condition == VenueCondition::Fresh)
            .filter_map(|state| state.last_seen.map(|seen| seen + STALE_AFTER))
            .min();
        if let Some(deadline) = next_deadline {
            ctx.set_timeout(
                STALE_TIMER,
                ReconcilerMsg::StaleSweep,
                deadline.saturating_duration_since(now),
            );
        } else {
            ctx.clear_timer(STALE_TIMER);
        }
    }

    fn report(&self) -> MarketReport {
        MarketReport {
            conditions: self
                .venues
                .iter()
                .map(|(&venue, state)| (venue, state.condition))
                .collect(),
            sequences: self
                .venues
                .iter()
                .map(|(&venue, state)| (venue, state.sequence))
                .collect(),
            transitions: self
                .venues
                .iter()
                .map(|(&venue, state)| (venue, state.transitions.clone()))
                .collect(),
            exits: self.exits.clone(),
        }
    }
}

impl Actor for MarketReconciler {
    type Msg = ReconcilerMsg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        // The venues subtree was inserted before this singleton. Root startup
        // cannot pass this point until every venue actor completed on_start.
        for (venue, exchange) in &self.exchanges {
            assert!(exchange.feed_sessions() > 0, "{venue} feed is ready");
            assert!(exchange.gateway_sessions() > 0, "{venue} gateway is ready");
        }
        for (&venue, feed) in &self.feeds {
            self.venues.entry(venue).or_default();
            ctx.watch(feed, move |event| ReconcilerMsg::FeedLifecycle {
                venue,
                event,
            });
        }
        Ok(())
    }

    async fn handle(&mut self, message: ReconcilerMsg, ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ReconcilerMsg::Market(tick) => {
                let state = self.venues.get_mut(tick.venue).expect("known venue");
                state.last_seen = Some(Instant::now());
                state.sequence = Some(tick.sequence);
                tracing::debug!(
                    venue = tick.venue,
                    symbol = tick.symbol,
                    spread = tick.ask - tick.bid,
                    sequence = tick.sequence,
                    "market tick reconciled"
                );
                self.transition(tick.venue, VenueCondition::Fresh);
                self.rearm_staleness(ctx);
            }
            ReconcilerMsg::FeedLifecycle { venue, event } => {
                match event.kind {
                    MonitorEventKind::Started { .. } => {
                        let state = self.venues.get_mut(venue).expect("known venue");
                        state.last_seen = None;
                        self.transition(venue, VenueCondition::Stale);
                    }
                    MonitorEventKind::Exited { status, .. } => {
                        self.exits.entry(venue).or_default().push(status);
                        self.transition(venue, VenueCondition::Down);
                    }
                    MonitorEventKind::Removed { .. } => {
                        self.transition(venue, VenueCondition::Down);
                    }
                    MonitorEventKind::Lagged { dropped } => {
                        tracing::warn!(venue, dropped, "feed lifecycle watch lagged");
                    }
                    // `MonitorEventKind` is non-exhaustive. Started, Exited,
                    // Removed, and Lagged are the only current variants; a
                    // future informational variant does not change health.
                    _ => {}
                }
                self.rearm_staleness(ctx);
            }
            ReconcilerMsg::StaleSweep => {
                let now = Instant::now();
                let stale = self
                    .venues
                    .iter()
                    .filter_map(|(&venue, state)| {
                        (state.condition == VenueCondition::Fresh
                            && state
                                .last_seen
                                .is_some_and(|seen| now.duration_since(seen) >= STALE_AFTER))
                        .then_some(venue)
                    })
                    .collect::<Vec<_>>();
                for venue in stale {
                    self.transition(venue, VenueCondition::Stale);
                }
                self.rearm_staleness(ctx);
            }
            ReconcilerMsg::Report { reply } => reply.send(self.report()),
        }
        Ok(())
    }
}
