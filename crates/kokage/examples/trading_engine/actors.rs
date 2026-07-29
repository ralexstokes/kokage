//! Actor protocols and implementations for the trading engine.

use std::io;

use kokage::{DrainPolicy, prelude::*};
use tokio::sync::mpsc;

use crate::domain::{Fill, Order, Quote};

pub enum MarketDataMsg {
    Publish(Quote),
}

pub enum StrategyMsg {
    Quote(Quote),
}

pub enum RiskMsg {
    Submit(Order),
}

pub enum VenueMsg {
    Execute(Order),
    Disconnect,
}

pub enum LedgerMsg {
    Record(Fill),
    Snapshot(Reply<Vec<Fill>>),
}

pub struct MarketData {
    strategy: ActorRef<StrategyMsg>,
}

impl MarketData {
    pub fn new(strategy: ActorRef<StrategyMsg>) -> Self {
        Self { strategy }
    }
}

impl Actor for MarketData {
    type Msg = MarketDataMsg;

    async fn handle(
        &mut self,
        message: MarketDataMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let MarketDataMsg::Publish(quote) = message;
        println!(
            "market: {} bid={} ask={}",
            quote.symbol, quote.bid, quote.ask
        );
        self.strategy.send(StrategyMsg::Quote(quote)).await?;
        Ok(())
    }
}

pub struct Strategy {
    risk: ActorRef<RiskMsg>,
    next_order_id: u64,
    buy_at_or_below: u64,
}

impl Strategy {
    pub fn new(risk: ActorRef<RiskMsg>, buy_at_or_below: u64) -> Self {
        Self {
            risk,
            next_order_id: 1,
            buy_at_or_below,
        }
    }
}

impl Actor for Strategy {
    type Msg = StrategyMsg;

    async fn handle(
        &mut self,
        message: StrategyMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let StrategyMsg::Quote(quote) = message;
        if quote.ask > self.buy_at_or_below {
            println!("strategy: no trade");
            return Ok(());
        }

        let order = Order {
            id: self.next_order_id,
            symbol: quote.symbol,
            quantity: 10,
            limit: quote.ask,
        };
        self.next_order_id += 1;
        println!(
            "strategy: buy {} {} at {} (order {})",
            order.quantity, order.symbol, order.limit, order.id
        );
        self.risk.send(RiskMsg::Submit(order)).await?;
        Ok(())
    }
}

pub struct RiskManager {
    venue: ActorRef<VenueMsg>,
    position: i64,
    max_position: i64,
}

impl RiskManager {
    pub fn new(venue: ActorRef<VenueMsg>, max_position: i64) -> Self {
        Self {
            venue,
            position: 0,
            max_position,
        }
    }
}

impl Actor for RiskManager {
    type Msg = RiskMsg;

    async fn handle(
        &mut self,
        message: RiskMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let RiskMsg::Submit(order) = message;
        let projected = self.position + order.quantity;
        if projected.abs() > self.max_position {
            println!("risk: rejected order {} (position limit)", order.id);
            return Ok(());
        }

        self.position = projected;
        println!("risk: approved order {}", order.id);
        self.venue.send(VenueMsg::Execute(order)).await?;
        Ok(())
    }
}

pub struct Venue {
    ledger: ActorRef<LedgerMsg>,
}

impl Venue {
    pub fn new(ledger: ActorRef<LedgerMsg>) -> Self {
        Self { ledger }
    }
}

impl Actor for Venue {
    type Msg = VenueMsg;

    async fn handle(
        &mut self,
        message: VenueMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            VenueMsg::Execute(order) => {
                println!("venue: filled order {}", order.id);
                self.ledger
                    .send(LedgerMsg::Record(Fill {
                        order_id: order.id,
                        symbol: order.symbol,
                        quantity: order.quantity,
                        price: order.limit,
                    }))
                    .await?;
                Ok(())
            }
            VenueMsg::Disconnect => Err(io::Error::other("venue disconnected").into()),
        }
    }
}

pub struct Ledger {
    fills: Vec<Fill>,
    observed: mpsc::UnboundedSender<Fill>,
}

impl Ledger {
    pub fn new(observed: mpsc::UnboundedSender<Fill>) -> Self {
        Self {
            fills: Vec::new(),
            observed,
        }
    }
}

impl Actor for Ledger {
    type Msg = LedgerMsg;

    async fn handle(
        &mut self,
        message: LedgerMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            LedgerMsg::Record(fill) => {
                self.fills.push(fill.clone());
                self.observed.send(fill).expect("example receiver alive");
            }
            LedgerMsg::Snapshot(reply) => reply.send(self.fills.clone()),
        }
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        DrainPolicy::Drain
    }
}
