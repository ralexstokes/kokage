//! A small trading engine that introduces Kokage through one end-to-end story.
//!
//! Quotes cross five independently supervised actors:
//!
//! ```text
//! market data -> strategy -> risk manager -> venue -> ledger
//! ```
//!
//! The example then disconnects the venue. Its supervisor starts a fresh
//! incarnation, while the `ActorRef<VenueMsg>` already held by the risk manager
//! reconnects automatically. A second quote proves that the application keeps
//! trading through the restart.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p kokage --example trading_engine
//! ```
//!
//! See `trading_engine_acceptance` for the exhaustive multi-venue scenario used
//! as a regression and observability fixture.

mod actors;
mod domain;

use std::{error::Error, io, time::Duration};

use actors::{
    Ledger, LedgerMsg, MarketData, MarketDataMsg, RiskManager, Strategy, Venue, VenueMsg,
};
use domain::{Fill, Quote};
use kokage::{ActorSpec, GraphBuilder, OrderedTree};
use tokio::sync::mpsc;

const STEP_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(5), run()).await??;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let (filled_tx, mut filled_rx) = mpsc::unbounded_channel();
    let mut graph = GraphBuilder::new();

    // Build from the effects inward. Every `actor` call returns a typed,
    // restart-stable ref that later factories can capture.
    let ledger = graph.actor(ActorSpec::new("ledger", move || {
        Ledger::new(filled_tx.clone())
    }));
    let venue = graph.actor(ActorSpec::new("venue", {
        let ledger = ledger.clone();
        move || Venue::new(ledger.clone())
    }));
    let risk = graph.actor(ActorSpec::new("risk", {
        let venue = venue.clone();
        move || RiskManager::new(venue.clone(), 20)
    }));
    let strategy = graph.actor(ActorSpec::new("strategy", {
        let risk = risk.clone();
        move || Strategy::new(risk.clone(), 101)
    }));
    let market = graph.actor(ActorSpec::new("market-data", {
        let strategy = strategy.clone();
        move || MarketData::new(strategy.clone())
    }));

    let runtime = OrderedTree::graph(graph.build()?).spawn()?;
    let handle = runtime.handle();
    handle.wait_started().await?;

    market
        .send(MarketDataMsg::Publish(Quote {
            symbol: "KOKG",
            bid: 100,
            ask: 101,
        }))
        .await?;
    let first = next_fill(&mut filled_rx).await?;
    println!("ledger: recorded {}\n", describe(&first));

    // Subscribe before inducing the failure so the restart cannot race the
    // observer. Snapshot generations make the wait deterministic.
    let baseline = handle
        .snapshot()
        .child("venue")
        .expect("venue is declared")
        .generation;
    let mut snapshots = handle.subscribe_snapshots();
    println!("exchange link lost; the venue actor will restart");
    venue.send(VenueMsg::Disconnect).await?;
    snapshots
        .wait_for_child("venue", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .map_err(|_| io::Error::other("venue restart could not be observed"))?;
    println!("venue restarted; existing actor refs remain valid\n");

    market
        .send(MarketDataMsg::Publish(Quote {
            symbol: "KOKG",
            bid: 98,
            ask: 99,
        }))
        .await?;
    let second = next_fill(&mut filled_rx).await?;
    println!("ledger: recorded {}", describe(&second));

    let fills = ledger.call(STEP_TIMEOUT, LedgerMsg::Snapshot).await?;
    println!(
        "\nsummary: {} fills survived the venue restart",
        fills.len()
    );

    runtime.shutdown_and_wait().await?;
    Ok(())
}

async fn next_fill(fills: &mut mpsc::UnboundedReceiver<Fill>) -> Result<Fill, Box<dyn Error>> {
    tokio::time::timeout(STEP_TIMEOUT, fills.recv())
        .await?
        .ok_or_else(|| io::Error::other("ledger stopped before recording the fill").into())
}

fn describe(fill: &Fill) -> String {
    format!(
        "order {}: {} {} at {}",
        fill.order_id, fill.quantity, fill.symbol, fill.price
    )
}
