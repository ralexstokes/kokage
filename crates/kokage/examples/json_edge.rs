//! Feeds newline-delimited JSON from a byte-oriented edge into a typed actor.

use std::io::{BufRead, Cursor};

use kokage::{Actor, ActorResult, DrainPolicy, GraphBuilder, MessageContext, OrderedTree};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    item: String,
    quantity: u64,
}

#[derive(Clone)]
struct Printer;

impl Actor for Printer {
    type Msg = Order;

    async fn handle(&mut self, order: Order, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        println!("{} x {}", order.quantity, order.item);
        Ok(())
    }

    fn drain_policy(&self) -> DrainPolicy {
        // Print every accepted order before the runtime shuts down.
        DrainPolicy::Drain
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphBuilder::new();
    let printer = graph.actor("Printer", || Printer);
    let runtime = OrderedTree::graph(graph.build()?).spawn()?;

    // A socket or file framing layer can supply the same byte slices.
    let input = b"{\"item\":\"labels\",\"quantity\":4}\n{\"item\":\"boxes\",\"quantity\":2}\n";
    for frame in Cursor::new(input).split(b'\n') {
        let frame = frame?;
        if !frame.is_empty() {
            let order = serde_json::from_slice(&frame)?;
            printer.send(order).await?;
        }
    }

    runtime.shutdown_and_wait().await?;
    Ok(())
}
