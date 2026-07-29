//! Feeds newline-delimited JSON from a byte-oriented edge into a typed actor.

use std::io::{BufRead, Cursor};

use kokage::{Actor, ActorResult, ActorSpec, Context, OrderedTree, Shutdown};
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

    async fn handle(&mut self, order: Order, _ctx: &mut Context<'_, Self>) -> ActorResult {
        println!("{} x {}", order.quantity, order.item);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let printer_spec = ActorSpec::new("Printer", || Printer)
        .shutdown(Shutdown::drain_for(std::time::Duration::from_secs(5)));
    let (printer_spec, printer) = printer_spec.actor_ref();
    let runtime = OrderedTree::new().actor(printer_spec).spawn()?;

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
