use std::error::Error;

use kokage::{Actor, ActorRef, ActorResult, ActorSpec, Context, DynamicTree};
use tokio::sync::mpsc;

#[derive(Clone)]
struct Frontend {
    rush: Option<ActorRef<String>>,
}

enum FrontendMsg {
    SetRushPress(ActorRef<String>),
    Order(String),
}

impl Actor for Frontend {
    type Msg = FrontendMsg;

    async fn handle(&mut self, message: FrontendMsg, _ctx: &mut Context<'_, Self>) -> ActorResult {
        match message {
            FrontendMsg::SetRushPress(rush) => self.rush = Some(rush),
            FrontendMsg::Order(order) => {
                self.rush
                    .as_ref()
                    .expect("rush press ref distributed before orders")
                    .send(order)
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RushPress {
    observed: mpsc::UnboundedSender<String>,
}

impl Actor for RushPress {
    type Msg = String;

    async fn handle(&mut self, order: String, _ctx: &mut Context<'_, Self>) -> ActorResult {
        self.observed.send(order).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

    let tree = DynamicTree::new();
    let dynamic = tree.handle();
    let runtime = tree.spawn()?;

    let orders = dynamic
        .add_actor(ActorSpec::new("front-desk", || Frontend { rush: None }))
        .await?;
    let rush = dynamic
        .add_actor(ActorSpec::new("rush-press", move || RushPress {
            observed: observed_tx.clone(),
        }))
        .await?;

    orders.send(FrontendMsg::SetRushPress(rush.clone())).await?;
    orders
        .send(FrontendMsg::Order("wedding invites x50".to_owned()))
        .await?;
    let observed = observed_rx.recv().await.expect("rush job");
    assert_eq!(observed, "wedding invites x50");
    println!("rush job {observed}");

    rush.send("vip banners x2".to_owned()).await?;
    let observed = observed_rx.recv().await.expect("rush job");
    assert_eq!(observed, "vip banners x2");
    println!("rush job {observed}");

    dynamic.remove_child("front-desk").await?;
    dynamic.remove_child("rush-press").await?;
    runtime.shutdown_and_wait().await?;
    Ok(())
}
