use std::error::Error;

use tokio::sync::mpsc;
use tokio_otp::{Actor, ActorRef, ActorResult, MessageContext, SupervisionTree};

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

    async fn handle(
        &mut self,
        message: FrontendMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
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

    async fn handle(&mut self, order: String, _ctx: &mut MessageContext<'_, Self>) -> ActorResult {
        self.observed.send(order).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

    let runtime = SupervisionTree::dynamic().build()?;
    let handle = runtime.spawn();

    let orders = handle
        .add_actor("front-desk", || Frontend { rush: None })
        .await?;
    let rush = handle
        .add_actor("rush-press", move || RushPress {
            observed: observed_tx.clone(),
        })
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

    handle.remove_child("front-desk").await?;
    handle.remove_child("rush-press").await?;
    handle.shutdown_and_wait().await?;
    Ok(())
}
