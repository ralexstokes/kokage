use std::error::Error;

use kokage::{Actor, ActorRef, ActorResult, MessageContext, Supervision};
use tokio::sync::mpsc;

enum FrontendMsg {
    Feed(String),
    Ack,
}

struct ParserMsg(String);
struct SinkMsg(String);

#[derive(Clone)]
struct Frontend {
    parser: ActorRef<ParserMsg>,
    acked: mpsc::UnboundedSender<()>,
}

impl Actor for Frontend {
    type Msg = FrontendMsg;

    async fn handle(
        &mut self,
        message: FrontendMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        match message {
            FrontendMsg::Feed(line) => self.parser.send(ParserMsg(line)).await?,
            FrontendMsg::Ack => self.acked.send(()).expect("receiver alive"),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Parser {
    frontend: ActorRef<FrontendMsg>,
    sink: ActorRef<SinkMsg>,
}

impl Actor for Parser {
    type Msg = ParserMsg;

    async fn handle(
        &mut self,
        message: ParserMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.sink.send(SinkMsg(message.0.to_uppercase())).await?;
        self.frontend.send(FrontendMsg::Ack).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct Sink {
    out: mpsc::UnboundedSender<String>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(
        &mut self,
        message: SinkMsg,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        self.out.send(message.0).expect("receiver alive");
        Ok(())
    }
}

#[derive(Supervision)]
struct Pipeline {
    frontend: Frontend,
    parser: Parser,
    sink: Sink,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (acked_tx, mut acked_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();

    let (tree, refs) = Pipeline::tree(|refs| PipelineFactories {
        frontend: {
            let refs = refs.clone();
            move || Frontend {
                parser: refs.parser.clone(),
                acked: acked_tx.clone(),
            }
        },
        parser: {
            let refs = refs.clone();
            move || Parser {
                frontend: refs.frontend.clone(),
                sink: refs.sink.clone(),
            }
        },
        sink: move || Sink {
            out: out_tx.clone(),
        },
    })?;
    let runtime = tree.spawn()?;

    refs.frontend
        .send(FrontendMsg::Feed("hello".to_owned()))
        .await?;
    println!(
        "sink observed {}",
        out_rx.recv().await.expect("sink output")
    );
    acked_rx.recv().await.expect("frontend ack");

    runtime.shutdown_and_wait().await?;
    Ok(())
}
