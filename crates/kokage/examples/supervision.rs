use std::error::Error;

use kokage::{Actor, ActorRef, ActorSlot, Context, ExitResult, Tree};
use tokio::sync::mpsc;

enum FrontendMsg {
    Feed(String),
    Ack,
}

struct ParserMsg(String);
struct SinkMsg(String);

struct Frontend {
    parser: ActorRef<ParserMsg>,
    acked: mpsc::UnboundedSender<()>,
}

impl Actor for Frontend {
    type Msg = FrontendMsg;

    async fn handle(&mut self, message: FrontendMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        match message {
            FrontendMsg::Feed(line) => self.parser.send(ParserMsg(line)).await?,
            FrontendMsg::Ack => self.acked.send(()).expect("receiver alive"),
        }
        Ok(())
    }
}

struct Parser {
    frontend: ActorRef<FrontendMsg>,
    sink: ActorRef<SinkMsg>,
}

impl Actor for Parser {
    type Msg = ParserMsg;

    async fn handle(&mut self, message: ParserMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.sink.send(SinkMsg(message.0.to_uppercase())).await?;
        self.frontend.send(FrontendMsg::Ack).await?;
        Ok(())
    }
}

struct Sink {
    out: mpsc::UnboundedSender<String>,
}

impl Actor for Sink {
    type Msg = SinkMsg;

    async fn handle(&mut self, message: SinkMsg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.out.send(message.0).expect("receiver alive");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (acked_tx, mut acked_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();

    let frontend_slot = ActorSlot::<FrontendMsg>::new("frontend");
    let frontend = frontend_slot.actor_ref();
    let parser_slot = ActorSlot::<ParserMsg>::new("parser");
    let parser = parser_slot.actor_ref();
    let sink_slot = ActorSlot::<SinkMsg>::new("sink");
    let sink = sink_slot.actor_ref();

    let frontend_actor = frontend_slot.define({
        let parser = parser.clone();
        move || Frontend {
            parser: parser.clone(),
            acked: acked_tx.clone(),
        }
    });
    let parser_actor = parser_slot.define({
        let frontend = frontend.clone();
        let sink = sink.clone();
        move || Parser {
            frontend: frontend.clone(),
            sink: sink.clone(),
        }
    });
    let sink_actor = sink_slot.define(move || Sink {
        out: out_tx.clone(),
    });
    let mut tree = Tree::new();
    tree.add_actor_spec(frontend_actor);
    tree.add_actor_spec(parser_actor);
    tree.add_actor_spec(sink_actor);
    let runtime = tree.spawn()?;

    frontend.send(FrontendMsg::Feed("hello".to_owned())).await?;
    println!(
        "sink observed {}",
        out_rx.recv().await.expect("sink output")
    );
    acked_rx.recv().await.expect("frontend ack");

    runtime.shutdown().await?;
    Ok(())
}
