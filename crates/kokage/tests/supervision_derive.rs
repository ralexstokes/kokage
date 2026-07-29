use kokage::{
    Actor, ActorRef, ActorResult, ActorSpec, GraphBuilder, MessageContext, OrderedTree, Supervision,
};
use tokio::sync::mpsc;

enum FrontendMsg {
    Feed(String),
    Ack,
}

struct ParserMsg(String);
struct SinkMsg(String);

struct Frontend {
    parser: ActorRef<ParserMsg>,
    acks: mpsc::UnboundedSender<()>,
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
            FrontendMsg::Ack => self.acks.send(()).expect("test receiver alive"),
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
        self.out.send(message.0).expect("test receiver alive");
        Ok(())
    }
}

#[derive(Supervision)]
struct Pipeline {
    frontend: Frontend,
    parser: Parser,
    sink: Sink,
}

#[tokio::test]
async fn wire_runs_a_cyclic_pipeline_with_explicit_topology() {
    let (acks, mut acks_rx) = mpsc::unbounded_channel();
    let (out, mut out_rx) = mpsc::unbounded_channel();
    let mut builder = GraphBuilder::new();
    builder.name("pipeline");

    let refs = Pipeline::wire(&mut builder, |refs| PipelineFactories {
        frontend: {
            let parser = refs.parser.clone();
            move || Frontend {
                parser: parser.clone(),
                acks: acks.clone(),
            }
        },
        parser: {
            let frontend = refs.frontend.clone();
            let sink = refs.sink.clone();
            move || Parser {
                frontend: frontend.clone(),
                sink: sink.clone(),
            }
        },
        sink: move || Sink { out: out.clone() },
    });
    let graph = builder.build().expect("derived graph builds");

    assert_eq!(graph.name(), "pipeline");
    assert_eq!(refs.frontend.id(), "frontend");
    assert_eq!(refs.clone().parser.id(), "parser");
    let runtime = OrderedTree::graph(graph).spawn().expect("tree builds");
    runtime
        .handle()
        .wait_started()
        .await
        .expect("runtime starts");

    refs.frontend
        .send(FrontendMsg::Feed("hello".to_owned()))
        .await
        .expect("send feed");
    assert_eq!(out_rx.recv().await.as_deref(), Some("HELLO"));
    assert_eq!(acks_rx.recv().await, Some(()));

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

struct Park {}

impl Actor for Park {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

struct Worker {}

impl Actor for Worker {
    type Msg = ();

    async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
        Ok(())
    }
}

#[derive(Supervision)]
struct NamedGroup {
    #[supervision(label = "renamed")]
    worker: Worker,
}

#[test]
fn wire_composes_with_existing_builder_entries_and_leaves_topology_external() {
    let mut builder = GraphBuilder::new();
    let extra = builder.actor(ActorSpec::new("extra", || Park {}));
    let refs = NamedGroup::wire(&mut builder, |_| NamedGroupFactories {
        worker: || Worker {},
    });
    let graph = builder.build().expect("composed graph builds");

    assert_eq!(refs.worker.id(), "renamed");
    assert_eq!(extra.id(), "extra");
    let mut nodes = graph.into_nodes();
    assert_eq!(
        nodes.iter().map(|node| node.label()).collect::<Vec<_>>(),
        ["extra", "renamed"]
    );
    let derived = nodes.pop().expect("derived actor node").child_id("worker");
    let extra = nodes.pop().expect("external actor node");
    let tree = OrderedTree::new().actor(derived).actor(extra);
    assert_eq!(tree.outline().child_ids(), ["worker", "extra"]);
}
