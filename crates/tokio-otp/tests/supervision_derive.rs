use tokio::{sync::mpsc, task::JoinHandle};
use tokio_otp::{
    Actor, ActorContext, ActorOptions, ActorRef, ActorResult, ActorRunError,
    DEFAULT_SHUTDOWN_BOUND, Graph, GraphBuildError, GraphBuilder, MailboxMode, MessageContext,
    MessageSize, RawActor, RestartPolicy, SendError, Supervision, prelude::Continue,
};
use tokio_util::sync::CancellationToken;

fn start_graph(
    graph: &Graph,
) -> (
    CancellationToken,
    Vec<JoinHandle<Result<(), ActorRunError>>>,
) {
    let stop = CancellationToken::new();
    let tasks = graph
        .actors()
        .iter()
        .cloned()
        .map(|actor| {
            let stop = stop.clone();
            tokio::spawn(async move {
                actor
                    .run_until(
                        stop.cancelled(),
                        RestartPolicy::Never,
                        DEFAULT_SHUTDOWN_BOUND,
                    )
                    .await
            })
        })
        .collect();
    (stop, tasks)
}

async fn stop_graph(stop: CancellationToken, tasks: Vec<JoinHandle<Result<(), ActorRunError>>>) {
    stop.cancel();
    for task in tasks {
        task.await
            .expect("actor task joined")
            .expect("actor stopped cleanly");
    }
}

enum FrontendMsg {
    Feed(String),
    Ack,
}

struct ParserMsg(String);

struct SinkMsg(String);

#[derive(Clone)]
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
        Ok(Continue)
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
        Ok(Continue)
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
        self.out.send(message.0).expect("test receiver alive");
        Ok(Continue)
    }
}

#[derive(Supervision)]
struct Pipeline {
    frontend: Frontend,
    parser: Parser,
    sink: Sink,
}

#[tokio::test]
async fn derived_graph_runs_cyclic_pipeline() {
    let (acks_tx, mut acks_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();

    let (graph, refs) = Pipeline::graph(|refs| PipelineFactories {
        frontend: {
            let refs = refs.clone();
            move || Frontend {
                parser: refs.parser.clone(),
                acks: acks_tx.clone(),
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
    })
    .expect("valid graph");

    let cloned_refs = refs.clone();
    assert_eq!(refs.frontend.id(), "frontend");
    assert_eq!(cloned_refs.parser.id(), "parser");
    assert_eq!(refs.sink.id(), "sink");
    let (stop, tasks) = start_graph(&graph);

    refs.frontend
        .send(FrontendMsg::Feed("hello".to_owned()))
        .await
        .expect("send feed");

    assert_eq!(out_rx.recv().await.as_deref(), Some("HELLO"));
    assert_eq!(acks_rx.recv().await, Some(()));

    stop_graph(stop, tasks).await;
}

#[test]
fn unfilled_slot_is_a_build_error() {
    let (out_tx, _out_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    let (_slot, _sink_ref) = builder.slot::<SinkMsg>("sink", tokio_otp::ActorOptions::new());
    let (defined_slot, _) = builder.slot::<SinkMsg>("defined", tokio_otp::ActorOptions::new());
    builder.define(defined_slot, move || Sink {
        out: out_tx.clone(),
    });

    match builder.build() {
        Err(GraphBuildError::MissingActor { actor_id, .. }) => assert_eq!(actor_id, "sink"),
        Ok(_) => panic!("expected MissingActor, got valid graph"),
        Err(error) => panic!("expected MissingActor, got {error:?}"),
    }
}

#[test]
fn duplicate_slot_name_is_a_build_error() {
    let mut builder = GraphBuilder::new();
    let (_a, _) = builder.slot::<SinkMsg>("sink", tokio_otp::ActorOptions::new());
    let (_b, _) = builder.slot::<SinkMsg>("sink", tokio_otp::ActorOptions::new());

    match builder.build() {
        Err(GraphBuildError::DuplicateActorId { actor_id, .. }) => assert_eq!(actor_id, "sink"),
        Ok(_) => panic!("expected DuplicateActorId, got valid graph"),
        Err(error) => panic!("expected DuplicateActorId, got {error:?}"),
    }
}

#[test]
fn slot_token_from_another_builder_is_a_build_error() {
    let (out_tx, _out_rx) = mpsc::unbounded_channel();

    let mut other = GraphBuilder::new();
    let (foreign_slot, _) = other.slot::<SinkMsg>("sink", tokio_otp::ActorOptions::new());

    let mut builder = GraphBuilder::new();
    let (_own_slot, _) = builder.slot::<SinkMsg>("sink", tokio_otp::ActorOptions::new());
    builder.define(foreign_slot, move || Sink {
        out: out_tx.clone(),
    });

    assert!(matches!(
        builder.build(),
        Err(GraphBuildError::InvalidConfig(
            "actor slot belongs to a different graph builder"
        ))
    ));
}

#[derive(Clone)]
struct Park;

impl RawActor for Park {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        ctx.shutdown_token().cancelled().await;
        Ok(Continue)
    }
}

#[derive(Supervision)]
struct ParkGraph {
    park: Park,
}

struct SizedMessage(Vec<u8>);

impl MessageSize for SizedMessage {
    fn size_hint(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone)]
struct OptionsActor;

impl RawActor for OptionsActor {
    type Msg = SizedMessage;

    async fn run(&mut self, ctx: ActorContext<SizedMessage>) -> ActorResult {
        ctx.shutdown_token().cancelled().await;
        Ok(Continue)
    }
}

#[derive(Supervision)]
struct OptionsGraph {
    #[supervision(options = ActorOptions::new().mailbox(MailboxMode::Conflate))]
    mailbox_only: OptionsActor,
    #[supervision(options = ActorOptions::new().message_size())]
    message_size_only: OptionsActor,
    #[supervision(
        options = ActorOptions::new()
            .mailbox(MailboxMode::Conflate)
            .message_size()
    )]
    combined: OptionsActor,
    defaults: OptionsActor,
}

#[tokio::test]
async fn derived_graph_applies_per_actor_options() {
    let (graph, refs) = OptionsGraph::graph(|_| OptionsGraphFactories {
        mailbox_only: || OptionsActor,
        message_size_only: || OptionsActor,
        combined: || OptionsActor,
        defaults: || OptionsActor,
    })
    .expect("options graph builds");
    let (stop, tasks) = start_graph(&graph);
    tokio::task::yield_now().await;

    refs.mailbox_only
        .try_send(SizedMessage(vec![0; 2]))
        .expect("conflating mailbox accepts first message");
    refs.mailbox_only
        .try_send(SizedMessage(vec![0; 3]))
        .expect("conflating mailbox replaces unread message");
    refs.message_size_only
        .try_send(SizedMessage(vec![0; 5]))
        .expect("sized queue accepts message");
    refs.combined
        .try_send(SizedMessage(vec![0; 7]))
        .expect("combined mailbox accepts first message");
    refs.combined
        .try_send(SizedMessage(vec![0; 11]))
        .expect("combined mailbox replaces unread message");

    assert_eq!(refs.mailbox_only.stats().messages_conflated, 1);
    assert_eq!(refs.mailbox_only.stats().message_bytes_accepted, None);
    assert_eq!(refs.message_size_only.stats().messages_conflated, 0);
    assert_eq!(
        refs.message_size_only.stats().message_bytes_accepted,
        Some(5)
    );
    assert_eq!(refs.combined.stats().messages_conflated, 1);
    assert_eq!(refs.combined.stats().message_bytes_accepted, Some(18));
    assert_eq!(refs.defaults.stats().messages_conflated, 0);
    assert_eq!(refs.defaults.stats().message_bytes_accepted, None);

    stop_graph(stop, tasks).await;
}

#[tokio::test]
async fn graph_with_applies_builder_config() {
    let mut builder = GraphBuilder::new();
    builder.name("configured");
    builder.mailbox_capacity(1);

    let mut park = None;
    let (graph, _refs) = ParkGraph::graph_with(builder, |refs| {
        park = Some(refs.park.clone());
        ParkGraphFactories { park: || Park }
    })
    .expect("configured graph builds");
    assert_eq!(graph.name(), "configured");

    let park = park.expect("wiring closure captured park ref");
    let (stop, tasks) = start_graph(&graph);

    park.send(()).await.expect("first message fits");
    assert!(matches!(
        park.try_send(()),
        Err(SendError::MailboxFull { actor_id , .. }) if actor_id == "park"
    ));

    stop_graph(stop, tasks).await;
}

#[test]
fn graph_with_reports_field_name_collision_with_pre_registered_actor() {
    let mut builder = GraphBuilder::new();
    let (actor_slot, _) = builder.slot("park", tokio_otp::ActorOptions::new());
    builder.define(actor_slot, || Park);

    match ParkGraph::graph_with(builder, |_| ParkGraphFactories { park: || Park }) {
        Err(GraphBuildError::DuplicateActorId { actor_id, .. }) => assert_eq!(actor_id, "park"),
        Ok(_) => panic!("expected DuplicateActorId, got valid graph"),
        Err(error) => panic!("expected DuplicateActorId, got {error:?}"),
    }
}

#[test]
fn empty_slot_name_records_invalid_config_and_detaches() {
    let mut builder = GraphBuilder::new();
    let (slot, actor_ref) = builder.slot::<()>("", tokio_otp::ActorOptions::new());
    assert_eq!(actor_ref.id(), "");
    builder.define(slot, || Park);
    let (actor_slot, _) = builder.slot("real", tokio_otp::ActorOptions::new());
    builder.define(actor_slot, || Park);

    match builder.build() {
        Err(GraphBuildError::InvalidConfig(msg)) => {
            assert_eq!(msg, "actor id must not be empty")
        }
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}

#[test]
fn define_on_duplicate_detached_token_does_not_corrupt_first_slot() {
    let mut builder = GraphBuilder::new();
    let (first_slot, _first_ref) = builder.slot::<()>("park", tokio_otp::ActorOptions::new());
    let (dup_slot, _dup_ref) = builder.slot::<()>("park", tokio_otp::ActorOptions::new());

    builder.define(first_slot, || Park);
    builder.define(dup_slot, || Park);

    match builder.build() {
        Err(GraphBuildError::DuplicateActorId { actor_id, .. }) => assert_eq!(actor_id, "park"),
        other => panic!("expected DuplicateActorId, got {other:?}"),
    }
}
