use kokage::{
    Actor, ActorOptions, ActorRef, ActorResult, GraphBuildError, GraphBuilder, MailboxMode,
    MessageContext, Supervision, TrySendError,
    host::{ActorContext, RawActor},
};
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
async fn derived_tree_runs_cyclic_pipeline() {
    let (acks_tx, mut acks_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();

    let (tree, refs) = Pipeline::tree(|refs| PipelineFactories {
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
    .expect("valid tree");

    let cloned_refs = refs.clone();
    assert_eq!(refs.frontend.id(), "frontend");
    assert_eq!(cloned_refs.parser.id(), "parser");
    assert_eq!(refs.sink.id(), "sink");
    let handle = tree.spawn().expect("tree builds");
    handle.wait_started().await.expect("runtime starts");

    refs.frontend
        .send(FrontendMsg::Feed("hello".to_owned()))
        .await
        .expect("send feed");

    assert_eq!(out_rx.recv().await.as_deref(), Some("HELLO"));
    assert_eq!(acks_rx.recv().await, Some(()));

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn unfilled_slot_is_a_build_error() {
    let (out_tx, _out_rx) = mpsc::unbounded_channel();

    let mut builder = GraphBuilder::new();
    let (_slot, _sink_ref) = builder.slot::<SinkMsg>("sink");
    let (defined_slot, _) = builder.slot::<SinkMsg>("defined");
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
    let (_a, _) = builder.slot::<SinkMsg>("sink");
    let (_b, _) = builder.slot::<SinkMsg>("sink");

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
    let (foreign_slot, _) = other.slot::<SinkMsg>("sink");

    let mut builder = GraphBuilder::new();
    let (_own_slot, _) = builder.slot::<SinkMsg>("sink");
    builder.define(foreign_slot, move || Sink {
        out: out_tx.clone(),
    });

    assert!(matches!(builder.build(), Err(GraphBuildError::ForeignSlot)));
}

#[derive(Clone)]
struct Park;

impl RawActor for Park {
    type Msg = ();

    async fn run(&mut self, ctx: ActorContext<()>) -> ActorResult {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[derive(Supervision)]
struct ParkGraph {
    park: Park,
}

struct SizedMessage(Vec<u8>);

fn sized_message_size(message: &SizedMessage) -> usize {
    message.0.len()
}

#[derive(Clone)]
struct OptionsActor;

impl RawActor for OptionsActor {
    type Msg = SizedMessage;

    async fn run(&mut self, ctx: ActorContext<SizedMessage>) -> ActorResult {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[derive(Supervision)]
struct OptionsGraph {
    #[supervision(options = ActorOptions::new().mailbox(MailboxMode::conflate()))]
    mailbox_only: OptionsActor,
    #[supervision(options = ActorOptions::new().message_size(sized_message_size))]
    message_size_only: OptionsActor,
    #[supervision(
        options = ActorOptions::new()
            .mailbox(MailboxMode::conflate())
            .message_size(sized_message_size)
    )]
    combined: OptionsActor,
    defaults: OptionsActor,
}

#[tokio::test]
async fn derived_tree_applies_per_actor_options() {
    let (tree, refs) = OptionsGraph::tree(|_| OptionsGraphFactories {
        mailbox_only: || OptionsActor,
        message_size_only: || OptionsActor,
        combined: || OptionsActor,
        defaults: || OptionsActor,
    })
    .expect("options tree builds");
    let handle = tree.spawn().expect("tree builds");
    handle.wait_started().await.expect("runtime starts");

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

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn tree_with_applies_graph_builder_settings() {
    let mut park = None;
    let mut builder = GraphBuilder::new();
    builder.name("configured").mailbox_capacity(1);
    let (tree, _refs) = ParkGraph::tree_with(builder, |refs| {
        park = Some(refs.park.clone());
        ParkGraphFactories { park: || Park }
    })
    .expect("configured graph builds");

    let park = park.expect("wiring closure captured park ref");
    let handle = tree.spawn().expect("tree builds");
    handle.wait_started().await.expect("runtime starts");

    park.send(()).await.expect("first message fits");
    assert!(matches!(
        park.try_send(()),
        Err(TrySendError::Full { actor_id, .. }) if actor_id == "park"
    ));

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn tree_with_reports_invalid_graph_builder_settings() {
    let mut builder = GraphBuilder::new();
    builder.mailbox_capacity(0);
    assert!(matches!(
        ParkGraph::tree_with(builder, |_| { ParkGraphFactories { park: || Park } }),
        Err(GraphBuildError::ZeroMailboxCapacity)
    ));
}

#[test]
fn empty_slot_name_records_empty_name_error_and_detaches() {
    let mut builder = GraphBuilder::new();
    let (slot, actor_ref) = builder.slot::<()>("");
    assert_eq!(actor_ref.id(), "");
    builder.define(slot, || Park);
    let (actor_slot, _) = builder.slot("real");
    builder.define(actor_slot, || Park);

    assert!(matches!(
        builder.build(),
        Err(GraphBuildError::EmptyActorId)
    ));
}

#[test]
fn define_on_duplicate_detached_token_does_not_corrupt_first_slot() {
    let mut builder = GraphBuilder::new();
    let (first_slot, _first_ref) = builder.slot::<()>("park");
    let (dup_slot, _dup_ref) = builder.slot::<()>("park");

    builder.define(first_slot, || Park);
    builder.define(dup_slot, || Park);

    match builder.build() {
        Err(GraphBuildError::DuplicateActorId { actor_id, .. }) => assert_eq!(actor_id, "park"),
        other => panic!("expected DuplicateActorId, got {other:?}"),
    }
}
