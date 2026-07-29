use std::marker::PhantomData;

use kokage::{
    ActorResult, ActorSlot, ActorSpec, GraphBuildError, GraphBuilder,
    host::{ActorContext, RawActor},
};

struct Idle<M>(PhantomData<fn(M)>);

impl<M> Idle<M> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Idle<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: ActorContext<M>) -> ActorResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

fn report(label: &str, result: Result<kokage::Graph, GraphBuildError>) {
    match result {
        Ok(_) => panic!("{label} unexpectedly built"),
        Err(error) => println!("{label}: {error}"),
    }
}

fn main() {
    report("empty graph", GraphBuilder::new().build());

    let mut zero_capacity = GraphBuilder::new();
    zero_capacity.mailbox_capacity(0);
    zero_capacity.actor(ActorSpec::new("worker", Idle::<()>::new));
    report("zero mailbox capacity", zero_capacity.build());

    let mut duplicate = GraphBuilder::new();
    duplicate.actor(ActorSpec::new("worker", Idle::<()>::new));
    duplicate.actor(ActorSpec::new("worker", Idle::<()>::new));
    report("duplicate actor ids", duplicate.build());

    let mut empty_id = GraphBuilder::new();
    empty_id.actor(ActorSpec::new("", Idle::<()>::new));
    report("empty actor id", empty_id.build());

    let mut empty_name = GraphBuilder::new();
    empty_name.name("");
    empty_name.actor(ActorSpec::new("worker", Idle::<()>::new));
    report("empty graph name", empty_name.build());

    // Message-type mismatches now fail at compile time:
    //
    // let mut builder = GraphBuilder::new();
    // let slot = ActorSlot::<u32>::new("worker");
    // let _worker = slot.actor_ref();
    // builder.define(slot, Idle::<String>::new);
    //
    // Reusing a slot token also fails at compile time because `define`
    // consumes it:
    //
    // let slot = ActorSlot::<String>::new("worker");
    // let _worker = slot.actor_ref();
    // builder.define(slot, Idle::<String>::new);
    // builder.define(slot, Idle::<String>::new);

    // Merely opening and dropping an ActorSlot cannot leave a partial graph:
    // a slot is only registered when it is consumed by `define`.
    let _unregistered = ActorSlot::<String>::new("ghost");
}
