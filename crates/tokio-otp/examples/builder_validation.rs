use std::marker::PhantomData;

use tokio_otp::{ActorContext, ActorResult, GraphBuildError, GraphBuilder, host::RawActor};

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

fn report(label: &str, result: Result<tokio_otp::Graph, GraphBuildError>) {
    match result {
        Ok(_) => panic!("{label} unexpectedly built"),
        Err(error) => println!("{label}: {error}"),
    }
}

fn main() {
    report("empty graph", GraphBuilder::new().build());

    let mut zero_capacity = GraphBuilder::new();
    zero_capacity.mailbox_capacity(0);
    let (actor_slot, _) = zero_capacity.slot("worker");
    zero_capacity.define(actor_slot, Idle::<()>::new);
    report("zero mailbox capacity", zero_capacity.build());

    let mut duplicate = GraphBuilder::new();
    let (actor_slot, _) = duplicate.slot("worker");
    duplicate.define(actor_slot, Idle::<()>::new);
    let (actor_slot, _) = duplicate.slot("worker");
    duplicate.define(actor_slot, Idle::<()>::new);
    report("duplicate actor ids", duplicate.build());

    let mut missing = GraphBuilder::new();
    let (_ghost_slot, _ghost_ref) = missing.slot::<String>("ghost");
    report("unfilled actor slot", missing.build());

    let mut empty_name = GraphBuilder::new();
    empty_name.name("");
    let (actor_slot, _) = empty_name.slot("worker");
    empty_name.define(actor_slot, Idle::<()>::new);
    report("empty graph name", empty_name.build());

    // Message-type mismatches now fail at compile time:
    //
    // let mut builder = GraphBuilder::new();
    // let (slot, _worker) = builder.slot::<u32>("worker");
    // builder.define(slot, Idle::<String>::new);
    //
    // Reusing a slot token also fails at compile time because `define`
    // consumes it:
    //
    // let (slot, _worker) = builder.slot::<String>("worker");
    // builder.define(slot, Idle::<String>::new);
    // builder.define(slot, Idle::<String>::new);
}
