use std::marker::PhantomData;

use kokage::{
    ActorResult, ActorSlot, ActorSpec, BuildError, OrderedTree,
    host::{RawActor, RawContext},
};

struct Idle<M>(PhantomData<fn(M)>);

impl<M> Idle<M> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Idle<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: RawContext<M>) -> ActorResult {
        while ctx.recv().await.is_some() {}
        Ok(())
    }
}

fn report(label: &str, result: Result<kokage::RunningTree, BuildError>) {
    match result {
        Ok(_) => panic!("{label} unexpectedly spawned"),
        Err(error) => println!("{label}: {error}"),
    }
}

#[tokio::main]
async fn main() {
    report(
        "zero mailbox capacity",
        OrderedTree::new()
            .mailbox_capacity(0)
            .actor(ActorSpec::new("worker", Idle::<()>::new))
            .spawn(),
    );

    report(
        "duplicate actor ids",
        OrderedTree::new()
            .actor(ActorSpec::new("worker", Idle::<()>::new))
            .actor(ActorSpec::new("worker", Idle::<()>::new))
            .spawn(),
    );

    report(
        "empty actor id",
        OrderedTree::new()
            .actor(ActorSpec::new("", Idle::<()>::new))
            .spawn(),
    );

    // Message-type mismatches now fail at compile time:
    //
    // let slot = ActorSlot::<u32>::new("worker");
    // let _worker = slot.actor_ref();
    // let _worker = slot.define(Idle::<String>::new);
    //
    // Reusing a slot token also fails at compile time because `define`
    // consumes it:
    //
    // let slot = ActorSlot::<String>::new("worker");
    // let _worker = slot.actor_ref();
    // let _worker = slot.define(Idle::<String>::new);
    // let _other = slot.define(Idle::<String>::new);

    // Merely opening and dropping an ActorSlot cannot leave a partial tree:
    // a slot only becomes placeable when `define` consumes it.
    let _unregistered = ActorSlot::<String>::new("ghost");
}
