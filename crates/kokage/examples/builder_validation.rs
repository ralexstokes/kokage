use std::marker::PhantomData;

use kokage::{
    ActorSlot, ActorSpec, BuildError, ExitResult, Tree,
    raw::{RawActor, RawContext},
};

struct Idle<M>(PhantomData<fn(M)>);

impl<M> Idle<M> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: Send + 'static> RawActor for Idle<M> {
    type Msg = M;

    async fn run(&mut self, mut ctx: RawContext<M>) -> ExitResult {
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
    let mut zero_capacity = Tree::new().mailbox_capacity(0);
    zero_capacity.add_actor_spec(ActorSpec::new("worker", Idle::<()>::new));
    report("zero mailbox capacity", zero_capacity.spawn());

    let mut duplicate_ids = Tree::new();
    duplicate_ids.add_actor_spec(ActorSpec::new("worker", Idle::<()>::new));
    duplicate_ids.add_actor_spec(ActorSpec::new("worker", Idle::<()>::new));
    report("duplicate actor ids", duplicate_ids.spawn());

    let mut empty_id = Tree::new();
    empty_id.add_actor_spec(ActorSpec::new("", Idle::<()>::new));
    report("empty actor id", empty_id.spawn());

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
