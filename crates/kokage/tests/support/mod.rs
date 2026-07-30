#![allow(dead_code)]

use kokage::{
    ActorFactory, ActorRef, ActorSlot, ActorSpec, DynamicRuntimeHandle, OrderedTree, Runtime,
    host::{RawActor, RunnableActor},
};

pub(crate) fn dynamic_root(runtime: &Runtime) -> DynamicRuntimeHandle {
    runtime
        .handle()
        .dynamic()
        .expect("dynamic root exposes membership capability")
}

/// Small test fixture for incrementally assembling heterogeneous actor specs.
///
/// Production code should keep each `ActorSpec` in a local variable, obtain
/// its ref, and pass the spec directly to `OrderedTree::actor`. Tests with many
/// unrelated fixture actors use this wrapper to keep that setup compact.
pub(crate) struct TreeBuilder {
    tree: Option<OrderedTree>,
}

pub(crate) struct RunnableBuilder {
    actors: Vec<RunnableActor>,
}

impl RunnableBuilder {
    pub(crate) fn new() -> Self {
        Self { actors: Vec::new() }
    }

    pub(crate) fn actor<M: Send + 'static>(&mut self, spec: ActorSpec<M>) -> ActorRef<M> {
        let actor_ref = spec.actor_ref();
        self.actors.push(spec.into_runnable());
        actor_ref
    }

    pub(crate) fn define<M, F>(&mut self, slot: ActorSlot<M>, factory: F) -> ActorRef<M>
    where
        M: Send + 'static,
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        self.actor(slot.define(factory))
    }

    pub(crate) fn build(self) -> RunnableActors {
        RunnableActors(self.actors)
    }
}

pub(crate) struct RunnableActors(Vec<RunnableActor>);

impl RunnableActors {
    pub(crate) fn into_nodes(self) -> Vec<RunnableNode> {
        self.0.into_iter().map(RunnableNode).collect()
    }
}

pub(crate) struct RunnableNode(RunnableActor);

impl RunnableNode {
    pub(crate) fn label(&self) -> &str {
        self.0.label()
    }

    pub(crate) fn into_runnable(self) -> RunnableActor {
        self.0
    }
}

impl TreeBuilder {
    pub(crate) fn new() -> Self {
        Self {
            tree: Some(OrderedTree::new()),
        }
    }

    pub(crate) fn actor<M: Send + 'static>(&mut self, spec: ActorSpec<M>) -> ActorRef<M> {
        let actor_ref = spec.actor_ref();
        self.tree = Some(
            self.tree
                .take()
                .expect("test tree builder is single-use")
                .actor(spec),
        );
        actor_ref
    }

    pub(crate) fn define<M, F>(&mut self, slot: ActorSlot<M>, factory: F) -> ActorRef<M>
    where
        M: Send + 'static,
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        self.actor(slot.define(factory))
    }

    pub(crate) fn mailbox_capacity(&mut self, capacity: usize) -> &mut Self {
        self.tree = Some(
            self.tree
                .take()
                .expect("test tree builder is single-use")
                .mailbox_capacity(capacity),
        );
        self
    }

    pub(crate) fn build(mut self) -> OrderedTree {
        self.tree.take().expect("test tree builder is single-use")
    }

    pub(crate) fn spawn(self) -> Result<kokage::Runtime, kokage::BuildError> {
        self.build().spawn()
    }
}
