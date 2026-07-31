#![allow(dead_code)]

use kokage::{
    ActorFactory, ActorRef, ActorSlot, DynamicScopeRef, RunningDynamicTree, Tree, raw::RawActor,
};
#[cfg(feature = "host")]
use kokage::{ActorSpec, raw::ActorHost};

pub(crate) fn dynamic_root(running_tree: &RunningDynamicTree) -> DynamicScopeRef {
    running_tree.scope()
}

/// Small test fixture for incrementally assembling heterogeneous actor specs.
///
/// Production code should add each `ActorSpec` directly through
/// `Tree::add_actor`. Tests with many unrelated fixture actors use this
/// wrapper to keep slot definition and tree configuration compact.
pub(crate) struct TreeBuilder {
    tree: Tree,
}

#[cfg(feature = "host")]
pub(crate) struct ActorHostBuilder {
    actors: Vec<ActorHost>,
}

#[cfg(feature = "host")]
impl ActorHostBuilder {
    pub(crate) fn new() -> Self {
        Self { actors: Vec::new() }
    }

    pub(crate) fn actor<M: Send + 'static>(&mut self, spec: ActorSpec<M>) -> ActorRef<M> {
        let actor_ref = spec.actor_ref();
        self.actors.push(spec.into_host());
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

    pub(crate) fn build(self) -> ActorHosts {
        ActorHosts(self.actors)
    }
}

#[cfg(feature = "host")]
pub(crate) struct ActorHosts(Vec<ActorHost>);

#[cfg(feature = "host")]
impl ActorHosts {
    pub(crate) fn into_nodes(self) -> Vec<ActorHostNode> {
        self.0.into_iter().map(ActorHostNode).collect()
    }
}

#[cfg(feature = "host")]
pub(crate) struct ActorHostNode(ActorHost);

#[cfg(feature = "host")]
impl ActorHostNode {
    pub(crate) fn label(&self) -> &str {
        self.0.label()
    }

    pub(crate) fn into_host(self) -> ActorHost {
        self.0
    }
}

impl TreeBuilder {
    pub(crate) fn new() -> Self {
        Self { tree: Tree::new() }
    }

    pub(crate) fn define<M, F>(&mut self, slot: ActorSlot<M>, factory: F) -> ActorRef<M>
    where
        M: Send + 'static,
        F: ActorFactory,
        F::Actor: RawActor<Msg = M>,
    {
        self.tree.add_actor_spec(slot.define(factory))
    }

    pub(crate) fn mailbox_capacity(&mut self, capacity: usize) -> &mut Self {
        self.tree = std::mem::take(&mut self.tree).mailbox_capacity(capacity);
        self
    }

    pub(crate) fn build(self) -> Tree {
        self.tree
    }

    pub(crate) fn spawn(self) -> Result<kokage::RunningTree, kokage::BuildError> {
        self.build().spawn()
    }
}
