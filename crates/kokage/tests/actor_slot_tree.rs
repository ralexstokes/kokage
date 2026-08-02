//! Runtime coverage for cyclic wiring across ordinary nested trees.

use std::time::Duration;

use kokage::{ActorSlot, DynamicTree, ScopeRef, observe::ScopeKind, prelude::*};

enum LeftMsg {
    Connected(Reply<bool>),
}

enum RightMsg {
    Connected(Reply<bool>),
}

enum ProbeMsg {
    Connected(Reply<bool>),
}

struct Left {
    right: ActorRef<RightMsg>,
    probe: ActorRef<ProbeMsg>,
}

impl Actor for Left {
    type Msg = LeftMsg;

    async fn handle(
        &mut self,
        LeftMsg::Connected(reply): LeftMsg,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        let _ = (self.right.stats(), self.probe.stats());
        reply.send(true);
        Ok(())
    }
}

struct Right {
    left: ActorRef<LeftMsg>,
}

impl Actor for Right {
    type Msg = RightMsg;

    async fn handle(
        &mut self,
        RightMsg::Connected(reply): RightMsg,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        let _ = self.left.stats();
        reply.send(true);
        Ok(())
    }
}

struct Probe {
    left: ActorRef<LeftMsg>,
}

impl Actor for Probe {
    type Msg = ProbeMsg;

    async fn handle(
        &mut self,
        ProbeMsg::Connected(reply): ProbeMsg,
        _ctx: &mut Context<'_, Self>,
    ) -> ExitResult {
        let _ = self.left.stats();
        reply.send(true);
        Ok(())
    }
}

struct Session;

impl Actor for Session {
    type Msg = ();

    async fn handle(&mut self, (): (), _ctx: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn child_ids(scope: &ScopeRef) -> Vec<String> {
    scope
        .snapshot()
        .children
        .into_iter()
        .map(|child| child.id)
        .collect()
}

#[tokio::test]
async fn slots_resolve_cyclic_refs_across_nested_tree_wiring() {
    let right_slot = ActorSlot::<RightMsg>::new("right");
    let right = right_slot.actor_ref();
    let probe_slot = ActorSlot::<ProbeMsg>::new("probe");
    let probe = probe_slot.actor_ref();

    let mut workers_tree = Tree::new().strategy(Strategy::OneForAll);
    let left = workers_tree.add_actor_spec(ActorSpec::new("left-worker", {
        let right = right.clone();
        let probe = probe.clone();
        move || Left {
            right: right.clone(),
            probe: probe.clone(),
        }
    }));
    workers_tree.add_actor_spec(right_slot.define({
        let left = left.clone();
        move || Right { left: left.clone() }
    }));

    let mut tree = Tree::new();
    let root = tree.scope();
    let sessions = tree.add_dynamic_subtree("sessions", DynamicTree::new());
    let workers = tree.add_subtree("workers", workers_tree);
    tree.add_actor_spec(probe_slot.define({
        let left = left.clone();
        move || Probe { left: left.clone() }
    }));

    assert_eq!(child_ids(&root), ["sessions", "workers", "probe"]);
    assert_eq!(root.kind(), ScopeKind::Ordered);
    assert_eq!(workers.kind(), ScopeKind::Ordered);
    assert_eq!(workers.snapshot().strategy, Strategy::OneForAll);
    assert_eq!(child_ids(&workers), ["left-worker", "right"]);
    assert_eq!(sessions.kind(), ScopeKind::Dynamic);
    assert!(sessions.snapshot().children.is_empty());

    let runtime = tree.spawn().expect("ordinary tree should spawn");
    runtime
        .scope()
        .wait_started()
        .await
        .expect("tree should become ready");

    assert!(
        left.call(LeftMsg::Connected, Duration::from_secs(1))
            .await
            .expect("left call")
    );
    assert!(
        right
            .call(RightMsg::Connected, Duration::from_secs(1))
            .await
            .expect("right call")
    );
    assert!(
        probe
            .call(ProbeMsg::Connected, Duration::from_secs(1))
            .await
            .expect("probe call")
    );

    let session = sessions
        .add_actor("session", || Session)
        .await
        .expect("dynamic scope should accept membership");
    session.send(()).await.expect("dynamic actor should run");

    runtime.shutdown().await.expect("tree should stop cleanly");
}
