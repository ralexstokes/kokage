//! Recursive supervision-tree declarations and lowering.

#[cfg(feature = "serde")]
mod support;

#[cfg(feature = "serde")]
use support::TreeBuilder;

use std::{sync::Arc, time::Duration};

use tokio::{sync::Notify, time::sleep};

use kokage::{
    ActorSpec, DynamicTree, MailboxMode, RestartConfig, RestartPolicy, ScopeKind, ShutdownPolicy,
    Strategy, SupervisorBuildError, TerminalMembership,
    host::{ActorContext, ChildSpec, RawActor},
    observe::ChildOutline,
    prelude::*,
};

struct Worker;

impl Actor for Worker {
    type Msg = Reply<u32>;

    async fn handle(
        &mut self,
        reply: Reply<u32>,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        reply.send(7);
        Ok(())
    }
}

struct Finite;

impl RawActor for Finite {
    type Msg = ();

    async fn run(&mut self, _ctx: ActorContext<Self::Msg>) -> ActorResult {
        Ok(())
    }
}

struct Parked;

impl RawActor for Parked {
    type Msg = Vec<u8>;

    async fn run(&mut self, ctx: ActorContext<Self::Msg>) -> ActorResult {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[cfg(feature = "serde")]
fn two_actor_tree() -> (OrderedTree, ActorRef<Reply<u32>>, ActorRef<Reply<u32>>) {
    let mut builder = TreeBuilder::new();
    let ingest = builder.actor(ActorSpec::new("ingest", || Worker));
    let parse = builder.actor(ActorSpec::new("parse", || Worker));
    (builder.build(), ingest, parse)
}

#[test]
fn a_tree_expresses_recursive_composition_and_actor_overrides() {
    let outline = OrderedTree::new()
        .strategy(Strategy::RestForOne)
        .default_restart(RestartPolicy::Always)
        .subtree("workers", OrderedTree::new().strategy(Strategy::OneForAll))
        .task(
            ChildSpec::task("clock", |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .restart(RestartPolicy::Always)
            .shutdown(ShutdownPolicy::Abort),
        )
        .actor(ActorSpec::new("ingest", || Worker).restart(RestartPolicy::Never))
        .actor(ActorSpec::new("parse", || Worker))
        .outline();

    assert_eq!(outline.kind, ScopeKind::Ordered);
    assert_eq!(outline.strategy, Strategy::RestForOne);
    assert_eq!(outline.default_restart, RestartPolicy::Always);
    assert_eq!(outline.default_shutdown, ShutdownPolicy::default());
    assert_eq!(outline.child_ids(), ["workers", "clock", "ingest", "parse"]);

    let ChildOutline::Scope {
        outline: nested, ..
    } = outline.child("workers").expect("subtree is present")
    else {
        panic!("expected a scope");
    };
    assert_eq!(nested.kind, ScopeKind::Ordered);
    assert_eq!(nested.strategy, Strategy::OneForAll);

    let ChildOutline::Task {
        restart, shutdown, ..
    } = outline.child("clock").expect("task is present")
    else {
        panic!("expected a task");
    };
    assert_eq!(*restart, RestartPolicy::Always);
    assert_eq!(*shutdown, ShutdownPolicy::Abort);

    let ChildOutline::Actor { restart, .. } = outline.child("ingest").expect("ingest is present")
    else {
        panic!("expected an actor");
    };
    assert_eq!(*restart, RestartPolicy::Never);
    let ChildOutline::Actor { restart, .. } = outline.child("parse").expect("parse is present")
    else {
        panic!("expected an actor");
    };
    assert_eq!(*restart, RestartPolicy::Always);
    assert!(matches!(
        outline.child("clock"),
        Some(ChildOutline::Task { .. })
    ));
}

#[test]
fn task_specs_preserve_explicit_policies_and_inherit_unset_defaults() {
    let explicit_shutdown = ShutdownPolicy::Cooperative {
        grace: Duration::from_millis(17),
    };
    let outline = OrderedTree::new()
        .default_restart(RestartPolicy::Always)
        .default_shutdown(ShutdownPolicy::Abort)
        .task(ChildSpec::task("inherited", |_| async { Ok(()) }))
        .task(
            ChildSpec::task("explicit", |_| async { Ok(()) })
                .restart(RestartPolicy::Never)
                .shutdown(explicit_shutdown),
        )
        .outline();

    assert!(matches!(
        outline.child("inherited"),
        Some(ChildOutline::Task {
            restart: RestartPolicy::Always,
            shutdown: ShutdownPolicy::Abort,
            ..
        })
    ));
    assert!(matches!(
        outline.child("explicit"),
        Some(ChildOutline::Task {
            restart: RestartPolicy::Never,
            shutdown,
            ..
        }) if *shutdown == explicit_shutdown
    ));
}

#[tokio::test]
async fn actor_specs_can_be_placed_across_ordered_scope_levels() {
    let ingest_actor = ActorSpec::new("ingest", || Worker);
    let ingest = ingest_actor.actor_ref();
    let parse_actor = ActorSpec::new("parse", || Worker);
    let parse = parse_actor.actor_ref();
    let handle = OrderedTree::new()
        .actor(ingest_actor)
        .subtree(
            "workers",
            OrderedTree::new()
                .strategy(Strategy::OneForAll)
                .actor(parse_actor),
        )
        .spawn()
        .expect("tree builds");

    assert_eq!(
        ingest
            .call(Duration::from_secs(5), |reply| reply)
            .await
            .expect("ingest replies"),
        7
    );
    assert_eq!(
        parse
            .call(Duration::from_secs(5), |reply| reply)
            .await
            .expect("nested parse replies"),
        7
    );
    let mut labels: Vec<_> = handle
        .handle()
        .actor_stats()
        .into_iter()
        .map(|stats| stats.actor_id.to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, ["ingest", "parse"]);
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn tree_placement_rejects_zero_actor_mailbox_capacity() {
    let result = OrderedTree::new()
        .actor(ActorSpec::new("worker", || Worker).mailbox_capacity(0))
        .spawn();

    assert!(matches!(
        result,
        Err(SupervisorBuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        ))
    ));
}

#[test]
fn tree_placement_rejects_zero_scope_mailbox_capacity() {
    let result = OrderedTree::new()
        .mailbox_capacity(0)
        .actor(ActorSpec::new("worker", || Worker))
        .spawn();

    assert!(matches!(
        result,
        Err(SupervisorBuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        ))
    ));
}

#[tokio::test]
async fn tree_placed_specs_inherit_the_scope_mailbox_default() {
    let first = ActorSpec::new("first", || Worker);
    let first_actor = first.actor_ref();
    let direct = ActorSpec::new("direct-actor", || Worker);
    let direct_actor = direct.actor_ref();
    let runtime = OrderedTree::new()
        .mailbox_capacity(9)
        .actor(first)
        .actor(direct)
        .spawn()
        .expect("tree builds");
    runtime.handle().wait_started().await.expect("actors start");

    assert_eq!(first_actor.stats().mailbox_capacity, 9);
    assert_eq!(direct_actor.stats().mailbox_capacity, 9);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn nested_scope_does_not_inherit_parent_mailbox_default() {
    let nested = ActorSpec::new("nested", || Worker);
    let nested_ref = nested.actor_ref();
    let runtime = OrderedTree::new()
        .mailbox_capacity(9)
        .subtree("nested", OrderedTree::new().actor(nested))
        .spawn()
        .expect("tree builds");
    runtime.handle().wait_started().await.expect("actors start");

    assert_eq!(nested_ref.stats().mailbox_capacity, 64);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn leader_owned_scope_declares_its_own_mailbox_default() {
    let peer = ActorSpec::new("peer", || Worker);
    let peer_ref = peer.actor_ref();
    let leader = ActorSpec::new("leader", || Worker);
    let leader_ref = leader.actor_ref();
    let runtime = OrderedTree::new()
        .actor(peer)
        .subtree(
            "owned",
            OrderedTree::new()
                .mailbox_capacity(9)
                .strategy(Strategy::OneForAll)
                .actor(leader)
                .subtree("children", DynamicTree::new()),
        )
        .spawn()
        .expect("tree builds");
    runtime.handle().wait_started().await.expect("actors start");

    assert_eq!(peer_ref.stats().mailbox_capacity, 64);
    assert_eq!(leader_ref.stats().mailbox_capacity, 9);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn pre_spawn_projection_preserves_declared_restart_policies() {
    let tree = OrderedTree::new()
        .default_restart(RestartPolicy::Always)
        .actor(ActorSpec::new("explicit", || Worker).restart(RestartPolicy::Never))
        .actor(ActorSpec::new("inherited", || Worker));
    let snapshot = tree.handle().snapshot();

    assert_eq!(
        snapshot
            .child("explicit")
            .expect("explicit actor is projected")
            .restart_policy,
        RestartPolicy::Never
    );
    assert_eq!(
        snapshot
            .child("inherited")
            .expect("inherited actor is projected")
            .restart_policy,
        RestartPolicy::Always
    );
}

#[tokio::test]
async fn tree_placed_specs_preserve_mailbox_mode_and_message_size_observation() {
    let spec = ActorSpec::new("buffered", || Parked)
        .mailbox(MailboxMode::conflate())
        .message_size(|message: &Vec<u8>| message.len());
    let actor = spec.actor_ref();
    let runtime = OrderedTree::new().actor(spec).spawn().expect("tree builds");
    runtime.handle().wait_started().await.expect("actor starts");

    actor
        .try_send(vec![0; 4])
        .expect("first message is accepted");
    actor
        .try_send(vec![0; 3])
        .expect("replacement message is accepted");

    let stats = actor.stats();
    assert_eq!(stats.mailbox_capacity, 1);
    assert_eq!(stats.mailbox_depth, 1);
    assert_eq!(stats.messages_conflated, 1);
    assert_eq!(stats.message_bytes_accepted, Some(7));

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn static_tree_actor_can_remove_its_terminal_membership() {
    let spec = ActorSpec::new("finite", || Finite)
        .restart(RestartPolicy::Never)
        .terminal_membership(TerminalMembership::Remove);
    let actor = spec.actor_ref();
    let tree = OrderedTree::new().actor(spec);
    let mut snapshots = tree.handle().subscribe_snapshots();
    let runtime = tree.spawn().expect("tree builds");

    tokio::time::timeout(
        Duration::from_secs(2),
        snapshots
            .wait_for(|snapshot| snapshot.lifecycle_seq >= 3 && snapshot.child("finite").is_none()),
    )
    .await
    .expect("terminal membership is removed")
    .expect("snapshot stream remains open");
    assert!(matches!(
        actor.try_send(()),
        Err(kokage::TrySendError::Terminated { actor_id, .. }) if actor_id == "finite"
    ));

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn dynamic_outlines_include_future_member_policy_defaults() {
    let standard = DynamicTree::new().outline();
    let customized = DynamicTree::new()
        .default_restart(RestartPolicy::Never)
        .default_shutdown(ShutdownPolicy::Abort)
        .outline();

    assert_ne!(standard, customized);
    assert_eq!(customized.default_restart, RestartPolicy::Never);
    assert_eq!(customized.default_shutdown, ShutdownPolicy::Abort);
}

#[tokio::test]
async fn leader_owned_scope_is_an_explicit_subtree() {
    let ingest = ActorSpec::new("ingest", || Worker);
    let tree = OrderedTree::new().subtree(
        "owned",
        OrderedTree::new()
            .strategy(Strategy::RestForOne)
            .actor(ingest)
            .subtree("children", DynamicTree::new()),
    );
    let outline = tree.outline();
    let ChildOutline::Scope { outline: owned, .. } =
        outline.child("owned").expect("owned node exists")
    else {
        panic!("expected explicit owned scope");
    };
    assert_eq!(owned.strategy, Strategy::RestForOne);
    assert_eq!(owned.child_ids(), ["ingest", "children"]);

    let handle = tree.spawn().expect("leader-owned scope lowers");
    handle
        .handle()
        .wait_started()
        .await
        .expect("generated scope starts");
    let snapshot = handle.handle().snapshot();
    let owned = snapshot
        .child("owned")
        .and_then(|child| child.supervisor.as_ref())
        .expect("generated ordered scope is visible");
    assert_eq!(owned.kind, ScopeKind::Ordered);
    assert_eq!(owned.children[0].id, "ingest");
    assert_eq!(owned.children[1].id, "children");
    assert_eq!(
        owned.children[1]
            .supervisor
            .as_ref()
            .expect("owned children scope is visible")
            .kind,
        ScopeKind::Dynamic
    );
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn leader_owned_scope_defaults_are_declared_on_the_intermediate_tree() {
    let ingest = ActorSpec::new("ingest", || Worker);
    let fail = Arc::new(Notify::new());
    let fail_child = Arc::clone(&fail);
    let children = OrderedTree::new()
        .restart_config(RestartConfig::new(0, Duration::from_secs(60)))
        .task(
            ChildSpec::task("fatal", move |_| {
                let fail = Arc::clone(&fail_child);
                async move {
                    fail.notified().await;
                    Err(std::io::Error::other("fatal child failure").into())
                }
            })
            .restart(RestartPolicy::Always)
            .shutdown(ShutdownPolicy::Abort),
        );
    let handle = OrderedTree::new()
        .subtree(
            "owned",
            OrderedTree::new()
                .default_restart(RestartPolicy::Never)
                .actor(ingest)
                .subtree("children", children),
        )
        .spawn()
        .expect("leader-owned scope builds");
    handle
        .handle()
        .wait_started()
        .await
        .expect("generated scope starts");

    fail.notify_one();
    handle
        .handle()
        .subscribe_snapshots()
        .wait_for(|snapshot| {
            snapshot
                .child("owned")
                .and_then(|child| child.supervisor.as_ref())
                .and_then(|owned| owned.child("children"))
                .is_some_and(|children| {
                    children.state.is_terminal()
                        && children
                            .state
                            .last_exit()
                            .is_some_and(|exit| exit.failure_message().is_some())
                })
        })
        .await
        .expect("fatal child scope becomes terminal");

    // A default `OnFailure` edge would immediately restart this nested scope.
    // Give that zero-delay transition room to occur before inspecting the
    // stable state promised by the inherited `Never` policy.
    sleep(Duration::from_millis(50)).await;
    let snapshot = handle.handle().snapshot();
    let owned_edge = snapshot.child("owned").expect("owned edge remains visible");
    assert!(owned_edge.state.is_running());
    let children_edge = owned_edge
        .supervisor
        .as_ref()
        .and_then(|owned| owned.child("children"))
        .expect("generated children edge remains visible");
    assert_eq!(children_edge.generation, 0);
    assert_eq!(children_edge.restart_count, 0);
    assert!(children_edge.state.is_terminal());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[cfg(feature = "serde")]
#[test]
fn an_outline_round_trips_through_serde_with_scope_kinds() {
    let (graph, _ingest, _parse) = two_actor_tree();
    let outline = graph
        .strategy(Strategy::RestForOne)
        .subtree(
            "workers",
            DynamicTree::new()
                .default_restart(RestartPolicy::Never)
                .default_shutdown(ShutdownPolicy::Abort),
        )
        .task(ChildSpec::task("clock", |_ctx| async { Ok(()) }))
        .outline();
    let json = serde_json::to_string(&outline).expect("outline serializes");
    assert!(
        json.contains("\"Task\""),
        "task outline uses its public tag"
    );
    let decoded: kokage::observe::SupervisionOutline =
        serde_json::from_str(&json).expect("outline deserializes");
    assert_eq!(outline, decoded);
    let ChildOutline::Scope {
        outline: workers, ..
    } = decoded.child("workers").expect("dynamic scope survives")
    else {
        panic!("expected dynamic scope");
    };
    assert_eq!(workers.default_restart, RestartPolicy::Never);
    assert_eq!(workers.default_shutdown, ShutdownPolicy::Abort);
    assert!(matches!(
        decoded.child("clock"),
        Some(ChildOutline::Task { .. })
    ));
}
