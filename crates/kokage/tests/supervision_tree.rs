//! Recursive supervision-tree declarations and lowering.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};

use kokage::{
    ActorSpec, BuildError, DynamicTree, MailboxMode, MailboxShutdown, RestartMode, RestartPolicy,
    Shutdown, Strategy, SubtreeSpec, TaskSpec,
    observe::ScopeKind,
    prelude::*,
    raw::{RawActor, RawContext},
};

#[cfg(feature = "serde")]
use kokage::{Backoff, observe::ChildOutline};

struct Worker;

impl Actor for Worker {
    type Msg = Reply<u32>;

    async fn handle(&mut self, reply: Reply<u32>, _ctx: &mut Context<'_, Self>) -> ExitResult {
        reply.send(7);
        Ok(())
    }
}

struct Finite;

impl RawActor for Finite {
    type Msg = ();

    async fn run(&mut self, _ctx: RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

struct Parked;

impl RawActor for Parked {
    type Msg = Vec<u8>;

    async fn run(&mut self, ctx: RawContext<Self::Msg>) -> ExitResult {
        ctx.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[cfg(feature = "serde")]
fn two_actor_tree() -> (Tree, ActorRef<Reply<u32>>, ActorRef<Reply<u32>>) {
    let mut builder = Tree::new();
    let ingest = builder.add_actor_spec(ActorSpec::new("ingest", || Worker).remove_when_done());
    let parse = builder.add_actor_spec(ActorSpec::new("parse", || Worker));
    (builder, ingest, parse)
}

#[test]
#[cfg(feature = "serde")]
fn a_tree_expresses_recursive_composition_and_actor_overrides() {
    let mut tree = Tree::new()
        .strategy(Strategy::RestForOne)
        .default_restart(RestartPolicy::always())
        .default_mailbox_shutdown(MailboxShutdown::Discard);
    tree.add_subtree("workers", Tree::new().strategy(Strategy::OneForAll));
    tree.add_task_spec(
        TaskSpec::new("clock", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(RestartMode::Always)
        .shutdown(Shutdown::abort()),
    );
    tree.add_actor_spec(
        ActorSpec::new("ingest", || Worker)
            .restart(RestartMode::Never)
            .mailbox_shutdown(MailboxShutdown::Drain),
    );
    tree.add_actor_spec(ActorSpec::new("parse", || Worker));
    let outline = tree.outline();

    assert_eq!(outline.kind, ScopeKind::Ordered);
    assert_eq!(outline.strategy, Strategy::RestForOne);
    assert_eq!(outline.default_restart, RestartPolicy::always());
    assert_eq!(outline.default_shutdown, Shutdown::default());
    assert_eq!(outline.default_mailbox_shutdown, MailboxShutdown::Discard);
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
    assert_eq!(*restart, RestartPolicy::always());
    assert_eq!(*shutdown, Shutdown::abort());

    let ChildOutline::Actor {
        restart,
        mailbox_shutdown,
        ..
    } = outline.child("ingest").expect("ingest is present")
    else {
        panic!("expected an actor");
    };
    assert_eq!(*restart, RestartPolicy::never());
    assert_eq!(*mailbox_shutdown, MailboxShutdown::Drain);
    let ChildOutline::Actor {
        restart,
        mailbox_shutdown,
        ..
    } = outline.child("parse").expect("parse is present")
    else {
        panic!("expected an actor");
    };
    assert_eq!(*restart, RestartPolicy::always());
    assert_eq!(*mailbox_shutdown, MailboxShutdown::Discard);
    assert!(matches!(
        outline.child("clock"),
        Some(ChildOutline::Task { .. })
    ));
}

#[test]
fn tree_debug_includes_nested_declarations_without_serde() {
    let mut workers = Tree::new().default_mailbox_shutdown(MailboxShutdown::Discard);
    workers.add_actor("press", || Worker);
    let mut tree = Tree::new();
    tree.add_subtree("workers", workers);

    let debug = format!("{tree:#?}");
    assert!(debug.contains("workers"));
    assert!(debug.contains("press"));
    assert!(debug.contains("default_mailbox_shutdown: Discard"));
}

#[test]
#[cfg(feature = "serde")]
fn task_specs_preserve_explicit_policies_and_inherit_unset_defaults() {
    let explicit_shutdown = Shutdown::graceful_for(Duration::from_millis(17));
    let mut tree = Tree::new()
        .default_restart(RestartPolicy::always())
        .default_shutdown(Shutdown::abort());
    tree.add_task_spec(TaskSpec::new("inherited", |_| async { Ok(()) }));
    tree.add_task_spec(
        TaskSpec::new("explicit", |_| async { Ok(()) })
            .restart(RestartMode::Never)
            .shutdown(explicit_shutdown),
    );
    let outline = tree.outline();

    assert!(matches!(
        outline.child("inherited"),
        Some(ChildOutline::Task {
            restart,
            shutdown,
            ..
        }) if *restart == RestartPolicy::always() && *shutdown == Shutdown::abort()
    ));
    assert!(matches!(
        outline.child("explicit"),
        Some(ChildOutline::Task {
            restart,
            shutdown,
            ..
        }) if *restart == RestartPolicy::never() && *shutdown == explicit_shutdown
    ));
}

#[tokio::test]
async fn subtree_edges_accept_explicit_policies_for_declared_and_dynamic_membership() {
    let declared_shutdown = Shutdown::abort();
    let mut stubborn = Tree::new();
    stubborn.add_task_spec(TaskSpec::new("stubborn", |_| async {
        std::future::pending::<()>().await;
        Ok(())
    }));
    let mut declared = Tree::new().default_restart(RestartPolicy::always());
    declared.add_subtree_spec(
        "declared",
        SubtreeSpec::from(stubborn)
            .restart(RestartMode::Never)
            .shutdown(declared_shutdown),
    );

    #[cfg(feature = "serde")]
    assert!(matches!(
        declared.outline().child("declared"),
        Some(ChildOutline::Scope {
            restart,
            shutdown,
            ..
        }) if *restart == RestartPolicy::never() && *shutdown == declared_shutdown
    ));

    let declared = declared.spawn().expect("declared tree builds");
    declared
        .scope()
        .wait_started()
        .await
        .expect("declared subtree starts");
    let declared_snapshot = declared.scope().snapshot();
    let declared_child = declared_snapshot
        .child("declared")
        .expect("declared subtree is present");
    assert_eq!(declared_child.restart_policy, RestartPolicy::never());
    timeout(Duration::from_millis(250), declared.shutdown_and_wait())
        .await
        .expect("subtree abort policy bounds declared shutdown")
        .expect("declared tree shuts down");

    let dynamic = DynamicTree::new().spawn().expect("dynamic tree builds");
    let mut stubborn = Tree::new();
    stubborn.add_task_spec(TaskSpec::new("stubborn", |_| async {
        std::future::pending::<()>().await;
        Ok(())
    }));
    let inserted = dynamic
        .scope()
        .add_subtree(
            "inserted",
            SubtreeSpec::from(stubborn)
                .restart(RestartMode::Never)
                .shutdown(Shutdown::abort()),
        )
        .await
        .expect("policy-bearing subtree inserts");
    inserted
        .wait_started()
        .await
        .expect("inserted subtree starts");
    let dynamic_snapshot = dynamic.scope().snapshot();
    let inserted = dynamic_snapshot
        .child("inserted")
        .expect("inserted subtree is present");
    assert_eq!(inserted.restart_policy, RestartPolicy::never());
    timeout(
        Duration::from_millis(250),
        dynamic.scope().remove_child("inserted"),
    )
    .await
    .expect("subtree abort policy bounds dynamic removal")
    .expect("policy-bearing subtree is removed");
    dynamic
        .shutdown_and_wait()
        .await
        .expect("dynamic tree shuts down");
}

#[tokio::test]
async fn actor_specs_can_be_placed_across_ordered_scope_levels() {
    let ingest_actor = ActorSpec::new("ingest", || Worker);
    let parse_actor = ActorSpec::new("parse", || Worker);
    let mut workers = Tree::new().strategy(Strategy::OneForAll);
    let parse = workers.add_actor_spec(parse_actor);
    let mut tree = Tree::new();
    let ingest = tree.add_actor_spec(ingest_actor);
    tree.add_subtree("workers", workers);
    let handle = tree.spawn().expect("tree builds");

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
        .scope()
        .actor_stats()
        .into_iter()
        .map(|stats| stats.stats.actor_id)
        .collect();
    labels.sort();
    assert_eq!(labels, ["ingest", "parse"]);
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn tree_placement_rejects_zero_actor_mailbox_capacity() {
    let mut tree = Tree::new();
    tree.add_actor_spec(ActorSpec::new("worker", || Worker).mailbox_capacity(0));
    let result = tree.spawn();

    assert!(matches!(
        result,
        Err(BuildError::InvalidConfig(
            "actor mailbox capacity must be non-zero"
        ))
    ));
}

#[test]
fn tree_placement_rejects_zero_scope_mailbox_capacity() {
    let mut tree = Tree::new().mailbox_capacity(0);
    tree.add_actor_spec(ActorSpec::new("worker", || Worker));
    let result = tree.spawn();

    assert!(matches!(
        result,
        Err(BuildError::InvalidConfig(
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
    let mut tree = Tree::new().mailbox_capacity(9);
    tree.add_actor_spec(first);
    tree.add_actor_spec(direct);
    let runtime = tree.spawn().expect("tree builds");
    runtime.scope().wait_started().await.expect("actors start");

    assert_eq!(first_actor.stats().mailbox_capacity, 9);
    assert_eq!(direct_actor.stats().mailbox_capacity, 9);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn nested_scope_does_not_inherit_parent_mailbox_default() {
    let nested = ActorSpec::new("nested", || Worker);
    let nested_ref = nested.actor_ref();
    let mut nested_tree = Tree::new();
    nested_tree.add_actor_spec(nested);
    let mut tree = Tree::new().mailbox_capacity(9);
    tree.add_subtree("nested", nested_tree);
    let runtime = tree.spawn().expect("tree builds");
    runtime.scope().wait_started().await.expect("actors start");

    assert_eq!(nested_ref.stats().mailbox_capacity, 64);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[tokio::test]
async fn leader_owned_scope_declares_its_own_mailbox_default() {
    let peer = ActorSpec::new("peer", || Worker);
    let peer_ref = peer.actor_ref();
    let leader = ActorSpec::new("leader", || Worker);
    let leader_ref = leader.actor_ref();
    let mut owned = Tree::new()
        .mailbox_capacity(9)
        .strategy(Strategy::OneForAll);
    owned.add_actor_spec(leader);
    owned.add_subtree("children", DynamicTree::new());
    let mut tree = Tree::new();
    tree.add_actor_spec(peer);
    tree.add_subtree("owned", owned);
    let runtime = tree.spawn().expect("tree builds");
    runtime.scope().wait_started().await.expect("actors start");

    assert_eq!(peer_ref.stats().mailbox_capacity, 64);
    assert_eq!(leader_ref.stats().mailbox_capacity, 9);
    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn pre_spawn_projection_preserves_declared_restart_policies() {
    let mut tree = Tree::new().default_restart(RestartPolicy::always());
    tree.add_actor_spec(ActorSpec::new("explicit", || Worker).restart(RestartMode::Never));
    tree.add_actor_spec(ActorSpec::new("inherited", || Worker));
    let snapshot = tree.scope().snapshot();

    assert_eq!(
        snapshot
            .child("explicit")
            .expect("explicit actor is projected")
            .restart_policy,
        RestartPolicy::never()
    );
    assert_eq!(
        snapshot
            .child("inherited")
            .expect("inherited actor is projected")
            .restart_policy,
        RestartPolicy::always()
    );
}

#[tokio::test]
async fn tree_placed_specs_allow_message_size_configuration_after_actor_ref() {
    let spec = ActorSpec::new("buffered", || Parked).mailbox(MailboxMode::conflate());
    let actor = spec.actor_ref();
    let spec = spec.message_size(|message: &Vec<u8>| message.len());
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    let runtime = tree.spawn().expect("tree builds");
    runtime.scope().wait_started().await.expect("actor starts");

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
async fn static_tree_actor_can_remove_itself_when_done() {
    let spec = ActorSpec::new("finite", || Finite)
        .restart(RestartMode::Never)
        .remove_when_done();
    let actor = spec.actor_ref();
    let mut tree = Tree::new();
    tree.add_actor_spec(spec);
    assert!(
        tree.scope()
            .snapshot()
            .child("finite")
            .expect("finite actor is projected")
            .remove_when_done
    );
    let mut snapshots = tree.scope().subscribe_snapshots();
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
        Err(kokage::SendError {
            actor_id,
            kind: kokage::SendErrorKind::Terminated,
            ..
        }) if actor_id == "finite"
    ));

    runtime.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
#[cfg(feature = "serde")]
fn dynamic_outlines_include_future_member_policy_defaults() {
    let standard = DynamicTree::new().outline();
    let customized = DynamicTree::new()
        .default_restart(RestartPolicy::never())
        .default_shutdown(Shutdown::abort())
        .default_mailbox_shutdown(MailboxShutdown::Discard)
        .outline();

    assert_ne!(standard, customized);
    assert_eq!(customized.default_restart, RestartPolicy::never());
    assert_eq!(customized.default_shutdown, Shutdown::abort());
    assert_eq!(
        customized.default_mailbox_shutdown,
        MailboxShutdown::Discard
    );
}

#[tokio::test]
async fn leader_owned_scope_is_an_explicit_subtree() {
    let ingest = ActorSpec::new("ingest", || Worker);
    let mut owned = Tree::new().strategy(Strategy::RestForOne);
    owned.add_actor_spec(ingest);
    owned.add_subtree("children", DynamicTree::new());
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    #[cfg(feature = "serde")]
    {
        let outline = tree.outline();
        let ChildOutline::Scope { outline: owned, .. } =
            outline.child("owned").expect("owned node exists")
        else {
            panic!("expected explicit owned scope");
        };
        assert_eq!(owned.strategy, Strategy::RestForOne);
        assert_eq!(owned.child_ids(), ["ingest", "children"]);
    }

    let handle = tree.spawn().expect("leader-owned scope lowers");
    handle
        .scope()
        .wait_started()
        .await
        .expect("generated scope starts");
    let snapshot = handle.scope().snapshot();
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
    let mut children =
        Tree::new().default_restart(RestartPolicy::on_failure().limit(0, Duration::from_secs(60)));
    children.add_task_spec(
        TaskSpec::new("fatal", move |_| {
            let fail = Arc::clone(&fail_child);
            async move {
                fail.notified().await;
                Err(std::io::Error::other("fatal child failure").into())
            }
        })
        .restart_policy(RestartPolicy::always().limit(0, Duration::from_secs(60)))
        .shutdown(Shutdown::abort()),
    );
    let mut owned = Tree::new().default_restart(RestartPolicy::never());
    owned.add_actor_spec(ingest);
    owned.add_subtree("children", children);
    let mut tree = Tree::new();
    tree.add_subtree("owned", owned);
    let handle = tree.spawn().expect("leader-owned scope builds");
    handle
        .scope()
        .wait_started()
        .await
        .expect("generated scope starts");

    fail.notify_one();
    handle
        .scope()
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
    let snapshot = handle.scope().snapshot();
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
    let mut graph = graph
        .strategy(Strategy::RestForOne)
        .default_mailbox_shutdown(MailboxShutdown::Discard);
    graph.add_subtree(
        "workers",
        DynamicTree::new()
            .default_restart(RestartPolicy::never())
            .default_shutdown(Shutdown::abort()),
    );
    graph.add_task_spec(TaskSpec::new("clock", |_ctx| async { Ok(()) }).remove_when_done());
    let outline = graph.outline();
    let json = serde_json::to_string(&outline).expect("outline serializes");
    assert!(
        json.contains("\"Task\""),
        "task outline uses its public tag"
    );
    let decoded: kokage::observe::SupervisionOutline =
        serde_json::from_str(&json).expect("outline deserializes");
    assert_eq!(outline, decoded);
    assert_eq!(decoded.default_mailbox_shutdown, MailboxShutdown::Discard);
    let ChildOutline::Scope {
        outline: workers, ..
    } = decoded.child("workers").expect("dynamic scope survives")
    else {
        panic!("expected dynamic scope");
    };
    assert_eq!(workers.default_restart, RestartPolicy::never());
    assert_eq!(workers.default_shutdown, Shutdown::abort());
    assert!(matches!(
        decoded.child("clock"),
        Some(ChildOutline::Task {
            remove_when_done: true,
            ..
        })
    ));
    assert!(matches!(
        decoded.child("ingest"),
        Some(ChildOutline::Actor {
            mailbox_shutdown: MailboxShutdown::Discard,
            remove_when_done: true,
            ..
        })
    ));
}

#[cfg(feature = "serde")]
#[test]
fn policy_enums_use_their_direct_wire_shape() {
    let restart = serde_json::to_value(RestartPolicy::on_failure()).expect("restart serializes");
    assert!(restart.get("remove_when_done").is_none());
    let exponential =
        Backoff::exponential_with_jitter(Duration::from_millis(25), 3, Duration::from_secs(2));
    let backoff = serde_json::to_value(exponential).expect("backoff serializes");
    assert_eq!(backoff["Exponential"]["factor"], 3);
    assert_eq!(backoff["Exponential"]["jitter"], true);
    assert!(backoff.get("kind").is_none());
    assert_eq!(
        serde_json::from_value::<Backoff>(backoff).expect("exponential backoff deserializes"),
        exponential
    );

    let fixed = Backoff::fixed(Duration::from_millis(50));
    assert_eq!(
        serde_json::from_value::<Backoff>(
            serde_json::to_value(fixed).expect("fixed backoff serializes")
        )
        .expect("fixed backoff deserializes"),
        fixed
    );

    let graceful_policy = Shutdown::graceful_for(Duration::from_secs(7));
    let graceful = serde_json::to_value(graceful_policy).expect("shutdown serializes");
    assert_eq!(graceful["Graceful"]["grace"]["secs"], 7);
    assert_eq!(
        serde_json::from_value::<Shutdown>(graceful.clone()).expect("shutdown deserializes"),
        graceful_policy
    );
    assert_eq!(
        serde_json::to_value(Shutdown::abort()).expect("abort serializes"),
        serde_json::json!("Abort")
    );
    assert!(graceful.get("mode").is_none());

    assert_eq!(
        serde_json::to_value(MailboxShutdown::Drain).expect("mailbox shutdown serializes"),
        serde_json::json!("Drain")
    );
    assert_eq!(
        serde_json::from_value::<MailboxShutdown>(serde_json::json!("Discard"))
            .expect("mailbox shutdown deserializes"),
        MailboxShutdown::Discard
    );

    assert!(
        serde_json::from_value::<Backoff>(serde_json::json!({ "kind": "None" })).is_err(),
        "the former mirrored backoff shape is intentionally unsupported"
    );
    assert!(
        serde_json::from_value::<Shutdown>(serde_json::json!({
            "mode": "Abort",
            "grace": { "secs": 0, "nanos": 0 }
        }))
        .is_err(),
        "the former shutdown struct shape is intentionally unsupported"
    );
}
