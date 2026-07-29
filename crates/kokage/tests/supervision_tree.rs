//! Recursive supervision-tree declarations and lowering.

use std::{sync::Arc, time::Duration};

use tokio::{sync::Notify, time::sleep};

use kokage::{
    ActorSpec, Graph, ScopeKind,
    host::ChildSpec,
    observe::{ChildOutline, ExitStatusView},
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

fn two_actor_graph() -> (Graph, ActorRef<Reply<u32>>, ActorRef<Reply<u32>>) {
    let mut builder = GraphBuilder::new();
    let (ingest_slot, ingest) = builder.slot("ingest");
    builder.define(ingest_slot, || Worker);
    let (parse_slot, parse) = builder.slot("parse");
    builder.define(parse_slot, || Worker);
    (builder.build().expect("graph builds"), ingest, parse)
}

#[test]
fn a_tree_expresses_recursive_composition_and_actor_overrides() {
    let (graph, _ingest, _parse) = two_actor_graph();
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
        .actor(ActorSpec::new(graph.actors()[0].clone()).restart(RestartPolicy::Never))
        .actor(graph.actors()[1].clone())
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

#[test]
fn graph_convenience_and_explicit_actors_outline_identically() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let actors = graph.actors().to_vec();
    let from_graph = OrderedTree::graph(graph)
        .strategy(Strategy::OneForAll)
        .default_restart(RestartPolicy::Never)
        .outline();
    let from_tree = OrderedTree::new()
        .strategy(Strategy::OneForAll)
        .default_restart(RestartPolicy::Never)
        .actor(actors[0].clone())
        .actor(actors[1].clone())
        .outline();
    assert_eq!(from_graph, from_tree);
}

#[tokio::test]
async fn a_tree_spreads_one_graph_across_ordered_scope_levels() {
    let (graph, ingest, parse) = two_actor_graph();
    let (ingest_actor, parse_actor) = (graph.actors()[0].clone(), graph.actors()[1].clone());
    let handle = OrderedTree::new()
        .actor(ActorSpec::new(ingest_actor).restart(RestartPolicy::Never))
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
async fn actor_with_scope_lowers_to_leader_then_children_scope() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let tree = OrderedTree::new().actor_with_scope(
        "owned",
        graph.actors()[0].clone(),
        DynamicTree::new(),
        Strategy::RestForOne,
    );
    let outline = tree.outline();
    let ChildOutline::ActorWithScope {
        leader,
        children,
        strategy,
        ..
    } = outline.child("owned").expect("owned node exists")
    else {
        panic!("expected ActorWithScope outline");
    };
    assert_eq!(leader.id(), "ingest");
    assert_eq!(children.kind, ScopeKind::Dynamic);
    assert_eq!(*strategy, Strategy::RestForOne);

    let handle = tree.spawn().expect("ActorWithScope lowers");
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
async fn actor_with_scope_children_edge_inherits_the_enclosing_restart_default() {
    let (graph, _ingest, _parse) = two_actor_graph();
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
        .default_restart(RestartPolicy::Never)
        .actor_with_scope(
            "owned",
            graph.actors()[0].clone(),
            children,
            Strategy::OneForOne,
        )
        .spawn()
        .expect("ActorWithScope builds");
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
                    children.state.is_stopped()
                        && matches!(
                            children.state.last_exit().map(|exit| &exit.status),
                            Some(ExitStatusView::Failed(_))
                        )
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
    assert!(children_edge.state.is_stopped());

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[cfg(feature = "serde")]
#[test]
fn an_outline_round_trips_through_serde_with_scope_kinds() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let outline = OrderedTree::graph(graph)
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
