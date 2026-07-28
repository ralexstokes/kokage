//! Recursive supervision-tree declarations and lowering.

use std::time::Duration;

use tokio_otp::{ActorSpec, ChildOutline, ChildSpec, ScopeKind, SupervisionTree, prelude::*};

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
    let outline = SupervisionTree::new()
        .strategy(Strategy::RestForOne)
        .default_restart(RestartPolicy::Always)
        .subtree(
            "workers",
            SupervisionTree::new().strategy(Strategy::OneForAll),
        )
        .task(ChildSpec::new("clock", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .actor(ActorSpec::new(graph.actors()[0].clone()).restart(RestartPolicy::Never))
        .actor(graph.actors()[1].clone())
        .outline()
        .expect("valid tree has an outline");

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
}

#[test]
fn a_hand_built_tree_and_the_equivalent_builder_outline_identically() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let from_builder = Runtime::builder()
        .graph(graph.clone())
        .strategy(Strategy::OneForAll)
        .default_restart(RestartPolicy::Never)
        .into_tree()
        .outline()
        .expect("valid builder tree has an outline");
    let from_tree = SupervisionTree::graph(&graph)
        .strategy(Strategy::OneForAll)
        .default_restart(RestartPolicy::Never)
        .outline()
        .expect("valid hand-built tree has an outline");
    assert_eq!(from_builder, from_tree);
}

#[tokio::test]
async fn a_tree_spreads_one_graph_across_ordered_scope_levels() {
    let (graph, ingest, parse) = two_actor_graph();
    let (ingest_actor, parse_actor) = (graph.actors()[0].clone(), graph.actors()[1].clone());
    let runtime = SupervisionTree::new()
        .actor(ActorSpec::new(ingest_actor).restart(RestartPolicy::Never))
        .subtree(
            "workers",
            SupervisionTree::new()
                .strategy(Strategy::OneForAll)
                .actor(parse_actor),
        )
        .build()
        .expect("tree builds");
    let handle = runtime.spawn();

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
        .actor_stats()
        .into_iter()
        .map(|stats| stats.actor_id.to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, ["ingest", "parse"]);
    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[test]
fn dynamic_scope_rejects_strategy_and_declared_children() {
    let strategy = SupervisionTree::dynamic()
        .strategy(Strategy::OneForAll)
        .build()
        .expect_err("dynamic scopes reject group strategies");
    assert!(strategy.to_string().contains("Strategy::OneForOne"));

    let child = SupervisionTree::dynamic()
        .task(ChildSpec::new("declared", |_| async { Ok(()) }))
        .build()
        .expect_err("dynamic scopes reject declared children");
    assert!(child.to_string().contains("cannot have declared children"));
}

#[test]
fn child_nodes_report_invalid_root_configuration_without_panicking() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let actor = ActorSpec::new(graph.actors()[0].clone());
    let leaf = SupervisionTree::Actor(actor.clone());

    assert_eq!(leaf.kind(), None);
    assert!(leaf.children().is_empty());
    let outline_error = leaf
        .outline()
        .expect_err("a child node cannot be an outline root");
    assert!(
        outline_error
            .to_string()
            .contains("root must be an ordered or dynamic scope")
    );

    let build_error = leaf
        .strategy(Strategy::OneForAll)
        .restart_intensity(RestartIntensity::default())
        .default_restart(RestartPolicy::Never)
        .default_shutdown(ShutdownPolicy::abort())
        .actor(actor.clone())
        .task(ChildSpec::new("ignored", |_| async { Ok(()) }))
        .child(SupervisionTree::Actor(actor.clone()))
        .subtree("ignored-scope", SupervisionTree::new())
        .actor_with_scope(
            "ignored-owned",
            actor,
            SupervisionTree::new(),
            Strategy::RestForOne,
        )
        .build()
        .expect_err("a child node cannot be built as a root");
    assert!(
        build_error
            .to_string()
            .contains("root must be an ordered or dynamic scope")
    );
}

#[test]
fn invalid_nested_scopes_are_deferred_to_outline_and_build() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let actor = ActorSpec::new(graph.actors()[0].clone());
    let invalid_subtree =
        SupervisionTree::new().subtree("workers", SupervisionTree::Actor(actor.clone()));
    let outline_error = invalid_subtree
        .outline()
        .expect_err("a subtree must be a scope");
    assert!(outline_error.to_string().contains("nested subtree"));
    let build_error = invalid_subtree
        .build()
        .expect_err("a subtree must be a scope");
    assert!(build_error.to_string().contains("nested subtree"));

    let invalid_owned_scope = SupervisionTree::new().actor_with_scope(
        "owned",
        actor.clone(),
        SupervisionTree::Actor(actor),
        Strategy::RestForOne,
    );
    let debug = format!("{invalid_owned_scope:?}");
    assert!(debug.contains("InvalidSupervisionTree"), "{debug}");
    let build_error = invalid_owned_scope
        .build()
        .expect_err("actor-owned children must be a scope");
    assert!(
        build_error
            .to_string()
            .contains("root must be an ordered or dynamic scope")
    );
}

#[test]
fn dynamic_outlines_include_future_member_policy_defaults() {
    let standard = SupervisionTree::dynamic()
        .outline()
        .expect("valid dynamic tree has an outline");
    let customized = SupervisionTree::dynamic()
        .default_restart(RestartPolicy::Never)
        .default_shutdown(ShutdownPolicy::abort())
        .outline()
        .expect("valid dynamic tree has an outline");

    assert_ne!(standard, customized);
    assert_eq!(customized.default_restart, RestartPolicy::Never);
    assert_eq!(customized.default_shutdown, ShutdownPolicy::abort());
}

#[tokio::test]
async fn actor_with_scope_lowers_to_leader_then_children_scope() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let tree = SupervisionTree::new().actor_with_scope(
        "owned",
        graph.actors()[0].clone(),
        SupervisionTree::dynamic(),
        Strategy::RestForOne,
    );
    let outline = tree.outline().expect("valid tree has an outline");
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

    let handle = tree.build().expect("ActorWithScope lowers").spawn();
    handle.wait_started().await.expect("generated scope starts");
    let snapshot = handle.snapshot();
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

#[cfg(feature = "serde")]
#[test]
fn an_outline_round_trips_through_serde_with_scope_kinds() {
    let (graph, _ingest, _parse) = two_actor_graph();
    let outline = SupervisionTree::graph(&graph)
        .strategy(Strategy::RestForOne)
        .subtree(
            "workers",
            SupervisionTree::dynamic()
                .default_restart(RestartPolicy::Never)
                .default_shutdown(ShutdownPolicy::abort()),
        )
        .outline()
        .expect("valid tree has an outline");
    let json = serde_json::to_string(&outline).expect("outline serializes");
    let decoded: tokio_otp::SupervisionOutline =
        serde_json::from_str(&json).expect("outline deserializes");
    assert_eq!(outline, decoded);
    let ChildOutline::Scope {
        outline: workers, ..
    } = decoded.child("workers").expect("dynamic scope survives")
    else {
        panic!("expected dynamic scope");
    };
    assert_eq!(workers.default_restart, RestartPolicy::Never);
    assert_eq!(workers.default_shutdown, ShutdownPolicy::abort());
}
