//! Supervision-shaped `#[derive(Topology)]`: nested scopes, path-qualified
//! labels, dynamic marker scopes, and actor-with-scope leaders.

use std::time::Duration;

use tokio_otp::{ChildOutline, DynamicActorOptions, DynamicScope, ScopeKind, prelude::*};

const CALL_TIMEOUT: Duration = Duration::from_secs(5);

struct Worker;

impl Actor for Worker {
    type Msg = Reply<u32>;

    async fn handle(
        &mut self,
        reply: Reply<u32>,
        _ctx: &mut ActorContext<Reply<u32>>,
    ) -> ActorResult {
        reply.send(7);
        Ok(Continue)
    }
}

#[derive(Topology)]
#[topology(strategy = Strategy::OneForAll)]
struct Workers {
    parse: Worker,
    render: Worker,
}

#[derive(Topology)]
#[topology(strategy = Strategy::OneForOne)]
struct App {
    #[topology(restart = RestartPolicy::Never)]
    ingest: Worker,
    #[topology(scope)]
    workers: Workers,
}

/// Naming the factory type lets several tests share one wiring closure.
type Build = fn() -> Worker;
type AppWiring = AppFactories<Build, WorkersFactories<Build, Build>>;

fn app_factories(_refs: &AppRefs) -> AppWiring {
    AppFactories {
        ingest: (|| Worker) as Build,
        workers: WorkersFactories {
            parse: (|| Worker) as Build,
            render: (|| Worker) as Build,
        },
    }
}

#[test]
fn a_nested_scope_becomes_a_named_subtree_with_path_qualified_labels() {
    let (tree, _refs) = App::tree_with_refs(app_factories).expect("tree builds");
    let outline = tree.outline();

    assert_eq!(outline.kind, ScopeKind::Ordered);
    assert_eq!(outline.strategy, Strategy::OneForOne);
    assert_eq!(outline.child_ids(), ["ingest", "workers"]);

    let ChildOutline::Actor { restart, .. } = outline.child("ingest").expect("ingest is declared")
    else {
        panic!("expected an actor");
    };
    assert_eq!(*restart, RestartPolicy::Never);

    let ChildOutline::Scope {
        outline: workers, ..
    } = outline.child("workers").expect("workers scope is declared")
    else {
        panic!("expected a scope");
    };
    assert_eq!(workers.strategy, Strategy::OneForAll);
    // An actor keeps one name everywhere: its qualified graph label is also its
    // supervisor child id, so stats, tracing, and snapshots share a key.
    assert_eq!(workers.child_ids(), ["workers.parse", "workers.render"]);
}

#[test]
fn graph_labels_are_qualified_by_scope_path() {
    let graph = App::graph(app_factories).expect("graph builds");
    let mut labels: Vec<_> = graph.actors().iter().map(|actor| actor.label()).collect();
    labels.sort_unstable();
    assert_eq!(labels, ["ingest", "workers.parse", "workers.render"]);
}

#[tokio::test]
async fn a_derived_runtime_runs_actors_across_scope_levels() {
    let (runtime, refs) = App::runtime_with_refs(app_factories).expect("runtime builds");
    let handle = runtime.spawn();

    for actor in [&refs.ingest, &refs.workers.parse, &refs.workers.render] {
        assert_eq!(
            actor
                .call(CALL_TIMEOUT, |reply| reply)
                .await
                .expect("actor replies"),
            7
        );
    }

    let mut labels: Vec<_> = handle
        .actor_stats()
        .into_iter()
        .map(|stats| stats.actor_id.to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, ["ingest", "workers.parse", "workers.render"]);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Topology)]
struct Renamed {
    #[topology(label = "collector")]
    ingest: Worker,
    #[topology(scope, label = "pool")]
    workers: Workers,
}

#[test]
fn a_label_attribute_overrides_the_field_name_in_paths_and_child_ids() {
    let wire = |_refs: &RenamedRefs| RenamedFactories {
        ingest: || Worker,
        workers: WorkersFactories {
            parse: || Worker,
            render: || Worker,
        },
    };
    let graph = Renamed::graph(wire).expect("graph builds");

    let mut labels: Vec<_> = graph.actors().iter().map(|actor| actor.label()).collect();
    labels.sort_unstable();
    assert_eq!(labels, ["collector", "pool.parse", "pool.render"]);

    let (tree, _refs) = Renamed::tree_with_refs(wire).expect("tree builds");
    assert_eq!(tree.outline().child_ids(), ["collector", "pool"]);
}

#[derive(Topology)]
struct WithDynamic {
    manager: Worker,
    #[topology(dynamic, restart = RestartPolicy::Never)]
    sessions: DynamicScope,
}

#[test]
fn a_dynamic_marker_field_declares_an_empty_runtime_written_scope() {
    let (tree, _refs) =
        WithDynamic::tree_with_refs(|_refs| WithDynamicFactories { manager: || Worker })
            .expect("tree builds");
    let outline = tree.outline();

    assert_eq!(outline.child_ids(), ["manager", "sessions"]);
    let ChildOutline::Scope {
        outline: sessions, ..
    } = outline
        .child("sessions")
        .expect("dynamic scope is declared")
    else {
        panic!("expected a scope");
    };
    assert_eq!(sessions.kind, ScopeKind::Dynamic);
    assert_eq!(sessions.default_restart, RestartPolicy::Never);
    assert!(sessions.children.is_empty());
}

#[tokio::test]
async fn a_dynamic_marker_scope_accepts_actors_at_runtime() {
    let runtime = WithDynamic::runtime(|_refs| WithDynamicFactories { manager: || Worker })
        .expect("runtime builds");
    let handle = runtime.spawn();
    handle.wait_started().await.expect("runtime starts");

    let sessions = handle.subtree("sessions").expect("dynamic subtree exists");
    let session = sessions
        .add_actor("session-1", || Worker, DynamicActorOptions::new())
        .await
        .expect("actor added");
    assert_eq!(
        session
            .call(CALL_TIMEOUT, |reply| reply)
            .await
            .expect("session replies"),
        7
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Topology)]
#[topology(leader_strategy = Strategy::OneForAll)]
struct Pool {
    #[topology(leader)]
    manager: Worker,
    #[topology(dynamic)]
    workers: DynamicScope,
}

#[derive(Topology)]
struct LeaderApp {
    front: Worker,
    #[topology(scope)]
    pool: Pool,
}

#[test]
fn a_leader_field_lowers_to_an_actor_with_scope_node() {
    let (tree, _refs) = LeaderApp::tree_with_refs(|_refs| LeaderAppFactories {
        front: || Worker,
        pool: PoolFactories { manager: || Worker },
    })
    .expect("tree builds");
    let outline = tree.outline();

    assert_eq!(outline.child_ids(), ["front", "pool"]);
    let ChildOutline::ActorWithScope {
        leader,
        children,
        strategy,
        ..
    } = outline.child("pool").expect("pool node is declared")
    else {
        panic!("expected an actor-with-scope node");
    };
    assert_eq!(*strategy, Strategy::OneForAll);
    assert_eq!(leader.id(), "pool.manager");
    assert_eq!(children.child_ids(), ["workers"]);
}

#[tokio::test]
async fn a_leader_runs_ahead_of_the_scope_it_owns() {
    let (runtime, refs) = LeaderApp::runtime_with_refs(|_refs| LeaderAppFactories {
        front: || Worker,
        pool: PoolFactories { manager: || Worker },
    })
    .expect("runtime builds");
    let handle = runtime.spawn();

    assert_eq!(
        refs.pool
            .manager
            .call(CALL_TIMEOUT, |reply| reply)
            .await
            .expect("leader replies"),
        7
    );
    assert_eq!(
        refs.front
            .call(CALL_TIMEOUT, |reply| reply)
            .await
            .expect("front replies"),
        7
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Topology)]
struct Flat {
    ingest: Worker,
    parse: Worker,
}

#[test]
fn a_topology_without_supervision_attributes_matches_a_whole_graph_tree() {
    let (tree, _refs) = Flat::tree_with_refs(|_refs| FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    })
    .expect("tree builds");
    let graph = Flat::graph(|_refs| FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    })
    .expect("graph builds");

    assert_eq!(tree.outline(), SupervisionTree::graph(&graph).outline());
}
