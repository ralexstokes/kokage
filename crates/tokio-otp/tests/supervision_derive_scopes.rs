//! Supervision-shaped `#[derive(Supervision)]`: nested scopes, path-qualified
//! labels, and dynamic marker scopes.

use std::time::Duration;

use tokio_otp::{ChildOutline, DynamicScope, ScopeKind, SupervisorBuildError, prelude::*};

const CALL_TIMEOUT: Duration = Duration::from_secs(5);

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

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForAll)]
struct Workers {
    parse: Worker,
    render: Worker,
}

#[derive(Supervision)]
#[supervision(strategy = Strategy::OneForOne)]
struct App {
    #[supervision(restart = RestartPolicy::Never)]
    ingest: Worker,
    #[supervision(scope)]
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
    let (tree, _refs) = App::tree(app_factories).expect("tree builds");
    let outline = tree.outline().expect("valid tree has an outline");

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
    // Child ids are local to their scope, so the supervisor path spells the
    // qualified graph label exactly once: root.workers.parse.
    assert_eq!(workers.child_ids(), ["parse", "render"]);
}

#[test]
fn graph_labels_are_qualified_by_scope_path() {
    let (graph, _refs) = App::graph(app_factories).expect("graph builds");
    let mut labels: Vec<_> = graph.actors().iter().map(|actor| actor.label()).collect();
    labels.sort_unstable();
    assert_eq!(labels, ["ingest", "workers.parse", "workers.render"]);
}

#[tokio::test]
async fn a_derived_runtime_runs_actors_across_scope_levels() {
    let (runtime, refs) = App::runtime(app_factories).expect("runtime builds");
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

    // Stats report the graph label, which is qualified...
    let mut labels: Vec<_> = handle
        .actor_stats()
        .into_iter()
        .map(|stats| stats.actor_id.to_string())
        .collect();
    labels.sort();
    assert_eq!(labels, ["ingest", "workers.parse", "workers.render"]);

    // ...while the supervisor names each child locally, so scope path plus
    // child id reconstructs the label instead of repeating the scope name.
    let workers = handle.subtree("workers").expect("workers scope");
    let mut ids: Vec<_> = workers
        .snapshot()
        .children
        .into_iter()
        .map(|child| child.id.to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, ["parse", "render"]);

    handle.shutdown_and_wait().await.expect("clean shutdown");
}

#[derive(Supervision)]
struct Renamed {
    #[supervision(label = "collector")]
    ingest: Worker,
    #[supervision(scope, label = "pool")]
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
    let (graph, _refs) = Renamed::graph(wire).expect("graph builds");

    let mut labels: Vec<_> = graph.actors().iter().map(|actor| actor.label()).collect();
    labels.sort_unstable();
    assert_eq!(labels, ["collector", "pool.parse", "pool.render"]);

    let (tree, _refs) = Renamed::tree(wire).expect("tree builds");
    assert_eq!(
        tree.outline()
            .expect("valid tree has an outline")
            .child_ids(),
        ["collector", "pool"]
    );
}

#[derive(Supervision)]
struct WithDynamic {
    manager: Worker,
    #[supervision(dynamic)]
    sessions: DynamicScope,
}

#[test]
fn a_dynamic_marker_field_declares_an_empty_runtime_written_scope() {
    let (tree, _refs) = WithDynamic::tree(|_refs| WithDynamicFactories {
        manager: || Worker,
        sessions: Runtime::dynamic().default_restart(RestartPolicy::Never),
    })
    .expect("tree builds");
    let outline = tree.outline().expect("valid tree has an outline");

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
    let (runtime, _refs) = WithDynamic::runtime(|_refs| WithDynamicFactories {
        manager: || Worker,
        sessions: Runtime::dynamic(),
    })
    .expect("runtime builds");
    let handle = runtime.spawn();
    handle.wait_started().await.expect("runtime starts");

    let sessions = handle.subtree("sessions").expect("dynamic subtree exists");
    let session = sessions
        .add_actor("session-1", || Worker)
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

#[derive(Supervision)]
struct Flat {
    ingest: Worker,
    parse: Worker,
}

#[test]
fn a_declaration_without_scope_attributes_matches_a_whole_graph_tree() {
    let (tree, _refs) = Flat::tree(|_refs| FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    })
    .expect("tree builds");
    let (graph, _refs) = Flat::graph(|_refs| FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    })
    .expect("graph builds");

    assert_eq!(
        tree.outline().expect("valid tree has an outline"),
        SupervisionTree::graph(&graph)
            .outline()
            .expect("valid tree has an outline")
    );
}

#[test]
fn a_node_built_from_a_foreign_graph_reports_a_build_error() {
    // `node` resolves each declared actor out of the graph `open` populated.
    // Handing it a different graph used to panic inside generated code; it now
    // poisons the scope, so the mismatch arrives as an ordinary build error.
    let mut builder = GraphBuilder::new();
    let (slots, _refs) = <Flat as Supervision>::open(&mut builder, "");
    let scopes = FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    }
    .define(&mut builder, slots);
    builder.build().expect("the declared graph still builds");

    let mut foreign = GraphBuilder::new();
    let (actor_slot, _) = foreign.slot("unrelated");
    foreign.define(actor_slot, || Worker);
    let foreign = foreign.build().expect("foreign graph builds");

    let tree = <Flat as Supervision>::node(&foreign, scopes, "flat", "");
    assert!(matches!(
        tree.outline(),
        Err(SupervisorBuildError::InvalidConfig(_))
    ));
    assert!(matches!(
        tree.build(),
        Err(SupervisorBuildError::InvalidConfig(_))
    ));
}

/// An actor holding the mount handle of a dynamic scope declared beside it.
struct Mounter {
    sessions: RuntimeHandle,
}

impl Actor for Mounter {
    type Msg = Reply<u32>;

    async fn handle(
        &mut self,
        reply: Reply<u32>,
        _ctx: &mut MessageContext<'_, Self>,
    ) -> ActorResult {
        let worker = self
            .sessions
            .add_actor("spawned", || Worker)
            .await
            .expect("dynamic scope accepts the actor");
        reply.send(worker.call(CALL_TIMEOUT, |reply| reply).await?);
        Ok(())
    }
}

#[derive(Supervision)]
struct Mounted {
    mounter: Mounter,
    #[supervision(dynamic)]
    sessions: DynamicScope,
}

#[tokio::test]
async fn a_dynamic_scope_hands_out_its_mount_before_wiring() {
    // The reservation is what a `#[supervision(dynamic)]` field buys over
    // appending the scope afterwards: the handle exists early enough to become
    // a durable factory field, so it survives restarts of the actor holding it.
    let sessions = Runtime::dynamic();
    let mount = sessions.handle();

    let (runtime, refs) = Mounted::runtime(|_refs| MountedFactories {
        mounter: move || Mounter {
            sessions: mount.clone(),
        },
        sessions,
    })
    .expect("runtime builds");
    let handle = runtime.spawn();
    handle.wait_started().await.expect("runtime starts");

    // The mounter reaches the declared scope through the handle it was built
    // with, and the actor it adds there answers.
    assert_eq!(
        refs.mounter
            .call(CALL_TIMEOUT, |reply| reply)
            .await
            .expect("mounter replies"),
        7
    );

    let sessions = handle.subtree("sessions").expect("dynamic subtree exists");
    assert_eq!(sessions.snapshot().kind, ScopeKind::Dynamic);
    assert_eq!(
        sessions
            .snapshot()
            .children
            .into_iter()
            .map(|child| child.id.to_string())
            .collect::<Vec<_>>(),
        ["spawned"]
    );

    handle.shutdown_and_wait().await.expect("clean shutdown");
}
