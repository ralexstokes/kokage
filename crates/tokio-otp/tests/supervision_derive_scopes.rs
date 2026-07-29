//! Supervision-shaped `#[derive(Supervision)]`: nested scopes, path-qualified
//! labels, and dynamic marker scopes.

use std::time::Duration;

use tokio_otp::{
    __private::SupervisionFactories, DynamicScope, GraphLookupError, ScopeKind, Supervision,
    observe::ChildOutline, prelude::*,
};

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
    // Child ids are local to their scope, so the supervisor path spells the
    // qualified graph label exactly once: root.workers.parse.
    assert_eq!(workers.child_ids(), ["parse", "render"]);
}

#[test]
fn actor_refs_are_qualified_by_scope_path() {
    let (_tree, refs) = App::tree(app_factories).expect("tree builds");
    let mut labels = vec![
        refs.ingest.id(),
        refs.workers.parse.id(),
        refs.workers.render.id(),
    ];
    labels.sort_unstable();
    assert_eq!(labels, ["ingest", "workers.parse", "workers.render"]);
}

#[tokio::test]
async fn a_derived_runtime_runs_actors_across_scope_levels() {
    let (tree, refs) = App::tree(app_factories).expect("tree builds");
    let handle = tree.spawn().expect("runtime builds");

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
    let (tree, refs) = Renamed::tree(wire).expect("tree builds");
    let mut labels = vec![
        refs.ingest.id(),
        refs.workers.parse.id(),
        refs.workers.render.id(),
    ];
    labels.sort_unstable();
    assert_eq!(labels, ["collector", "pool.parse", "pool.render"]);

    assert_eq!(tree.outline().child_ids(), ["collector", "pool"]);
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
        sessions: DynamicTree::new().default_restart(RestartPolicy::Never),
    })
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
    let (tree, _refs) = WithDynamic::tree(|_refs| WithDynamicFactories {
        manager: || Worker,
        sessions: DynamicTree::new(),
    })
    .expect("tree builds");
    let handle = tree.spawn().expect("runtime builds");
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
fn a_declaration_without_scope_attributes_is_an_ordered_tree() {
    let (tree, _refs) = Flat::tree(|_refs| FlatFactories {
        ingest: || Worker,
        parse: || Worker,
    })
    .expect("tree builds");

    let outline = tree.outline();
    assert_eq!(outline.kind, ScopeKind::Ordered);
    assert_eq!(outline.child_ids(), ["ingest", "parse"]);
}

#[test]
fn a_node_built_from_a_foreign_graph_reports_a_build_error() {
    // `node` resolves each declared actor out of the graph `open` populated.
    // Handing it a different graph returns an ordinary graph build error.
    let mut builder = GraphBuilder::new();
    let (slots, refs) = <Flat as Supervision>::open(&mut builder, "");
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

    assert!(matches!(
        <Flat as Supervision>::node(&foreign, &refs, scopes),
        Err(GraphLookupError::ForeignActorRef { actor_id, .. }) if actor_id == "ingest"
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
    // The identity-owning tree is what a `#[supervision(dynamic)]` field buys
    // over appending the scope afterwards: the handle exists early enough to become
    // a durable factory field, so it survives restarts of the actor holding it.
    let sessions = DynamicTree::new();
    let mount = sessions.handle();

    let (tree, refs) = Mounted::tree(|_refs| MountedFactories {
        mounter: move || Mounter {
            sessions: mount.clone(),
        },
        sessions,
    })
    .expect("tree builds");
    let handle = tree.spawn().expect("runtime builds");
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
