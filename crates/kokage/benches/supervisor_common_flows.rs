use std::{
    env,
    future::Future,
    hint::black_box,
    io::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use kokage::{
    BoxError, DynamicTree, RestartPolicy, Strategy, TaskSpec, Tree,
    observe::{LifecycleEventKind, LifecycleWatch},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::Notify,
};

const DEFAULT_WARMUP_ITERS: usize = 10;
const DEFAULT_MEASURE_ITERS: usize = 100;

fn main() {
    let warmup_iters = iterations_from_env("BENCH_WARMUP_ITERS", DEFAULT_WARMUP_ITERS);
    let measure_iters = iterations_from_env("BENCH_ITERS", DEFAULT_MEASURE_ITERS);
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build");

    println!("kokage common flow benchmarks (warmup={warmup_iters}, measure={measure_iters})");

    // A child contributes both Added and Started to this direct watch. Keep
    // this case comfortably below the 128-event buffer or teach the helper to
    // fail explicitly on Lagged before increasing the child count.
    bench_async(
        &runtime,
        warmup_iters,
        measure_iters,
        "spawn_shutdown/8_children",
        || spawn_shutdown_flow(8),
    );
    bench_async(
        &runtime,
        warmup_iters,
        measure_iters,
        "one_for_one_restart/4_children",
        one_for_one_restart_flow,
    );
    bench_async(
        &runtime,
        warmup_iters,
        measure_iters,
        "one_for_all_restart/4_children",
        one_for_all_restart_flow,
    );
    bench_async(
        &runtime,
        warmup_iters,
        measure_iters,
        "dynamic_add_remove",
        dynamic_add_remove_flow,
    );
}

fn iterations_from_env(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn bench_async<F, Fut>(
    runtime: &Runtime,
    warmup_iters: usize,
    measure_iters: usize,
    name: &str,
    mut bench_case: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    for _ in 0..warmup_iters {
        runtime.block_on(bench_case());
    }

    let started = Instant::now();
    for _ in 0..measure_iters {
        runtime.block_on(bench_case());
    }

    let elapsed = started.elapsed();
    let micros_per_iter = elapsed.as_secs_f64() * 1_000_000.0 / measure_iters as f64;
    println!("{name:30} {micros_per_iter:10.2} us/op");
}

async fn spawn_shutdown_flow(children: usize) {
    let mut builder = Tree::new();
    for index in 0..children {
        builder.add_task_spec(TaskSpec::new(format!("worker-{index}"), |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    }

    let handle_owner = builder.spawn().expect("benchmark tree should spawn");
    let handle = handle_owner.scope();
    let mut events = handle.watch_lifecycle();
    let started = wait_for_child_start_count(&mut events, children).await;
    black_box(started);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

async fn one_for_one_restart_flow() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let trigger_failure = Arc::new(Notify::new());
    let flaky_attempts = Arc::clone(&attempts);
    let flaky_trigger = Arc::clone(&trigger_failure);
    let flaky = TaskSpec::new("flaky", move |ctx| {
        let attempts = Arc::clone(&flaky_attempts);
        let trigger_failure = Arc::clone(&flaky_trigger);
        async move {
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                trigger_failure.notified().await;
                return Err(bench_error("restart me"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::on_failure());

    let mut builder = Tree::new().strategy(Strategy::OneForOne);
    builder.add_task_spec(flaky);
    for index in 0..3 {
        builder.add_task_spec(TaskSpec::new(format!("peer-{index}"), |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }));
    }

    let handle_owner = builder.spawn().expect("benchmark tree should spawn");
    let handle = handle_owner.scope();
    let mut snapshots = handle.subscribe_snapshots();
    let baseline = handle
        .snapshot()
        .child("flaky")
        .expect("flaky child should be known")
        .generation;
    trigger_failure.notify_one();
    let generation = snapshots
        .wait_for_child("flaky", |child| {
            child.generation > baseline && child.state.is_running()
        })
        .await
        .expect("supervisor remains live during benchmark")
        .generation;
    black_box(generation);
    black_box(attempts.load(Ordering::Relaxed));

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

async fn one_for_all_restart_flow() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let trigger_attempts = Arc::clone(&attempts);
    let trigger = TaskSpec::new("trigger", move |ctx| {
        let attempts = Arc::clone(&trigger_attempts);
        async move {
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(bench_error("restart group"));
            }

            ctx.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .restart(RestartPolicy::on_failure());

    let mut builder = Tree::new().strategy(Strategy::OneForAll);
    builder.add_task_spec(trigger);
    for index in 0..3 {
        builder.add_task_spec(
            TaskSpec::new(format!("peer-{index}"), |ctx| async move {
                ctx.shutdown_token().cancelled().await;
                Ok(())
            })
            .restart(RestartPolicy::always()),
        );
    }

    let handle_owner = builder.spawn().expect("benchmark tree should spawn");
    let handle = handle_owner.scope();
    let mut events = handle.watch_lifecycle();
    let restarted = wait_for_restart_count(&mut events, 4).await;
    black_box(restarted);

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

async fn dynamic_add_remove_flow() {
    let handle_owner = DynamicTree::new()
        .spawn()
        .expect("benchmark tree should spawn");
    let handle = handle_owner.scope();
    let mut events = handle.watch_lifecycle();

    handle
        .add_task_spec(TaskSpec::new("seed", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("seed child should be accepted");

    wait_for_named_child_started(&mut events, "seed").await;

    handle
        .add_task_spec(TaskSpec::new("dynamic", |ctx| async move {
            ctx.shutdown_token().cancelled().await;
            Ok(())
        }))
        .await
        .expect("dynamic child should be accepted");
    wait_for_named_child_started(&mut events, "dynamic").await;

    handle
        .remove_child("dynamic")
        .await
        .expect("dynamic child should be removable");
    wait_for_named_child_removed(&mut events, "dynamic").await;

    handle.shutdown();
    handle.wait().await.expect("shutdown should succeed");
}

async fn wait_for_child_start_count(events: &mut LifecycleWatch, expected: usize) -> usize {
    let mut started = 0;
    while started < expected {
        let event = events.next().await.expect("lifecycle stream");
        if matches!(event.kind, LifecycleEventKind::ChildStarted { .. }) {
            started += 1;
        }
    }
    started
}

async fn wait_for_restart_count(events: &mut LifecycleWatch, expected: usize) -> usize {
    let mut restarted = 0;
    while restarted < expected {
        let event = events.next().await.expect("lifecycle stream");
        if matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { generation: 1, .. }
        ) {
            restarted += 1;
        }
    }
    restarted
}

async fn wait_for_named_child_started(events: &mut LifecycleWatch, id: &str) {
    loop {
        let event = events.next().await.expect("lifecycle stream");
        if matches!(
            event.kind,
            LifecycleEventKind::ChildStarted { ref child_id, .. } if child_id == id
        ) {
            return;
        }
    }
}

async fn wait_for_named_child_removed(events: &mut LifecycleWatch, id: &str) {
    loop {
        let event = events.next().await.expect("lifecycle stream");
        if matches!(
            event.kind,
            LifecycleEventKind::ChildRemoved { ref child_id, .. } if child_id == id
        ) {
            return;
        }
    }
}

fn bench_error(message: &'static str) -> BoxError {
    Box::new(Error::other(message))
}
