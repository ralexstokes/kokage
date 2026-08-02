use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use kokage::{
    Actor, ActorSpec, Context, ExitResult, RestartPolicy, Shutdown, StopContext, TimerKey, Tree,
};
use tokio::{
    sync::{Notify, mpsc},
    time::timeout,
};

const SHARED_TIMER: TimerKey = TimerKey::new("shared");

#[derive(Debug, PartialEq, Eq)]
enum Event {
    WrapperStartBefore,
    InnerStart,
    WrapperStartAfter,
    WrapperHandleBefore {
        draining: bool,
    },
    InnerHandle {
        message: &'static str,
        draining: bool,
    },
    WrapperHandleAfter {
        draining: bool,
    },
    WrapperStopBefore,
    InnerStop,
    WrapperStopAfter,
}

type Events = Arc<Mutex<Vec<Event>>>;

struct Middleware<A: Actor> {
    inner: A,
    events: Events,
    startup_timer: Option<A::Msg>,
}

impl<A: Actor> Middleware<A> {
    fn record(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

impl<A: Actor> Actor for Middleware<A> {
    type Msg = A::Msg;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.record(Event::WrapperStartBefore);
        if let Some(message) = self.startup_timer.take() {
            ctx.set_timeout(SHARED_TIMER, message, Duration::from_millis(20));
        }
        let mut inner_ctx = ctx.for_actor();
        let result = self.inner.on_start(&mut inner_ctx).await;
        self.record(Event::WrapperStartAfter);
        result
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.record(Event::WrapperHandleBefore {
            draining: ctx.is_draining(),
        });
        let mut inner_ctx = ctx.for_actor();
        let result = self.inner.handle(message, &mut inner_ctx).await;
        self.record(Event::WrapperHandleAfter {
            draining: ctx.is_draining(),
        });
        result
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>) -> ExitResult {
        self.record(Event::WrapperStopBefore);
        let mut inner_ctx = ctx.for_actor();
        let result = self.inner.on_stop(&mut inner_ctx).await;
        self.record(Event::WrapperStopAfter);
        result
    }
}

struct Inner {
    events: Events,
    stop_started: Arc<Notify>,
    release_stop: Arc<Notify>,
    stopped: Arc<Notify>,
}

impl Inner {
    fn record(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

impl Actor for Inner {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        assert_eq!(ctx.id(), "wrapped");
        self.record(Event::InnerStart);
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, ctx: &mut Context<'_, Self>) -> ExitResult {
        self.record(Event::InnerHandle {
            message,
            draining: ctx.is_draining(),
        });

        match message {
            "stop" => {
                ctx.stop();
                self.stop_started.notify_one();
                self.release_stop.notified().await;
            }
            "drain" => assert!(ctx.is_draining()),
            other => panic!("unexpected message: {other}"),
        }

        Ok(())
    }

    async fn on_stop(&mut self, ctx: &mut StopContext<'_, Self>) -> ExitResult {
        assert_eq!(ctx.id(), "wrapped");
        self.record(Event::InnerStop);
        self.stopped.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn same_message_middleware_delegates_lifecycle_stop_and_drain_context() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let stop_started = Arc::new(Notify::new());
    let release_stop = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let mut tree = Tree::new();
    let actor = tree.add_actor_spec(
        ActorSpec::new("wrapped", {
            let events = Arc::clone(&events);
            let stop_started = Arc::clone(&stop_started);
            let release_stop = Arc::clone(&release_stop);
            let stopped = Arc::clone(&stopped);
            move || Middleware {
                inner: Inner {
                    events: Arc::clone(&events),
                    stop_started: Arc::clone(&stop_started),
                    release_stop: Arc::clone(&release_stop),
                    stopped: Arc::clone(&stopped),
                },
                events: Arc::clone(&events),
                startup_timer: None,
            }
        })
        .shutdown(Shutdown::graceful_for(Duration::from_secs(1))),
    );
    let handle = tree
        .default_child_restart(RestartPolicy::never())
        .spawn()
        .unwrap();
    handle.scope().wait_started().await.unwrap();

    actor.send("stop").await.unwrap();
    stop_started.notified().await;
    actor.send("drain").await.unwrap();
    release_stop.notify_one();
    let expected = [
        Event::WrapperStartBefore,
        Event::InnerStart,
        Event::WrapperStartAfter,
        Event::WrapperHandleBefore { draining: false },
        Event::InnerHandle {
            message: "stop",
            draining: false,
        },
        Event::WrapperHandleAfter { draining: false },
        Event::WrapperHandleBefore { draining: true },
        Event::InnerHandle {
            message: "drain",
            draining: true,
        },
        Event::WrapperHandleAfter { draining: true },
        Event::WrapperStopBefore,
        Event::InnerStop,
        Event::WrapperStopAfter,
    ];
    tokio::time::timeout(Duration::from_secs(1), stopped.notified())
        .await
        .expect("inner on_stop runs after the inner view requests a stop");
    tokio::time::timeout(Duration::from_secs(1), async {
        while events.lock().unwrap().len() < expected.len() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wrapper observes completion of every delegated hook");

    assert_eq!(&*events.lock().unwrap(), &expected);

    handle.shutdown().await.unwrap();
}

struct TimerInner {
    observed: mpsc::UnboundedSender<&'static str>,
}

impl Actor for TimerInner {
    type Msg = &'static str;

    async fn on_start(&mut self, ctx: &mut Context<'_, Self>) -> ExitResult {
        ctx.set_timeout(SHARED_TIMER, "inner", Duration::from_millis(20));
        Ok(())
    }

    async fn handle(&mut self, message: Self::Msg, _ctx: &mut Context<'_, Self>) -> ExitResult {
        self.observed.send(message).expect("observer remains live");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn inner_same_key_timer_replaces_the_wrapper_timer() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut tree = Tree::new();
    tree.add_actor_spec(ActorSpec::new("timer-wrapped", {
        let events = Arc::clone(&events);
        move || Middleware {
            inner: TimerInner {
                observed: observed_tx.clone(),
            },
            events: Arc::clone(&events),
            startup_timer: Some("wrapper"),
        }
    }));
    let handle = tree.spawn().unwrap();

    assert_eq!(
        timeout(Duration::from_secs(1), observed_rx.recv())
            .await
            .expect("inner timer fires"),
        Some("inner")
    );
    assert!(
        timeout(Duration::from_millis(60), observed_rx.recv())
            .await
            .is_err(),
        "the wrapper timer is replaced rather than delivered separately"
    );

    handle.shutdown().await.unwrap();
}
