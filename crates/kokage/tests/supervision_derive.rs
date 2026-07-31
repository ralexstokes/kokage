//! Runtime coverage for nested supervision declarations.

#![cfg(feature = "derive")]

use std::time::Duration;

use kokage::{ScopeRef, observe::ScopeKind, prelude::*};

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

fn probe_message_size(_message: &ProbeMsg) -> usize {
    1
}

#[derive(kokage::Supervision)]
#[supervision(
    strategy = Strategy::OneForAll,
    default_shutdown = Shutdown::graceful_for(Duration::from_secs(1))
)]
struct Workers {
    #[supervision(id = "left-worker")]
    left: Left,
    right: Right,
}

#[derive(kokage::Supervision)]
#[supervision(
    default_restart = RestartPolicy::never(),
    default_mailbox_shutdown = MailboxShutdown::Drain,
    mailbox_capacity = 16
)]
struct AppTree {
    #[supervision(dynamic, mailbox_capacity = 4)]
    sessions: kokage::DynamicScope,
    #[supervision(scope, restart = RestartPolicy::always())]
    workers: Workers,
    #[supervision(
        mailbox = Mailbox::queue(8),
        message_size = probe_message_size,
        mailbox_shutdown = MailboxShutdown::Discard
    )]
    probe: Probe,
}

#[derive(kokage::Supervision)]
struct HygieneTree {
    __scope: Session,
    __tree: Session,
    __kokage_scope: Session,
    __kokage_tree: Session,
    r#type: Session,
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
async fn nested_declaration_reserves_every_ref_before_wiring() {
    let (tree, handles) = AppTree::tree(|handles| AppTreeFactories {
        workers: WorkersFactories {
            left: {
                let right = handles.workers.right.clone();
                let probe = handles.probe.clone();
                move || Left {
                    right: right.clone(),
                    probe: probe.clone(),
                }
            },
            right: {
                let left = handles.workers.left.clone();
                move || Right { left: left.clone() }
            },
        },
        probe: {
            let left = handles.workers.left.clone();
            move || Probe { left: left.clone() }
        },
    });

    assert_eq!(
        child_ids(&handles.scope()),
        ["sessions", "workers", "probe"]
    );
    assert_eq!(handles.scope().kind(), ScopeKind::Ordered);
    assert_eq!(handles.workers.scope().kind(), ScopeKind::Ordered);
    assert_eq!(
        handles.workers.scope().snapshot().strategy,
        Strategy::OneForAll
    );
    assert_eq!(
        child_ids(&handles.workers.scope()),
        ["left-worker", "right"]
    );
    assert_eq!(handles.sessions.kind(), ScopeKind::Dynamic);
    assert!(handles.sessions.snapshot().children.is_empty());

    let runtime = tree.spawn().expect("derived tree should spawn");
    runtime
        .wait_started()
        .await
        .expect("tree should become ready");

    assert!(
        handles
            .workers
            .left
            .call(LeftMsg::Connected, Duration::from_secs(1))
            .await
            .expect("left call")
    );
    assert!(
        handles
            .workers
            .right
            .call(RightMsg::Connected, Duration::from_secs(1))
            .await
            .expect("right call")
    );
    assert!(
        handles
            .probe
            .call(ProbeMsg::Connected, Duration::from_secs(1))
            .await
            .expect("probe call")
    );

    let session = handles
        .sessions
        .add_actor("session", || Session)
        .await
        .expect("dynamic marker should expose dynamic membership");
    session.send(()).await.expect("dynamic actor should run");

    runtime.shutdown().await.expect("tree should stop cleanly");
}

#[test]
fn generated_state_does_not_reserve_field_names_or_raw_identifier_prefixes() {
    let (tree, handles) = HygieneTree::tree(|_| HygieneTreeFactories {
        __scope: || Session,
        __tree: || Session,
        __kokage_scope: || Session,
        __kokage_tree: || Session,
        r#type: || Session,
    });

    assert_eq!(
        child_ids(&handles.scope()),
        [
            "__scope",
            "__tree",
            "__kokage_scope",
            "__kokage_tree",
            "type",
        ]
    );
    let _: ActorRef<()> = handles.__scope;
    let _: ActorRef<()> = handles.__tree;
    let _: ActorRef<()> = handles.__kokage_scope;
    let _: ActorRef<()> = handles.__kokage_tree;
    let _: ActorRef<()> = handles.r#type;
    drop(tree);
}
