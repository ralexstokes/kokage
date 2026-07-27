//! Cross-actor timer utilities.
//!
//! Self-scheduled timers belong to the actor loop and are available through
//! [`LiveContext`](crate::LiveContext). These utilities cross an actor
//! boundary, so they deliver through the target's ordinary public
//! [`ActorRef`] API instead.

use std::time::Duration;

use tokio::time::MissedTickBehavior;

use crate::{ActorRef, CancellationHandle, Lifetime, actor::deadline_after};

/// Sends `message` to `target` after `delay` has elapsed.
///
/// The timer is bound to `lifetime`, normally obtained from the scheduling
/// actor's [`LiveContext::lifetime`](crate::LiveContext::lifetime). It is not
/// bound to the target: if the target restarts before fire time, delivery
/// follows the target's restart-stable `ActorRef`.
pub fn send_after_to<T: Send + 'static>(
    lifetime: &Lifetime,
    target: &ActorRef<T>,
    message: T,
    delay: Duration,
) -> CancellationHandle {
    let timer = CancellationHandle::new();
    let task_timer = timer.clone();
    let lifetime = lifetime.clone();
    let target = target.clone();

    tokio::spawn(async move {
        tokio::select! {
            biased;
            () = task_timer.cancelled() => {}
            () = lifetime.ended() => task_timer.cancel(),
            () = tokio::time::sleep(delay) => {
                tokio::select! {
                    biased;
                    () = task_timer.cancelled() => {}
                    () = lifetime.ended() => task_timer.cancel(),
                    _ = target.send(message) => {}
                }
            }
        }
    });

    timer
}

/// Sends a clone of `message` to `target` after every `period`.
///
/// Missed ticks are skipped. The timer stops when cancelled, when `lifetime`
/// ends, or when the target permanently terminates. A zero period returns an
/// already-cancelled handle and sends no messages.
pub fn interval_to<T: Clone + Send + 'static>(
    lifetime: &Lifetime,
    target: &ActorRef<T>,
    message: T,
    period: Duration,
) -> CancellationHandle {
    let timer = CancellationHandle::new();
    if period.is_zero() {
        timer.cancel();
        return timer;
    }

    let task_timer = timer.clone();
    let lifetime = lifetime.clone();
    let target = target.clone();
    tokio::spawn(async move {
        let start = deadline_after(period);
        let mut interval = tokio::time::interval_at(start, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = task_timer.cancelled() => break,
                () = lifetime.ended() => {
                    task_timer.cancel();
                    break;
                }
                _ = interval.tick() => {
                    let sent = tokio::select! {
                        biased;
                        () = task_timer.cancelled() => break,
                        () = lifetime.ended() => {
                            task_timer.cancel();
                            break;
                        }
                        sent = target.send(message.clone()) => sent,
                    };
                    if sent.is_err() {
                        task_timer.cancel();
                        break;
                    }
                }
            }
        }
    });

    timer
}
