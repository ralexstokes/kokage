//! The build graph: targets, their dependencies, and the simulated work.
//!
//! Everything here is deterministic. Digests are content addresses computed
//! from a target's own source plus its dependencies' digests, so the same plan
//! always produces the same addresses and a warm cache is a guaranteed hit.

use std::{thread::sleep, time::Duration};

use tokio_otp::CancellationToken;

/// A build target name, which is also its supervisor-visible work id.
pub type TargetId = &'static str;

/// A content address for one action and its transitive inputs.
pub type Digest = u64;

/// Scripted misbehavior, applied as a function of the attempt number so that a
/// retry can succeed where the first try did not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Behavior {
    /// Compiles normally on every attempt.
    Sound,
    /// Stalls past the pool's dispatch deadline on the first attempt only.
    ///
    /// The stall is cancellation-aware, so retiring the stuck worker cuts it
    /// short instead of leaking a blocking thread for its full duration.
    StallsOnce,
    /// Panics inside the blocking compile on every attempt.
    ///
    /// Termination comes from the attempt log, not from the action: once the
    /// shared log says the target is out of attempts, the worker refuses it
    /// before running any code that can panic.
    Poison,
}

/// One unit of build work.
#[derive(Clone, Debug)]
pub struct Action {
    /// Target name.
    pub target: TargetId,
    /// Targets that must be built before this one.
    pub deps: &'static [TargetId],
    /// Stand-in for the target's own source contents.
    pub source: &'static str,
    /// Simulated compile cost, spent as real CPU on the blocking pool.
    pub cycles: u64,
    /// Scripted misbehavior.
    pub behavior: Behavior,
}

/// The immutable build request.
#[derive(Debug)]
pub struct BuildPlan {
    actions: Vec<Action>,
}

impl BuildPlan {
    /// Builds the fixed nine-target plan used by this example.
    ///
    /// ```text
    /// fetch-deps
    ///   ├── proto-gen
    ///   │     ├── core-lib ──┬── ui-lib      (poison: fails, quarantined)
    ///   │     │              ├── docs
    ///   │     │              └── cli-bin ──┬── test-suite
    ///   │     └── net-lib ────────────────-┘
    ///   └──                                  app-bundle (needs cli-bin + ui-lib: skipped)
    /// ```
    pub fn demo() -> Self {
        Self {
            actions: vec![
                Action {
                    target: "fetch-deps",
                    deps: &[],
                    source: "lockfile-v3",
                    cycles: 40_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "proto-gen",
                    deps: &["fetch-deps"],
                    source: "schema.proto",
                    cycles: 60_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "core-lib",
                    deps: &["fetch-deps", "proto-gen"],
                    source: "core/**.rs",
                    cycles: 90_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "net-lib",
                    deps: &["fetch-deps", "proto-gen"],
                    source: "net/**.rs",
                    cycles: 70_000,
                    behavior: Behavior::StallsOnce,
                },
                Action {
                    target: "ui-lib",
                    deps: &["core-lib"],
                    source: "ui/**.rs",
                    cycles: 50_000,
                    behavior: Behavior::Poison,
                },
                Action {
                    target: "docs",
                    deps: &["core-lib"],
                    source: "docs/**.md",
                    cycles: 30_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "cli-bin",
                    deps: &["core-lib", "net-lib"],
                    source: "cli/main.rs",
                    cycles: 80_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "test-suite",
                    deps: &["cli-bin"],
                    source: "tests/**.rs",
                    cycles: 60_000,
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "app-bundle",
                    deps: &["cli-bin", "ui-lib"],
                    source: "bundle.toml",
                    cycles: 40_000,
                    behavior: Behavior::Sound,
                },
            ],
        }
    }

    /// Returns every action in declaration order.
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }
}

/// Computes the content address of an action given its resolved dependencies.
///
/// Dependency digests are folded in ordered by target name so the address does
/// not depend on the order the scheduler happened to finish them in.
pub fn digest(action: &Action, mut dep_digests: Vec<(TargetId, Digest)>) -> Digest {
    dep_digests.sort_unstable();
    let mut hash = fnv(FNV_OFFSET, action.source.as_bytes());
    hash = fnv(hash, action.target.as_bytes());
    for (target, dep) in dep_digests {
        hash = fnv(hash, target.as_bytes());
        hash = fnv(hash, &dep.to_le_bytes());
    }
    hash
}

/// Number of slices one compile is split into.
///
/// Real work is chunked so the worker can report progress between slices
/// instead of disappearing into one opaque blocking call.
pub const CHUNKS: u32 = 4;

/// Runs one slice of an action's simulated compile on the blocking pool.
///
/// Returns the slice's partial hash, or `None` when cancellation cut the work
/// short. `cancel` is the token handed to
/// [`LiveContext::run_blocking`](tokio_otp::LiveContext::run_blocking): it
/// fires on graph shutdown and on cooperative removal of the worker, which is
/// how a stalled action stops burning a thread instead of holding it for its
/// full scripted duration.
///
/// # Panics
///
/// Panics on the final slice for [`Behavior::Poison`], simulating a compiler
/// crash. The panic propagates out of the blocking pool and fails the worker
/// actor, which is the supervised failure this example is built around.
pub fn compile_chunk(
    action: &Action,
    attempt: u32,
    chunk: u32,
    cancel: &CancellationToken,
) -> Option<u64> {
    if action.behavior == Behavior::StallsOnce && attempt == 1 && chunk == 0 {
        // A wedged toolchain: sleep well past the pool's dispatch deadline,
        // but in slices so cancellation is observed promptly.
        for _ in 0..40 {
            if cancel.is_cancelled() {
                return None;
            }
            sleep(Duration::from_millis(25));
        }
        return None;
    }

    let mut hash = fnv(FNV_OFFSET, action.source.as_bytes());
    hash = fnv(hash, &chunk.to_le_bytes());
    for round in 0..action.cycles / u64::from(CHUNKS) {
        if round % 8_192 == 0 && cancel.is_cancelled() {
            return None;
        }
        hash = fnv(hash, &round.to_le_bytes());
    }

    assert!(
        action.behavior != Behavior::Poison || chunk + 1 < CHUNKS,
        "toolchain crashed compiling {}",
        action.target
    );

    Some(hash)
}

/// Derives a stable, plausible artifact size from a completed compile.
pub fn artifact_bytes(hash: u64) -> usize {
    1_024 + (hash % 4_096) as usize
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
