//! Build-plan data and deterministic simulated compilation.

use std::{thread, time::Duration};

use tokio_otp::CancellationToken;

pub type TargetId = &'static str;
pub type Digest = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Behavior {
    Sound,
    CrashOnce,
}

#[derive(Clone, Debug)]
pub struct Action {
    pub target: TargetId,
    pub dependencies: &'static [TargetId],
    pub source: &'static str,
    pub behavior: Behavior,
}

#[derive(Debug)]
pub struct BuildPlan {
    actions: Vec<Action>,
}

impl BuildPlan {
    pub fn demo() -> Self {
        Self {
            actions: vec![
                Action {
                    target: "fetch",
                    dependencies: &[],
                    source: "Cargo.lock",
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "codegen",
                    dependencies: &["fetch"],
                    source: "schema.proto",
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "core",
                    dependencies: &["fetch", "codegen"],
                    source: "src/core/**",
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "network",
                    dependencies: &["fetch", "codegen"],
                    source: "src/network/**",
                    behavior: Behavior::CrashOnce,
                },
                Action {
                    target: "cli",
                    dependencies: &["core", "network"],
                    source: "src/bin/cli.rs",
                    behavior: Behavior::Sound,
                },
                Action {
                    target: "tests",
                    dependencies: &["cli"],
                    source: "tests/**",
                    behavior: Behavior::Sound,
                },
            ],
        }
    }

    pub fn actions(&self) -> &[Action] {
        &self.actions
    }
}

pub fn digest(action: &Action, mut dependencies: Vec<(TargetId, Digest)>) -> Digest {
    dependencies.sort_unstable();
    let mut hash = fnv(FNV_OFFSET, action.target.as_bytes());
    hash = fnv(hash, action.source.as_bytes());
    for (target, dependency) in dependencies {
        hash = fnv(hash, target.as_bytes());
        hash = fnv(hash, &dependency.to_le_bytes());
    }
    hash
}

pub fn compile(action: &Action, attempt: u32, cancellation: &CancellationToken) -> Option<usize> {
    let mut hash = fnv(FNV_OFFSET, action.source.as_bytes());
    for chunk in 0_u64..4 {
        if cancellation.is_cancelled() {
            return None;
        }
        thread::sleep(Duration::from_millis(8));
        for round in 0_u64..15_000 {
            hash = fnv(hash, &(round ^ chunk).to_le_bytes());
        }
    }
    assert!(
        action.behavior != Behavior::CrashOnce || attempt != 1,
        "toolchain crashed compiling {}",
        action.target
    );
    Some(1_024 + (hash % 4_096) as usize)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
