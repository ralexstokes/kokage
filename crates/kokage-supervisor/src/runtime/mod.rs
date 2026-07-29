//! Internal runtime implementation for the supervisor loop.
//!
//! The supervisor's hot path lives in [`supervision`], with shutdown, spawn,
//! restart-intensity tracking, and child-state bookkeeping split into
//! submodules. All types here are `pub(crate)`.

pub(crate) mod child_runtime;
pub(crate) mod exit;
pub(crate) mod intensity;
pub(crate) mod shutdown;
pub(crate) mod spawn;
pub(crate) mod supervision;

pub(crate) use supervision::SupervisorRuntime;
