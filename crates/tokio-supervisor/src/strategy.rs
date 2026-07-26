/// Restart strategy that determines how sibling children are affected when one
/// child exits unexpectedly.
///
/// Modelled after Erlang/OTP supervisor strategies.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Strategy {
    /// Only the exited child is restarted. Other children are unaffected.
    #[default]
    OneForOne,
    /// All children are stopped and restarted when any single child exits
    /// unexpectedly. Use this when children have hard interdependencies and
    /// cannot function correctly without their siblings.
    ///
    /// Draining the old group is an atomic critical section: control commands
    /// wait until every old-generation task has exited or reached its shutdown
    /// backstop. Readiness sequencing after the new tasks are scheduled does
    /// not block control dispatch.
    OneForAll,
    /// The exited child and every child declared after it are stopped and
    /// restarted. Children declared before it are unaffected. Use this for
    /// ordered pipelines where downstream state depends on upstream output.
    ///
    /// Declaration order defines the restart group: a failure in the last
    /// child restarts only that child. Put a component before every dependent
    /// sibling if its failure must restart them too, or use
    /// [`OneForAll`](Self::OneForAll) when the whole group must restart after
    /// any member fails.
    ///
    /// Draining the old suffix is an atomic critical section: control commands
    /// wait until every selected old-generation task has exited or reached its
    /// shutdown backstop. Readiness sequencing after respawn is loop-owned and
    /// does not block control dispatch.
    RestForOne,
}
