//! Supervisor scope identity and kinds.

/// One nested-scope edge in an actor-stats or lifecycle-event observation path.
///
/// The tuple `(id, lineage, generation)` identifies the exact scope
/// incarnation containing the observed actor or forwarding the event. In
/// particular, `lineage` distinguishes a removed subtree from a later subtree
/// inserted under the same id.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct ScopePathSegment {
    /// Child id of the nested scope.
    pub id: String,
    /// Identity of that child membership in its parent scope.
    pub lineage: u64,
    /// Generation of the nested scope child.
    pub generation: u64,
}

impl ScopePathSegment {
    /// Creates one exact nested-scope path segment.
    #[cfg(test)]
    pub(crate) fn new(id: impl Into<String>, lineage: u64, generation: u64) -> Self {
        Self {
            id: id.into(),
            lineage,
            generation,
        }
    }
}

/// The immutable membership and ordering model of a supervisor scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ScopeKind {
    /// A declared sequence with readiness-gated startup and reverse-order
    /// teardown. Runtime membership operations are unsupported.
    #[default]
    Ordered,
    /// A runtime-written membership set with concurrent startup and teardown.
    Dynamic,
}

impl std::fmt::Display for ScopeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Ordered => "ordered",
            Self::Dynamic => "dynamic",
        };
        f.write_str(kind)
    }
}
