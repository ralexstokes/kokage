//! Supervisor scope kinds.

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
