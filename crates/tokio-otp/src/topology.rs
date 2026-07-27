//! The plumbing a derived topology lowers onto.
//!
//! `#[derive(Topology)]` generates implementations of the traits declared here.
//! They are public because a nested scope in one crate can be declared by a
//! topology in another, but application code normally touches only the
//! generated `graph`, `tree`, and `runtime` constructors.

use tokio_supervisor::SupervisorBuildError;

use crate::{Graph, GraphBuildError, GraphBuilder, SupervisionTree};

/// A derived group of actors together with the supervision scope running them.
///
/// A topology contributes its actors to one shared [`Graph`], so typed refs
/// cross scope boundaries freely, while the nesting of topologies determines
/// only supervision placement.
pub trait Topology: Sized {
    /// Restart-stable typed refs for every actor this topology declares,
    /// including those declared by nested scopes.
    type Refs: Clone;

    /// Unfilled slot tokens, one per actor this topology declares.
    type Slots;

    /// Reserved dynamic scopes, one per `#[topology(dynamic)]` field, plus one
    /// nested bundle per nested scope.
    ///
    /// A dynamic scope is supplied as a [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder) through the
    /// factories bundle, so its mount handle can be taken with
    /// [`handle`](crate::DynamicRuntimeBuilder::handle) before wiring — early enough
    /// for an actor factory to capture it.
    type Scopes;

    /// Opens a graph slot for every declared actor beneath `prefix`.
    ///
    /// `prefix` is the qualified label of the enclosing scope, or empty at the
    /// root. Implementations must open slots for nested scopes under the
    /// prefix extended by the nested scope's own name.
    fn open(builder: &mut GraphBuilder, prefix: &str) -> (Self::Slots, Self::Refs);

    /// Builds this topology's supervision node, named `id` in its parent scope.
    ///
    /// `prefix` must be the same value passed to [`open`](Self::open), so that
    /// the node resolves the actors it was built with.
    fn node(graph: &Graph, scopes: Self::Scopes, id: &str, prefix: &str) -> SupervisionTree;
}

/// A bundle of factories filling every slot a topology declares.
///
/// The generated `<Topology>Factories` struct implements this trait, with one
/// field per actor, per nested scope, and per dynamic scope. A nested scope's
/// field holds that scope's own factories bundle, so wiring nests the same way
/// the topology does; a dynamic scope's field holds a
/// [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder).
pub trait TopologyFactories<T: Topology> {
    /// Fills every slot returned by [`Topology::open`], yielding the dynamic
    /// scopes this bundle carried.
    fn define(self, builder: &mut GraphBuilder, slots: T::Slots) -> T::Scopes;
}

/// Marker field type declaring an empty dynamic scope in a derived topology.
///
/// A field of this type carries no actor and is never constructed; it declares
/// a [`SupervisionTree::dynamic`] scope whose membership is written at runtime.
/// The field must be marked `#[topology(dynamic)]`, and its wiring entry is a
/// [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder) rather than an actor
/// factory — which is what makes
/// the scope's mount handle available before any actor is constructed, so a
/// factory can capture it:
///
/// ```
/// # use tokio_otp::{
/// #     Actor, ActorContext, ActorResult, DynamicScope, RestartPolicy, Runtime,
/// #     RuntimeHandle, TopologyBuildError, prelude::Continue,
/// # };
/// # struct Manager {
/// #     sessions: RuntimeHandle,
/// # }
/// # impl Actor for Manager {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
/// #         let _ = &self.sessions;
/// #         Ok(Continue)
/// #     }
/// # }
/// #[derive(tokio_otp::Topology)]
/// struct App {
///     manager: Manager,
///     #[topology(dynamic)]
///     sessions: DynamicScope,
/// }
///
/// # fn main() -> Result<(), TopologyBuildError> {
/// let sessions = Runtime::dynamic().restart(RestartPolicy::Never);
/// // Reserved before wiring, so the manager can hold it across restarts.
/// let mount = sessions.handle();
///
/// let (runtime, _refs) = App::runtime(|_refs| AppFactories {
///     manager: move || Manager {
///         sessions: mount.clone(),
///     },
///     sessions,
/// })?;
/// # drop(runtime);
/// # Ok(())
/// # }
/// ```
///
/// Policy for the scope comes from the builder — `Runtime::dynamic().restart(..)`
/// and friends — rather than from attributes on the field.
pub enum DynamicScope {}

/// Errors returned when building a runtime from a derived topology.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TopologyBuildError {
    /// The actor graph failed validation.
    #[error(transparent)]
    Graph(#[from] GraphBuildError),
    /// The supervision tree failed validation.
    #[error(transparent)]
    Supervision(#[from] SupervisorBuildError),
}

/// Joins a scope prefix and a node name into a qualified label.
///
/// Not a stable surface: generated code calls this to build actor labels and
/// nested scope prefixes.
#[doc(hidden)]
pub fn qualified_label(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}.{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::qualified_label;

    #[test]
    fn labels_are_unqualified_at_the_root_and_dotted_beneath_a_scope() {
        assert_eq!(qualified_label("", "parse"), "parse");
        assert_eq!(qualified_label("workers", "parse"), "workers.parse");
        assert_eq!(
            qualified_label("workers.inner", "parse"),
            "workers.inner.parse"
        );
    }
}
