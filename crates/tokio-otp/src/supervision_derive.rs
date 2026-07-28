//! The plumbing a derived supervision struct lowers onto.
//!
//! `#[derive(Supervision)]` generates implementations of the traits declared
//! here. They are public because a nested scope in one crate can be declared by
//! a supervision struct in another, but application code normally touches only
//! the generated `graph`, `tree`, and `runtime` constructors.

use tokio_supervisor::SupervisorBuildError;

use crate::{Graph, GraphBuildError, GraphBuilder, ReservedSupervisionTree};

/// A derived group of actors together with the supervision scope running them.
///
/// A supervision struct contributes its actors to one shared [`Graph`], so
/// typed refs cross scope boundaries freely, while the nesting of these structs
/// determines only supervision placement.
pub trait Supervision: Sized {
    /// Restart-stable typed refs for every actor this struct declares,
    /// including those declared by nested scopes.
    type Refs: Clone;

    /// Unfilled slot tokens, one per actor this struct declares.
    type Slots;

    /// Reserved dynamic scopes, one per `#[supervision(dynamic)]` field, plus
    /// one nested bundle per nested scope.
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

    /// Builds this struct's supervision node, named `id` in its parent scope.
    ///
    /// `graph` must be the graph [`open`](Self::open) populated, and `prefix`
    /// the same value passed to it, so that the node resolves the actors it was
    /// built with. A node handed some other graph cannot find them; it records
    /// the mismatch on the scope rather than failing here, so
    /// [`ReservedSupervisionTree::build`] and
    /// [`outline`](ReservedSupervisionTree::outline) report it as
    /// [`InvalidConfig`](tokio_supervisor::SupervisorBuildError::InvalidConfig).
    fn node(graph: &Graph, scopes: Self::Scopes, id: &str, prefix: &str)
    -> ReservedSupervisionTree;
}

/// A bundle of factories filling every slot a supervision struct declares.
///
/// The generated `<Name>Factories` struct implements this trait, with one field
/// per actor, per nested scope, and per dynamic scope. A nested scope's field
/// holds that scope's own factories bundle, so wiring nests the same way the
/// declaration does; a dynamic scope's field holds a
/// [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder).
pub trait SupervisionFactories<T: Supervision> {
    /// Fills every slot returned by [`Supervision::open`], yielding the dynamic
    /// scopes this bundle carried.
    fn define(self, builder: &mut GraphBuilder, slots: T::Slots) -> T::Scopes;
}

/// Marker field type declaring an empty dynamic scope in a derived struct.
///
/// A field of this type carries no actor and is never constructed; it declares
/// a [`SupervisionTree::dynamic`](crate::SupervisionTree::dynamic) scope whose
/// membership is written at runtime.
/// The field must be marked `#[supervision(dynamic)]`, and its wiring entry is a
/// [`DynamicRuntimeBuilder`](crate::DynamicRuntimeBuilder) rather than an actor
/// factory — which is what makes
/// the scope's mount handle available before any actor is constructed, so a
/// factory can capture it:
///
/// ```
/// # use tokio_otp::{
/// #     Actor, ActorResult, DynamicScope, MessageContext, RestartPolicy, Runtime,
/// #     RuntimeHandle, SupervisionBuildError, prelude::Continue,
/// # };
/// # struct Manager {
/// #     sessions: RuntimeHandle,
/// # }
/// # impl Actor for Manager {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         let _ = &self.sessions;
/// #         Ok(Continue)
/// #     }
/// # }
/// #[derive(tokio_otp::Supervision)]
/// struct App {
///     manager: Manager,
///     #[supervision(dynamic)]
///     sessions: DynamicScope,
/// }
///
/// # fn main() -> Result<(), SupervisionBuildError> {
/// let sessions = Runtime::dynamic().default_restart(RestartPolicy::Never);
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
/// Policy for the scope comes from the builder —
/// `Runtime::dynamic().default_restart(..)` and friends — rather than from
/// attributes on the field.
pub enum DynamicScope {}

/// Errors returned when building a runtime from a derived supervision struct.
///
/// This is the derive's own error union; the
/// [`SupervisorBuildError`] it wraps is the lower-level supervisor validation
/// error from `tokio-supervisor`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SupervisionBuildError {
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
