//! The plumbing a derived supervision struct lowers onto.
//!
//! `#[derive(Supervision)]` generates implementations of the traits declared
//! here. They are public because a nested scope in one crate can be declared by
//! a supervision struct in another, but application code normally touches only
//! the generated `tree` and `tree_with` constructors.

use crate::{Graph, GraphBuilder, GraphLookupError, OrderedTree};

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

    /// Identity-owning dynamic trees, one per `#[supervision(dynamic)]` field,
    /// plus one nested bundle per nested scope.
    ///
    /// A dynamic scope is supplied as a [`DynamicTree`](crate::DynamicTree)
    /// through the factories bundle, so its mount handle can be taken with
    /// [`handle`](crate::DynamicTree::handle) before wiring — early enough
    /// for an actor factory to capture it.
    type Scopes;

    /// Opens a graph slot for every declared actor beneath `prefix`.
    ///
    /// `prefix` is the qualified label of the enclosing scope, or empty at the
    /// root. Implementations must open slots for nested scopes under the
    /// prefix extended by the nested scope's own name.
    fn open(builder: &mut GraphBuilder, prefix: &str) -> (Self::Slots, Self::Refs);

    /// Builds this struct's identity-owning supervision node for attachment to its parent scope.
    ///
    /// `graph` must be the graph [`open`](Self::open) populated, so that the
    /// node resolves the actors it was built with. A node handed some other
    /// graph cannot find them and returns
    /// [`GraphLookupError::ForeignActorRef`].
    fn node(
        graph: &Graph,
        refs: &Self::Refs,
        scopes: Self::Scopes,
    ) -> Result<OrderedTree, GraphLookupError>;
}

/// A bundle of factories filling every slot a supervision struct declares.
///
/// The generated `<Name>Factories` struct implements this trait, with one field
/// per actor, per nested scope, and per dynamic scope. A nested scope's field
/// holds that scope's own factories bundle, so wiring nests the same way the
/// declaration does; a dynamic scope's field holds a
/// [`DynamicTree`](crate::DynamicTree).
pub trait SupervisionFactories<T: Supervision> {
    /// Fills every slot returned by [`Supervision::open`], yielding the dynamic
    /// scopes this bundle carried.
    fn define(self, builder: &mut GraphBuilder, slots: T::Slots) -> T::Scopes;
}

/// Marker field type declaring an empty dynamic scope in a derived struct.
///
/// A field of this type carries no actor and is never constructed; it declares
/// a [`DynamicTree`](crate::DynamicTree) scope whose
/// membership is written at runtime.
/// The field must be marked `#[supervision(dynamic)]`, and its wiring entry is a
/// [`DynamicTree`](crate::DynamicTree) rather than an actor factory — which is
/// what makes
/// the scope's mount handle available before any actor is constructed, so a
/// factory can capture it:
///
/// ```
/// # use tokio_otp::{
/// #     Actor, ActorResult, DynamicScope, GraphBuildError, MessageContext, RestartPolicy,
/// #     DynamicTree, RuntimeHandle,
/// # };
/// # struct Manager {
/// #     sessions: RuntimeHandle,
/// # }
/// # impl Actor for Manager {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         let _ = &self.sessions;
/// #         Ok(())
/// #     }
/// # }
/// #[derive(tokio_otp::Supervision)]
/// struct App {
///     manager: Manager,
///     #[supervision(dynamic)]
///     sessions: DynamicScope,
/// }
///
/// # fn main() -> Result<(), GraphBuildError> {
/// let sessions = DynamicTree::new().default_restart(RestartPolicy::Never);
/// // Its identity exists before wiring, so the manager can hold it across restarts.
/// let mount = sessions.handle();
///
/// let (tree, _refs) = App::tree(|_refs| AppFactories {
///     manager: move || Manager {
///         sessions: mount.clone(),
///     },
///     sessions,
/// })?;
/// # drop(tree);
/// # Ok(())
/// # }
/// ```
///
/// Policy for the scope comes from the tree —
/// `DynamicTree::new().default_restart(..)` and friends —
/// rather than from attributes on the field.
pub enum DynamicScope {}

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
