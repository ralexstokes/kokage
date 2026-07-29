#![warn(missing_docs)]

//! Derive macros for `kokage`.
//!
//! Do not depend on this crate directly: `kokage` re-exports
//! `#[derive(ActorFactory)]` and `#[derive(Supervision)]` under its default
//! `derive` feature, and the generated code refers to `kokage` paths.

use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};

use syn::{Data, DeriveInput, Expr, Field, Fields, Type, parse_macro_input, spanned::Spanned};

/// Derives a reusable factory from an actor's named fields.
///
/// This derive is not imported by `kokage::prelude::*`. Use
/// `use kokage::ActorFactory;` for the unqualified
/// `#[derive(ActorFactory)]` form, or write
/// `#[derive(kokage::ActorFactory)]`.
///
/// For an actor named `Worker`, the derive generates `WorkerFactory`. Fields
/// without an attribute become factory fields and are cloned into every new
/// actor incarnation. Mark incarnation-local fields with `#[factory(default)]`
/// to omit them from the factory and initialize them with `Default::default()`
/// on every build:
///
/// ```
/// # use std::collections::VecDeque;
/// # use kokage::{Actor, MessageContext, ActorResult, GraphBuilder};
/// # struct Job;
/// # struct Client;
/// # impl Clone for Client { fn clone(&self) -> Self { Self } }
/// #[derive(kokage::ActorFactory)]
/// struct Worker {
///     client: Client,
///     #[factory(default)]
///     pending: VecDeque<Job>,
/// }
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         let _ = (&self.client, &self.pending);
/// #         Ok(())
/// #     }
/// # }
///
/// let mut graph = GraphBuilder::new();
/// let (worker_slot, _worker_ref) = graph.slot("worker");
/// graph.define(worker_slot, WorkerFactory { client: Client });
/// ```
///
/// The generated factory and its configuration fields inherit the actor's
/// visibility. Configuration fields must implement `Clone`; fields marked
/// `default` must implement `Default`. Generic, tuple, and unit structs are
/// rejected. Hand-write `ActorFactory` when an incarnation-local field needs
/// construction more specialized than `Default`.
#[proc_macro_derive(ActorFactory, attributes(factory))]
pub fn derive_actor_factory(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_actor_factory(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_actor_factory(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(attr) = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("factory"))
    {
        return Err(syn::Error::new_spanned(
            attr,
            "`factory` attributes belong on actor fields",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "ActorFactory cannot be derived for generic structs",
        ));
    }

    let actor = input.ident;
    let vis = input.vis;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(syn::Error::new_spanned(
                    fields,
                    "ActorFactory can only be derived for structs with named fields",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    &actor,
                    "ActorFactory can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &actor,
                "ActorFactory can only be derived for structs with named fields",
            ));
        }
    };
    let defaults = parse_factory_attributes(&fields)?;
    let factory = format_ident!("{actor}Factory");
    let factory_doc = format!(
        "Reusable factory generated from [`{actor}`].\n\nUnmarked fields are durable configuration cloned into every incarnation; fields marked `#[factory(default)]` are freshly default-constructed."
    );

    let factory_fields: Vec<_> = fields
        .iter()
        .zip(&defaults)
        .filter(|(_, default)| !**default)
        .map(|(field, _)| {
            let ident = field.ident.as_ref().expect("named fields");
            let ty = &field.ty;
            let doc =
                format!("Durable `{ident}` configuration cloned into every actor incarnation.");
            quote_spanned! {field.span()=>
                #[doc = #doc]
                #vis #ident: #ty
            }
        })
        .collect();
    let actor_fields: Vec<_> = fields
        .iter()
        .zip(&defaults)
        .map(|(field, default)| {
            let ident = field.ident.as_ref().expect("named fields");
            let ty = &field.ty;
            if *default {
                quote_spanned! {field.span()=>
                    #ident: <#ty as ::core::default::Default>::default()
                }
            } else {
                quote_spanned! {field.span()=>
                    #ident: <#ty as ::core::clone::Clone>::clone(&self.#ident)
                }
            }
        })
        .collect();

    Ok(quote! {
        #[doc = #factory_doc]
        #[derive(Clone)]
        #vis struct #factory {
            #(#factory_fields,)*
        }

        impl ::kokage::ActorFactory for #factory {
            type Actor = #actor;

            fn build(&self) -> Self::Actor {
                #actor {
                    #(#actor_fields,)*
                }
            }
        }
    })
}

fn parse_factory_attributes(
    fields: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
) -> syn::Result<Vec<bool>> {
    let mut defaults = Vec::with_capacity(fields.len());

    for field in fields {
        let mut default = false;
        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("factory"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("default") {
                    if default {
                        return Err(meta.error("duplicate `default` option"));
                    }
                    default = true;
                    return Ok(());
                }
                Err(meta.error("expected `default`"))
            })?;
        }
        defaults.push(default);
    }

    Ok(defaults)
}

/// Derives a static actor graph and the supervision scope running it.
///
/// This derive is not imported by `kokage::prelude::*`. Use
/// `use kokage::Supervision;` for the unqualified
/// `#[derive(Supervision)]` form, or write
/// `#[derive(kokage::Supervision)]`.
///
/// Each ordinary field declares one actor type in the graph and must implement
/// `kokage::host::RawActor`; any `Actor` qualifies through the blanket impl.
/// Nested and dynamic scope fields are described below. For a struct named
/// `Pipeline`, the derive generates:
///
/// * a `PipelineRefs` struct containing typed actor refs, including nested refs;
/// * a generic `PipelineFactories` struct with actor factories and any nested
///   factories or dynamic trees needed to wire the declaration; and
/// * `Pipeline::tree(wire)` and `Pipeline::tree_with(builder, wire)`
///   constructors, producing a non-cloneable `OrderedTree` declaration over
///   the graph, including any pre-wired dynamic-scope identities.
///
/// Both constructors return the tree paired with the `PipelineRefs` bundle,
/// for use as application entry points; write `let (tree, _) = ...` when the
/// refs are not needed. `tree_with` takes an otherwise empty `GraphBuilder`
/// whose graph name and mailbox capacity can be configured before it is
/// passed to the generated constructor.
///
/// The `wire` closure receives `&PipelineRefs` before any actor incarnation is
/// constructed, so factories can capture each other's refs even when the graph
/// is cyclic — no forward references or string lookups required. Each factory
/// is called once for the initial start and once per supervised restart:
///
/// ```
/// # use kokage::{MessageContext, ActorRef, ActorResult, Actor};
/// # struct FrontendMsg;
/// # struct ParserMsg;
/// # struct SinkMsg;
/// #
/// # struct Frontend {
/// #     parser: ActorRef<ParserMsg>,
/// # }
/// # impl Actor for Frontend {
/// #     type Msg = FrontendMsg;
/// #     async fn handle(
/// #         &mut self,
/// #         _: FrontendMsg,
/// #         _: &mut MessageContext<'_, Self>,
/// #     ) -> ActorResult {
/// #         Ok(())
/// #     }
/// # }
/// #
/// # struct Parser {
/// #     frontend: ActorRef<FrontendMsg>,
/// #     sink: ActorRef<SinkMsg>,
/// # }
/// # impl Actor for Parser {
/// #     type Msg = ParserMsg;
/// #     async fn handle(&mut self, _: ParserMsg, _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         Ok(())
/// #     }
/// # }
/// #
/// # struct Sink;
/// # impl Actor for Sink {
/// #     type Msg = SinkMsg;
/// #     async fn handle(&mut self, _: SinkMsg, _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         Ok(())
/// #     }
/// # }
/// #
/// #[derive(kokage::Supervision)]
/// struct Pipeline {
///     frontend: Frontend,
///     parser: Parser,
///     sink: Sink,
/// }
///
/// # fn main() -> Result<(), kokage::GraphBuildError> {
/// let (tree, refs) = Pipeline::tree(|refs| {
///     PipelineFactories {
///         frontend: {
///             let refs = refs.clone();
///             move || Frontend {
///                 parser: refs.parser.clone(),
///             }
///         },
///         parser: {
///             let refs = refs.clone();
///             move || Parser {
///                 frontend: refs.frontend.clone(),
///                 sink: refs.sink.clone(),
///             }
///         },
///         sink: || Sink,
///     }
/// })?;
/// # let _ = (tree, refs.frontend);
/// # Ok(())
/// # }
/// ```
///
/// # Cycles and bounded mailboxes
///
/// The derive makes cyclic wiring easy, but mailboxes stay bounded, so
/// cycles inherit the deadlock hazard: two actors that `send` to each other
/// while both mailboxes are full wait forever, and a `call` cycle deadlocks
/// at depth one. Use `try_send` on feedback edges, and `call` only
/// "downhill" along a DAG ordering of the declared actors.
///
/// # Actor labels
///
/// Field names become actor labels, qualified by the path of enclosing
/// scopes: a `parse` field inside a `workers` scope is labelled
/// `workers.parse`. Root-level fields are unqualified. Override the name of
/// any node — actor or scope — with `#[supervision(label = "...")]`; the
/// override replaces that one path component, so it must not contain `.`.
///
/// Labels are display names, not addresses: they appear in tracing fields and
/// actor stats, and renaming a field renames both, but never affects type
/// checking or message routing.
///
/// A supervisor child id is local to its scope, so a nested actor is named
/// `parse` within the `workers` supervisor while its graph label stays
/// `workers.parse`. Scope names and label components come from the same field
/// names, so the supervisor path spells the label rather than repeating the
/// scope: `root.workers.parse`, whose tail past `root.` is exactly the label.
/// Snapshot and lifecycle lookups therefore take the local id, while
/// `actor_stats` reports the qualified label.
///
/// # Visibility
///
/// The refs struct and the generated constructors inherit the derived
/// struct's visibility; each refs field inherits the corresponding field's
/// visibility. A `pub` struct with `pub` fields can therefore be wired from
/// another module or crate.
///
/// # Compile-time guarantees
///
/// The derive rejects shapes it cannot wire, and the generated code keeps the
/// rest in the type system:
///
/// * enums, unions, tuple structs, and unit structs are rejected — actor ids
///   come from field names;
/// * generic structs are rejected;
/// * a struct with zero fields is rejected, because a graph must contain at
///   least one actor;
/// * a field whose type is not an actor fails to compile;
/// * wiring a ref whose message type does not match fails to compile;
/// * omitting or repeating a factory field is rejected by ordinary struct
///   literal checking;
/// * returning the wrong actor type from a field factory fails to compile;
/// * filling the same slot twice is unrepresentable — the generated code owns
///   exactly one slot token per field;
/// * a `DynamicScope` field declares a dynamic scope without an attribute;
/// * the removed `#[supervision(dynamic)]` attribute is rejected;
/// * marking a `DynamicScope` field as a nested `scope` is rejected;
/// * a `label` that is empty or contains `.` is rejected; and
/// * two nodes sharing a name — whether from field names or `label`
///   overrides — are rejected.
///
/// # Errors
///
/// `tree` and `tree_with` return `GraphBuildError` for graph configuration and
/// generated wiring errors. Spawning the returned declaration separately with
/// `OrderedTree::spawn` returns `SupervisorBuildError` for invalid supervision
/// policy.
///
/// # Panics
///
/// The generated constructors panic if their private nested-scope plumbing
/// rejects refs created while opening the same derived graph. Correctly
/// generated implementations preserve that invariant.
///
/// For dynamic graphs — actors created in a loop, or ids chosen at runtime —
/// use `GraphBuilder` directly instead of this derive.
///
/// # Per-actor options
///
/// Add `#[supervision(options = expression)]` to a field to pass an
/// `ActorOptions` expression to `GraphBuilder::slot_with`. Fields
/// without this attribute continue to use the default options:
///
/// ```
/// # use kokage::{
/// #     host::ActorContext, ActorOptions, ActorResult, MailboxMode,
/// #     host::RawActor,
/// # };
/// # struct Snapshot(Vec<u8>);
/// # fn snapshot_size(message: &Snapshot) -> usize {
/// #     message.0.len()
/// # }
/// # struct SnapshotActor;
/// # impl RawActor for SnapshotActor {
/// #     type Msg = Snapshot;
/// #     async fn run(&mut self, _: ActorContext<Snapshot>) -> ActorResult {
/// #         Ok(())
/// #     }
/// # }
/// #[derive(kokage::Supervision)]
/// struct MarketData {
///     #[supervision(options = ActorOptions::new()
///         .mailbox(MailboxMode::conflate())
///         .message_size(snapshot_size))]
///     snapshots: SnapshotActor,
/// }
/// ```
///
/// # Supervision shape
///
/// Struct nesting is scope nesting. A `#[supervision(scope)]` field whose type
/// is another derived struct becomes a named child scope; the actors still join
/// one shared graph, so refs cross scope boundaries freely and cyclic wiring
/// keeps working. Only supervision placement is hierarchical.
///
/// ```
/// # use kokage::{
/// #     Actor, ActorResult, DynamicScope, GraphBuildError, MessageContext, RestartPolicy,
/// #     DynamicTree, Strategy,
/// # };
/// # struct Worker;
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, Self>) -> ActorResult {
/// #         Ok(())
/// #     }
/// # }
/// #[derive(kokage::Supervision)]
/// #[supervision(strategy = Strategy::OneForAll)]
/// struct Workers {
///     parse: Worker,
///     render: Worker,
/// }
///
/// #[derive(kokage::Supervision)]
/// #[supervision(strategy = Strategy::OneForOne)]
/// struct App {
///     #[supervision(restart = RestartPolicy::Never)]
///     ingest: Worker,
///     #[supervision(scope)]
///     workers: Workers,
///     sessions: DynamicScope,
/// }
///
/// # fn main() -> Result<(), GraphBuildError> {
/// let (tree, refs) = App::tree(|_refs| AppFactories {
///     ingest: || Worker,
///     workers: WorkersFactories {
///         parse: || Worker,
///         render: || Worker,
///     },
///     sessions: DynamicTree::new().default_restart(RestartPolicy::Never),
/// })?;
/// # let _ = (tree, refs.ingest, refs.workers.parse);
/// # Ok(())
/// # }
/// ```
///
/// Field order is semantic for supervision, unlike for a graph alone: an
/// ordered scope starts children in declaration order and `Strategy::RestForOne`
/// restarts the ones that follow. Reordering fields changes restart behaviour.
///
/// ## Scope attributes
///
/// On the struct, each taking `= <expression>`:
///
/// | Key | Effect |
/// |-----|--------|
/// | `strategy` | This scope's restart strategy. |
/// | `restart` | Default restart policy inherited by actor fields. |
/// | `shutdown` | Default shutdown policy inherited by actor fields. |
/// | `restart_intensity` | This scope's restart-intensity window. |
///
/// ## Field attributes
///
/// `label = "..."` renames a node. `options = <expression>` configures an
/// actor's mailbox. `restart`, `shutdown`, and `restart_intensity` override
/// the enclosing scope's defaults for one actor. A nested scope declares those
/// three on its own struct instead. `scope` selects a nested derived struct:
///
/// * `scope` — a nested derived struct, contributing a named child scope.
///
/// A field whose type is `DynamicScope` declares an empty scope whose
/// membership is written at runtime. The marker is never constructed. Its
/// wiring entry is a `DynamicTree` rather than an actor factory. Construct one
/// with `DynamicTree::new()`; it configures the scope and makes its mount handle
/// available before any actor is built, so a factory can capture it.
///
#[proc_macro_derive(Supervision, attributes(supervision))]
pub fn derive_supervision(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_supervision(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Scope-level `#[supervision(...)]` configuration.
#[derive(Default)]
struct ScopeAttrs {
    strategy: Option<Expr>,
    restart: Option<Expr>,
    shutdown: Option<Expr>,
    restart_intensity: Option<Expr>,
}

/// What a declared field is.
#[derive(Clone, Copy, Eq, PartialEq)]
enum FieldKind {
    /// An actor, the default.
    Actor,
    /// A nested derived struct, contributing a named child scope.
    Scope,
    /// An empty runtime-written scope, declared by a `DynamicScope` marker.
    Dynamic,
}

/// Field-level `#[supervision(...)]` configuration.
struct FieldAttrs {
    kind: FieldKind,
    label: Option<String>,
    options: Option<Expr>,
    restart: Option<Expr>,
    shutdown: Option<Expr>,
    restart_intensity: Option<Expr>,
}

impl Default for FieldAttrs {
    fn default() -> Self {
        Self {
            kind: FieldKind::Actor,
            label: None,
            options: None,
            restart: None,
            shutdown: None,
            restart_intensity: None,
        }
    }
}

fn take_expr(
    slot: &mut Option<Expr>,
    meta: &syn::meta::ParseNestedMeta,
    key: &str,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(meta.error(format!("duplicate `{key}` option")));
    }
    *slot = Some(meta.value()?.parse()?);
    Ok(())
}

fn parse_scope_attributes(attrs: &[syn::Attribute]) -> syn::Result<ScopeAttrs> {
    let mut parsed = ScopeAttrs::default();

    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("supervision"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("strategy") {
                return take_expr(&mut parsed.strategy, &meta, "strategy");
            }
            if meta.path.is_ident("restart") {
                return take_expr(&mut parsed.restart, &meta, "restart");
            }
            if meta.path.is_ident("shutdown") {
                return take_expr(&mut parsed.shutdown, &meta, "shutdown");
            }
            if meta.path.is_ident("restart_intensity") {
                return take_expr(&mut parsed.restart_intensity, &meta, "restart_intensity");
            }
            Err(meta.error(
                "expected `strategy`, `restart`, `shutdown`, or `restart_intensity`, \
                 each `= <expression>`",
            ))
        })?;
    }

    Ok(parsed)
}

fn parse_supervision_field(field: &Field) -> syn::Result<FieldAttrs> {
    let mut parsed = FieldAttrs::default();
    let dynamic_marker = is_dynamic_scope(&field.ty);
    if dynamic_marker {
        parsed.kind = FieldKind::Dynamic;
    }
    let mut kind_span = None;

    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("supervision"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("scope") {
                if dynamic_marker {
                    return Err(meta.error(
                        "a `DynamicScope` field declares a dynamic scope without an attribute; \
                         it cannot also be marked `scope`",
                    ));
                }
                if kind_span.is_some() {
                    return Err(meta.error("duplicate `scope` option"));
                }
                kind_span = Some(attr.span());
                parsed.kind = FieldKind::Scope;
                return Ok(());
            }
            if meta.path.is_ident("dynamic") {
                return Err(meta.error(
                    "`dynamic` is no longer supported; use a `DynamicScope` field without this \
                     attribute",
                ));
            }
            if meta.path.is_ident("label") {
                if parsed.label.is_some() {
                    return Err(meta.error("duplicate `label` option"));
                }
                let literal: syn::LitStr = meta.value()?.parse()?;
                let label = literal.value();
                if label.is_empty() {
                    return Err(syn::Error::new_spanned(&literal, "label must not be empty"));
                }
                if label.contains('.') {
                    return Err(syn::Error::new_spanned(
                        &literal,
                        "label must not contain `.`, which separates path components",
                    ));
                }
                parsed.label = Some(label);
                return Ok(());
            }
            if meta.path.is_ident("options") {
                return take_expr(&mut parsed.options, &meta, "options");
            }
            if meta.path.is_ident("restart") {
                return take_expr(&mut parsed.restart, &meta, "restart");
            }
            if meta.path.is_ident("shutdown") {
                return take_expr(&mut parsed.shutdown, &meta, "shutdown");
            }
            if meta.path.is_ident("restart_intensity") {
                return take_expr(&mut parsed.restart_intensity, &meta, "restart_intensity");
            }
            Err(meta.error(
                "expected `scope`, `label = \"...\"`, \
                 or `options`/`restart`/`shutdown`/`restart_intensity` = <expression>",
            ))
        })?;
    }

    if parsed.kind != FieldKind::Actor && parsed.options.is_some() {
        return Err(syn::Error::new_spanned(
            field,
            "`options` applies only to actor fields; a scope configures its own",
        ));
    }
    if parsed.kind == FieldKind::Scope
        && (parsed.restart.is_some()
            || parsed.shutdown.is_some()
            || parsed.restart_intensity.is_some())
    {
        return Err(syn::Error::new_spanned(
            field,
            "a nested scope declares its own `restart`, `shutdown`, and `restart_intensity` \
             on its own struct",
        ));
    }
    if parsed.kind == FieldKind::Dynamic
        && (parsed.restart.is_some()
            || parsed.shutdown.is_some()
            || parsed.restart_intensity.is_some())
    {
        return Err(syn::Error::new_spanned(
            field,
            "a dynamic scope takes its policy from the `DynamicTree` wired for this field, as in \
             `DynamicTree::new().default_restart(..)`",
        ));
    }

    Ok(parsed)
}

fn is_dynamic_scope(ty: &Type) -> bool {
    match ty {
        Type::Group(group) => is_dynamic_scope(&group.elem),
        Type::Paren(paren) => is_dynamic_scope(&paren.elem),
        Type::Path(path) if path.qself.is_none() => path.path.segments.last().is_some_and(|part| {
            part.ident == "DynamicScope" && matches!(part.arguments, syn::PathArguments::None)
        }),
        _ => false,
    }
}

fn expand_supervision(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let declared = input.ident;
    let vis = input.vis;

    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "Supervision cannot be derived for generic structs",
        ));
    }

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(syn::Error::new_spanned(
                    fields,
                    "Supervision can only be derived for structs with named fields",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    &declared,
                    "Supervision can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &declared,
                "Supervision can only be derived for structs with named fields",
            ));
        }
    };

    if fields.is_empty() {
        return Err(syn::Error::new_spanned(
            &declared,
            "Supervision requires at least one actor field",
        ));
    }

    let scope_attrs = parse_scope_attributes(&input.attrs)?;
    let field_attrs = fields
        .iter()
        .map(parse_supervision_field)
        .collect::<syn::Result<Vec<_>>>()?;

    let refs = format_ident!("{declared}Refs");
    let factories = format_ident!("{declared}Factories");
    let slots = format_ident!("{declared}Slots");
    let scopes = format_ident!("{declared}Scopes");

    let field_idents: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named fields"))
        .collect();
    let node_names: Vec<String> = field_idents
        .iter()
        .zip(&field_attrs)
        .map(|(ident, attrs)| attrs.label.clone().unwrap_or_else(|| ident.to_string()))
        .collect();

    // Names address nodes in both the graph label path and the supervisor
    // scope, so a collision would silently shadow a node rather than fail at
    // build time.
    let mut seen_names = std::collections::HashSet::with_capacity(node_names.len());
    for (index, name) in node_names.iter().enumerate() {
        if !seen_names.insert(name) {
            return Err(syn::Error::new_spanned(
                &fields[index],
                format!(
                    "duplicate node name `{name}`; node names must be unique within one struct"
                ),
            ));
        }
    }

    // Type parameters are minted only for fields that carry a factory, so a
    // `dynamic` marker field neither takes a parameter nor appears in the
    // wiring struct literal: it has nothing to construct.
    let mut factory_params: Vec<Option<syn::Ident>> = Vec::with_capacity(fields.len());
    let mut next_param = 0usize;
    for attrs in &field_attrs {
        if attrs.kind == FieldKind::Dynamic {
            factory_params.push(None);
        } else {
            factory_params.push(Some(format_ident!("F{next_param}")));
            next_param += 1;
        }
    }
    let all_params: Vec<&syn::Ident> = factory_params.iter().flatten().collect();

    let mut scope_fields = Vec::new();
    let mut scope_ctor = Vec::new();
    let mut slot_fields = Vec::new();
    let mut refs_fields = Vec::new();
    let mut factory_fields = Vec::new();
    let mut factory_bounds = Vec::new();
    let mut open_stmts = Vec::new();
    let mut define_stmts = Vec::new();
    let mut bound_idents = Vec::new();
    let mut marker_assertions = Vec::new();

    for (index, field) in fields.iter().enumerate() {
        let ident = field_idents[index];
        let field_vis = &field.vis;
        let ty = &field.ty;
        let attrs = &field_attrs[index];
        let name = &node_names[index];
        let slot_ident = format_ident!("{ident}_slot");

        match attrs.kind {
            FieldKind::Actor => {
                let param = factory_params[index]
                    .as_ref()
                    .expect("actor field parameter");
                // Uses of a field type behind the `RawActor` bound are spanned
                // at that field type, so a non-actor field reports E0277 there
                // rather than at the derive attribute. User-code spans opt the
                // refs fields into dead-code lints, hence the explicit allow:
                // an unread ref is normal for leaf actors and not actionable,
                // since the macro mints one per field.
                let ref_ty = quote_spanned! {ty.span()=>
                    ::kokage::ActorRef<<#ty as ::kokage::host::RawActor>::Msg>
                };
                let slot_ty = quote_spanned! {ty.span()=>
                    ::kokage::ActorSlot<<#ty as ::kokage::host::RawActor>::Msg>
                };
                slot_fields.push(quote! { #field_vis #ident: #slot_ty });
                refs_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #ref_ty
                });
                factory_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #param
                });
                factory_bounds.push(quote! { #param: ::kokage::ActorFactory<Actor = #ty> });
                bound_idents.push(ident);
                if let Some(options) = &attrs.options {
                    open_stmts.push(quote_spanned! {ty.span()=>
                        let (#slot_ident, #ident) = builder.slot_with::
                            <<#ty as ::kokage::host::RawActor>::Msg>(
                                &::kokage::__private::qualified_label(prefix, #name),
                                #options,
                            );
                    });
                } else {
                    open_stmts.push(quote_spanned! {ty.span()=>
                        let (#slot_ident, #ident) = builder.slot::
                            <<#ty as ::kokage::host::RawActor>::Msg>(
                                &::kokage::__private::qualified_label(prefix, #name),
                            );
                    });
                }
                define_stmts.push(quote! {
                    builder.define(slots.#ident, self.#ident);
                });
            }
            FieldKind::Scope => {
                let param = factory_params[index]
                    .as_ref()
                    .expect("scope field parameter");
                let ref_ty = quote_spanned! {ty.span()=>
                    <#ty as ::kokage::__private::Supervision>::Refs
                };
                let slot_ty = quote_spanned! {ty.span()=>
                    <#ty as ::kokage::__private::Supervision>::Slots
                };
                slot_fields.push(quote! { #field_vis #ident: #slot_ty });
                refs_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #ref_ty
                });
                factory_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #param
                });
                factory_bounds
                    .push(quote! { #param: ::kokage::__private::SupervisionFactories<#ty> });
                bound_idents.push(ident);
                open_stmts.push(quote_spanned! {ty.span()=>
                    let (#slot_ident, #ident) = <#ty as ::kokage::__private::Supervision>::open(
                        builder,
                        &::kokage::__private::qualified_label(prefix, #name),
                    );
                });
                scope_fields.push(quote! {
                    #field_vis #ident: <#ty as ::kokage::__private::Supervision>::Scopes
                });
                scope_ctor.push(ident);
                define_stmts.push(quote! {
                    let #ident = <#param as ::kokage::__private::SupervisionFactories<#ty>>::define(
                        self.#ident,
                        builder,
                        slots.#ident,
                    );
                });
            }
            FieldKind::Dynamic => {
                let assertion = format_ident!("_assert_{ident}_is_a_dynamic_scope");
                marker_assertions.push(quote_spanned! {ty.span()=>
                    #[allow(non_snake_case, dead_code)]
                    fn #assertion(marker: #ty) -> ::kokage::DynamicScope {
                        marker
                    }
                });
                factory_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: ::kokage::DynamicTree
                });
                scope_fields.push(quote! {
                    #field_vis #ident: ::kokage::DynamicTree
                });
                scope_ctor.push(ident);
                define_stmts.push(quote! {
                    let #ident = self.#ident;
                });
            }
        }
    }

    let slot_ctor_idents: Vec<_> = fields
        .iter()
        .enumerate()
        .filter(|(index, _)| field_attrs[*index].kind != FieldKind::Dynamic)
        .map(|(index, _)| field_idents[index])
        .collect();
    let slot_ctor_values: Vec<_> = slot_ctor_idents
        .iter()
        .map(|ident| format_ident!("{ident}_slot"))
        .collect();

    // Scope construction, in declaration order.
    let mut scope_stmts = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        let ident = field_idents[index];
        let ty = &field.ty;
        let attrs = &field_attrs[index];
        let name = &node_names[index];

        match attrs.kind {
            FieldKind::Actor => {
                let spec = actor_spec_expr(name, attrs);
                scope_stmts.push(quote! {
                    let actor = graph.actor_for(&refs.#ident)?;
                    let tree = tree.actor(#spec);
                });
            }
            FieldKind::Scope => {
                scope_stmts.push(quote! {
                    let tree = tree.subtree(#name, <#ty as ::kokage::__private::Supervision>::node(
                        graph,
                        &refs.#ident,
                        scopes.#ident,
                    )?);
                });
            }
            FieldKind::Dynamic => {
                // The identity-owning tree carries this scope's policy and the
                // identity behind any handle already handed out; the graph only
                // supplies execution defaults for actors added later.
                scope_stmts.push(quote! {
                    let tree = tree.subtree(
                        #name,
                        scopes.#ident
                            .derived_defaults(graph),
                    );
                });
            }
        }
    }

    let mut scope_root = quote! {
        ::kokage::OrderedTree::new().derived_defaults(graph)
    };
    if let Some(strategy) = &scope_attrs.strategy {
        scope_root = quote! { #scope_root.strategy(#strategy) };
    }
    if let Some(restart) = &scope_attrs.restart {
        scope_root = quote! { #scope_root.default_restart(#restart) };
    }
    if let Some(shutdown) = &scope_attrs.shutdown {
        scope_root = quote! { #scope_root.default_shutdown(#shutdown) };
    }
    if let Some(intensity) = &scope_attrs.restart_intensity {
        scope_root = quote! { #scope_root.restart_config(#intensity) };
    }
    let root_constructors = quote! {
        impl #declared {
            #[doc = "Builds this derived supervision declaration with the default graph configuration."]
            #[doc = ""]
            #[doc = "# Panics"]
            #[doc = ""]
            #[doc = "Panics if private nested-scope plumbing rejects refs created while opening the same derived graph."]
            #vis fn tree<#(#all_params),*>(
                wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
            ) -> ::core::result::Result<
                (::kokage::OrderedTree, #refs),
                ::kokage::GraphBuildError,
            >
            where
                #(#factory_bounds,)*
            {
                Self::tree_with(::kokage::GraphBuilder::new(), wire)
            }

            #[doc = "Builds this derived supervision declaration with the supplied graph builder."]
            #[doc = ""]
            #[doc = "The builder should have graph-wide settings configured but no actors registered; this constructor registers the actors declared by the derive."]
            #[doc = ""]
            #[doc = "# Panics"]
            #[doc = ""]
            #[doc = "Panics if private nested-scope plumbing rejects refs created while opening the same derived graph."]
            #vis fn tree_with<#(#all_params),*>(
                builder: ::kokage::GraphBuilder,
                wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
            ) -> ::core::result::Result<
                (::kokage::OrderedTree, #refs),
                ::kokage::GraphBuildError,
            >
            where
                #(#factory_bounds,)*
            {
                let (graph, refs, scopes) = Self::__supervision_graph(builder, wire)?;
                let tree = Self::__supervision_scope(&graph, &refs, scopes)
                    .expect("derived refs belong to the graph that opened them");
                ::core::result::Result::Ok((tree, refs))
            }
        }
    };

    Ok(quote! {
        #vis struct #refs {
            #(#refs_fields,)*
        }

        impl ::core::clone::Clone for #refs {
            fn clone(&self) -> Self {
                Self {
                    #(#bound_idents: self.#bound_idents.clone(),)*
                }
            }
        }

        #[doc(hidden)]
        #vis struct #slots {
            #(#slot_fields,)*
        }

        #[doc(hidden)]
        #vis struct #scopes {
            #(#scope_fields,)*
        }

        #vis struct #factories<#(#all_params),*> {
            #(#factory_fields,)*
        }

        impl<#(#all_params),*> ::kokage::__private::SupervisionFactories<#declared>
            for #factories<#(#all_params),*>
        where
            #(#factory_bounds,)*
        {
            fn define(
                self,
                builder: &mut ::kokage::GraphBuilder,
                slots: <#declared as ::kokage::__private::Supervision>::Slots,
            ) -> <#declared as ::kokage::__private::Supervision>::Scopes {
                #(#define_stmts)*
                #scopes { #(#scope_ctor,)* }
            }
        }

        impl ::kokage::__private::Supervision for #declared {
            type Refs = #refs;
            type Slots = #slots;
            type Scopes = #scopes;

            fn open(
                builder: &mut ::kokage::GraphBuilder,
                prefix: &str,
            ) -> (Self::Slots, Self::Refs) {
                // The derived struct is never constructed; its fields only name actor types.
                // Destructuring it here marks the user's fields as read so they do not trigger
                // `dead_code` warnings on every derive.
                let _mark_declared_fields_used = |value: Self| {
                    let Self { #(#field_idents),* } = value;
                    let _ = (#(#field_idents),*);
                };
                #(#marker_assertions)*
                #(#open_stmts)*

                (
                    #slots { #(#slot_ctor_idents: #slot_ctor_values,)* },
                    #refs { #(#bound_idents,)* },
                )
            }

            fn node(
                graph: &::kokage::Graph,
                refs: &Self::Refs,
                scopes: Self::Scopes,
            ) -> ::core::result::Result<
                ::kokage::OrderedTree,
                ::kokage::GraphLookupError,
            > {
                Self::__supervision_scope(graph, refs, scopes)
            }
        }

        impl #declared {
            #[doc(hidden)]
            fn __supervision_scope(
                graph: &::kokage::Graph,
                refs: &#refs,
                scopes: #scopes,
            ) -> ::core::result::Result<
                ::kokage::OrderedTree,
                ::kokage::GraphLookupError,
            > {
                let tree = #scope_root;
                #(#scope_stmts)*
                ::core::result::Result::Ok(tree)
            }

            #[doc(hidden)]
            fn __supervision_graph<#(#all_params),*>(
                mut builder: ::kokage::GraphBuilder,
                wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
            ) -> ::core::result::Result<
                (::kokage::Graph, #refs, #scopes),
                ::kokage::GraphBuildError,
            >
            where
                #(#factory_bounds,)*
            {
                let (slots, refs) =
                    <Self as ::kokage::__private::Supervision>::open(&mut builder, "");
                let factories = wire(&refs);
                let scopes =
                    ::kokage::__private::SupervisionFactories::<Self>::define(
                        factories,
                        &mut builder,
                        slots,
                    );
                let graph = builder.build()?;
                ::core::result::Result::Ok((graph, refs, scopes))
            }
        }

        #root_constructors
    })
}

/// Builds the `ActorSpec` expression placing one actor field in its scope.
///
/// The expression reads an `actor` binding holding the resolved
/// owned `RunnableActor`, resolved from the matching typed ref.
fn actor_spec_expr(name: &str, attrs: &FieldAttrs) -> proc_macro2::TokenStream {
    let mut spec = quote! {
        ::kokage::ActorSpec::new(actor).child_id(#name)
    };
    if let Some(restart) = &attrs.restart {
        spec = quote! { #spec.restart(#restart) };
    }
    if let Some(shutdown) = &attrs.shutdown {
        spec = quote! { #spec.shutdown(#shutdown) };
    }
    if let Some(intensity) = &attrs.restart_intensity {
        spec = quote! { #spec.restart_config(#intensity) };
    }
    spec
}
