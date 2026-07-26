#![warn(missing_docs)]

//! Derive macros for `tokio-otp`.
//!
//! Do not depend on this crate directly: `tokio-otp` re-exports
//! `#[derive(ActorFactory)]` and `#[derive(Topology)]` under its default
//! `derive` feature, and the generated code refers to `tokio_otp` paths.

use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};

use syn::{Data, DeriveInput, Expr, Field, Fields, parse_macro_input, spanned::Spanned};

/// Derives a reusable factory from an actor's named fields.
///
/// For an actor named `Worker`, the derive generates `WorkerFactory`. Fields
/// without an attribute become factory fields and are cloned into every new
/// actor incarnation. Mark incarnation-local fields with `#[factory(default)]`
/// to omit them from the factory and initialize them with `Default::default()`
/// on every build:
///
/// ```
/// # use std::collections::VecDeque;
/// # use tokio_otp::{Actor, MessageContext, ActorResult, GraphBuilder, prelude::Continue};
/// # struct Job;
/// # struct Client;
/// # impl Clone for Client { fn clone(&self) -> Self { Self } }
/// #[derive(tokio_otp::ActorFactory)]
/// struct Worker {
///     client: Client,
///     #[factory(default)]
///     pending: VecDeque<Job>,
/// }
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut MessageContext<'_, ()>) -> ActorResult {
/// #         let _ = (&self.client, &self.pending);
/// #         Ok(Continue)
/// #     }
/// # }
///
/// let mut graph = GraphBuilder::new();
/// graph.actor("worker", WorkerFactory { client: Client });
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

        impl ::tokio_otp::ActorFactory for #factory {
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

/// Derives a static actor topology from a named-field struct.
///
/// Each field declares one actor type in the graph. Every field type must
/// implement `tokio_otp::RawActor`; any `Actor` qualifies through the blanket
/// impl. For a struct named `Pipeline`, the derive generates:
///
/// * a `PipelineRefs` struct with one field per topology field, typed
///   `ActorRef<<FieldType as RawActor>::Msg>`;
/// * a generic `PipelineFactories` struct with one factory field per topology
///   field, implementing `tokio_otp::TopologyFactories<Pipeline>`;
/// * a `PipelineSlots` struct holding the unfilled graph slots;
/// * an implementation of the `tokio_otp::Topology` trait; and
/// * three families of constructors, each in a plain, `_with_refs`, and
///   `_with` (preconfigured `GraphBuilder`) form:
///   * `Pipeline::graph(wire)` — the actor graph alone;
///   * `Pipeline::tree(wire)` — a `SupervisionTree` declaration over that
///     graph; and
///   * `Pipeline::runtime(wire)` — a built `Runtime`, ready to `spawn`.
///
/// The `_with_refs` forms additionally return the `PipelineRefs` bundle for
/// use as application entry points.
///
/// The `wire` closure receives `&PipelineRefs` before any actor incarnation is
/// constructed, so factories can capture each other's refs even when the graph
/// is cyclic — no forward references or string lookups required. Each factory
/// is called once for the initial start and once per supervised restart:
///
/// ```
/// # use tokio_otp::{MessageContext, ActorRef, ActorResult, Actor};
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
/// #         _: &mut MessageContext<'_, FrontendMsg>,
/// #     ) -> ActorResult {
/// #         Ok(tokio_otp::prelude::Continue)
/// #     }
/// # }
/// #
/// # struct Parser {
/// #     frontend: ActorRef<FrontendMsg>,
/// #     sink: ActorRef<SinkMsg>,
/// # }
/// # impl Actor for Parser {
/// #     type Msg = ParserMsg;
/// #     async fn handle(&mut self, _: ParserMsg, _: &mut MessageContext<'_, ParserMsg>) -> ActorResult {
/// #         Ok(tokio_otp::prelude::Continue)
/// #     }
/// # }
/// #
/// # struct Sink;
/// # impl Actor for Sink {
/// #     type Msg = SinkMsg;
/// #     async fn handle(&mut self, _: SinkMsg, _: &mut MessageContext<'_, SinkMsg>) -> ActorResult {
/// #         Ok(tokio_otp::prelude::Continue)
/// #     }
/// # }
/// #
/// #[derive(tokio_otp::Topology)]
/// struct Pipeline {
///     frontend: Frontend,
///     parser: Parser,
///     sink: Sink,
/// }
///
/// # fn main() -> Result<(), tokio_otp::GraphBuildError> {
/// let (graph, refs) = Pipeline::graph_with_refs(|refs| {
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
/// # let _ = (graph, refs.frontend);
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
/// "downhill" along a DAG ordering of the topology.
///
/// # Actor labels
///
/// Field names become actor labels, qualified by the path of enclosing
/// scopes: a `parse` field inside a `workers` scope is labelled
/// `workers.parse`. Root-level fields are unqualified. Override the name of
/// any node — actor or scope — with `#[topology(label = "...")]`; the
/// override replaces that one path component, so it must not contain `.`.
///
/// Labels are display names, not addresses: they appear in tracing fields,
/// actor stats, and supervisor child ids — renaming a field renames all of
/// those, but never affects type checking or message routing. An actor keeps
/// exactly one name across all of them, so a nested actor's supervisor child
/// id is its qualified label, making the supervisor path
/// `workers/workers.parse`. The repetition is deliberate: one key correlates
/// a snapshot, a stats row, and a tracing span.
///
/// # Visibility
///
/// The refs struct and the generated `graph` / `graph_with_refs` /
/// `graph_with` methods inherit the topology struct's visibility; each refs field inherits the
/// corresponding topology field's visibility. A `pub` topology with `pub`
/// fields can therefore be wired from another module or crate.
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
/// * a `#[topology(dynamic)]` field whose type is not `DynamicScope` fails to
///   compile;
/// * `scope` and `dynamic` on one field, more than one `leader` field, a
///   `leader` that is not first, a `leader` with no scope to own, and
///   `leader_strategy` without a `leader` are all rejected; and
/// * a `label` that is empty or contains `.` is rejected.
///
/// # Errors
///
/// `graph`, `graph_with_refs`, and `graph_with` return `GraphBuildError` for
/// the runtime configuration checks that remain, such as passing `graph_with`
/// a builder that already has an actor registered under the same id as a
/// topology field.
///
/// For dynamic graphs — actors created in a loop, or ids chosen at runtime —
/// use `GraphBuilder` directly instead of this derive.
///
/// # Per-actor options
///
/// Add `#[topology(options = expression)]` to a field to pass an
/// `ActorOptions` expression to `GraphBuilder::slot_with_options`. Fields
/// without this attribute continue to use the default options:
///
/// ```
/// # use tokio_otp::{
/// #     ActorContext, ActorOptions, ActorResult, MailboxMode, MessageSize, RawActor,
/// # };
/// # struct Snapshot(Vec<u8>);
/// # impl MessageSize for Snapshot {
/// #     fn size_hint(&self) -> usize {
/// #         self.0.len()
/// #     }
/// # }
/// # struct SnapshotActor;
/// # impl RawActor for SnapshotActor {
/// #     type Msg = Snapshot;
/// #     async fn run(&mut self, _: ActorContext<Snapshot>) -> ActorResult {
/// #         Ok(tokio_otp::prelude::Continue)
/// #     }
/// # }
/// #[derive(tokio_otp::Topology)]
/// struct MarketData {
///     #[topology(options = ActorOptions::new()
///         .mailbox(MailboxMode::Conflate)
///         .message_size())]
///     snapshots: SnapshotActor,
/// }
/// ```
///
/// # Supervision shape
///
/// Struct nesting is scope nesting. A `#[topology(scope)]` field whose type is
/// another derived topology becomes a named child scope; the actors still join
/// one shared graph, so refs cross scope boundaries freely and cyclic wiring
/// keeps working. Only supervision placement is hierarchical.
///
/// ```
/// # use tokio_otp::{
/// #     Actor, ActorContext, ActorResult, DynamicScope, RestartPolicy, Strategy,
/// #     TopologyBuildError, prelude::Continue,
/// # };
/// # struct Worker;
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut ActorContext<()>) -> ActorResult {
/// #         Ok(Continue)
/// #     }
/// # }
/// #[derive(tokio_otp::Topology)]
/// #[topology(strategy = Strategy::OneForAll)]
/// struct Workers {
///     parse: Worker,
///     render: Worker,
/// }
///
/// #[derive(tokio_otp::Topology)]
/// #[topology(strategy = Strategy::OneForOne)]
/// struct App {
///     #[topology(restart = RestartPolicy::Never)]
///     ingest: Worker,
///     #[topology(scope)]
///     workers: Workers,
///     #[topology(dynamic)]
///     sessions: DynamicScope,
/// }
///
/// # fn main() -> Result<(), TopologyBuildError> {
/// let (runtime, refs) = App::runtime_with_refs(|_refs| AppFactories {
///     ingest: || Worker,
///     workers: WorkersFactories {
///         parse: || Worker,
///         render: || Worker,
///     },
/// })?;
/// # let _ = (runtime, refs.ingest, refs.workers.parse);
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
/// | `leader_strategy` | Relates a `leader` field to the scope it owns. |
///
/// ## Field attributes
///
/// `label = "..."` renames a node. `options = <expression>` configures an
/// actor's mailbox. `restart`, `shutdown`, and `restart_intensity` override
/// the enclosing scope's defaults for one actor. A nested scope declares those
/// three on its own struct instead. The remaining keys select what a field is:
///
/// * `scope` — a nested derived topology, contributing a named child scope.
/// * `dynamic` — an empty scope whose membership is written at runtime. The
///   field type must be `DynamicScope`, a marker that is never constructed;
///   `restart`, `shutdown`, and `restart_intensity` set the scope's defaults
///   for actors added later.
/// * `leader` — an actor started before, and owning, the scope formed by the
///   struct's remaining fields. It must be the first field, and lowers to
///   `SupervisionTree::leader`, relating the two by `leader_strategy`
///   (`Strategy::RestForOne` by default). A topology with a `leader` is a
///   fragment rather than an application root, so it generates no `graph`,
///   `tree`, or `runtime` constructors — use it as a `scope` field.
///
#[proc_macro_derive(Topology, attributes(topology))]
pub fn derive_topology(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_topology(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Scope-level `#[topology(...)]` configuration.
#[derive(Default)]
struct ScopeAttrs {
    strategy: Option<Expr>,
    leader_strategy: Option<Expr>,
    restart: Option<Expr>,
    shutdown: Option<Expr>,
    restart_intensity: Option<Expr>,
}

/// What a topology field declares.
#[derive(Clone, Copy, Eq, PartialEq)]
enum FieldKind {
    /// An actor, the default.
    Actor,
    /// A nested topology, contributing a named child scope.
    Scope,
    /// An empty runtime-written scope, declared by a `DynamicScope` marker.
    Dynamic,
}

/// Field-level `#[topology(...)]` configuration.
struct FieldAttrs {
    kind: FieldKind,
    leader: bool,
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
            leader: false,
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

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("topology")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("strategy") {
                return take_expr(&mut parsed.strategy, &meta, "strategy");
            }
            if meta.path.is_ident("leader_strategy") {
                return take_expr(&mut parsed.leader_strategy, &meta, "leader_strategy");
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
                "expected `strategy`, `leader_strategy`, `restart`, `shutdown`, \
                 or `restart_intensity`, each `= <expression>`",
            ))
        })?;
    }

    Ok(parsed)
}

fn parse_topology_field(field: &Field) -> syn::Result<FieldAttrs> {
    let mut parsed = FieldAttrs::default();
    let mut kind_span = None;

    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("topology"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("scope") || meta.path.is_ident("dynamic") {
                let kind = if meta.path.is_ident("scope") {
                    FieldKind::Scope
                } else {
                    FieldKind::Dynamic
                };
                if kind_span.is_some() {
                    return Err(meta.error("`scope` and `dynamic` are mutually exclusive"));
                }
                kind_span = Some(attr.span());
                parsed.kind = kind;
                return Ok(());
            }
            if meta.path.is_ident("leader") {
                if parsed.leader {
                    return Err(meta.error("duplicate `leader` option"));
                }
                parsed.leader = true;
                return Ok(());
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
                "expected `scope`, `dynamic`, `leader`, `label = \"...\"`, \
                 or `options`/`restart`/`shutdown`/`restart_intensity` = <expression>",
            ))
        })?;
    }

    if parsed.kind != FieldKind::Actor {
        if parsed.leader {
            return Err(syn::Error::new_spanned(
                field,
                "`leader` applies only to actor fields",
            ));
        }
        if parsed.options.is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "`options` applies only to actor fields; a nested scope configures its own",
            ));
        }
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

    Ok(parsed)
}

fn expand_topology(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let topology = input.ident;
    let vis = input.vis;

    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "Topology cannot be derived for generic structs",
        ));
    }

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(syn::Error::new_spanned(
                    fields,
                    "Topology can only be derived for structs with named fields",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    &topology,
                    "Topology can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &topology,
                "Topology can only be derived for structs with named fields",
            ));
        }
    };

    if fields.is_empty() {
        return Err(syn::Error::new_spanned(
            &topology,
            "Topology requires at least one actor field",
        ));
    }

    let scope_attrs = parse_scope_attributes(&input.attrs)?;
    let field_attrs = fields
        .iter()
        .map(parse_topology_field)
        .collect::<syn::Result<Vec<_>>>()?;

    let leaders: Vec<usize> = field_attrs
        .iter()
        .enumerate()
        .filter(|(_, attrs)| attrs.leader)
        .map(|(index, _)| index)
        .collect();
    if let Some(&extra) = leaders.get(1) {
        return Err(syn::Error::new_spanned(
            &fields[extra],
            "a topology can declare at most one `leader` field",
        ));
    }
    let leader_index = leaders.first().copied();
    if let Some(index) = leader_index {
        if index != 0 {
            return Err(syn::Error::new_spanned(
                &fields[index],
                "the `leader` field must come first: it is started before the scope it owns",
            ));
        }
        if fields.len() < 2 {
            return Err(syn::Error::new_spanned(
                &topology,
                "a `leader` field requires at least one more field to form the scope it owns",
            ));
        }
    } else if let Some(strategy) = &scope_attrs.leader_strategy {
        return Err(syn::Error::new_spanned(
            strategy,
            "`leader_strategy` requires a `#[topology(leader)]` field",
        ));
    }

    let refs = format_ident!("{topology}Refs");
    let factories = format_ident!("{topology}Factories");
    let slots = format_ident!("{topology}Slots");

    let field_idents: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named fields"))
        .collect();
    let node_names: Vec<String> = field_idents
        .iter()
        .zip(&field_attrs)
        .map(|(ident, attrs)| attrs.label.clone().unwrap_or_else(|| ident.to_string()))
        .collect();

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
                    ::tokio_otp::ActorRef<<#ty as ::tokio_otp::RawActor>::Msg>
                };
                let slot_ty = quote_spanned! {ty.span()=>
                    ::tokio_otp::ActorSlot<<#ty as ::tokio_otp::RawActor>::Msg>
                };
                let options = attrs.options.clone().map_or_else(
                    || quote! { ::tokio_otp::ActorOptions::new() },
                    |options| quote! { #options },
                );
                slot_fields.push(quote! { #field_vis #ident: #slot_ty });
                refs_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #ref_ty
                });
                factory_fields.push(quote! {
                    #[allow(dead_code)]
                    #field_vis #ident: #param
                });
                factory_bounds.push(quote! { #param: ::tokio_otp::ActorFactory<Actor = #ty> });
                bound_idents.push(ident);
                open_stmts.push(quote_spanned! {ty.span()=>
                    let (#slot_ident, #ident) = builder.slot_with_options::
                        <<#ty as ::tokio_otp::RawActor>::Msg>(
                            &::tokio_otp::qualified_label(prefix, #name),
                            #options,
                        );
                });
                define_stmts.push(quote! {
                    builder.define(slots.#ident, self.#ident);
                });
            }
            FieldKind::Scope => {
                let param = factory_params[index]
                    .as_ref()
                    .expect("scope field parameter");
                let ref_ty = quote_spanned! {ty.span()=>
                    <#ty as ::tokio_otp::Topology>::Refs
                };
                let slot_ty = quote_spanned! {ty.span()=>
                    <#ty as ::tokio_otp::Topology>::Slots
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
                factory_bounds.push(quote! { #param: ::tokio_otp::TopologyFactories<#ty> });
                bound_idents.push(ident);
                open_stmts.push(quote_spanned! {ty.span()=>
                    let (#slot_ident, #ident) = <#ty as ::tokio_otp::Topology>::open(
                        builder,
                        &::tokio_otp::qualified_label(prefix, #name),
                    );
                });
                define_stmts.push(quote! {
                    <#param as ::tokio_otp::TopologyFactories<#ty>>::define(
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
                    fn #assertion(marker: #ty) -> ::tokio_otp::DynamicScope {
                        marker
                    }
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

    // Scope construction, in declaration order and skipping the leader, which
    // its parent installs ahead of the scope this builds.
    let mut scope_stmts = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        if Some(index) == leader_index {
            continue;
        }
        let ident = field_idents[index];
        let ty = &field.ty;
        let attrs = &field_attrs[index];
        let name = &node_names[index];

        match attrs.kind {
            FieldKind::Actor => {
                let spec = actor_spec_expr(name, attrs);
                scope_stmts.push(quote! { let tree = tree.actor(#spec); });
            }
            FieldKind::Scope => {
                scope_stmts.push(quote! {
                    let tree = tree.child(<#ty as ::tokio_otp::Topology>::node(
                        graph,
                        #name,
                        &::tokio_otp::qualified_label(prefix, #name),
                    ));
                });
            }
            FieldKind::Dynamic => {
                let mut node = quote! {
                    ::tokio_otp::SupervisionTree::dynamic().dynamic_defaults(graph)
                };
                if let Some(restart) = &attrs.restart {
                    node = quote! { #node.default_restart(#restart) };
                }
                if let Some(shutdown) = &attrs.shutdown {
                    node = quote! { #node.default_shutdown(#shutdown) };
                }
                if let Some(intensity) = &attrs.restart_intensity {
                    node = quote! { #node.restart_intensity(#intensity) };
                }
                let _ = ident;
                scope_stmts.push(quote! { let tree = tree.child(#node.id(#name)); });
            }
        }
    }

    let mut scope_root = quote! {
        ::tokio_otp::SupervisionTree::new().dynamic_defaults(graph)
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
        scope_root = quote! { #scope_root.restart_intensity(#intensity) };
    }

    let node_body = match leader_index {
        Some(index) => {
            let spec = actor_spec_expr(&node_names[index], &field_attrs[index]);
            let strategy = scope_attrs.leader_strategy.clone().map_or_else(
                || quote! { ::tokio_otp::Strategy::RestForOne },
                |strategy| quote! { #strategy },
            );
            quote! {
                ::tokio_otp::SupervisionTree::leader(
                    id,
                    #spec,
                    Self::__topology_scope(graph, prefix),
                    #strategy,
                )
            }
        }
        None => quote! { Self::__topology_scope(graph, prefix).id(id) },
    };

    // A leader topology is a fragment: its node is an actor-with-scope child,
    // which has no meaning without a parent to own it. Root constructors are
    // therefore generated only for ordinary scopes.
    let root_constructors = if leader_index.is_some() {
        quote! {}
    } else {
        quote! {
            impl #topology {
                #vis fn graph<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<::tokio_otp::Graph, ::tokio_otp::GraphBuildError>
                where
                    #(#factory_bounds,)*
                {
                    Self::graph_with_refs(wire).map(|(graph, _refs)| graph)
                }

                #vis fn graph_with_refs<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    (::tokio_otp::Graph, #refs),
                    ::tokio_otp::GraphBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    Self::__topology_graph(::tokio_otp::GraphBuilder::new(), wire)
                }

                #vis fn graph_with<#(#all_params),*>(
                    builder: ::tokio_otp::GraphBuilder,
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<::tokio_otp::Graph, ::tokio_otp::GraphBuildError>
                where
                    #(#factory_bounds,)*
                {
                    Self::__topology_graph(builder, wire).map(|(graph, _refs)| graph)
                }

                #vis fn tree<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    ::tokio_otp::SupervisionTree,
                    ::tokio_otp::GraphBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    Self::tree_with_refs(wire).map(|(tree, _refs)| tree)
                }

                #vis fn tree_with_refs<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    (::tokio_otp::SupervisionTree, #refs),
                    ::tokio_otp::GraphBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    Self::tree_with(::tokio_otp::GraphBuilder::new(), wire)
                }

                #vis fn tree_with<#(#all_params),*>(
                    builder: ::tokio_otp::GraphBuilder,
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    (::tokio_otp::SupervisionTree, #refs),
                    ::tokio_otp::GraphBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    let (graph, refs) = Self::__topology_graph(builder, wire)?;
                    let tree = Self::__topology_scope(&graph, "");
                    ::core::result::Result::Ok((tree, refs))
                }

                #vis fn runtime<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    ::tokio_otp::Runtime,
                    ::tokio_otp::TopologyBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    Self::runtime_with_refs(wire).map(|(runtime, _refs)| runtime)
                }

                #vis fn runtime_with_refs<#(#all_params),*>(
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    (::tokio_otp::Runtime, #refs),
                    ::tokio_otp::TopologyBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    Self::runtime_with(::tokio_otp::GraphBuilder::new(), wire)
                }

                #vis fn runtime_with<#(#all_params),*>(
                    builder: ::tokio_otp::GraphBuilder,
                    wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
                ) -> ::core::result::Result<
                    (::tokio_otp::Runtime, #refs),
                    ::tokio_otp::TopologyBuildError,
                >
                where
                    #(#factory_bounds,)*
                {
                    let (tree, refs) = Self::tree_with(builder, wire)?;
                    ::core::result::Result::Ok((tree.build()?, refs))
                }
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

        #vis struct #slots {
            #(#slot_fields,)*
        }

        #vis struct #factories<#(#all_params),*> {
            #(#factory_fields,)*
        }

        impl<#(#all_params),*> ::tokio_otp::TopologyFactories<#topology>
            for #factories<#(#all_params),*>
        where
            #(#factory_bounds,)*
        {
            fn define(
                self,
                builder: &mut ::tokio_otp::GraphBuilder,
                slots: <#topology as ::tokio_otp::Topology>::Slots,
            ) {
                #(#define_stmts)*
            }
        }

        impl ::tokio_otp::Topology for #topology {
            type Refs = #refs;
            type Slots = #slots;

            fn open(
                builder: &mut ::tokio_otp::GraphBuilder,
                prefix: &str,
            ) -> (Self::Slots, Self::Refs) {
                // The topology struct is never constructed; its fields only name actor types.
                // Destructuring it here marks the user's fields as read so they do not trigger
                // `dead_code` warnings on every derive.
                let _mark_topology_fields_used = |value: Self| {
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
                graph: &::tokio_otp::Graph,
                id: &str,
                prefix: &str,
            ) -> ::tokio_otp::SupervisionTree {
                #node_body
            }
        }

        impl #topology {
            #[doc(hidden)]
            fn __topology_scope(
                graph: &::tokio_otp::Graph,
                prefix: &str,
            ) -> ::tokio_otp::SupervisionTree {
                let tree = #scope_root;
                #(#scope_stmts)*
                tree
            }

            #[doc(hidden)]
            fn __topology_graph<#(#all_params),*>(
                mut builder: ::tokio_otp::GraphBuilder,
                wire: impl FnOnce(&#refs) -> #factories<#(#all_params),*>,
            ) -> ::core::result::Result<(::tokio_otp::Graph, #refs), ::tokio_otp::GraphBuildError>
            where
                #(#factory_bounds,)*
            {
                let (slots, refs) =
                    <Self as ::tokio_otp::Topology>::open(&mut builder, "");
                let factories = wire(&refs);
                ::tokio_otp::TopologyFactories::<Self>::define(factories, &mut builder, slots);
                let graph = builder.build()?;
                ::core::result::Result::Ok((graph, refs))
            }
        }

        #root_constructors
    })
}

/// Builds the `ActorSpec` expression placing one actor field in its scope.
fn actor_spec_expr(name: &str, attrs: &FieldAttrs) -> proc_macro2::TokenStream {
    let mut spec = quote! {
        ::tokio_otp::ActorSpec::new(
            graph
                .actor(&::tokio_otp::qualified_label(prefix, #name))
                .expect("a derived topology places actors it declared")
                .clone(),
        )
    };
    if let Some(restart) = &attrs.restart {
        spec = quote! { #spec.restart(#restart) };
    }
    if let Some(shutdown) = &attrs.shutdown {
        spec = quote! { #spec.shutdown(#shutdown) };
    }
    if let Some(intensity) = &attrs.restart_intensity {
        spec = quote! { #spec.restart_intensity(#intensity) };
    }
    spec
}
