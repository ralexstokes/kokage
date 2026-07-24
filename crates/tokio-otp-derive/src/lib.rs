#![warn(missing_docs)]

//! Derive macros for `tokio-otp`.
//!
//! Do not depend on this crate directly: `tokio-otp` re-exports
//! `#[derive(Topology)]` under its default `derive` feature, and the generated
//! code refers to `tokio_otp` paths.

use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};

use syn::{Data, DeriveInput, Expr, Field, Fields, parse_macro_input, spanned::Spanned};

/// Derives a static actor topology from a named-field struct.
///
/// Each field declares one actor type in the graph. Every field type must
/// implement `tokio_otp::RawActor`; any `Actor` qualifies through the blanket
/// impl. For a struct named `Pipeline`, the derive generates:
///
/// * a `PipelineRefs` struct with one field per topology field, typed
///   `ActorRef<<FieldType as RawActor>::Msg>`;
/// * a generic `PipelineFactories` struct with one factory field per topology
///   field;
/// * `Pipeline::graph(wire)`, which builds the graph with a default
///   `GraphBuilder`;
/// * `Pipeline::graph_with(builder, wire)`, which accepts a preconfigured
///   `GraphBuilder` — graph name, mailbox capacity, shutdown timeouts, and
///   any extra actors registered by hand.
///
/// The `wire` closure receives `&PipelineRefs` before any actor incarnation is
/// constructed, so factories can capture each other's refs even when the graph
/// is cyclic — no forward references or string lookups required. Each factory
/// is called once for the initial start and once per supervised restart:
///
/// ```
/// # use tokio_otp::{ActorContext, ActorRef, ActorResult, Actor};
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
/// #         _: &ActorContext<FrontendMsg>,
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
/// #     async fn handle(&mut self, _: ParserMsg, _: &ActorContext<ParserMsg>) -> ActorResult {
/// #         Ok(tokio_otp::prelude::Continue)
/// #     }
/// # }
/// #
/// # struct Sink;
/// # impl Actor for Sink {
/// #     type Msg = SinkMsg;
/// #     async fn handle(&mut self, _: SinkMsg, _: &ActorContext<SinkMsg>) -> ActorResult {
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
/// let graph = Pipeline::graph(|refs| {
///     let parser = refs.parser.clone();
///     let frontend = refs.frontend.clone();
///     let sink = refs.sink.clone();
///     PipelineFactories {
///         frontend: move || Frontend {
///             parser: parser.clone(),
///         },
///         parser: move || Parser {
///             frontend: frontend.clone(),
///             sink: sink.clone(),
///         },
///         sink: || Sink,
///     }
/// })?;
/// # let _ = graph;
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
/// Field names become actor labels verbatim. Labels are display names, not
/// addresses: they appear in tracing fields, actor stats, and supervisor
/// child ids — renaming a field renames all of those, but never affects
/// type checking or message routing.
///
/// # Visibility
///
/// The refs struct and the generated `graph` / `graph_with` methods inherit
/// the topology struct's visibility; each refs field inherits the
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
///   exactly one slot token per field.
///
/// # Errors
///
/// `graph` and `graph_with` return `GraphBuildError` for the runtime
/// configuration checks that remain, such as passing `graph_with` a builder
/// that already has an actor registered under the same id as a topology
/// field.
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
#[proc_macro_derive(Topology, attributes(topology))]
pub fn derive_topology(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_topology(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
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

    let refs = format_ident!("{topology}Refs");
    let factories = format_ident!("{topology}Factories");

    let field_idents: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named fields"))
        .collect();
    let field_vis: Vec<_> = fields.iter().map(|field| &field.vis).collect();
    let field_types: Vec<_> = fields.iter().map(|field| &field.ty).collect();
    let field_names: Vec<_> = field_idents.iter().map(|ident| ident.to_string()).collect();
    let factory_params: Vec<_> = (0..field_idents.len())
        .map(|index| format_ident!("F{index}"))
        .collect();
    let actor_options = parse_field_attributes(&fields)?;
    let slot_idents: Vec<_> = field_idents
        .iter()
        .map(|ident| format_ident!("{ident}_slot"))
        .collect();
    // Uses of a field type behind the `RawActor` bound are spanned at that field
    // type, so a non-actor field reports E0277 there rather than at the
    // derive attribute. User-code spans opt the refs fields into dead-code
    // lints, hence the explicit allow: an unread ref is normal for leaf
    // actors and not actionable, since the macro mints one per field.
    let ref_tys: Vec<_> = field_types
        .iter()
        .map(|ty| {
            quote_spanned! {ty.span()=>
                ::tokio_otp::ActorRef<<#ty as ::tokio_otp::RawActor>::Msg>
            }
        })
        .collect();
    let slot_calls: Vec<_> = field_types
        .iter()
        .zip(&slot_idents)
        .zip(&field_idents)
        .zip(&field_names)
        .zip(&actor_options)
        .map(|((((ty, slot), ident), name), options)| {
            if let Some(options) = options {
                quote_spanned! {ty.span()=>
                    let (#slot, #ident) = builder.slot_with_options::
                        <<#ty as ::tokio_otp::RawActor>::Msg>(#name, #options);
                }
            } else {
                quote_spanned! {ty.span()=>
                    let (#slot, #ident) =
                        builder.slot::<<#ty as ::tokio_otp::RawActor>::Msg>(#name);
                }
            }
        })
        .collect();
    Ok(quote! {
        #vis struct #refs {
            #(
                #[allow(dead_code)]
                #field_vis #field_idents: #ref_tys,
            )*
        }

        #vis struct #factories<#(#factory_params),*> {
            #(
                #[allow(dead_code)]
                #field_vis #field_idents: #factory_params,
            )*
        }

        impl #topology {
            #vis fn graph<#(#factory_params),*>(
                wire: impl FnOnce(&#refs) -> #factories<#(#factory_params),*>,
            ) -> ::core::result::Result<::tokio_otp::Graph, ::tokio_otp::GraphBuildError>
            where
                #(
                    #factory_params: ::tokio_otp::ActorFactory<Actor = #field_types>,
                )*
            {
                Self::graph_with(::tokio_otp::GraphBuilder::new(), wire)
            }

            #vis fn graph_with<#(#factory_params),*>(
                mut builder: ::tokio_otp::GraphBuilder,
                wire: impl FnOnce(&#refs) -> #factories<#(#factory_params),*>,
            ) -> ::core::result::Result<::tokio_otp::Graph, ::tokio_otp::GraphBuildError>
            where
                #(
                    #factory_params: ::tokio_otp::ActorFactory<Actor = #field_types>,
                )*
            {
                // The topology struct is never constructed; its fields only name actor types.
                // Destructuring it here marks the user's fields as read so they do not trigger
                // `dead_code` warnings on every derive.
                let _mark_topology_fields_used = |value: Self| {
                    let Self { #(#field_idents),* } = value;
                    let _ = (#(#field_idents),*);
                };
                #(#slot_calls)*

                let refs = #refs {
                    #(#field_idents,)*
                };
                let factories = wire(&refs);

                #(
                    builder.define(#slot_idents, factories.#field_idents);
                )*

                builder.build()
            }
        }
    })
}

fn parse_field_attributes(
    fields: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
) -> syn::Result<Vec<Option<Expr>>> {
    let mut actor_options = Vec::with_capacity(fields.len());

    for field in fields {
        let mut options = None;
        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("topology"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("options") {
                    if options.is_some() {
                        return Err(meta.error("duplicate `options` option"));
                    }
                    let value = meta.value()?;
                    options = Some(value.parse()?);
                    return Ok(());
                }
                Err(meta.error("expected `options = <expression>`"))
            })?;
        }
        actor_options.push(options);
    }

    Ok(actor_options)
}
