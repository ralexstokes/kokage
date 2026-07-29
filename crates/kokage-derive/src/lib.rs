#![warn(missing_docs)]

//! Derive macros for `kokage`.
//!
//! Do not depend on this crate directly: `kokage` re-exports
//! `#[derive(ActorFactory)]` and `#[derive(Supervision)]` under its default
//! `derive` feature, and the generated code refers to `kokage` paths.

use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{Data, DeriveInput, Field, Fields, parse_macro_input, spanned::Spanned};

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

/// Derives cyclic actor-graph wiring and a bundle of typed actor refs.
///
/// This derive is not imported by `kokage::prelude::*`. Use
/// `use kokage::Supervision;` for the unqualified form, or write
/// `#[derive(kokage::Supervision)]`.
///
/// Each field is a concrete [`kokage::ActorFactory`] type. For a struct named
/// `Pipeline`, the derive generates a cloneable `PipelineRefs` struct and one
/// method: `Pipeline::wire(&mut GraphBuilder, wire) -> PipelineRefs`.
/// `wire` opens every actor slot before invoking its closure, so the closure
/// can construct factories that capture refs to each other. It fills the
/// slots, but graph validation and supervision topology stay explicit:
///
/// ```
/// # use kokage::{Actor, ActorFactory, ActorRef, ActorResult, GraphBuilder, MessageContext, OrderedTree};
/// # struct FrontendMsg;
/// # struct ParserMsg;
/// #
/// #[derive(ActorFactory)]
/// struct Frontend {
///     parser: ActorRef<ParserMsg>,
/// }
/// # impl Actor for Frontend {
/// #     type Msg = FrontendMsg;
/// #     async fn handle(&mut self, _: FrontendMsg, _: &mut MessageContext<'_, Self>) -> ActorResult { Ok(()) }
/// # }
///
/// #[derive(ActorFactory)]
/// struct Parser {
///     frontend: ActorRef<FrontendMsg>,
/// }
/// # impl Actor for Parser {
/// #     type Msg = ParserMsg;
/// #     async fn handle(&mut self, _: ParserMsg, _: &mut MessageContext<'_, Self>) -> ActorResult { Ok(()) }
/// # }
///
/// #[derive(kokage::Supervision)]
/// struct Pipeline {
///     frontend: FrontendFactory,
///     parser: ParserFactory,
/// }
///
/// # fn main() -> Result<(), kokage::GraphBuildError> {
/// let mut graph = GraphBuilder::new();
/// let refs = Pipeline::wire(&mut graph, |refs| Pipeline {
///     frontend: FrontendFactory { parser: refs.parser.clone() },
///     parser: ParserFactory { frontend: refs.frontend.clone() },
/// });
/// let tree = OrderedTree::graph(graph.build()?);
/// # let _ = (tree, refs);
/// # Ok(())
/// # }
/// ```
///
/// The derived struct is the factory bundle; no parallel `Factories`, `Slots`,
/// or `Scopes` types are generated. The closure is still necessary because
/// cycles require refs before their factories can be constructed.
///
/// Field names become graph actor labels. A field may use
/// `#[supervision(label = "...")]` to select another non-empty label. No other
/// derive attributes are supported. Mailbox configuration belongs on the
/// explicit actor declaration, while restart/shutdown policy, nested scopes,
/// and dynamic scopes belong on the explicit supervision tree.
///
/// The refs struct and `wire` inherit the derived struct's visibility; each
/// refs field inherits the corresponding factory field's visibility. Generic,
/// tuple, unit, empty, and non-struct declarations are rejected. Every field
/// must implement `ActorFactory`; invalid factories and mismatched message
/// types fail through the generated typed slot and ref expressions.
#[proc_macro_derive(Supervision, attributes(supervision))]
pub fn derive_supervision(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_supervision(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct SupervisionFieldAttrs {
    label: Option<String>,
}

fn parse_supervision_field(field: &Field) -> syn::Result<SupervisionFieldAttrs> {
    let mut parsed = SupervisionFieldAttrs::default();
    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("supervision"))
    {
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("label") {
                return Err(meta.error(
                    "expected `label = \"...\"`; graph configuration and supervision topology \
                     are explicit outside the derive",
                ));
            }
            if parsed.label.is_some() {
                return Err(meta.error("duplicate `label` option"));
            }
            let literal: syn::LitStr = meta.value()?.parse()?;
            let label = literal.value();
            if label.is_empty() {
                return Err(syn::Error::new_spanned(&literal, "label must not be empty"));
            }
            parsed.label = Some(label);
            Ok(())
        })?;
    }
    Ok(parsed)
}

fn expand_supervision(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(attr) = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("supervision"))
    {
        return Err(syn::Error::new_spanned(
            attr,
            "`supervision` attributes on structs are no longer supported; configure topology \
             and policy explicitly on the supervision tree",
        ));
    }
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "Supervision cannot be derived for generic structs",
        ));
    }

    let declared = input.ident;
    let vis = input.vis;
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
            "Supervision requires at least one actor factory field",
        ));
    }

    let attrs = fields
        .iter()
        .map(parse_supervision_field)
        .collect::<syn::Result<Vec<_>>>()?;
    let refs = format_ident!("{declared}Refs");
    let idents: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named fields"))
        .collect();
    let labels: Vec<_> = idents
        .iter()
        .zip(&attrs)
        .map(|(ident, attrs)| attrs.label.clone().unwrap_or_else(|| ident.to_string()))
        .collect();

    let mut seen = std::collections::HashSet::with_capacity(labels.len());
    for (index, label) in labels.iter().enumerate() {
        if !seen.insert(label) {
            return Err(syn::Error::new_spanned(
                &fields[index],
                format!("duplicate actor label `{label}`; labels must be unique within one struct"),
            ));
        }
    }

    let refs_fields = fields.iter().zip(&idents).map(|(field, ident)| {
        let field_vis = &field.vis;
        let factory = &field.ty;
        quote_spanned! {factory.span()=>
            #[allow(dead_code)]
            #field_vis #ident: ::kokage::ActorRef<
                <<#factory as ::kokage::ActorFactory>::Actor as
                    ::kokage::host::RawActor>::Msg
            >
        }
    });
    let open_stmts = fields
        .iter()
        .zip(&labels)
        .zip(&idents)
        .map(|((field, label), ident)| {
            let factory = &field.ty;
            let slot = format_ident!("__kokage_{ident}_slot");
            quote_spanned! {field.span()=>
                let (#slot, #ident) = builder.slot::
                    <<<#factory as ::kokage::ActorFactory>::Actor as
                        ::kokage::host::RawActor>::Msg>(#label);
            }
        });
    let define_stmts = idents.iter().map(|ident| {
        let slot = format_ident!("__kokage_{ident}_slot");
        quote! { builder.define(#slot, #ident); }
    });
    let refs_doc = format!(
        "Typed, restart-stable actor refs generated for [`{declared}`]. The bundle is cloneable so factories can retain refs for cyclic wiring."
    );
    let wire_doc = format!(
        "Opens every `{declared}` actor slot, invokes `wire` with all typed refs, and fills the slots from the returned factory bundle. Graph validation and supervision topology remain explicit on the caller-owned builder."
    );

    Ok(quote! {
        #[doc = #refs_doc]
        #[derive(Clone)]
        #vis struct #refs {
            #(#refs_fields,)*
        }

        impl #declared {
            #[doc = #wire_doc]
            #vis fn wire(
                builder: &mut ::kokage::GraphBuilder,
                wire: impl ::core::ops::FnOnce(&#refs) -> Self,
            ) -> #refs {
                #(#open_stmts)*
                let refs = #refs { #(#idents,)* };
                let Self { #(#idents,)* } = wire(&refs);
                #(#define_stmts)*
                refs
            }
        }
    })
}
