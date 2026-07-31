#![warn(missing_docs)]

//! Derive macros for `kokage`.
//!
//! Do not depend on this crate directly: `kokage` re-exports
//! `#[derive(ActorFactory)]` and `#[derive(Supervision)]` under its opt-in
//! `derive` feature, and the generated code refers to `kokage` paths.

use proc_macro::TokenStream;
use quote::{ToTokens, format_ident, quote, quote_spanned};
use std::collections::HashSet;
use syn::{
    Data, DeriveInput, Expr, Field, Fields, Ident, LitStr, parse_macro_input, spanned::Spanned,
};

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
/// on every build. Use `#[factory(default = expression)]` when fresh state has
/// a non-`Default` initial value; the expression is evaluated for every actor
/// incarnation and `Self` refers to the actor declaration:
///
/// ```
/// # use std::collections::VecDeque;
/// # use kokage::{Actor, ActorSpec, Context, ExitResult};
/// # struct Job;
/// # struct Client;
/// # impl Clone for Client { fn clone(&self) -> Self { Self } }
/// #[derive(kokage::ActorFactory)]
/// struct Worker {
///     client: Client,
///     #[factory(default = VecDeque::with_capacity(Self::INITIAL_CAPACITY))]
///     pending: VecDeque<Job>,
/// }
/// # impl Worker { const INITIAL_CAPACITY: usize = 8; }
/// # impl Actor for Worker {
/// #     type Msg = ();
/// #     async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
/// #         let _ = (&self.client, &self.pending);
/// #         Ok(())
/// #     }
/// # }
///
/// let worker = ActorSpec::new("worker", WorkerFactory { client: Client });
/// let _worker_ref = worker.actor_ref();
/// ```
///
/// The generated factory and its configuration fields inherit the actor's
/// visibility. Configuration fields must implement `Clone`; fields marked with
/// bare `default` must implement `Default`. Generic, tuple, and unit structs
/// are rejected. Hand-write `ActorFactory` when construction needs logic
/// beyond cloning configuration and synchronously initializing fields.
#[proc_macro_derive(ActorFactory, attributes(factory))]
pub fn derive_actor_factory(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_actor_factory(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_actor_factory(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let mut reserved_names = HashSet::new();
    reserve_ident_names(input.to_token_stream(), &mut reserved_names);

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
    let initializers = parse_factory_attributes(&fields)?;
    let factory = format_ident!("{actor}Factory");
    let custom_defaults_trait =
        fresh_generated_ident("__KokageActorFactoryDefaults", &mut reserved_names);
    let factory_doc = format!(
        "Reusable factory generated from [`{actor}`].\n\nUnmarked fields are durable configuration cloned into every actor incarnation; fields marked with `#[factory(default)]` or `#[factory(default = expression)]` are freshly initialized."
    );

    let factory_fields: Vec<_> = fields
        .iter()
        .zip(&initializers)
        .filter(|(_, initializer)| matches!(initializer, FactoryFieldInit::Clone))
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
        .zip(&initializers)
        .map(|(field, initializer)| {
            let ident = field.ident.as_ref().expect("named fields");
            let ty = &field.ty;
            match initializer {
                FactoryFieldInit::Clone => quote_spanned! {field.span()=>
                    #ident: <#ty as ::core::clone::Clone>::clone(&self.#ident)
                },
                FactoryFieldInit::Default => quote_spanned! {field.span()=>
                    #ident: <#ty as ::core::default::Default>::default()
                },
                FactoryFieldInit::Expr(_) => quote_spanned! {field.span()=>
                    #ident: <#actor as #custom_defaults_trait>::#ident()
                },
            }
        })
        .collect();

    let custom_default_signatures: Vec<_> = fields
        .iter()
        .zip(&initializers)
        .filter_map(|(field, initializer)| match initializer {
            FactoryFieldInit::Expr(_) => {
                let ident = field.ident.as_ref().expect("named fields");
                let ty = &field.ty;
                Some(quote_spanned! {field.span()=> fn #ident() -> #ty; })
            }
            FactoryFieldInit::Clone | FactoryFieldInit::Default => None,
        })
        .collect();
    let custom_default_methods: Vec<_> = fields
        .iter()
        .zip(&initializers)
        .filter_map(|(field, initializer)| match initializer {
            FactoryFieldInit::Expr(expr) => {
                let ident = field.ident.as_ref().expect("named fields");
                let ty = &field.ty;
                Some(quote_spanned! {expr.span()=>
                    fn #ident() -> #ty {
                        #expr
                    }
                })
            }
            FactoryFieldInit::Clone | FactoryFieldInit::Default => None,
        })
        .collect();

    let factory_impl = quote! {
        impl ::kokage::ActorFactory for #factory {
            type Actor = #actor;

            fn build(&self) -> Self::Actor {
                #actor {
                    #(#actor_fields,)*
                }
            }
        }
    };
    let factory_impl = if custom_default_methods.is_empty() {
        factory_impl
    } else {
        quote! {
            const _: () = {
                trait #custom_defaults_trait {
                    #(#custom_default_signatures)*
                }

                impl #custom_defaults_trait for #actor {
                    #(#custom_default_methods)*
                }

                #factory_impl
            };
        }
    };

    Ok(quote! {
        #[doc = #factory_doc]
        #[derive(Clone)]
        #vis struct #factory {
            #(#factory_fields,)*
        }

        #factory_impl
    })
}

enum FactoryFieldInit {
    Clone,
    Default,
    Expr(Expr),
}

fn parse_factory_attributes(
    fields: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
) -> syn::Result<Vec<FactoryFieldInit>> {
    let mut initializers = Vec::with_capacity(fields.len());

    for field in fields {
        let mut default = None;
        for attr in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("factory"))
        {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("default") {
                    if default.is_some() {
                        return Err(meta.error("duplicate `default` option"));
                    }
                    default = if meta.input.peek(syn::Token![=]) {
                        Some(Some(meta.value()?.parse()?))
                    } else {
                        Some(None)
                    };
                    return Ok(());
                }
                Err(meta.error("expected `default`"))
            })?;
        }
        initializers.push(match default {
            None => FactoryFieldInit::Clone,
            Some(None) => FactoryFieldInit::Default,
            Some(Some(expr)) => FactoryFieldInit::Expr(expr),
        });
    }

    Ok(initializers)
}

/// Derives one statically declared supervision tree from a nested struct.
///
/// Actor fields are the default. Mark a field containing another derived
/// declaration with `#[supervision(scope)]`, or declare an empty dynamic leaf
/// with `#[supervision(dynamic)]` and the `kokage::DynamicScope` marker.
/// Field order is declaration order in the resulting `kokage::Tree`.
///
/// For a declaration named `App`, the derive generates `AppHandles`,
/// `AppFactories`, and `App::tree`. `tree` reserves every actor binding and
/// nested scope identity before invoking its wiring closure, so the closure can
/// wire cycles and cross-scope references without manually opening
/// `kokage::ActorSlot` values. It returns an ordinary `kokage::Tree` and the
/// generated handles bundle:
///
/// ```
/// # use kokage::{Actor, ActorRef, Context, DynamicScope, ExitResult, Strategy};
/// # struct Left(ActorRef<()>);
/// # struct Right(ActorRef<()>);
/// # impl Actor for Left { type Msg = (); async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult { Ok(()) } }
/// # impl Actor for Right { type Msg = (); async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult { Ok(()) } }
/// #[derive(kokage::Supervision)]
/// #[supervision(strategy = Strategy::OneForAll)]
/// struct Pair {
///     left: Left,
///     right: Right,
///     #[supervision(dynamic)]
///     workers: DynamicScope,
/// }
///
/// let (tree, handles) = Pair::tree(|handles| PairFactories {
///     left: {
///         let right = handles.right.clone();
///         move || Left(right.clone())
///     },
///     right: {
///         let left = handles.left.clone();
///         move || Right(left.clone())
///     },
/// });
/// # let _ = (tree, handles.workers);
/// ```
///
/// Scope attributes accept `strategy`, `default_restart`, `default_shutdown`,
/// `default_mailbox_shutdown`, and `mailbox_capacity`. Actor fields accept
/// `id`, `restart`, `shutdown`, `mailbox_shutdown`, `mailbox`, and
/// `message_size`. Nested scope fields accept `id`, `restart`, and `shutdown`
/// for their edge in the parent. Dynamic fields additionally accept the four
/// `default_*`/capacity settings for their empty dynamic scope.
#[proc_macro_derive(Supervision, attributes(supervision))]
pub fn derive_supervision(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_supervision(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct SupervisionScopeAttrs {
    strategy: Option<Expr>,
    default_restart: Option<Expr>,
    default_shutdown: Option<Expr>,
    default_mailbox_shutdown: Option<Expr>,
    mailbox_capacity: Option<Expr>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SupervisionFieldKind {
    Actor,
    Scope,
    Dynamic,
}

struct SupervisionFieldAttrs {
    kind: SupervisionFieldKind,
    id: Option<LitStr>,
    restart: Option<Expr>,
    shutdown: Option<Expr>,
    mailbox_shutdown: Option<Expr>,
    mailbox: Option<Expr>,
    message_size: Option<Expr>,
    default_restart: Option<Expr>,
    default_shutdown: Option<Expr>,
    default_mailbox_shutdown: Option<Expr>,
    mailbox_capacity: Option<Expr>,
}

impl Default for SupervisionFieldAttrs {
    fn default() -> Self {
        Self {
            kind: SupervisionFieldKind::Actor,
            id: None,
            restart: None,
            shutdown: None,
            mailbox_shutdown: None,
            mailbox: None,
            message_size: None,
            default_restart: None,
            default_shutdown: None,
            default_mailbox_shutdown: None,
            mailbox_capacity: None,
        }
    }
}

fn take_supervision_expr(
    slot: &mut Option<Expr>,
    meta: &syn::meta::ParseNestedMeta<'_>,
    key: &str,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(meta.error(format!("duplicate `{key}` option")));
    }
    *slot = Some(meta.value()?.parse()?);
    Ok(())
}

fn parse_supervision_scope_attrs(attrs: &[syn::Attribute]) -> syn::Result<SupervisionScopeAttrs> {
    let mut parsed = SupervisionScopeAttrs::default();
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("supervision"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("strategy") {
                return take_supervision_expr(&mut parsed.strategy, &meta, "strategy");
            }
            if meta.path.is_ident("default_restart") {
                return take_supervision_expr(
                    &mut parsed.default_restart,
                    &meta,
                    "default_restart",
                );
            }
            if meta.path.is_ident("default_shutdown") {
                return take_supervision_expr(
                    &mut parsed.default_shutdown,
                    &meta,
                    "default_shutdown",
                );
            }
            if meta.path.is_ident("default_mailbox_shutdown") {
                return take_supervision_expr(
                    &mut parsed.default_mailbox_shutdown,
                    &meta,
                    "default_mailbox_shutdown",
                );
            }
            if meta.path.is_ident("mailbox_capacity") {
                return take_supervision_expr(
                    &mut parsed.mailbox_capacity,
                    &meta,
                    "mailbox_capacity",
                );
            }
            Err(meta.error(
                "expected `strategy`, `default_restart`, `default_shutdown`, \
                 `default_mailbox_shutdown`, or `mailbox_capacity`, each `= <expression>`",
            ))
        })?;
    }
    Ok(parsed)
}

fn parse_supervision_field_attrs(field: &Field) -> syn::Result<SupervisionFieldAttrs> {
    let mut parsed = SupervisionFieldAttrs::default();
    let mut kind_selected = false;

    for attr in field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("supervision"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("scope") || meta.path.is_ident("dynamic") {
                if kind_selected {
                    return Err(meta.error("`scope` and `dynamic` are mutually exclusive"));
                }
                kind_selected = true;
                parsed.kind = if meta.path.is_ident("scope") {
                    SupervisionFieldKind::Scope
                } else {
                    SupervisionFieldKind::Dynamic
                };
                return Ok(());
            }
            if meta.path.is_ident("id") {
                if parsed.id.is_some() {
                    return Err(meta.error("duplicate `id` option"));
                }
                let literal: syn::LitStr = meta.value()?.parse()?;
                if literal.value().is_empty() {
                    return Err(syn::Error::new_spanned(
                        literal,
                        "supervision child id must not be empty",
                    ));
                }
                parsed.id = Some(literal);
                return Ok(());
            }
            if meta.path.is_ident("restart") {
                return take_supervision_expr(&mut parsed.restart, &meta, "restart");
            }
            if meta.path.is_ident("shutdown") {
                return take_supervision_expr(&mut parsed.shutdown, &meta, "shutdown");
            }
            if meta.path.is_ident("mailbox_shutdown") {
                return take_supervision_expr(
                    &mut parsed.mailbox_shutdown,
                    &meta,
                    "mailbox_shutdown",
                );
            }
            if meta.path.is_ident("mailbox") {
                return take_supervision_expr(&mut parsed.mailbox, &meta, "mailbox");
            }
            if meta.path.is_ident("message_size") {
                return take_supervision_expr(&mut parsed.message_size, &meta, "message_size");
            }
            if meta.path.is_ident("default_restart") {
                return take_supervision_expr(
                    &mut parsed.default_restart,
                    &meta,
                    "default_restart",
                );
            }
            if meta.path.is_ident("default_shutdown") {
                return take_supervision_expr(
                    &mut parsed.default_shutdown,
                    &meta,
                    "default_shutdown",
                );
            }
            if meta.path.is_ident("default_mailbox_shutdown") {
                return take_supervision_expr(
                    &mut parsed.default_mailbox_shutdown,
                    &meta,
                    "default_mailbox_shutdown",
                );
            }
            if meta.path.is_ident("mailbox_capacity") {
                return take_supervision_expr(
                    &mut parsed.mailbox_capacity,
                    &meta,
                    "mailbox_capacity",
                );
            }
            Err(meta.error(
                "expected `scope`, `dynamic`, `id`, `restart`, `shutdown`, \
                 `mailbox_shutdown`, `mailbox`, `message_size`, `default_restart`, \
                 `default_shutdown`, `default_mailbox_shutdown`, or `mailbox_capacity`",
            ))
        })?;
    }

    let actor_only = parsed.mailbox_shutdown.is_some()
        || parsed.mailbox.is_some()
        || parsed.message_size.is_some();
    let dynamic_only = parsed.default_restart.is_some()
        || parsed.default_shutdown.is_some()
        || parsed.default_mailbox_shutdown.is_some()
        || parsed.mailbox_capacity.is_some();
    match parsed.kind {
        SupervisionFieldKind::Actor if dynamic_only => {
            return Err(syn::Error::new_spanned(
                field,
                "`default_*` and `mailbox_capacity` configure dynamic scope fields",
            ));
        }
        SupervisionFieldKind::Scope if actor_only || dynamic_only => {
            return Err(syn::Error::new_spanned(
                field,
                "a nested scope configures its contents on its own derived struct; only `id`, \
                 `restart`, and `shutdown` configure its parent edge",
            ));
        }
        SupervisionFieldKind::Dynamic if actor_only => {
            return Err(syn::Error::new_spanned(
                field,
                "actor mailbox options do not apply to a dynamic scope",
            ));
        }
        _ => {}
    }
    Ok(parsed)
}

fn expand_supervision(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "Supervision cannot be derived for generic structs",
        ));
    }

    let mut reserved_param_names = HashSet::new();
    reserve_ident_names(input.to_token_stream(), &mut reserved_param_names);

    let declaration = input.ident;
    let vis = input.vis;
    let scope_attrs = parse_supervision_scope_attrs(&input.attrs)?;
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
                    &declaration,
                    "Supervision can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &declaration,
                "Supervision can only be derived for structs with named fields",
            ));
        }
    };
    let field_attrs = fields
        .iter()
        .map(parse_supervision_field_attrs)
        .collect::<syn::Result<Vec<_>>>()?;

    let declaration_name = unraw_ident_name(&declaration);
    let handles = format_ident!("{declaration_name}Handles");
    let factories = format_ident!("{declaration_name}Factories");
    let slots = format_ident!("{declaration_name}Slots");
    let field_idents: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named field"))
        .collect();
    let child_ids: Vec<_> = field_idents
        .iter()
        .zip(&field_attrs)
        .map(|(ident, attrs)| {
            attrs
                .id
                .clone()
                .unwrap_or_else(|| LitStr::new(&unraw_ident_name(ident), ident.span()))
        })
        .collect();
    for (index, id) in child_ids.iter().enumerate() {
        if child_ids[..index]
            .iter()
            .any(|previous| previous.value() == id.value())
        {
            return Err(syn::Error::new_spanned(
                id,
                format!("duplicate supervision child id `{}`", id.value()),
            ));
        }
    }
    let scope_field = fresh_supervision_ident("__kokage_scope", &field_idents);
    let tree_field = fresh_supervision_ident("__kokage_tree", &field_idents);

    let mut factory_params = Vec::with_capacity(fields.len());
    let mut next_param = 0usize;
    for attrs in &field_attrs {
        if attrs.kind == SupervisionFieldKind::Dynamic {
            factory_params.push(None);
        } else {
            factory_params.push(Some(fresh_factory_param(
                &mut next_param,
                &mut reserved_param_names,
            )));
        }
    }
    let all_params: Vec<_> = factory_params.iter().flatten().collect();
    let generics = if all_params.is_empty() {
        quote! {}
    } else {
        quote! { <#(#all_params),*> }
    };

    let mut slot_fields = Vec::new();
    let mut handle_fields = Vec::new();
    let mut clone_fields = Vec::new();
    let mut factory_fields = Vec::new();
    let mut factory_bounds = Vec::new();
    let mut open_stmts = Vec::new();
    let mut slot_values = Vec::new();
    let mut handle_values = Vec::new();
    let mut marker_assertions = Vec::new();
    let mut define_stmts = Vec::new();

    for (index, field) in fields.iter().enumerate() {
        let ident = field_idents[index];
        let field_vis = &field.vis;
        let ty = &field.ty;
        let attrs = &field_attrs[index];
        let id = &child_ids[index];
        let ident_name = unraw_ident_name(ident);
        let slot_local = format_ident!("__kokage_{ident_name}_slot");
        let handle_local = format_ident!("__kokage_{ident_name}_handle");
        let field_doc = format!("Handle state generated for the `{ident}` child.");

        clone_fields.push(quote! { #ident: self.#ident.clone() });
        match attrs.kind {
            SupervisionFieldKind::Actor => {
                let param = factory_params[index].as_ref().expect("actor factory");
                let slot_ty = quote_spanned! {ty.span()=>
                    ::kokage::ActorSlot<<#ty as ::kokage::raw::RawActor>::Msg>
                };
                let handle_ty = quote_spanned! {ty.span()=>
                    ::kokage::ActorRef<<#ty as ::kokage::raw::RawActor>::Msg>
                };
                slot_fields.push(quote! { #ident: #slot_ty });
                handle_fields.push(quote! {
                    #[doc = #field_doc]
                    #field_vis #ident: #handle_ty
                });
                factory_fields.push(quote! {
                    #[doc = #field_doc]
                    #vis #ident: #param
                });
                factory_bounds.push(quote_spanned! {ty.span()=>
                    #param: ::kokage::ActorFactory<Actor = #ty>
                });
                open_stmts.push(quote_spanned! {ty.span()=>
                    let #slot_local = ::kokage::ActorSlot::
                        <<#ty as ::kokage::raw::RawActor>::Msg>::new(#id);
                    let #handle_local = #slot_local.actor_ref();
                });
                slot_values.push(quote! { #ident: #slot_local });
                handle_values.push(quote! { #ident: #handle_local });

                let spec = configured_actor_spec(
                    quote! {
                        slots.#ident.define(factories.#ident)
                    },
                    attrs,
                );
                define_stmts.push(quote! {
                    tree.add_actor_spec(#spec);
                });
            }
            SupervisionFieldKind::Scope => {
                let param = factory_params[index].as_ref().expect("scope factories");
                slot_fields.push(quote_spanned! {ty.span()=>
                    #ident: <#ty as ::kokage::Supervision>::Slots
                });
                handle_fields.push(quote_spanned! {ty.span()=>
                    #[doc = #field_doc]
                    #field_vis #ident: <#ty as ::kokage::Supervision>::Handles
                });
                factory_fields.push(quote! {
                    #[doc = #field_doc]
                    #vis #ident: #param
                });
                factory_bounds.push(quote_spanned! {ty.span()=>
                    #param: ::kokage::SupervisionFactories<#ty>
                });
                open_stmts.push(quote_spanned! {ty.span()=>
                    let (#slot_local, #handle_local) =
                        <#ty as ::kokage::Supervision>::__open();
                });
                slot_values.push(quote! { #ident: #slot_local });
                handle_values.push(quote! { #ident: #handle_local });

                let subtree = configured_subtree(
                    quote! {
                        <#param as ::kokage::SupervisionFactories<#ty>>::__define(
                            factories.#ident,
                            slots.#ident,
                        )
                    },
                    attrs,
                );
                define_stmts.push(quote! {
                    tree.add_subtree_spec(#id, #subtree);
                });
            }
            SupervisionFieldKind::Dynamic => {
                let assertion = format_ident!("__kokage_assert_{ident_name}_is_dynamic_scope");
                marker_assertions.push(quote_spanned! {ty.span()=>
                    #[allow(non_snake_case, dead_code)]
                    fn #assertion(value: #ty) -> ::kokage::DynamicScope { value }
                });
                slot_fields.push(quote! { #ident: ::kokage::DynamicTree });
                handle_fields.push(quote! {
                    #[doc = #field_doc]
                    #field_vis #ident: ::kokage::DynamicScopeRef
                });
                let dynamic = configured_dynamic_tree(attrs);
                open_stmts.push(quote! {
                    let #slot_local = #dynamic;
                    let #handle_local = #slot_local.scope();
                });
                slot_values.push(quote! { #ident: #slot_local });
                handle_values.push(quote! { #ident: #handle_local });

                let subtree = configured_subtree(quote! { slots.#ident }, attrs);
                define_stmts.push(quote! {
                    tree.add_subtree_spec(#id, #subtree);
                });
            }
        }
    }

    let where_clause = if factory_bounds.is_empty() {
        quote! {}
    } else {
        quote! { where #(#factory_bounds,)* }
    };
    let tree_expr = configured_tree(&scope_attrs);
    let mark_fields = if field_idents.is_empty() {
        quote! {
            let _mark_declaration_used = |_value: Self| {};
        }
    } else {
        quote! {
            let _mark_declaration_used = |value: Self| {
                let Self { #(#field_idents),* } = value;
                let _ = (#(#field_idents,)*);
            };
        }
    };
    let handles_doc = format!("Nested actor and scope handles generated for [`{declaration}`].");
    let factories_doc = format!(
        "Actor factory tree generated for [`{declaration}`]. Dynamic fields are omitted because they have no static factory."
    );
    let slots_doc = format!("Reserved construction state for [`{declaration}`].");
    let tree_doc = format!(
        "Reserves every handle, invokes `wire`, and lowers [`{declaration}`] into an ordinary [`kokage::Tree`]."
    );

    Ok(quote! {
        #[doc = #handles_doc]
        #vis struct #handles {
            #scope_field: ::kokage::ScopeRef,
            #(#handle_fields,)*
        }

        impl ::core::clone::Clone for #handles {
            fn clone(&self) -> Self {
                Self {
                    #scope_field: self.#scope_field.clone(),
                    #(#clone_fields,)*
                }
            }
        }

        impl #handles {
            /// Returns the stable handle for this generated ordered scope.
            #vis fn scope(&self) -> ::kokage::ScopeRef {
                self.#scope_field.clone()
            }
        }

        #[doc(hidden)]
        #[doc = #slots_doc]
        #vis struct #slots {
            #tree_field: ::kokage::Tree,
            #(#slot_fields,)*
        }

        #[doc = #factories_doc]
        #vis struct #factories #generics {
            #(#factory_fields,)*
        }

        const _: () = {
            trait __KokageSupervisionDefine #generics {
                fn __define(
                    factories: #factories #generics,
                    slots: #slots,
                ) -> ::kokage::Tree;
            }

            impl #generics __KokageSupervisionDefine #generics for #declaration
            #where_clause
            {
                fn __define(
                    factories: #factories #generics,
                    slots: #slots,
                ) -> ::kokage::Tree {
                    let _ = &factories;
                    let mut tree = slots.#tree_field;
                    #(#define_stmts)*
                    tree
                }
            }

            impl #generics ::kokage::SupervisionFactories<#declaration>
                for #factories #generics
            #where_clause
            {
                fn __define(
                    self,
                    slots: <#declaration as ::kokage::Supervision>::Slots,
                ) -> ::kokage::Tree {
                    <#declaration as __KokageSupervisionDefine #generics>::
                        __define(self, slots)
                }
            }
        };

        impl ::kokage::Supervision for #declaration {
            type Handles = #handles;
            type Slots = #slots;

            fn __open() -> (Self::Slots, Self::Handles) {
                #mark_fields
                #(#marker_assertions)*
                let #tree_field = #tree_expr;
                let #scope_field = #tree_field.scope();
                #(#open_stmts)*
                (
                    #slots {
                        #tree_field,
                        #(#slot_values,)*
                    },
                    #handles {
                        #scope_field,
                        #(#handle_values,)*
                    },
                )
            }
        }

        impl #declaration {
            #[doc = #tree_doc]
            #vis fn tree #generics (
                wire: impl ::core::ops::FnOnce(&#handles) -> #factories #generics,
            ) -> (::kokage::Tree, #handles)
            #where_clause
            {
                let (slots, handles) = <Self as ::kokage::Supervision>::__open();
                let factories = wire(&handles);
                let tree = <#factories #generics as
                    ::kokage::SupervisionFactories<Self>>::__define(factories, slots);
                (tree, handles)
            }
        }
    })
}

fn reserve_ident_names(tokens: proc_macro2::TokenStream, reserved: &mut HashSet<String>) {
    for token in tokens {
        match token {
            proc_macro2::TokenTree::Group(group) => {
                reserve_ident_names(group.stream(), reserved);
            }
            proc_macro2::TokenTree::Ident(ident) => {
                reserved.insert(unraw_ident_name(&ident));
            }
            proc_macro2::TokenTree::Literal(_) | proc_macro2::TokenTree::Punct(_) => {}
        }
    }
}

fn fresh_factory_param(next: &mut usize, reserved: &mut HashSet<String>) -> Ident {
    loop {
        let candidate = format!("F{next}");
        *next += 1;
        if reserved.insert(candidate.clone()) {
            return format_ident!("{candidate}");
        }
    }
}

fn fresh_generated_ident(base: &str, reserved: &mut HashSet<String>) -> Ident {
    if reserved.insert(base.to_owned()) {
        return format_ident!("{base}");
    }
    for suffix in 0usize.. {
        let candidate = format!("{base}_{suffix}");
        if reserved.insert(candidate.clone()) {
            return format_ident!("{candidate}");
        }
    }
    unreachable!("the generated identifier suffix space is inexhaustible")
}

fn fresh_supervision_ident(base: &str, fields: &[&Ident]) -> Ident {
    let is_available = |candidate: &str| {
        fields
            .iter()
            .all(|field| unraw_ident_name(field) != candidate)
    };
    if is_available(base) {
        return format_ident!("{base}");
    }
    for suffix in 0usize.. {
        let candidate = format!("{base}_{suffix}");
        if is_available(&candidate) {
            return format_ident!("{candidate}");
        }
    }
    unreachable!("the generated identifier suffix space is inexhaustible")
}

fn unraw_ident_name(ident: &Ident) -> String {
    let ident = ident.to_string();
    ident.strip_prefix("r#").unwrap_or(&ident).to_owned()
}

fn configured_tree(attrs: &SupervisionScopeAttrs) -> proc_macro2::TokenStream {
    let mut tree = quote! { ::kokage::Tree::new() };
    if let Some(value) = &attrs.strategy {
        tree = quote! { #tree.strategy(#value) };
    }
    if let Some(value) = &attrs.default_restart {
        tree = quote! { #tree.default_restart(#value) };
    }
    if let Some(value) = &attrs.default_shutdown {
        tree = quote! { #tree.default_shutdown(#value) };
    }
    if let Some(value) = &attrs.default_mailbox_shutdown {
        tree = quote! { #tree.default_mailbox_shutdown(#value) };
    }
    if let Some(value) = &attrs.mailbox_capacity {
        tree = quote! { #tree.mailbox_capacity(#value) };
    }
    tree
}

fn configured_dynamic_tree(attrs: &SupervisionFieldAttrs) -> proc_macro2::TokenStream {
    let mut tree = quote! { ::kokage::DynamicTree::new() };
    if let Some(value) = &attrs.default_restart {
        tree = quote! { #tree.default_restart(#value) };
    }
    if let Some(value) = &attrs.default_shutdown {
        tree = quote! { #tree.default_shutdown(#value) };
    }
    if let Some(value) = &attrs.default_mailbox_shutdown {
        tree = quote! { #tree.default_mailbox_shutdown(#value) };
    }
    if let Some(value) = &attrs.mailbox_capacity {
        tree = quote! { #tree.mailbox_capacity(#value) };
    }
    tree
}

fn configured_actor_spec(
    mut spec: proc_macro2::TokenStream,
    attrs: &SupervisionFieldAttrs,
) -> proc_macro2::TokenStream {
    if let Some(value) = &attrs.mailbox {
        spec = quote! { #spec.mailbox(#value) };
    }
    if let Some(value) = &attrs.message_size {
        spec = quote! { #spec.message_size(#value) };
    }
    if let Some(value) = &attrs.restart {
        spec = quote! { #spec.restart(#value) };
    }
    if let Some(value) = &attrs.shutdown {
        spec = quote! { #spec.shutdown(#value) };
    }
    if let Some(value) = &attrs.mailbox_shutdown {
        spec = quote! { #spec.mailbox_shutdown(#value) };
    }
    spec
}

fn configured_subtree(
    tree: proc_macro2::TokenStream,
    attrs: &SupervisionFieldAttrs,
) -> proc_macro2::TokenStream {
    let mut spec = quote! { ::kokage::SubtreeSpec::from(#tree) };
    if let Some(value) = &attrs.restart {
        spec = quote! { #spec.restart(#value) };
    }
    if let Some(value) = &attrs.shutdown {
        spec = quote! { #spec.shutdown(#value) };
    }
    spec
}
