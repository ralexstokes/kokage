#![warn(missing_docs)]

//! Derive macros for `kokage`.
//!
//! Do not depend on this crate directly: `kokage` re-exports
//! `#[derive(ActorFactory)]` under its opt-in `derive` feature, and the
//! generated code refers to `kokage` paths.

use proc_macro::TokenStream;
use quote::{ToTokens, format_ident, quote, quote_spanned};
use std::collections::HashSet;
use syn::{Data, DeriveInput, Expr, Field, Fields, Ident, parse_macro_input, spanned::Spanned};

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

fn unraw_ident_name(ident: &Ident) -> String {
    let ident = ident.to_string();
    ident.strip_prefix("r#").unwrap_or(&ident).to_owned()
}
