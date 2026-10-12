//! Derives for `duckdb-neo`'s raw handle traits.

use proc_macro::TokenStream;
use syn::spanned::Spanned;

/// Pick the handle field: the one marked `#[raw]`, the only field, or the field named `handle`.
fn raw_handle_field(input: &syn::DeriveInput) -> syn::Result<(syn::Member, &syn::Type, &syn::Fields)> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(input.span(), "raw handle derives only support structs"));
    };
    let fields: Vec<(syn::Member, &syn::Field)> = data
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let member = match &field.ident {
                Some(ident) => syn::Member::Named(ident.clone()),
                None => syn::Member::Unnamed(index.into()),
            };
            (member, field)
        })
        .collect();

    let marked: Vec<_> = fields
        .iter()
        .filter(|(_, field)| field.attrs.iter().any(|attr| attr.path().is_ident("raw")))
        .collect();
    let selected = match marked.as_slice() {
        [one] => Some(*one),
        [] if fields.len() == 1 => fields.first(),
        [] => fields
            .iter()
            .find(|(_, field)| field.ident.as_ref().is_some_and(|ident| ident == "handle")),
        _ => return Err(syn::Error::new(input.span(), "only one field may be marked #[raw]")),
    };
    let Some((member, field)) = selected else {
        return Err(syn::Error::new(
            input.span(),
            "mark the handle field with #[raw], or name it `handle`",
        ));
    };
    Ok((member.clone(), &field.ty, &data.fields))
}

pub(crate) fn derive_as_raw(input: syn::DeriveInput) -> TokenStream {
    let (member, ty, _) = match raw_handle_field(&input) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote::quote! {
        impl #impl_generics ::duckdb_neo::AsRaw for #name #ty_generics #where_clause {
            type Raw = #ty;

            unsafe fn get_raw(&self) -> Self::Raw {
                self.#member
            }
        }
    }
    .into()
}

pub(crate) fn derive_from_raw(input: syn::DeriveInput) -> TokenStream {
    let (member, _, fields) = match raw_handle_field(&input) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error().into(),
    };
    let others = fields
        .iter()
        .enumerate()
        .map(|(index, field)| match &field.ident {
            Some(ident) => syn::Member::Named(ident.clone()),
            None => syn::Member::Unnamed(index.into()),
        })
        .filter(|other| *other != member);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    quote::quote! {
        impl #impl_generics ::duckdb_neo::FromRaw for #name #ty_generics #where_clause {
            unsafe fn from_raw(raw: Self::Raw) -> Self {
                Self {
                    #member: raw,
                    #(#others: ::core::default::Default::default(),)*
                }
            }
        }
    }
    .into()
}
