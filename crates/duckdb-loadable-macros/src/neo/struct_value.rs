use proc_macro2::TokenStream;
use quote::quote;

use super::struct_fields::{StructFields, bounded_generics, fresh_type_ident, option_inner_type};

pub(crate) fn as_duckdb_type(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsDuckDBType")?;
    let field_names = &fields.names;
    let field_types = &fields.types;
    let generics = bounded_generics(input, field_types, &syn::parse_quote!(::duckdb_neo::DuckDBType));
    let ident = &input.ident;
    let link_type = fresh_type_ident(input, "__DuckDBLink");
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::duckdb_neo::DuckDBType for #ident #type_generics #where_clause {
            fn logical_type<#link_type: ::duckdb_neo::connection::FFILink + ?::core::marker::Sized>(
                link: &#link_type,
            ) -> ::duckdb_neo::Result<::duckdb_neo::logical_type::LogicalType> {
                link.logical_type_create(
                    "STRUCT",
                    ::duckdb_neo::Parameters::named(&[
                        #((#field_names, &<#field_types as ::duckdb_neo::DuckDBType>::logical_type(link)?)),*
                    ]),
                )
            }
        }
    })
}

pub(crate) fn as_to_value(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsToValue")?;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let generics = bounded_generics(input, &fields.types, &syn::parse_quote!(::duckdb_neo::ToValue));
    let ident = &input.ident;
    let link_type = fresh_type_ident(input, "__DuckDBLink");
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::duckdb_neo::ToValue for #ident #type_generics #where_clause {
            fn value<#link_type: ::duckdb_neo::connection::FFILink + ?::core::marker::Sized>(
                &self,
                link: &#link_type,
            ) -> ::duckdb_neo::Result<::duckdb_neo::value::Value> {
                link.create_value(::duckdb_neo::value::ValueInput::Struct {
                    names: &[#(#field_names),*],
                    children: &[#(::duckdb_neo::ToValue::value(&self.#field_idents, link)?),*],
                })
            }
        }
    })
}

pub(crate) fn as_from_value(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsFromValue")?;
    let decoded_types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let generics = bounded_generics(input, &decoded_types, &syn::parse_quote!(::duckdb_neo::FromValue));
    let mut initializers = Vec::new();
    for (field, name) in fields.fields.iter().zip(&fields.names) {
        let ident = field.ident.as_ref().unwrap();
        let optional_type = option_inner_type(&field.ty);
        let decoded_type = optional_type.unwrap_or(&field.ty);
        let decode = quote! {
            <#decoded_type as ::duckdb_neo::FromValue>::from_value(
                &::duckdb_neo::__derive::value_field(value, &names, #name)?
            )?
        };
        let decode = if optional_type.is_some() {
            decode
        } else {
            quote!(::duckdb_neo::__derive::required(#decode, #name)?)
        };
        initializers.push(quote!(#ident: #decode));
    }
    let ident = &input.ident;
    let field_count = fields.fields.len();
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::duckdb_neo::FromValue for #ident #type_generics #where_clause {
            fn _get_inner(value: &::duckdb_neo::value::Value) -> ::duckdb_neo::Result<Self> {
                let names = ::duckdb_neo::__derive::value_field_names(value, #field_count)?;
                ::core::result::Result::Ok(Self { #(#initializers),* })
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{as_duckdb_type, as_from_value, as_to_value};

    #[test]
    fn value_derives_require_named_fields() {
        type Derive = fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>;
        let derives: [(&str, Derive); 3] = [
            ("AsDuckDBType", as_duckdb_type),
            ("AsToValue", as_to_value),
            ("AsFromValue", as_from_value),
        ];
        for input in [
            syn::parse_quote!(
                struct Tuple(i32);
            ),
            syn::parse_quote!(
                struct Unit;
            ),
            syn::parse_quote!(
                enum Enum {
                    Value,
                }
            ),
            syn::parse_quote!(union Union { value: i32 }),
        ] {
            for (name, derive) in derives {
                assert_eq!(
                    derive(&input).unwrap_err().to_string(),
                    format!("{name} requires a struct with named fields")
                );
            }
        }
    }
}
