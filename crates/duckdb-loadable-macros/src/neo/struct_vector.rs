use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::struct_fields::{StructFields, bounded_generics, fresh_type_ident};

struct Projection {
    ident: syn::Ident,
    lifetime: syn::Lifetime,
    generics: syn::Generics,
    declaration: TokenStream,
    marker_init: TokenStream,
}

fn projection(
    input: &syn::DeriveInput,
    fields: &StructFields<'_>,
    types: &[&syn::Type],
    bound: &syn::Path,
    associated: &str,
) -> Projection {
    let ident = &input.ident;
    let projected_ident = format_ident!("{ident}{associated}");
    let associated_ident = format_ident!("{associated}");
    let vis = &input.vis;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let field_vis = fields.fields.iter().map(|field| &field.vis);
    let mut lifetime_name = "__duckdb_row".to_owned();
    while input
        .generics
        .lifetimes()
        .any(|param| param.lifetime.ident == lifetime_name)
    {
        lifetime_name.push('_');
    }
    let lifetime = syn::Lifetime::new(&format!("'{lifetime_name}"), ident.span());
    let mut generics = bounded_generics(input, types, bound);
    generics.params.insert(0, syn::parse_quote!(#lifetime));
    for ty in types {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #lifetime));
    }
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let marker = fields.fields.is_empty().then(|| {
        quote! {
            __duckdb_marker: ::core::marker::PhantomData<&#lifetime ()>,
        }
    });
    let marker_init = if fields.fields.is_empty() {
        quote! { __duckdb_marker: ::core::marker::PhantomData, }
    } else {
        TokenStream::new()
    };
    let mut debug_generics = generics.clone();
    for ty in types {
        debug_generics.make_where_clause().predicates.push(syn::parse_quote!(
            <#ty as #bound>::#associated_ident<#lifetime>: ::core::fmt::Debug
        ));
    }
    let (debug_impl_generics, _, debug_where_clause) = debug_generics.split_for_impl();
    let default = (associated == "Write").then(|| {
        quote! {
            impl #impl_generics ::core::default::Default for #projected_ident #type_generics #where_clause {
                fn default() -> Self {
                    Self {
                        #(#field_idents: ::core::option::Option::None,)*
                        #marker_init
                    }
                }
            }
        }
    });
    let declaration = quote! {
        #[doc = concat!("Projected ", #associated, " fields of [`", stringify!(#ident), "`].")]
        #vis struct #projected_ident #generics #where_clause {
            #(#field_vis #field_idents: ::core::option::Option<
                <#types as #bound>::#associated_ident<#lifetime>
            >,)*
            #marker
        }

        impl #debug_impl_generics ::core::fmt::Debug for #projected_ident #type_generics #debug_where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_struct(stringify!(#projected_ident))
                    #(.field(#field_names, &self.#field_idents))*
                    .finish()
            }
        }
        #default
    };
    Projection {
        ident: projected_ident,
        lifetime,
        generics,
        declaration,
        marker_init,
    }
}

pub(crate) fn as_vector_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsVectorElement")?;
    let types = fields.optional_types("AsVectorElement")?;
    let bound = syn::parse_quote!(::duckdb_neo::vector::VectorElement);
    let projected = projection(input, &fields, &types, &bound, "Ref");
    let Projection {
        ident: ref_ident,
        lifetime,
        declaration,
        marker_init,
        ..
    } = &projected;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let count = types.len();
    let ident = &input.ident;
    let reader_type = fresh_type_ident(input, "__DuckDBVector");
    let generics = bounded_generics(input, &types, &bound);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let (_, ref_type_generics, _) = projected.generics.split_for_impl();

    Ok(quote! {
        #declaration

        // SAFETY: validation checks the ordered child schema; reads use only those prepared children.
        unsafe impl #impl_generics #bound for #ident #type_generics #where_clause {
            const TYPE_ID: ::duckdb_neo::logical_type::LogicalTypeID =
                ::duckdb_neo::logical_type::LogicalTypeID::Struct;
            type Internal<#lifetime> = ();
            type Ref<#lifetime> = #ref_ident #ref_type_generics where Self: #lifetime;

            fn validate(
                other: &::duckdb_neo::logical_type::LogicalType,
                children: ::core::option::Option<&::duckdb_neo::data_chunk::VectorCollection>,
            ) -> ::duckdb_neo::Result<bool> {
                if other.id()? != Self::TYPE_ID || other.param_count()? != #count
                    || children.map_or(0, |children| children.col_count()) != #count
                {
                    return Ok(false);
                }
                #(
                    if !other.get_param(#indexes)?.0.eq_ignore_ascii_case(#field_names) {
                        return Ok(false);
                    }
                    let child = children.expect("validated struct has children").get_untyped_vector_at(#indexes)?;
                    if !<#types as #bound>::validate(child.logical_type(), child.children())? {
                        return Ok(false);
                    }
                )*
                Ok(true)
            }

            unsafe fn get<#lifetime, #reader_type: #bound>(
                reader: &::duckdb_neo::vector::VectorReader<#lifetime, #reader_type>,
                _physical: usize,
                logical: usize,
            ) -> Self::Ref<#lifetime>
            where Self: #lifetime {
                #ref_ident {
                    // SAFETY: validation checked every child, and the reader prepared their views.
                    #(#field_idents: unsafe { reader.get_child_unchecked::<#types>(#indexes, logical) },)*
                    #marker_init
                }
            }
        }
    })
}

pub(crate) fn as_writable_vector_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsWritableVectorElement")?;
    let types = fields.optional_types("AsWritableVectorElement")?;
    let bound = syn::parse_quote!(::duckdb_neo::vector::WritableVectorElement);
    let projected = projection(input, &fields, &types, &bound, "Write");
    let Projection {
        ident: write_ident,
        lifetime,
        declaration,
        ..
    } = &projected;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let ident = &input.ident;
    let mut generics = bounded_generics(input, &types, &bound);
    generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!(Self: ::duckdb_neo::vector::VectorElement));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let (_, write_type_generics, _) = projected.generics.split_for_impl();
    let write_fields = quote! {
        // SAFETY: the parent validated every child, and the caller guarantees row bounds.
        #(unsafe { writer.write_child_unchecked::<#types>(#indexes, index, value.#field_idents)? };)*
    };

    Ok(quote! {
        #declaration

        // SAFETY: fields use their typed child writers, and NULL rows recursively null descendants.
        unsafe impl #impl_generics #bound for #ident #type_generics #where_clause {
            type Write<#lifetime> = #write_ident #write_type_generics where Self: #lifetime;

            fn prepare(writer: &mut ::duckdb_neo::vector::VectorWriter<'_, '_, Self>) -> ::duckdb_neo::Result<()> {
                writer.validate_struct_fields(&[#(#field_names),*])?;
                #(writer.validate_child::<#types>(#indexes)?;)*
                Ok(())
            }

            unsafe fn write(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, '_, Self>,
                index: usize,
                value: ::core::option::Option<Self::Write<'_>>,
            ) -> ::duckdb_neo::Result<()> {
                let Some(value) = value else {
                    return writer.set_null_slow(index);
                };
                #write_fields
                Ok(())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{as_vector_element, as_writable_vector_element};

    #[test]
    fn vector_derives_require_optional_named_fields() {
        type Derive = fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>;
        let derives: [(&str, Derive); 2] = [
            ("AsVectorElement", as_vector_element),
            ("AsWritableVectorElement", as_writable_vector_element),
        ];
        for input in [
            syn::parse_quote!(
                struct Tuple(Option<i32>);
            ),
            syn::parse_quote!(
                struct Unit;
            ),
            syn::parse_quote!(
                enum Enum {
                    Value,
                }
            ),
        ] {
            for (name, derive) in derives {
                assert_eq!(
                    derive(&input).unwrap_err().to_string(),
                    format!("{name} requires a struct with named fields")
                );
            }
        }
        for (name, derive) in derives {
            assert_eq!(
                derive(&syn::parse_quote!(
                    struct Required {
                        value: i32,
                    }
                ))
                .unwrap_err()
                .to_string(),
                format!("{name} fields must be Option<T>")
            );
        }
    }
}
