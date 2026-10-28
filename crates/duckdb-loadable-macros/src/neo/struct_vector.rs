use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::struct_fields::{StructFields, bounded_generics, fresh_type_ident, option_inner_type};

fn row_lifetime(input: &syn::DeriveInput) -> syn::Lifetime {
    let mut name = "__duckdb_row".to_owned();
    while input.generics.lifetimes().any(|param| param.lifetime.ident == name) {
        name.push('_');
    }
    syn::Lifetime::new(&format!("'{name}"), input.ident.span())
}

fn ref_bounded_generics(
    generics: &syn::Generics,
    types: &[&syn::Type],
    lifetime: &syn::Lifetime,
    bound: TokenStream,
) -> syn::Generics {
    let mut generics = generics.clone();
    for ty in types {
        generics.make_where_clause().predicates.push(syn::parse_quote!(
            <#ty as ::duckdb_neo::vector::VectorElement>::Ref<#lifetime>: #bound
        ));
    }
    generics
}

pub(crate) fn as_vector_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsVectorElement")?;
    let types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::VectorElement);
    let ident = &input.ident;
    let ref_ident = format_ident!("{ident}Ref");
    let ref_doc = format!("Borrowed, nullable fields of [`{ident}`].");
    let lifetime = row_lifetime(input);
    let vis = &input.vis;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let field_vis = fields.fields.iter().map(|field| &field.vis);
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let count = types.len();
    let reader_type = fresh_type_ident(input, "__DuckDBVector");
    let hasher_type = fresh_type_ident(input, "__DuckDBHasher");
    let generics = bounded_generics(input, &types, &bound);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let mut ref_generics = generics.clone();
    ref_generics.params.insert(0, syn::parse_quote!(#lifetime));
    for ty in &types {
        ref_generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #lifetime));
    }
    let (_, ref_type_generics, ref_where_clause) = ref_generics.split_for_impl();
    let ref_impl = |bound: TokenStream| ref_bounded_generics(&ref_generics, &types, &lifetime, bound);
    let debug_generics = ref_impl(quote!(::core::fmt::Debug));
    let (debug_impl_generics, _, debug_where_clause) = debug_generics.split_for_impl();
    let clone_generics = ref_impl(quote!(::core::clone::Clone));
    let (clone_impl_generics, _, clone_where_clause) = clone_generics.split_for_impl();
    let copy_generics = ref_impl(quote!(::core::marker::Copy));
    let (copy_impl_generics, _, copy_where_clause) = copy_generics.split_for_impl();
    let eq_generics = ref_impl(quote!(::core::cmp::PartialEq));
    let (eq_impl_generics, _, eq_where_clause) = eq_generics.split_for_impl();
    let total_eq_generics = ref_impl(quote!(::core::cmp::Eq));
    let (total_eq_impl_generics, _, total_eq_where_clause) = total_eq_generics.split_for_impl();
    let hash_generics = ref_impl(quote!(::core::hash::Hash));
    let (hash_impl_generics, _, hash_where_clause) = hash_generics.split_for_impl();
    let marker = fields.fields.is_empty().then(|| {
        quote! {
            __duckdb_marker: ::core::marker::PhantomData<&#lifetime ()>,
        }
    });
    let marker_init = fields.fields.is_empty().then(|| {
        quote! {
            __duckdb_marker: ::core::marker::PhantomData,
        }
    });
    let fields_eq = if fields.fields.is_empty() {
        quote!(true)
    } else {
        quote!(#(::core::cmp::PartialEq::eq(&self.#field_idents, &other.#field_idents))&&*)
    };

    Ok(quote! {
        #[doc = #ref_doc]
        #vis struct #ref_ident #ref_generics #ref_where_clause {
            #(#field_vis #field_idents: ::core::option::Option<<#types as #bound>::Ref<#lifetime>>,)*
            #marker
        }

        impl #debug_impl_generics ::core::fmt::Debug for #ref_ident #ref_type_generics #debug_where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_struct(::core::stringify!(#ref_ident))
                    #(.field(#field_names, &self.#field_idents))*
                    .finish()
            }
        }

        impl #clone_impl_generics ::core::clone::Clone for #ref_ident #ref_type_generics #clone_where_clause {
            fn clone(&self) -> Self {
                Self {
                    #(#field_idents: ::core::clone::Clone::clone(&self.#field_idents),)*
                    #marker_init
                }
            }
        }

        impl #copy_impl_generics ::core::marker::Copy for #ref_ident #ref_type_generics #copy_where_clause {}

        impl #eq_impl_generics ::core::cmp::PartialEq for #ref_ident #ref_type_generics #eq_where_clause {
            fn eq(&self, other: &Self) -> ::core::primitive::bool {
                #fields_eq
            }
        }

        impl #total_eq_impl_generics ::core::cmp::Eq for #ref_ident #ref_type_generics #total_eq_where_clause {}

        impl #hash_impl_generics ::core::hash::Hash for #ref_ident #ref_type_generics #hash_where_clause {
            fn hash<#hasher_type: ::core::hash::Hasher>(&self, state: &mut #hasher_type) {
                #(::core::hash::Hash::hash(&self.#field_idents, state);)*
            }
        }

        // SAFETY: validation checks the ordered child schema; reads use only those prepared children.
        unsafe impl #impl_generics #bound for #ident #type_generics #where_clause {
            const TYPE_ID: ::duckdb_neo::logical_type::LogicalTypeID =
                ::duckdb_neo::logical_type::LogicalTypeID::Struct;
            type Internal<#lifetime> = ();
            type Ref<#lifetime> = #ref_ident #ref_type_generics where Self: #lifetime;

            fn validate(
                other: &::duckdb_neo::logical_type::LogicalType,
                children: ::core::option::Option<&::duckdb_neo::data_chunk::VectorCollection>,
            ) -> ::duckdb_neo::Result<::core::primitive::bool> {
                if !::duckdb_neo::__derive::validate_layout(other, children, #count)? {
                    return ::core::result::Result::Ok(false);
                }
                #(
                    if !::duckdb_neo::__derive::validate_field::<#types>(other, children, #indexes, #field_names)? {
                        return ::core::result::Result::Ok(false);
                    }
                )*
                ::core::result::Result::Ok(true)
            }

            unsafe fn get<#lifetime, #reader_type: #bound>(
                reader: &::duckdb_neo::vector::VectorReader<#lifetime, #reader_type>,
                _physical: ::core::primitive::usize,
                logical: ::core::primitive::usize,
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
    let types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let values = fields.fields.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        if option_inner_type(&field.ty).is_some() {
            quote!(::core::option::Option::as_ref(&value.#ident))
        } else {
            quote!(::core::option::Option::Some(&value.#ident))
        }
    });
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let ident = &input.ident;
    let lifetime = row_lifetime(input);
    let mut generics = bounded_generics(
        input,
        &types,
        &syn::parse_quote!(::duckdb_neo::vector::OwnedVectorElement),
    );
    generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!(Self: ::duckdb_neo::vector::VectorElement));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        // SAFETY: rows are written through the validated `OwnedVectorElement` implementation below.
        unsafe impl #impl_generics ::duckdb_neo::vector::WritableVectorElement for #ident #type_generics #where_clause {
            type Write<#lifetime> = Self;

            #[inline]
            unsafe fn write(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                index: ::core::primitive::usize,
                value: ::core::option::Option<Self>,
            ) -> ::duckdb_neo::Result<()> {
                // SAFETY: the caller guarantees row bounds.
                unsafe {
                    <Self as ::duckdb_neo::vector::OwnedVectorElement>::write_ref(
                        writer,
                        index,
                        ::core::option::Option::as_ref(&value),
                    )
                }
            }
        }

        // SAFETY: VectorElement::validate establishes the field layout; child writers maintain NULL validity.
        unsafe impl #impl_generics ::duckdb_neo::vector::OwnedVectorElement for #ident #type_generics #where_clause {
            #[inline]
            unsafe fn write_ref(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                index: ::core::primitive::usize,
                value: ::core::option::Option<&Self>,
            ) -> ::duckdb_neo::Result<()> {
                match value {
                    ::core::option::Option::Some(value) => {
                        // SAFETY: validation checked every child, and the caller guarantees row bounds.
                        #(unsafe { writer.write_child_ref_unchecked::<#types>(#indexes, index, #values)? };)*
                    }
                    ::core::option::Option::None => {
                        // SAFETY: a NULL struct must also null each of its validated children.
                        #(unsafe {
                            writer.write_child_ref_unchecked::<#types>(#indexes, index, ::core::option::Option::None)?
                        };)*
                    }
                }
                // SAFETY: every child of this row was written above.
                unsafe { writer.set_row_validity(index, ::core::option::Option::is_some(&value)) }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{as_vector_element, as_writable_vector_element};

    #[test]
    fn vector_derives_require_named_fields() {
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
    }

    #[test]
    fn writer_uses_self_and_accepts_required_fields() {
        let input = syn::parse_quote!(
            struct Row {
                required: String,
                optional: Option<i32>,
            }
        );
        let output = syn::parse2::<syn::File>(as_writable_vector_element(&input).unwrap()).unwrap();
        assert_eq!(output.items.len(), 2);
        let syn::Item::Impl(implementation) = &output.items[0] else {
            panic!("expected writer implementation")
        };
        let syn::ImplItem::Type(write) = &implementation.items[0] else {
            panic!("expected write input type")
        };
        assert_eq!(write.ty, syn::parse_quote!(Self));
        syn::parse2::<syn::File>(as_vector_element(&input).unwrap()).unwrap();
    }
}
