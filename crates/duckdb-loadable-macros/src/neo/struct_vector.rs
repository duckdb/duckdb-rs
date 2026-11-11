use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::struct_fields::{StructFields, bounded_generics, fresh_type_ident, option_inner_type};

pub(super) fn row_lifetime(input: &syn::DeriveInput) -> syn::Lifetime {
    let mut name = "__duckdb_row".to_owned();
    while input.generics.lifetimes().any(|param| param.lifetime.ident == name) {
        name.push('_');
    }
    syn::Lifetime::new(&format!("'{name}"), input.ident.span())
}

pub(super) fn ref_bounded_generics(
    generics: &syn::Generics,
    types: &[&syn::Type],
    lifetime: &syn::Lifetime,
    bound: TokenStream,
) -> syn::Generics {
    let mut generics = generics.clone();
    for ty in types {
        generics.make_where_clause().predicates.push(syn::parse_quote!(
            <#ty as ::duckdb_neo::vector::NativeElement>::Ref<#lifetime>: #bound
        ));
    }
    generics
}

pub(crate) fn as_native_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsNativeElement")?;
    let types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::NativeElement);
    let ident = &input.ident;
    let ref_ident = format_ident!("{ident}Ref");
    let ref_doc = format!("Borrowed, nullable fields of [`{ident}`].");
    let lifetime = row_lifetime(input);
    let vis = &input.vis;
    let field_idents = &fields.idents;
    let field_names = &fields.names;
    let rust_names = &fields.rust_names;
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
                    #(.field(#rust_names, &self.#field_idents))*
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
            type Native<#lifetime> = ();
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

            #[inline]
            unsafe fn get<#lifetime, #reader_type: #bound>(
                reader: &::duckdb_neo::vector::VectorReader<#lifetime, #reader_type>,
                _physical: ::core::primitive::usize,
                logical: ::core::primitive::usize,
            ) -> Self::Ref<#lifetime>
            where Self: #lifetime {
                #ref_ident {
                    // SAFETY: validation checked every child, and the reader prepared their child readers.
                    #(#field_idents: unsafe { reader.get_child_value_unchecked::<#types>(#indexes, logical) },)*
                    #marker_init
                }
            }
        }

        impl #impl_generics ::duckdb_neo::vector::WithReader for #ident #type_generics #where_clause {
            type Reader<#lifetime> = ::duckdb_neo::vector::VectorReader<#lifetime, Self> where Self: #lifetime;

            fn row_reader<#lifetime>(
                vector: &#lifetime ::duckdb_neo::vector::Vector<#lifetime, Self>,
            ) -> ::duckdb_neo::Result<Self::Reader<#lifetime>> {
                vector.reader()
            }
        }
    })
}

pub(crate) fn as_writable_native_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsWritableNativeElement")?;
    let types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let values = fields.fields.iter().map(|field| {
        let ident = field.ident.as_ref().unwrap();
        if option_inner_type(&field.ty).is_some() {
            quote!(::core::option::Option::as_ref(&value.#ident).map(::core::borrow::Borrow::borrow))
        } else {
            quote!(::core::option::Option::Some(::core::borrow::Borrow::borrow(&value.#ident)))
        }
    });
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let ident = &input.ident;
    let mut generics = bounded_generics(
        input,
        &types,
        &syn::parse_quote!(::duckdb_neo::vector::WritableNativeElement),
    );
    for ty in &types {
        generics.make_where_clause().predicates.push(syn::parse_quote!(
            #ty: ::core::borrow::Borrow<<#ty as ::duckdb_neo::vector::WritableNativeElement>::Write>
        ));
    }
    generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!(Self: ::duckdb_neo::vector::NativeElement));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        // SAFETY: NativeElement::validate establishes the field layout; child writers maintain NULL validity.
        unsafe impl #impl_generics ::duckdb_neo::vector::WritableNativeElement for #ident #type_generics #where_clause {
            type Write = Self;

            #[inline]
            unsafe fn write(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                index: ::core::primitive::usize,
                value: ::core::option::Option<&Self>,
            ) -> ::duckdb_neo::Result<()> {
                match value {
                    ::core::option::Option::Some(value) => {
                        let mut written: ::duckdb_neo::Result<()> = ::core::result::Result::Ok(());
                        #(if written.is_ok() {
                            // SAFETY: validation checked every child, and the caller guarantees row bounds.
                            written = unsafe { writer.write_child_unchecked::<#types>(#indexes, index, #values) };
                        })*
                        if let ::core::result::Result::Err(error) = written {
                            // Earlier fields were written, so NULL the row to keep its children consistent.
                            // SAFETY: the caller guarantees row bounds.
                            unsafe { writer.set_unchecked(index, ::core::option::Option::None)? };
                            return ::core::result::Result::Err(error);
                        }
                    }
                    ::core::option::Option::None => {
                        // SAFETY: a NULL struct must also null each of its validated children.
                        #(unsafe {
                            writer.write_child_unchecked::<#types>(#indexes, index, ::core::option::Option::None)?
                        };)*
                    }
                }
                ::core::result::Result::Ok(())
            }
        }

        // SAFETY: the vector writer only writes values through the validated layout.
        unsafe impl #impl_generics ::duckdb_neo::vector::WithWriter for #ident #type_generics #where_clause {
            type Writer<'__duckdb_writer> = ::duckdb_neo::vector::VectorWriter<'__duckdb_writer, Self>
            where
                Self: '__duckdb_writer;

            unsafe fn row_writer<'__duckdb_writer>(
                vector: ::core::ptr::NonNull<::duckdb_neo::vector::Vector<'__duckdb_writer, Self>>,
            ) -> ::duckdb_neo::Result<Self::Writer<'__duckdb_writer>> {
                // SAFETY: the caller upholds the borrow contract.
                unsafe { ::duckdb_neo::__derive::vector_writer(vector) }
            }
        }

        // SAFETY: every STRUCT field has one row per parent row.
        unsafe impl #impl_generics ::duckdb_neo::vector::HasChildren for #ident #type_generics #where_clause {
            fn child_len(
                writer: &::duckdb_neo::vector::VectorWriter<'_, Self>,
            ) -> ::duckdb_neo::Result<::core::primitive::usize> {
                ::core::result::Result::Ok(writer.len())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{as_native_element, as_writable_native_element};

    #[test]
    fn generated_gets_are_inline() {
        let structure = syn::parse_quote!(
            struct Row {
                value: i32,
            }
        );
        let union = syn::parse_quote!(
            enum Input {
                Num(i32),
                Text(String),
            }
        );
        for (tokens, expected) in [
            (as_native_element(&structure).unwrap(), 1),
            (super::super::union::as_duckdb_union(&union).unwrap(), 2),
        ] {
            let output = syn::parse2::<syn::File>(tokens).unwrap();
            let getters = output
                .items
                .iter()
                .filter_map(|item| match item {
                    syn::Item::Impl(item) => Some(&item.items),
                    _ => None,
                })
                .flatten()
                .filter_map(|item| match item {
                    syn::ImplItem::Fn(method) if method.sig.ident == "get" => Some(method),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(getters.len(), expected);
            for getter in getters {
                assert!(getter.attrs.iter().any(|attr| attr.meta == syn::parse_quote!(inline)));
            }
        }
    }

    #[test]
    fn vector_derives_require_named_fields() {
        type Derive = fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>;
        let derives: [(&str, Derive); 2] = [
            ("AsNativeElement", as_native_element),
            ("AsWritableNativeElement", as_writable_native_element),
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
        let output = syn::parse2::<syn::File>(as_writable_native_element(&input).unwrap()).unwrap();
        assert_eq!(output.items.len(), 3);
        let syn::Item::Impl(implementation) = &output.items[0] else {
            panic!("expected writer implementation")
        };
        let syn::ImplItem::Type(write) = &implementation.items[0] else {
            panic!("expected write input type")
        };
        assert_eq!(write.ty, syn::parse_quote!(Self));
        syn::parse2::<syn::File>(as_native_element(&input).unwrap()).unwrap();
    }
}
