//! `AsDuckDBUnion`: maps an enum of single-field tuple variants to a DuckDB `UNION`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;

use super::struct_fields::{
    bounded_generics, fresh_type_ident, option_inner_type, reject_container_attributes, sql_name,
};
use super::struct_vector::{ref_bounded_generics, row_lifetime};

const DERIVE: &str = "AsDuckDBUnion";
const MAX_MEMBERS: usize = 255;

struct UnionVariants<'a> {
    idents: Vec<&'a syn::Ident>,
    types: Vec<&'a syn::Type>,
    names: Vec<String>,
    rust_names: Vec<String>,
}

impl<'a> UnionVariants<'a> {
    fn new(input: &'a syn::DeriveInput) -> syn::Result<Self> {
        let syn::Data::Enum(data) = &input.data else {
            return Err(syn::Error::new_spanned(input, format!("{DERIVE} requires an enum")));
        };
        if data.variants.is_empty() {
            return Err(syn::Error::new_spanned(
                input,
                format!("{DERIVE} requires at least one variant"),
            ));
        }
        if let Some(variant) = data.variants.iter().nth(MAX_MEMBERS) {
            return Err(syn::Error::new_spanned(
                variant,
                format!("{DERIVE} supports at most {MAX_MEMBERS} variants"),
            ));
        }
        reject_container_attributes(input, DERIVE)?;
        let mut variants = Self {
            idents: Vec::new(),
            types: Vec::new(),
            names: Vec::new(),
            rust_names: Vec::new(),
        };
        for variant in &data.variants {
            let rust_name = variant.ident.unraw().to_string();
            let ty = match &variant.fields {
                syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => &fields.unnamed[0].ty,
                _ => {
                    return Err(syn::Error::new_spanned(
                        variant,
                        format!("{DERIVE} variant '{rust_name}' must have exactly one unnamed field"),
                    ));
                }
            };
            let name = sql_name(&variant.attrs, &rust_name, DERIVE)?;
            if let Some(previous) = variants
                .names
                .iter()
                .find(|previous| previous.eq_ignore_ascii_case(&name))
            {
                return Err(syn::Error::new_spanned(
                    &variant.ident,
                    format!(
                        "{DERIVE} variant '{name}' conflicts with variant '{previous}': UNION member names are case-insensitive"
                    ),
                ));
            }
            variants.idents.push(&variant.ident);
            variants.types.push(ty);
            variants.names.push(name);
            variants.rust_names.push(rust_name);
        }
        Ok(variants)
    }

    fn inner_types(&self) -> Vec<&'a syn::Type> {
        self.types
            .iter()
            .map(|ty| option_inner_type(ty).unwrap_or(ty))
            .collect()
    }
}

pub(crate) fn as_duckdb_union(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let variants = UnionVariants::new(input)?;
    let logical_type = logical_type(input, &variants);
    let to_value = to_value(input, &variants);
    let from_value = from_value(input, &variants);
    let vector_element = vector_element(input, &variants);
    let writable_vector_element = writable_vector_element(input, &variants);
    Ok(quote! {
        #logical_type
        #to_value
        #from_value
        #vector_element
        #writable_vector_element
    })
}

fn logical_type(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let names = &variants.names;
    let types = &variants.types;
    let generics = bounded_generics(input, types, &syn::parse_quote!(::duckdb_neo::DuckDBType));
    let ident = &input.ident;
    let link_type = fresh_type_ident(input, "__DuckDBLink");
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::duckdb_neo::DuckDBType for #ident #type_generics #where_clause {
            fn logical_type<#link_type: ::duckdb_neo::connection::FFILink + ?::core::marker::Sized>(
                link: &#link_type,
            ) -> ::duckdb_neo::Result<::duckdb_neo::logical_type::LogicalType> {
                link.logical_type_create(
                    "UNION",
                    ::duckdb_neo::Parameters::named(&[
                        #((#names, &<#types as ::duckdb_neo::DuckDBType>::logical_type(link)?)),*
                    ]),
                )
            }
        }
    }
}

fn to_value(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let idents = &variants.idents;
    let indexes = 0..variants.idents.len();
    let mut generics = bounded_generics(input, &variants.types, &syn::parse_quote!(::duckdb_neo::ToValue));
    for ty in &variants.types {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: ::duckdb_neo::DuckDBType));
    }
    let ident = &input.ident;
    let link_type = fresh_type_ident(input, "__DuckDBLink");
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::duckdb_neo::ToValue for #ident #type_generics #where_clause {
            fn value<#link_type: ::duckdb_neo::connection::FFILink + ?::core::marker::Sized>(
                &self,
                link: &#link_type,
            ) -> ::duckdb_neo::Result<::duckdb_neo::value::Value> {
                let (tag, member) = match self {
                    #(Self::#idents(member) => (#indexes, ::duckdb_neo::ToValue::value(member, link)?),)*
                };
                ::duckdb_neo::__derive::union_value(
                    link,
                    &<Self as ::duckdb_neo::DuckDBType>::logical_type(link)?,
                    tag,
                    member,
                )
            }
        }
    }
}

fn from_value(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let decoded_types = variants.inner_types();
    let generics = bounded_generics(input, &decoded_types, &syn::parse_quote!(::duckdb_neo::FromValue));
    let arms = variants
        .idents
        .iter()
        .zip(&variants.types)
        .zip(&variants.names)
        .enumerate()
        .map(|(index, ((ident, ty), name))| {
            let optional_type = option_inner_type(ty);
            let decoded_type = optional_type.unwrap_or(ty);
            let decode = quote!(<#decoded_type as ::duckdb_neo::FromValue>::from_value(&member)?);
            let decode = if optional_type.is_some() {
                decode
            } else {
                quote!(::duckdb_neo::__derive::required_member(#decode, #name)?)
            };
            quote!(#index => Self::#ident(#decode))
        });
    let names = &variants.names;
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::duckdb_neo::FromValue for #ident #type_generics #where_clause {
            fn _get_inner(value: &::duckdb_neo::value::Value) -> ::duckdb_neo::Result<Self> {
                let (tag, member) = ::duckdb_neo::__derive::union_member(value, &[#(#names),*])?;
                ::core::result::Result::Ok(match tag {
                    #(#arms,)*
                    _ => ::core::unreachable!("union_member returns the index of a declared member"),
                })
            }
        }
    }
}

fn vector_element(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let types = variants.inner_types();
    let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::VectorElement);
    let ident = &input.ident;
    let ref_ident = format_ident!("{ident}Ref");
    let ref_doc = format!("Borrowed, nullable members of [`{ident}`].");
    let lifetime = row_lifetime(input);
    let vis = &input.vis;
    let idents = &variants.idents;
    let names = &variants.names;
    let rust_names = &variants.rust_names;
    let count = types.len();
    let indexes = 0..count;
    let tags = (0..count).map(|tag| tag as u8).collect::<Vec<_>>();
    let child_indexes = (1..=count).collect::<Vec<_>>();
    let reader_type = fresh_type_ident(input, "__DuckDBVector");
    let hasher_type = fresh_type_ident(input, "__DuckDBHasher");
    let read_lifetime = fresh_lifetime(input, "__duckdb_read");
    let generics = bounded_generics(input, &types, &bound);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let ref_generics = ref_generics(&generics, &types, &lifetime);
    let (ref_impl_generics, ref_type_generics, ref_where_clause) = ref_generics.split_for_impl();
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

    quote! {
        #[doc = #ref_doc]
        #[allow(non_camel_case_types)]
        #vis enum #ref_ident #ref_generics #ref_where_clause {
            #(#idents(::core::option::Option<<#types as #bound>::Ref<#lifetime>>),)*
        }

        impl #debug_impl_generics ::core::fmt::Debug for #ref_ident #ref_type_generics #debug_where_clause {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                match self {
                    #(Self::#idents(member) => f.debug_tuple(#rust_names).field(member).finish(),)*
                }
            }
        }

        impl #clone_impl_generics ::core::clone::Clone for #ref_ident #ref_type_generics #clone_where_clause {
            fn clone(&self) -> Self {
                match self {
                    #(Self::#idents(member) => Self::#idents(::core::clone::Clone::clone(member)),)*
                }
            }
        }

        impl #copy_impl_generics ::core::marker::Copy for #ref_ident #ref_type_generics #copy_where_clause {}

        impl #eq_impl_generics ::core::cmp::PartialEq for #ref_ident #ref_type_generics #eq_where_clause {
            #[allow(unreachable_patterns)]
            fn eq(&self, other: &Self) -> ::core::primitive::bool {
                match (self, other) {
                    #((Self::#idents(member), Self::#idents(other)) => ::core::cmp::PartialEq::eq(member, other),)*
                    _ => false,
                }
            }
        }

        impl #total_eq_impl_generics ::core::cmp::Eq for #ref_ident #ref_type_generics #total_eq_where_clause {}

        impl #hash_impl_generics ::core::hash::Hash for #ref_ident #ref_type_generics #hash_where_clause {
            fn hash<#hasher_type: ::core::hash::Hasher>(&self, state: &mut #hasher_type) {
                match self {
                    #(Self::#idents(member) => {
                        ::core::hash::Hash::hash(&#tags, state);
                        ::core::hash::Hash::hash(member, state);
                    })*
                }
            }
        }

        // SAFETY: validation checks the tag and ordered member schema; reads use only those prepared children.
        unsafe impl #impl_generics #bound for #ident #type_generics #where_clause {
            const TYPE_ID: ::duckdb_neo::logical_type::LogicalTypeID =
                ::duckdb_neo::logical_type::LogicalTypeID::Union;
            type Internal<#lifetime> = ();
            type Ref<#lifetime> = #ref_ident #ref_type_generics where Self: #lifetime;

            fn validate(
                other: &::duckdb_neo::logical_type::LogicalType,
                children: ::core::option::Option<&::duckdb_neo::data_chunk::VectorCollection>,
            ) -> ::duckdb_neo::Result<::core::primitive::bool> {
                if !::duckdb_neo::__derive::validate_union_layout(other, children, #count)? {
                    return ::core::result::Result::Ok(false);
                }
                #(
                    if !::duckdb_neo::__derive::validate_union_member::<#types>(other, children, #indexes, #names)? {
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
            ) -> <Self as #bound>::Ref<#lifetime>
            where Self: #lifetime {
                // SAFETY: validation checked the tag child, and the reader prepared every child reader.
                let tag = unsafe { reader.get_child_value_unchecked::<::core::primitive::u8>(0, logical) };
                match tag {
                    #(::core::option::Option::Some(&#tags) => #ref_ident::#idents(
                        // SAFETY: validation checked this member's child type.
                        unsafe { reader.get_child_value_unchecked::<#types>(#child_indexes, logical) }
                    ),)*
                    tag => ::duckdb_neo::__derive::invalid_union_tag(tag, #count),
                }
            }
        }

        // SAFETY: borrowed rows share the owned union's layout, validation and reads.
        unsafe impl #ref_impl_generics #bound for #ref_ident #ref_type_generics #ref_where_clause {
            const TYPE_ID: ::duckdb_neo::logical_type::LogicalTypeID =
                ::duckdb_neo::logical_type::LogicalTypeID::Union;
            type Internal<#read_lifetime> = ();
            type Ref<#read_lifetime> = <#ident #type_generics as #bound>::Ref<#read_lifetime> where Self: #read_lifetime;

            fn validate(
                other: &::duckdb_neo::logical_type::LogicalType,
                children: ::core::option::Option<&::duckdb_neo::data_chunk::VectorCollection>,
            ) -> ::duckdb_neo::Result<::core::primitive::bool> {
                <#ident #type_generics as #bound>::validate(other, children)
            }

            #[inline]
            unsafe fn get<#read_lifetime, #reader_type: #bound>(
                reader: &::duckdb_neo::vector::VectorReader<#read_lifetime, #reader_type>,
                physical: ::core::primitive::usize,
                logical: ::core::primitive::usize,
            ) -> <Self as #bound>::Ref<#read_lifetime>
            where Self: #read_lifetime {
                // SAFETY: the caller upholds the owned union's contract, which has the same layout.
                unsafe { <#ident #type_generics as #bound>::get(reader, physical, logical) }
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
    }
}

fn ref_generics(generics: &syn::Generics, types: &[&syn::Type], lifetime: &syn::Lifetime) -> syn::Generics {
    let mut generics = generics.clone();
    generics.params.insert(0, syn::parse_quote!(#lifetime));
    for ty in types {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #lifetime));
    }
    generics
}

fn fresh_lifetime(input: &syn::DeriveInput, name: &str) -> syn::Lifetime {
    let mut name = name.to_owned();
    while input.generics.lifetimes().any(|param| param.lifetime.ident == name) {
        name.push('_');
    }
    syn::Lifetime::new(&format!("'{name}"), input.ident.span())
}

fn writable_vector_element(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let types = variants.inner_types();
    let ident = &input.ident;
    let ref_ident = format_ident!("{ident}Ref");
    let lifetime = row_lifetime(input);
    let element: syn::Path = syn::parse_quote!(::duckdb_neo::vector::VectorElement);
    let writable: syn::Path = syn::parse_quote!(::duckdb_neo::vector::WritableVectorElement);

    let owned_members = variants.idents.iter().zip(&variants.types).map(|(ident, ty)| {
        if option_inner_type(ty).is_some() {
            quote!(Self::#ident(member) => ::core::option::Option::as_ref(member).map(::core::borrow::Borrow::borrow))
        } else {
            quote!(Self::#ident(member) => ::core::option::Option::Some(::core::borrow::Borrow::borrow(member)))
        }
    });
    let mut owned_generics = bounded_generics(input, &types, &writable);
    for ty in &types {
        owned_generics.make_where_clause().predicates.push(syn::parse_quote!(
            #ty: ::core::borrow::Borrow<<#ty as #writable>::Write>
        ));
    }
    owned_generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!(Self: #element));
    let (_, type_generics, _) = input.generics.split_for_impl();
    let owned = writable_impl(
        &owned_generics,
        quote!(#ident #type_generics),
        variants,
        &types,
        owned_members,
    );

    let ref_members = variants.idents.iter().map(|ident| {
        quote!(Self::#ident(member) => ::core::option::Option::as_ref(member).map(::core::borrow::Borrow::borrow))
    });
    let mut borrowed_generics = ref_generics(&bounded_generics(input, &types, &element), &types, &lifetime);
    for ty in &types {
        borrowed_generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #writable));
        borrowed_generics.make_where_clause().predicates.push(syn::parse_quote!(
            <#ty as #element>::Ref<#lifetime>: ::core::borrow::Borrow<<#ty as #writable>::Write>
        ));
    }
    let (_, ref_type_generics, _) = borrowed_generics.split_for_impl();
    let borrowed = writable_impl(
        &borrowed_generics,
        quote!(#ref_ident #ref_type_generics),
        variants,
        &types,
        ref_members,
    );

    quote! {
        #owned
        #borrowed
    }
}

fn writable_impl(
    generics: &syn::Generics,
    target: TokenStream,
    variants: &UnionVariants<'_>,
    types: &[&syn::Type],
    members: impl Iterator<Item = TokenStream>,
) -> TokenStream {
    let idents = &variants.idents;
    let tags = (0..types.len()).map(|tag| tag as u8);
    let child_indexes = (1..=types.len()).collect::<Vec<_>>();
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    quote! {
        // SAFETY: VectorElement::validate establishes the tag and member layout; child writers maintain NULL validity.
        unsafe impl #impl_generics ::duckdb_neo::vector::WritableVectorElement for #target #where_clause {
            type Write = Self;

            #[inline]
            #[allow(unreachable_patterns)]
            unsafe fn write(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                index: ::core::primitive::usize,
                value: ::core::option::Option<&Self>,
            ) -> ::duckdb_neo::Result<()> {
                let ::core::option::Option::Some(value) = value else {
                    // SAFETY: a NULL union must also null its tag and each of its validated members.
                    unsafe {
                        writer.write_child_unchecked::<::core::primitive::u8>(0, index, ::core::option::Option::None)?;
                        #(writer.write_child_unchecked::<#types>(#child_indexes, index, ::core::option::Option::None)?;)*
                        return ::core::result::Result::Ok(());
                    }
                };
                let tag: ::core::primitive::u8 = match value {
                    #(Self::#idents(_) => #tags,)*
                };
                let mut write_children = || -> ::duckdb_neo::Result<()> {
                    // SAFETY: validation checked the tag child, and the caller guarantees row bounds.
                    unsafe {
                        writer.write_child_unchecked::<::core::primitive::u8>(0, index, ::core::option::Option::Some(&tag))?
                    };
                    #(
                        // Every member other than the active one is NULL.
                        let member = match value {
                            #members,
                            _ => ::core::option::Option::None,
                        };
                        // SAFETY: validation checked every member, and the caller guarantees row bounds.
                        unsafe { writer.write_child_unchecked::<#types>(#child_indexes, index, member)? };
                    )*
                    ::core::result::Result::Ok(())
                };
                if let ::core::result::Result::Err(error) = write_children() {
                    // The tag and earlier members were written, so NULL the row to keep its children consistent.
                    // SAFETY: the caller guarantees row bounds.
                    unsafe { writer.set_unchecked(index, ::core::option::Option::None)? };
                    return ::core::result::Result::Err(error);
                }
                ::core::result::Result::Ok(())
            }
        }

        // SAFETY: the vector writer only writes values through the validated layout.
        unsafe impl #impl_generics ::duckdb_neo::vector::WithWriter for #target #where_clause {
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

        // SAFETY: the tag and every UNION member have one row per parent row.
        unsafe impl #impl_generics ::duckdb_neo::vector::HasChildren for #target #where_clause {
            fn child_len(
                writer: &::duckdb_neo::vector::VectorWriter<'_, Self>,
            ) -> ::duckdb_neo::Result<::core::primitive::usize> {
                ::core::result::Result::Ok(writer.len())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::as_duckdb_union;

    fn error(input: syn::DeriveInput) -> String {
        as_duckdb_union(&input).unwrap_err().to_string()
    }

    #[test]
    fn union_derive_emits_all_implementations() {
        let input = syn::parse_quote!(
            enum Input {
                Num(i16),
                str(Option<String>),
            }
        );
        let output = syn::parse2::<syn::File>(as_duckdb_union(&input).unwrap()).unwrap();
        let items = output
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Impl(item) => item
                    .trait_
                    .as_ref()
                    .map(|(_, path, _)| path.segments.last().unwrap().ident.to_string()),
                syn::Item::Enum(item) => Some(format!("enum {}", item.ident)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            items,
            [
                "DuckDBType",
                "ToValue",
                "FromValue",
                "enum InputRef",
                "Debug",
                "Clone",
                "Copy",
                "PartialEq",
                "Eq",
                "Hash",
                "VectorElement",
                "VectorElement",
                "WithReader",
                "WritableVectorElement",
                "WithWriter",
                "HasChildren",
                "WritableVectorElement",
                "WithWriter",
                "HasChildren"
            ]
        );
    }

    #[test]
    fn union_derive_rejects_invalid_input() {
        assert_eq!(
            error(syn::parse_quote!(
                struct Record {
                    value: i32,
                }
            )),
            "AsDuckDBUnion requires an enum"
        );
        assert_eq!(
            error(syn::parse_quote!(
                enum Empty {}
            )),
            "AsDuckDBUnion requires at least one variant"
        );
        for (input, name) in [
            (
                syn::parse_quote!(
                    enum Input {
                        Unit,
                    }
                ),
                "Unit",
            ),
            (
                syn::parse_quote!(
                    enum Input {
                        Pair(i32, i32),
                    }
                ),
                "Pair",
            ),
            (
                syn::parse_quote!(
                    enum Input {
                        Named { value: i32 },
                    }
                ),
                "Named",
            ),
            (
                syn::parse_quote!(
                    enum Input {
                        Empty(),
                    }
                ),
                "Empty",
            ),
            (
                syn::parse_quote!(
                    enum Input {
                        r#type,
                    }
                ),
                "type",
            ),
        ] {
            assert_eq!(
                error(input),
                format!("AsDuckDBUnion variant '{name}' must have exactly one unnamed field")
            );
        }
    }

    #[test]
    fn union_derive_rejects_case_insensitive_duplicate_variants() {
        for (input, first, second) in [
            (
                syn::parse_quote!(
                    enum Input {
                        num(i32),
                        Num(i64),
                    }
                ),
                "num",
                "Num",
            ),
            (
                syn::parse_quote!(
                    enum Input {
                        r#type(i32),
                        Type(i32),
                    }
                ),
                "type",
                "Type",
            ),
        ] {
            assert_eq!(
                error(input),
                format!(
                    "AsDuckDBUnion variant '{second}' conflicts with variant '{first}': UNION member names are case-insensitive"
                )
            );
        }
    }

    #[test]
    fn union_derive_renames_variants() {
        let input = syn::parse_quote!(
            enum Input {
                #[duckdb(rename = "num")]
                Num(i16),
                Text(String),
            }
        );
        let variants = super::UnionVariants::new(&input).unwrap();
        assert_eq!(variants.names, ["num", "Text"]);
        assert_eq!(variants.rust_names, ["Num", "Text"]);
        assert_eq!(
            error(syn::parse_quote!(
                enum Input {
                    Num(i16),
                    #[duckdb(rename = "NUM")]
                    Text(String),
                }
            )),
            "AsDuckDBUnion variant 'NUM' conflicts with variant 'Num': UNION member names are case-insensitive"
        );
        assert_eq!(
            error(syn::parse_quote!(
                enum Input {
                    #[duckdb(rename = "")]
                    Num(i16),
                }
            )),
            "AsDuckDBUnion names cannot be empty"
        );
        assert_eq!(
            error(syn::parse_quote!(
                #[duckdb(rename = "x")]
                enum Input {
                    Num(i16),
                }
            )),
            "AsDuckDBUnion supports #[duckdb(...)] only on fields and variants"
        );
    }

    #[test]
    fn union_derive_limits_variant_count() {
        let variants = |count: usize| {
            (0..count)
                .map(|index| quote::format_ident!("V{index}"))
                .collect::<Vec<_>>()
        };
        let max = variants(255);
        assert!(as_duckdb_union(&syn::parse_quote!(enum Wide { #(#max(i32)),* })).is_ok());
        let over = variants(256);
        assert_eq!(
            error(syn::parse_quote!(enum Wide { #(#over(i32)),* })),
            "AsDuckDBUnion supports at most 255 variants"
        );
    }
}
