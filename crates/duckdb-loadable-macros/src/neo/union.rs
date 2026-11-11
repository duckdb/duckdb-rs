//! `AsDuckDBUnion`: maps an enum of single-field tuple variants to a DuckDB `UNION`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;

use super::struct_fields::{
    ContainerAttributes, bounded_generics, container_attributes, fresh_type_ident, option_inner_type, sql_name,
};
use super::struct_vector::{fresh_lifetime, item_params, native_impls, with_lifetime};

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

/// The most members the generated readers and writers support, the arity of the member tuples.
const MAX_ROW_MEMBERS: usize = 16;

/// Method names of the generated writer that variant methods must not shadow.
const WRITER_METHODS: [&str; 7] = [
    "len",
    "is_empty",
    "set",
    "set_null",
    "set_option",
    "resize_rows",
    "refresh",
];

pub(crate) fn as_duckdb_union(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let variants = UnionVariants::new(input)?;
    let attributes = container_attributes(input, DERIVE)?;
    if (attributes.reader || attributes.writer) && variants.types.len() > MAX_ROW_MEMBERS {
        return Err(syn::Error::new_spanned(
            input,
            format!("{DERIVE} readers and writers support at most {MAX_ROW_MEMBERS} variants"),
        ));
    }
    let logical_type = logical_type(input, &variants);
    let to_value = to_value(input, &variants);
    let from_value = from_value(input, &variants);
    let native = union_native(input, &variants);
    let rows = union_rows(input, &variants, attributes)?;
    Ok(quote! {
        #logical_type
        #to_value
        #from_value
        #native
        #rows
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

fn union_native(input: &syn::DeriveInput, variants: &UnionVariants<'_>) -> TokenStream {
    let types = variants.inner_types();
    let names = &variants.names;
    let count = types.len();
    let indexes = 0..count;
    native_impls(
        input,
        &types,
        quote!(Union),
        quote! {
            if !::duckdb_neo::__derive::validate_union_layout(other, children, #count)? {
                return ::core::result::Result::Ok(false);
            }
            #(
                if !::duckdb_neo::__derive::validate_union_member::<#types>(other, children, #indexes, #names)? {
                    return ::core::result::Result::Ok(false);
                }
            )*
            ::core::result::Result::Ok(true)
        },
    )
}

fn snake_case(name: &str) -> String {
    let mut snake = String::new();
    for (index, c) in name.char_indices() {
        if c.is_uppercase() {
            if index > 0 && !snake.ends_with('_') {
                snake.push('_');
            }
            snake.extend(c.to_lowercase());
        } else {
            snake.push(c);
        }
    }
    snake
}

fn union_rows(
    input: &syn::DeriveInput,
    variants: &UnionVariants<'_>,
    attributes: ContainerAttributes,
) -> syn::Result<TokenStream> {
    let types = variants.inner_types();
    let ident = &input.ident;
    let vis = &input.vis;
    let idents = &variants.idents;
    let count = types.len();
    let indexes = (0..count).collect::<Vec<_>>();
    let tags = (0..count).map(|tag| tag as u8).collect::<Vec<_>>();
    let (_, type_generics, _) = input.generics.split_for_impl();
    let members = quote!((#(#types,)*));
    let mut output = TokenStream::new();

    if attributes.reader {
        let reader_ident = format_ident!("{ident}Reader");
        let ref_ident = format_ident!("{ident}Ref");
        let reader_doc = format!("Reads [`{ident}`] rows, with every member reader prepared once.");
        let ref_doc = format!("One [`{ident}`] row read by [`{reader_ident}`]; each member value is nullable.");
        let lifetime = fresh_lifetime(input, "__duckdb_reader");
        let item_lifetime = fresh_lifetime(input, "__duckdb_item");
        let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::WithReader);
        let generics = with_lifetime(&input.generics, &types, &lifetime, &bound);
        let (impl_generics, reader_generics, where_clause) = generics.split_for_impl();
        let element_generics = bounded_generics(input, &types, &bound);
        let (element_impl_generics, _, element_where_clause) = element_generics.split_for_impl();
        let params = item_params(input, count);
        output.extend(quote! {
            #[doc = #ref_doc]
            #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::marker::Copy, ::core::cmp::PartialEq, ::core::cmp::Eq, ::core::hash::Hash)]
            #[allow(non_camel_case_types)]
            #vis enum #ref_ident<#(#params),*> {
                #(#idents(::core::option::Option<#params>),)*
            }

            #[doc = #reader_doc]
            #vis struct #reader_ident #generics #where_clause {
                inner: ::duckdb_neo::vector::UnionReader<#lifetime, #members>,
            }

            impl #impl_generics #reader_ident #reader_generics #where_clause {
                /// Prepare a reader for `vector`.
                pub fn new(
                    vector: &#lifetime ::duckdb_neo::vector::Vector<#lifetime, #ident #type_generics>,
                ) -> ::duckdb_neo::Result<Self> {
                    ::core::result::Result::Ok(Self { inner: vector.union_reader()? })
                }

                /// Borrow the untyped union reader, for columnar reads of the tags and members.
                pub fn members(&self) -> &::duckdb_neo::vector::UnionReader<#lifetime, #members> {
                    &self.inner
                }
            }

            impl #impl_generics ::core::fmt::Debug for #reader_ident #reader_generics #where_clause {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct(::core::stringify!(#reader_ident)).field("len", &self.inner.len()).finish()
                }
            }

            impl #impl_generics ::duckdb_neo::vector::RowReader for #reader_ident #reader_generics #where_clause {
                type Item<#item_lifetime> = #ref_ident<
                    #(<<#types as #bound>::Reader<#lifetime> as ::duckdb_neo::vector::RowReader>::Item<#item_lifetime>),*
                > where Self: #item_lifetime;

                fn len(&self) -> ::core::primitive::usize {
                    self.inner.len()
                }

                fn is_null(&self, row: ::core::primitive::usize) -> ::duckdb_neo::Result<::core::primitive::bool> {
                    ::core::result::Result::Ok(self.inner.tag(row)?.is_none())
                }

                #[inline]
                #[allow(unreachable_patterns)]
                unsafe fn get_unchecked(&self, row: ::core::primitive::usize) -> ::core::option::Option<Self::Item<'_>> {
                    // SAFETY: the caller guarantees bounds; members share the union's rows.
                    unsafe {
                        ::core::option::Option::Some(match self.inner.tag_unchecked(row)? {
                            #(#tags => #ref_ident::#idents(
                                ::duckdb_neo::__derive::get_unchecked(self.inner.member::<#indexes>(), row)
                            ),)*
                            tag => ::duckdb_neo::__derive::invalid_union_tag(::core::option::Option::Some(&tag), #count),
                        })
                    }
                }
            }

            impl #element_impl_generics #bound for #ident #type_generics #element_where_clause {
                type Reader<#lifetime> = #reader_ident #reader_generics where Self: #lifetime;

                fn row_reader<#lifetime>(
                    vector: &#lifetime ::duckdb_neo::vector::Vector<#lifetime, Self>,
                ) -> ::duckdb_neo::Result<Self::Reader<#lifetime>> {
                    #reader_ident::new(vector)
                }
            }
        });
    }

    if attributes.writer {
        let writer_ident = format_ident!("{ident}Writer");
        let writer_doc = format!(
            "Writes [`{ident}`] rows, with every member writer prepared once; each variant's method makes it a row's active member."
        );
        let lifetime = fresh_lifetime(input, "__duckdb_writer");
        let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::WithWriter);
        let row_writer: syn::Path = syn::parse_quote!(::duckdb_neo::vector::RowWriter);
        let generics = with_lifetime(&input.generics, &types, &lifetime, &bound);
        let (impl_generics, writer_generics, where_clause) = generics.split_for_impl();
        let mut row_generics = generics.clone();
        for ty in &types {
            row_generics.make_where_clause().predicates.push(syn::parse_quote!(
                #ty: ::core::borrow::Borrow<<<#ty as #bound>::Writer<#lifetime> as #row_writer>::Value>
            ));
        }
        let (row_impl_generics, _, row_where_clause) = row_generics.split_for_impl();
        let mut element_generics = bounded_generics(input, &types, &bound);
        for ty in &types {
            element_generics.make_where_clause().predicates.push(syn::parse_quote!(
                #ty: for<#lifetime> ::core::borrow::Borrow<<<#ty as #bound>::Writer<#lifetime> as #row_writer>::Value>
            ));
        }
        let (element_impl_generics, _, element_where_clause) = element_generics.split_for_impl();
        let mut methods = Vec::new();
        for (index, (variant, ty)) in variants.idents.iter().zip(&types).enumerate() {
            let name = snake_case(&variants.rust_names[index]);
            if WRITER_METHODS.contains(&name.as_str()) {
                return Err(syn::Error::new_spanned(
                    variant,
                    format!("{DERIVE} writer method `{name}` of variant '{variant}' collides with a writer method"),
                ));
            }
            let method = syn::Ident::new_raw(&name, variant.span());
            let doc =
                format!("Make `{variant}` a row's active member and borrow its writer, to write the member's row.");
            methods.push(quote! {
                #[doc = #doc]
                pub fn #method(
                    &mut self,
                    row: ::core::primitive::usize,
                ) -> ::duckdb_neo::Result<&mut <#ty as #bound>::Writer<#lifetime>> {
                    self.inner.member::<#index>(row)
                }
            });
        }
        let writes = variants.idents.iter().zip(&variants.types).zip(&indexes).map(|((variant, ty), index)| {
            if option_inner_type(ty).is_some() {
                quote!(Self::Value::#variant(member) => self.inner.set_option::<#index>(
                    row,
                    ::core::option::Option::as_ref(member).map(::core::borrow::Borrow::borrow),
                ))
            } else {
                quote!(Self::Value::#variant(member) => self.inner.set::<#index>(row, ::core::borrow::Borrow::borrow(member)))
            }
        });
        output.extend(quote! {
            #[doc = #writer_doc]
            #vis struct #writer_ident #generics #where_clause {
                inner: ::duckdb_neo::vector::UnionWriter<#lifetime, #members>,
            }

            impl #impl_generics #writer_ident #writer_generics #where_clause {
                #(#methods)*

                /// Borrow the untyped union writer, for columnar writes of the members.
                pub fn members(&mut self) -> &mut ::duckdb_neo::vector::UnionWriter<#lifetime, #members> {
                    &mut self.inner
                }
            }

            impl #impl_generics ::core::fmt::Debug for #writer_ident #writer_generics #where_clause {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct(::core::stringify!(#writer_ident)).field("len", &self.inner.len()).finish()
                }
            }

            impl #row_impl_generics #row_writer for #writer_ident #writer_generics #row_where_clause {
                type Value = #ident #type_generics;

                fn len(&self) -> ::core::primitive::usize {
                    self.inner.len()
                }

                #[inline]
                fn set(&mut self, row: ::core::primitive::usize, value: &Self::Value) -> ::duckdb_neo::Result<()> {
                    match value {
                        #(#writes,)*
                    }
                }

                fn set_null(&mut self, row: ::core::primitive::usize) -> ::duckdb_neo::Result<()> {
                    self.inner.set_null(row)
                }

                fn resize_rows(&mut self, len: ::core::primitive::usize) -> ::duckdb_neo::Result<()> {
                    self.inner.resize_rows(len)
                }

                fn refresh(&mut self) -> ::duckdb_neo::Result<()> {
                    self.inner.refresh()
                }
            }

            // SAFETY: the writer keeps inactive members NULL, and NULL rows null every member.
            unsafe impl #element_impl_generics #bound for #ident #type_generics #element_where_clause {
                type Writer<#lifetime> = #writer_ident #writer_generics where Self: #lifetime;

                unsafe fn row_writer<#lifetime>(
                    vector: ::core::ptr::NonNull<::duckdb_neo::vector::Vector<#lifetime, Self>>,
                ) -> ::duckdb_neo::Result<Self::Writer<#lifetime>> {
                    // SAFETY: the caller upholds the borrow contract.
                    ::core::result::Result::Ok(#writer_ident {
                        inner: unsafe { ::duckdb_neo::vector::UnionWriter::from_raw(vector.cast())? },
                    })
                }
            }
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::as_duckdb_union;

    fn error(input: syn::DeriveInput) -> String {
        as_duckdb_union(&input).unwrap_err().to_string()
    }

    fn items(input: syn::DeriveInput) -> Vec<String> {
        syn::parse2::<syn::File>(as_duckdb_union(&input).unwrap())
            .unwrap()
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Impl(item) => {
                    let name = match &item.trait_ {
                        Some((_, path, _)) => path.segments.last().unwrap().ident.to_string(),
                        None => "impl".to_owned(),
                    };
                    let syn::Type::Path(target) = &*item.self_ty else {
                        return None;
                    };
                    Some(format!("{name} for {}", target.path.segments.last().unwrap().ident))
                }
                syn::Item::Enum(item) => Some(format!("enum {}", item.ident)),
                syn::Item::Struct(item) => Some(format!("struct {}", item.ident)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn union_derive_emits_all_implementations() {
        assert_eq!(
            items(syn::parse_quote!(
                enum Input {
                    Num(i16),
                    str(Option<String>),
                }
            )),
            [
                "DuckDBType for Input",
                "ToValue for Input",
                "FromValue for Input",
                "NativeElement for Input",
                "ReadableNativeElement for Input",
                "WritableNativeElement for Input"
            ]
        );
        assert_eq!(
            items(syn::parse_quote!(
                #[duckdb(reader, writer)]
                enum Input {
                    Num(i16),
                    str(Option<String>),
                }
            ))[6..],
            [
                "enum InputRef",
                "struct InputReader",
                "impl for InputReader",
                "Debug for InputReader",
                "RowReader for InputReader",
                "WithReader for Input",
                "struct InputWriter",
                "impl for InputWriter",
                "Debug for InputWriter",
                "RowWriter for InputWriter",
                "WithWriter for Input"
            ]
        );
    }

    #[test]
    fn union_writer_methods_must_not_collide() {
        assert_eq!(
            error(syn::parse_quote!(
                #[duckdb(writer)]
                enum Input {
                    Num(i16),
                    SetNull(i32),
                }
            )),
            "AsDuckDBUnion writer method `set_null` of variant 'SetNull' collides with a writer method"
        );
        let many = (0..17).map(|index| quote::format_ident!("V{index}"));
        let input: syn::DeriveInput = syn::parse_quote!(
            #[duckdb(reader)]
            enum Input {
                #(#many(i32),)*
            }
        );
        assert_eq!(
            error(input),
            "AsDuckDBUnion readers and writers support at most 16 variants"
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
            "unsupported AsDuckDBUnion container attribute, expected `reader` or `writer`"
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
