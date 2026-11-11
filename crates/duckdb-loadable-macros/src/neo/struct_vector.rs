use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;

use super::struct_fields::{
    ContainerAttributes, StructFields, bounded_generics, container_attributes, fresh_type_ident, option_inner_type,
};

pub(super) fn row_lifetime(input: &syn::DeriveInput) -> syn::Lifetime {
    fresh_lifetime(input, "__duckdb_row")
}

pub(super) fn fresh_lifetime(input: &syn::DeriveInput, name: &str) -> syn::Lifetime {
    let mut name = name.to_owned();
    while input.generics.lifetimes().any(|param| param.lifetime.ident == name) {
        name.push('_');
    }
    syn::Lifetime::new(&format!("'{name}"), input.ident.span())
}

/// Copy `generics` with `lifetime` first, and every type in `types` bounded by `bound + lifetime`.
pub(super) fn with_lifetime(
    generics: &syn::Generics,
    types: &[&syn::Type],
    lifetime: &syn::Lifetime,
    bound: &syn::Path,
) -> syn::Generics {
    let mut generics = generics.clone();
    generics.params.insert(0, syn::parse_quote!(#lifetime));
    for ty in types {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #bound + #lifetime));
    }
    generics
}

/// Fresh type parameters for the generated `Ref` type, one per field or member.
pub(super) fn item_params(input: &syn::DeriveInput, count: usize) -> Vec<syn::Ident> {
    (0..count)
        .map(|index| fresh_type_ident(input, &format!("__DuckDBItem{index}")))
        .collect()
}

/// The native element impls of a STRUCT or UNION whose rows carry only validity.
pub(super) fn native_impls(
    input: &syn::DeriveInput,
    types: &[&syn::Type],
    type_id: TokenStream,
    validate: TokenStream,
) -> TokenStream {
    let ident = &input.ident;
    let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::NativeElement);
    let generics = bounded_generics(input, types, &bound);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let lifetime = row_lifetime(input);
    let reader_type = fresh_type_ident(input, "__DuckDBVector");
    quote! {
        // SAFETY: there is no physical row data, and validation checks the ordered child schema.
        unsafe impl #impl_generics #bound for #ident #type_generics #where_clause {
            const TYPE_ID: ::duckdb_neo::logical_type::LogicalTypeID = ::duckdb_neo::logical_type::LogicalTypeID::#type_id;
            type Native<#lifetime> = ();

            fn validate(
                other: &::duckdb_neo::logical_type::LogicalType,
                children: ::core::option::Option<&::duckdb_neo::data_chunk::VectorCollection>,
            ) -> ::duckdb_neo::Result<::core::primitive::bool> {
                #validate
            }
        }

        // SAFETY: `get` reads nothing.
        unsafe impl #impl_generics ::duckdb_neo::vector::ReadableNativeElement for #ident #type_generics #where_clause {
            type Ref<#lifetime> = () where Self: #lifetime;

            #[inline]
            unsafe fn get<#lifetime, #reader_type: #bound>(
                _reader: &::duckdb_neo::vector::VectorReader<#lifetime, #reader_type>,
                _physical: ::core::primitive::usize,
                _logical: ::core::primitive::usize,
            ) where Self: #lifetime {}
        }

        // SAFETY: the children are always initialized, and NULL rows null their children.
        unsafe impl #impl_generics ::duckdb_neo::vector::WritableNativeElement for #ident #type_generics #where_clause {
            type Write = ();

            #[inline]
            unsafe fn write(
                _writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                _index: ::core::primitive::usize,
                _value: &(),
            ) -> ::duckdb_neo::Result<()> {
                ::core::result::Result::Ok(())
            }

            #[inline]
            unsafe fn write_null(
                writer: &mut ::duckdb_neo::vector::VectorWriter<'_, Self>,
                index: ::core::primitive::usize,
            ) -> ::duckdb_neo::Result<()> {
                ::duckdb_neo::__derive::null_children(writer, index)
            }
        }
    }
}

pub(crate) fn as_native_element(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let fields = StructFields::new(input, "AsNativeElement")?;
    let attributes = container_attributes(input, "AsNativeElement")?;
    let types = fields
        .types
        .iter()
        .map(|ty| option_inner_type(ty).unwrap_or(ty))
        .collect::<Vec<_>>();
    let names = &fields.names;
    let indexes = 0..types.len();
    let count = types.len();
    let native = native_impls(
        input,
        &types,
        quote!(Struct),
        quote! {
            if !::duckdb_neo::__derive::validate_layout(other, children, #count)? {
                return ::core::result::Result::Ok(false);
            }
            #(
                if !::duckdb_neo::__derive::validate_field::<#types>(other, children, #indexes, #names)? {
                    return ::core::result::Result::Ok(false);
                }
            )*
            ::core::result::Result::Ok(true)
        },
    );
    let rows = struct_rows(input, &fields, &types, attributes)?;
    Ok(quote! {
        #native
        #rows
    })
}

/// Reader methods that a field accessor of the same name would shadow or duplicate.
const READER_METHODS: [&str; 7] = ["new", "len", "is_empty", "is_null", "get", "get_unchecked", "iter"];

fn struct_rows(
    input: &syn::DeriveInput,
    fields: &StructFields<'_>,
    types: &[&syn::Type],
    attributes: ContainerAttributes,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let vis = &input.vis;
    let field_idents = &fields.idents;
    let field_vis = fields.fields.iter().map(|field| &field.vis).collect::<Vec<_>>();
    let indexes = (0..types.len()).collect::<Vec<_>>();
    let (_, type_generics, _) = input.generics.split_for_impl();
    let mut output = TokenStream::new();

    if attributes.reader {
        let reader_ident = format_ident!("{ident}Reader");
        let ref_ident = format_ident!("{ident}Ref");
        let reader_doc = format!("Reads [`{ident}`] rows, with every field reader prepared once.");
        let ref_doc = format!("One [`{ident}`] row read by [`{reader_ident}`]; every field is nullable.");
        let lifetime = fresh_lifetime(input, "__duckdb_reader");
        let item_lifetime = fresh_lifetime(input, "__duckdb_item");
        let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::WithReader);
        let generics = with_lifetime(&input.generics, types, &lifetime, &bound);
        let (impl_generics, reader_generics, where_clause) = generics.split_for_impl();
        let element_generics = bounded_generics(input, types, &bound);
        let (element_impl_generics, _, element_where_clause) = element_generics.split_for_impl();
        let params = item_params(input, types.len());
        for (field, ident) in fields.fields.iter().zip(field_idents) {
            let name = ident.unraw().to_string();
            if READER_METHODS.contains(&name.as_str()) {
                return Err(syn::Error::new_spanned(
                    field,
                    format!(
                        "reader accessor `{name}` collides with a reader method; rename the Rust field and keep \
                         its SQL name with #[duckdb(rename = \"{name}\")]"
                    ),
                ));
            }
        }
        let field_indexes = indexes.iter().map(|index| syn::Index::from(*index)).collect::<Vec<_>>();
        let accessor_docs = field_idents
            .iter()
            .map(|ident| format!("Borrow the reader of field `{}`, for columnar reads.", ident.unraw()))
            .collect::<Vec<_>>();
        output.extend(quote! {
            #[doc = #ref_doc]
            #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::marker::Copy, ::core::cmp::PartialEq, ::core::cmp::Eq, ::core::hash::Hash)]
            #vis struct #ref_ident<#(#params),*> {
                #(#field_vis #field_idents: ::core::option::Option<#params>,)*
            }

            #[doc = #reader_doc]
            #vis struct #reader_ident #generics #where_clause {
                parts: ::duckdb_neo::__derive::ReaderParts<
                    ::duckdb_neo::vector::VectorReader<#lifetime, #ident #type_generics>,
                    (#(<#types as #bound>::Reader<#lifetime>,)*),
                >,
            }

            impl #impl_generics #reader_ident #reader_generics #where_clause {
                /// Prepare a reader for `vector`.
                pub fn new(
                    vector: &#lifetime ::duckdb_neo::vector::Vector<#lifetime, #ident #type_generics>,
                ) -> ::duckdb_neo::Result<Self> {
                    let rows = vector.reader()?;
                    // SAFETY: validation checked every child as its field's type.
                    let fields = (#(unsafe {
                        ::duckdb_neo::__derive::child_reader::<#ident #type_generics, #types>(vector, #indexes)?
                    },)*);
                    ::core::result::Result::Ok(Self {
                        // SAFETY: DuckDB sizes every field of a STRUCT like the STRUCT itself.
                        parts: unsafe { ::duckdb_neo::__derive::ReaderParts::new(rows, fields) },
                    })
                }

                #(
                    #[doc = #accessor_docs]
                    #field_vis fn #field_idents(&self) -> &<#types as #bound>::Reader<#lifetime> {
                        &self.parts.fields().#field_indexes
                    }
                )*
            }

            impl #impl_generics ::core::fmt::Debug for #reader_ident #reader_generics #where_clause {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct(::core::stringify!(#reader_ident))
                        .field("len", &::duckdb_neo::vector::RowReader::len(self.parts.rows()))
                        .finish()
                }
            }

            impl #impl_generics ::duckdb_neo::vector::RowReader for #reader_ident #reader_generics #where_clause {
                type Item<#item_lifetime> = #ref_ident<
                    #(<<#types as #bound>::Reader<#lifetime> as ::duckdb_neo::vector::RowReader>::Item<#item_lifetime>),*
                > where Self: #item_lifetime;

                fn len(&self) -> ::core::primitive::usize {
                    ::duckdb_neo::vector::RowReader::len(self.parts.rows())
                }

                fn is_null(&self, row: ::core::primitive::usize) -> ::duckdb_neo::Result<::core::primitive::bool> {
                    ::duckdb_neo::vector::RowReader::is_null(self.parts.rows(), row)
                }

                #[inline]
                unsafe fn get_unchecked(&self, row: ::core::primitive::usize) -> ::core::option::Option<Self::Item<'_>> {
                    // SAFETY: the caller guarantees bounds; every field has one row per parent row.
                    unsafe {
                        ::duckdb_neo::__derive::get_unchecked(self.parts.rows(), row)?;
                        let fields = self.parts.fields();
                        ::core::option::Option::Some(#ref_ident {
                            #(#field_idents: ::duckdb_neo::__derive::get_unchecked(&fields.#field_indexes, row),)*
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
            "Writes [`{ident}`] rows, with every field writer prepared once; fields are public for columnar writes."
        );
        let lifetime = fresh_lifetime(input, "__duckdb_writer");
        let bound: syn::Path = syn::parse_quote!(::duckdb_neo::vector::WithWriter);
        let row_writer: syn::Path = syn::parse_quote!(::duckdb_neo::vector::RowWriter);
        let generics = with_lifetime(&input.generics, types, &lifetime, &bound);
        let (impl_generics, writer_generics, where_clause) = generics.split_for_impl();
        let mut row_generics = generics.clone();
        for ty in types {
            row_generics.make_where_clause().predicates.push(syn::parse_quote!(
                #ty: ::core::borrow::Borrow<<<#ty as #bound>::Writer<#lifetime> as #row_writer>::Value>
            ));
        }
        let (row_impl_generics, _, row_where_clause) = row_generics.split_for_impl();
        let mut element_generics = bounded_generics(input, types, &bound);
        for ty in types {
            element_generics.make_where_clause().predicates.push(syn::parse_quote!(
                #ty: for<#lifetime> ::core::borrow::Borrow<<<#ty as #bound>::Writer<#lifetime> as #row_writer>::Value>
            ));
        }
        let (element_impl_generics, _, element_where_clause) = element_generics.split_for_impl();
        let writes = fields.fields.iter().map(|field| {
            let name = field.ident.as_ref().unwrap();
            if option_inner_type(&field.ty).is_some() {
                quote!(#row_writer::set_option(
                    &mut self.#name,
                    row,
                    ::core::option::Option::as_ref(&value.#name).map(::core::borrow::Borrow::borrow),
                )?;)
            } else {
                quote!(#row_writer::set(&mut self.#name, row, ::core::borrow::Borrow::borrow(&value.#name))?;)
            }
        });
        output.extend(quote! {
            #[doc = #writer_doc]
            #vis struct #writer_ident #generics #where_clause {
                rows: ::duckdb_neo::vector::VectorWriter<#lifetime, #ident #type_generics>,
                #(#field_vis #field_idents: <#types as #bound>::Writer<#lifetime>,)*
            }

            impl #impl_generics #writer_ident #writer_generics #where_clause {
                /// # Safety
                /// As for [`WithWriter::row_writer`](::duckdb_neo::vector::WithWriter::row_writer).
                #[doc(hidden)]
                pub unsafe fn from_raw(
                    vector: ::core::ptr::NonNull<::duckdb_neo::vector::Vector<#lifetime, #ident #type_generics>>,
                ) -> ::duckdb_neo::Result<Self> {
                    // SAFETY: the caller upholds the borrow contract; validation checked every field's type,
                    // and the field writers touch disjoint children.
                    unsafe {
                        let children = ::duckdb_neo::__derive::children(vector)?;
                        ::core::result::Result::Ok(Self {
                            rows: ::duckdb_neo::__derive::vector_writer(vector)?,
                            #(#field_idents: ::duckdb_neo::__derive::child_writer::<#types>(children[#indexes])?,)*
                        })
                    }
                }

                /// Return the number of rows.
                pub fn len(&self) -> ::core::primitive::usize {
                    self.rows.len()
                }

                /// Return whether there are no rows.
                pub fn is_empty(&self) -> ::core::primitive::bool {
                    self.rows.is_empty()
                }

                /// Make a row valid after writing its fields through the public field writers.
                pub fn set_valid(&mut self, row: ::core::primitive::usize) -> ::duckdb_neo::Result<()> {
                    // SAFETY: every field is always initialized.
                    unsafe { ::duckdb_neo::__derive::set_row_validity(&mut self.rows, row, true) }
                }
            }

            impl #impl_generics ::core::fmt::Debug for #writer_ident #writer_generics #where_clause {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    f.debug_struct(::core::stringify!(#writer_ident)).field("len", &self.rows.len()).finish()
                }
            }

            impl #row_impl_generics #row_writer for #writer_ident #writer_generics #row_where_clause {
                type Value = #ident #type_generics;

                fn len(&self) -> ::core::primitive::usize {
                    self.rows.len()
                }

                #[inline]
                fn set(&mut self, row: ::core::primitive::usize, value: &Self::Value) -> ::duckdb_neo::Result<()> {
                    let written = (|| -> ::duckdb_neo::Result<()> {
                        #(#writes)*
                        ::core::result::Result::Ok(())
                    })();
                    if let ::core::result::Result::Err(error) = written {
                        // Earlier fields were written, so NULL the row to keep its fields consistent.
                        #row_writer::set_null(self, row)?;
                        return ::core::result::Result::Err(error);
                    }
                    self.set_valid(row)
                }

                fn set_null(&mut self, row: ::core::primitive::usize) -> ::duckdb_neo::Result<()> {
                    #(#row_writer::set_null(&mut self.#field_idents, row)?;)*
                    // SAFETY: every field of the row was just nulled.
                    unsafe { ::duckdb_neo::__derive::set_row_validity(&mut self.rows, row, false) }
                }

                unsafe fn resize_rows(&mut self, len: ::core::primitive::usize) -> ::duckdb_neo::Result<()> {
                    // SAFETY: the caller upholds the contract for the struct's rows; DuckDB resizes
                    // every field along with them.
                    unsafe { ::duckdb_neo::__derive::resize_rows(&mut self.rows, len)? };
                    #(#row_writer::refresh(&mut self.#field_idents)?;)*
                    ::core::result::Result::Ok(())
                }

                fn refresh(&mut self) -> ::duckdb_neo::Result<()> {
                    ::duckdb_neo::__derive::refresh(&mut self.rows)?;
                    #(#row_writer::refresh(&mut self.#field_idents)?;)*
                    ::core::result::Result::Ok(())
                }
            }

            // SAFETY: the writer only writes fields through their own writers, and NULL rows null every field.
            unsafe impl #element_impl_generics #bound for #ident #type_generics #element_where_clause {
                type Writer<#lifetime> = #writer_ident #writer_generics where Self: #lifetime;

                unsafe fn row_writer<#lifetime>(
                    vector: ::core::ptr::NonNull<::duckdb_neo::vector::Vector<#lifetime, Self>>,
                ) -> ::duckdb_neo::Result<Self::Writer<#lifetime>> {
                    // SAFETY: the caller upholds the borrow contract.
                    unsafe { #writer_ident::from_raw(vector) }
                }
            }
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::as_native_element;

    fn items(tokens: proc_macro2::TokenStream) -> Vec<String> {
        syn::parse2::<syn::File>(tokens)
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
                syn::Item::Struct(item) => Some(format!("struct {}", item.ident)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn rows_are_generated_only_on_request() {
        let plain = syn::parse_quote!(
            struct Row {
                value: i32,
            }
        );
        assert_eq!(
            items(as_native_element(&plain).unwrap()),
            [
                "NativeElement for Row",
                "ReadableNativeElement for Row",
                "WritableNativeElement for Row"
            ]
        );
        let rows = syn::parse_quote!(
            #[duckdb(reader, writer)]
            struct Row {
                required: String,
                optional: Option<i32>,
            }
        );
        assert_eq!(
            items(as_native_element(&rows).unwrap())[3..],
            [
                "struct RowRef",
                "struct RowReader",
                "impl for RowReader",
                "Debug for RowReader",
                "RowReader for RowReader",
                "WithReader for Row",
                "struct RowWriter",
                "impl for RowWriter",
                "Debug for RowWriter",
                "RowWriter for RowWriter",
                "WithWriter for Row"
            ]
        );
    }

    #[test]
    fn reader_accessors_must_not_collide() {
        let input = syn::parse_quote!(
            #[duckdb(reader)]
            struct Row {
                len: i32,
            }
        );
        assert!(
            as_native_element(&input)
                .unwrap_err()
                .to_string()
                .starts_with("reader accessor `len` collides with a reader method")
        );
        let renamed = syn::parse_quote!(
            #[duckdb(reader)]
            struct Row {
                #[duckdb(rename = "len")]
                length: i32,
                rows: i32,
            }
        );
        assert!(as_native_element(&renamed).is_ok());
    }

    #[test]
    fn native_derive_requires_named_fields() {
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
            assert_eq!(
                as_native_element(&input).unwrap_err().to_string(),
                "AsNativeElement requires a struct with named fields"
            );
        }
    }
}
