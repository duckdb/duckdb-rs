use quote::format_ident;
use syn::ext::IdentExt;

pub(super) struct StructFields<'a> {
    pub fields: &'a syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
    pub idents: Vec<&'a syn::Ident>,
    pub types: Vec<&'a syn::Type>,
    pub names: Vec<String>,
}

impl<'a> StructFields<'a> {
    pub fn new(input: &'a syn::DeriveInput, derive: &str) -> syn::Result<Self> {
        let fields = match &input.data {
            syn::Data::Struct(syn::DataStruct {
                fields: syn::Fields::Named(fields),
                ..
            }) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    format!("{derive} requires a struct with named fields"),
                ));
            }
        };
        container_attributes(input, derive)?;
        let idents = fields
            .iter()
            .map(|field| field.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let types = fields.iter().map(|field| &field.ty).collect();
        let rust_names = idents.iter().map(|ident| ident.unraw().to_string()).collect::<Vec<_>>();
        let names = fields
            .iter()
            .zip(&rust_names)
            .map(|(field, name)| sql_name(&field.attrs, name, derive))
            .collect::<syn::Result<Vec<_>>>()?;
        for (index, name) in names.iter().enumerate() {
            if let Some(previous) = names[..index]
                .iter()
                .find(|previous| previous.eq_ignore_ascii_case(name))
            {
                return Err(syn::Error::new_spanned(
                    idents[index],
                    format!(
                        "{derive} field '{name}' conflicts with field '{previous}': STRUCT field names are case-insensitive"
                    ),
                ));
            }
        }
        Ok(Self {
            fields,
            idents,
            types,
            names,
        })
    }
}

pub(super) fn sql_name(attrs: &[syn::Attribute], name: &str, derive: &str) -> syn::Result<String> {
    let mut rename: Option<syn::LitStr> = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("duckdb")) {
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("rename") {
                return Err(meta.error(format!("unsupported {derive} attribute, expected `rename = \"...\"`")));
            }
            let value: syn::LitStr = meta.value()?.parse()?;
            if rename.is_some() {
                return Err(syn::Error::new_spanned(
                    &value,
                    format!("{derive}: duplicate rename of '{name}'"),
                ));
            }
            if value.value().is_empty() {
                return Err(syn::Error::new_spanned(
                    &value,
                    format!("{derive} names cannot be empty"),
                ));
            }
            rename = Some(value);
            Ok(())
        })?;
    }
    Ok(rename.map_or_else(|| name.to_owned(), |rename| rename.value()))
}

/// Which row reader and writer types a derive generates, from `#[duckdb(reader, writer)]`.
#[derive(Clone, Copy, Default)]
pub(super) struct ContainerAttributes {
    pub reader: bool,
    pub writer: bool,
}

pub(super) fn container_attributes(input: &syn::DeriveInput, derive: &str) -> syn::Result<ContainerAttributes> {
    let mut attributes = ContainerAttributes::default();
    for attr in input.attrs.iter().filter(|attr| attr.path().is_ident("duckdb")) {
        attr.parse_nested_meta(|meta| {
            let flag = if meta.path.is_ident("reader") {
                &mut attributes.reader
            } else if meta.path.is_ident("writer") {
                &mut attributes.writer
            } else {
                return Err(meta.error(format!(
                    "unsupported {derive} container attribute, expected `reader` or `writer`"
                )));
            };
            if std::mem::replace(flag, true) {
                return Err(meta.error(format!("{derive}: duplicate container attribute")));
            }
            Ok(())
        })?;
    }
    Ok(attributes)
}

pub(super) fn bounded_generics(input: &syn::DeriveInput, types: &[&syn::Type], bound: &syn::Path) -> syn::Generics {
    let mut generics = input.generics.clone();
    for ty in types {
        generics
            .make_where_clause()
            .predicates
            .push(syn::parse_quote!(#ty: #bound));
    }
    generics
}

pub(super) fn fresh_type_ident(input: &syn::DeriveInput, name: &str) -> syn::Ident {
    let mut ident = format_ident!("{name}");
    while input.generics.params.iter().any(|param| match param {
        syn::GenericParam::Type(param) => param.ident == ident,
        syn::GenericParam::Const(param) => param.ident == ident,
        syn::GenericParam::Lifetime(_) => false,
    }) {
        ident = format_ident!("{}_", ident);
    }
    ident
}

pub(super) fn option_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some() {
        return None;
    }
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    match &segment.arguments {
        syn::PathArguments::AngleBracketed(arguments) if arguments.args.len() == 1 => match arguments.args.first()? {
            syn::GenericArgument::Type(ty) => Some(ty),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::StructFields;
    use crate::neo::{as_duckdb, as_duckdb_type, as_from_value, as_native_element, as_to_value};

    #[test]
    fn raw_identifiers_use_unescaped_field_names() {
        let input = syn::parse_quote! {
            struct Record {
                r#type: i32,
                r#match: Option<String>,
                plain: bool,
            }
        };
        let fields = StructFields::new(&input, "AsDuckDB").unwrap();
        assert_eq!(fields.names, ["type", "match", "plain"]);
        assert_eq!(fields.idents[0].to_string(), "r#type");
    }

    type Derive = fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>;
    const DERIVES: [(&str, Derive); 5] = [
        ("AsDuckDB", as_duckdb),
        ("AsDuckDBType", as_duckdb_type),
        ("AsFromValue", as_from_value),
        ("AsToValue", as_to_value),
        ("AsNativeElement", as_native_element),
    ];

    #[test]
    fn renamed_fields_keep_their_rust_names() {
        let input = syn::parse_quote! {
            struct Record {
                #[duckdb(rename = "Kind")]
                r#type: i32,
                #[duckdb(rename = "with space")]
                value: Option<String>,
                plain: bool,
            }
        };
        let fields = StructFields::new(&input, "AsDuckDB").unwrap();
        assert_eq!(fields.names, ["Kind", "with space", "plain"]);
        for (_, derive) in DERIVES {
            derive(&input).unwrap();
        }
    }

    #[test]
    fn struct_derives_reject_invalid_attributes() {
        for (input, message) in [
            (
                syn::parse_quote! { struct Record { #[duckdb(name = "x")] value: i32 } },
                "unsupported {} attribute, expected `rename = \"...\"`",
            ),
            (
                syn::parse_quote! { struct Record { #[duckdb(rename)] value: i32 } },
                "expected `=`",
            ),
            (
                syn::parse_quote! { struct Record { #[duckdb(rename = 1)] value: i32 } },
                "expected string literal",
            ),
            (
                syn::parse_quote! { struct Record { #[duckdb(rename = "")] value: i32 } },
                "{} names cannot be empty",
            ),
            (
                syn::parse_quote! { struct Record { #[duckdb(rename = "a", rename = "b")] value: i32 } },
                "{}: duplicate rename of 'value'",
            ),
            (
                syn::parse_quote! { struct Record { #[duckdb(rename = "a")] #[duckdb(rename = "b")] value: i32 } },
                "{}: duplicate rename of 'value'",
            ),
            (
                syn::parse_quote! { #[duckdb(rename = "a")] struct Record { value: i32 } },
                "unsupported {} container attribute, expected `reader` or `writer`",
            ),
            (
                syn::parse_quote! { #[duckdb(reader, reader)] struct Record { value: i32 } },
                "{}: duplicate container attribute",
            ),
        ] {
            for (name, derive) in DERIVES {
                assert_eq!(
                    derive(&input)
                        .expect_err("invalid attributes must be rejected")
                        .to_string(),
                    message.replace("{}", name)
                );
            }
        }
    }

    #[test]
    fn struct_derives_reject_case_insensitive_duplicate_fields() {
        let derives = DERIVES;
        for (input, first, second) in [
            (
                syn::parse_quote! {
                    struct Record { x: i32, X: i32 }
                },
                "x",
                "X",
            ),
            (
                syn::parse_quote! {
                    struct Record { r#type: i32, Type: i32 }
                },
                "type",
                "Type",
            ),
            (
                syn::parse_quote! {
                    struct Record { x: i32, #[duckdb(rename = "X")] y: i32 }
                },
                "x",
                "X",
            ),
        ] {
            for (name, derive) in derives {
                assert_eq!(
                    derive(&input)
                        .expect_err("duplicate fields must be rejected")
                        .to_string(),
                    format!(
                        "{name} field '{second}' conflicts with field '{first}': STRUCT field names are case-insensitive"
                    )
                );
            }
        }
    }
}
