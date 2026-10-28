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
        let idents = fields
            .iter()
            .map(|field| field.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let types = fields.iter().map(|field| &field.ty).collect();
        let names = idents.iter().map(|ident| ident.unraw().to_string()).collect::<Vec<_>>();
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
    use crate::neo::{
        as_duckdb, as_duckdb_type, as_from_value, as_to_value, as_vector_element, as_writable_vector_element,
    };

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

    #[test]
    fn struct_derives_reject_case_insensitive_duplicate_fields() {
        type Derive = fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>;
        let derives: [(&str, Derive); 6] = [
            ("AsDuckDB", as_duckdb),
            ("AsDuckDBType", as_duckdb_type),
            ("AsFromValue", as_from_value),
            ("AsToValue", as_to_value),
            ("AsVectorElement", as_vector_element),
            ("AsWritableVectorElement", as_writable_vector_element),
        ];
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
