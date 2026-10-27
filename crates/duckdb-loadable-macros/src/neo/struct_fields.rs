use quote::format_ident;

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
        let names = idents.iter().map(|ident| ident.to_string()).collect();
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
