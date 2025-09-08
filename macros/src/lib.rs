use proc_macro::{self, TokenStream};
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::ItemFn;
use syn::{
    DeriveInput, Ident, Result, Token, Type, bracketed,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let fn_tokens = quote! {
        #[unsafe(no_mangle)]
        #input

        reactor_actor::register_actor!(#name);
    };
    fn_tokens.into()
}

#[proc_macro_derive(DefaultPrio)]
pub fn auto_default_priority(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl reactor_actor::HasPriority for #name {}
    })
}

#[proc_macro_derive(Msg)]
pub fn auto_msg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    TokenStream::from(quote! {
        impl reactor_actor::Msg for #name {}
    })
}

#[derive(Debug)]
struct MsgConverters {
    pub unions: Vec<UnionDef>,
    pub adapters: Vec<AdapterDef>,
    pub decoders: Vec<DecoderDef>,
}

#[derive(Debug)]
struct UnionDef {
    pub name: Ident,
    pub variants: Vec<Type>,
}

#[derive(Debug)]
struct AdapterDef {
    pub target: Ident,
    pub source: Ident,
    pub via: Ident,
}

#[derive(Debug)]
struct DecoderDef {
    pub decoder_name: Ident,
    pub inputs: Vec<Ident>,
    pub output: Ident,
}

impl Parse for MsgConverters {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut unions = vec![];
        let mut conversions = vec![];
        let mut decoders = vec![];

        while !input.is_empty() {
            if input.peek(Ident) {
                let section_keyword: Ident = input.parse()?;
                match section_keyword.to_string().as_str() {
                    "Unions" => {
                        input.parse::<Token![:]>()?;
                        let content;
                        bracketed!(content in input);
                        while !content.is_empty() {
                            let name: Ident = content.parse()?;
                            content.parse::<Token![=]>()?;
                            let variants: Punctuated<Type, Token![,]> =
                                Punctuated::parse_separated_nonempty(&content)?;
                            content.parse::<Token![;]>()?;
                            unions.push(UnionDef {
                                name,
                                variants: variants.into_iter().collect(),
                            });
                        }

                        input.parse::<Token![;]>()?;
                    }
                    "Adapters" => {
                        input.parse::<Token![:]>()?;
                        let content;
                        bracketed!(content in input);

                        while !content.is_empty() {
                            let target: Ident = content.parse()?;
                            let from_kw: Ident = content.parse()?;
                            if from_kw != "from" {
                                return Err(syn::Error::new(
                                    from_kw.span(),
                                    "expected `from` keyword",
                                ));
                            }
                            let source: Ident = content.parse()?;
                            let via_kw: Ident = content.parse()?;
                            if via_kw != "via" {
                                return Err(syn::Error::new(
                                    via_kw.span(),
                                    "expected `via` keyword",
                                ));
                            }
                            let via: Ident = content.parse()?;
                            content.parse::<Token![;]>()?;

                            conversions.push(AdapterDef {
                                target,
                                source,
                                via,
                            });
                        }

                        input.parse::<Token![;]>()?;
                    }
                    "Decoders" => {
                        input.parse::<Token![:]>()?;
                        let content;
                        bracketed!(content in input);

                        while !content.is_empty() {
                            let decoder_name: Ident = content.parse()?;
                            let can_kw: Ident = content.parse()?;
                            if can_kw != "can" {
                                return Err(syn::Error::new(
                                    can_kw.span(),
                                    "expected `can` keyword",
                                ));
                            }
                            let decode_kw: Ident = content.parse()?;
                            if decode_kw != "decode" {
                                return Err(syn::Error::new(
                                    decode_kw.span(),
                                    "expected `decode` keyword",
                                ));
                            }

                            let inputs: Punctuated<Ident, Token![,]> =
                                Punctuated::parse_separated_nonempty(&content)?;
                            let to: Ident = content.parse()?;
                            assert_eq!(to.to_string(), "to");
                            let output: Ident = content.parse()?;
                            content.parse::<Token![;]>()?;

                            decoders.push(DecoderDef {
                                decoder_name,
                                inputs: inputs.into_iter().collect(),
                                output,
                            });
                        }

                        input.parse::<Token![;]>()?;
                    }
                    other => {
                        return Err(syn::Error::new(
                            section_keyword.span(),
                            format!("Unexpected section keyword: {other}"),
                        ));
                    }
                }
            }
        }

        Ok(MsgConverters {
            unions,
            adapters: conversions,
            decoders,
        })
    }
}

fn generate_enum(union: &UnionDef) -> TokenStream2 {
    let enum_name = &union.name;
    let variants = &union.variants;

    // Define enum itself
    let enum_def = quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::bincode::Encode,
            ::bincode::Decode,
        )]
        pub enum #enum_name {
            #(#variants(#variants)),*
        }
        impl ::reactor_actor::HasPriority for #enum_name {}
        impl ::reactor_actor::Msg for #enum_name {}
    };
    // Implement From<Variant> for Enum
    let impl_from_variant = variants.iter().map(|variant| {
        quote! {
            impl From<#variant> for #enum_name {
                fn from(v: #variant) -> Self {
                    #enum_name::#variant(v)
                }
            }
        }
    });

    // Implement From<Enum> for Variant (with panic on mismatch)
    let impl_into_variant = variants.iter().map(|variant| {
        quote! {
            impl From<#enum_name> for #variant {
                fn from(e: #enum_name) -> Self {
                    if let #enum_name::#variant(inner) = e {
                        inner
                    } else {
                        panic!("cannot convert {} to {}", stringify!(#enum_name), stringify!(#variant));
                    }
                }
            }
        }
    });

    quote! {
        #enum_def
        #(#impl_from_variant)*
        #(#impl_into_variant)*
    }
}

fn generate_adapters(adapter: &AdapterDef) -> TokenStream2 {
    let source = &adapter.source;
    let via = &adapter.via;
    let target = &adapter.target;

    quote! {
        impl From<#source> for #target {
            fn from(value: #source) -> Self {
                let intermediate: #via = value.into();
                intermediate.into()
            }
        }
        impl From<#target> for #source {
            fn from(value: #target) -> Self {
                let intermediate: #via = value.into();
                intermediate.into()
            }
        }
    }
}

fn generate_decoders(def: &DecoderDef) -> TokenStream2 {
    let func_name = &def.decoder_name;
    let output_ty = &def.output;

    let arms: Vec<TokenStream2> = def.inputs.iter().map(|input_ty| {
        quote! {
            if name == ::std::any::type_name::<#input_ty>() {
                fn decoder_cons() -> ::std::boxed::Box<dyn ::tokio_util::codec::Decoder<Item = #output_ty, Error = ::std::io::Error> + ::core::marker::Sync + ::core::marker::Send> {
                    ::std::boxed::Box::new(::reactor_actor::codec::BincodeSubdecoder::<#input_ty, #output_ty>::default())
                }

                fn any_to_m(msg: ::std::boxed::Box<dyn ::std::any::Any>) -> #output_ty {
                    let msg = msg.downcast::<#input_ty>().unwrap();
                    (*msg).into()
                }

                return Some(::reactor_actor::DecoderProvider {
                    decoder_cons,
                    any_to_m,
                });
            }
        }
    }).collect();
    let type_names: Vec<TokenStream2> = def
        .inputs
        .iter()
        .map(|input_ty| {
            quote! {
                ::std::any::type_name::<#input_ty>().to_string()
            }
        })
        .collect();

    quote! {
        fn #func_name(name: &str) -> ::std::option::Option<::reactor_actor::DecoderProvider<#output_ty>> {
            #(#arms)*
            println!("Avaialable decoders: {:?}", vec![#(#type_names),*]);
            None
        }
    }
}

#[proc_macro]
pub fn msg_converter(input: TokenStream) -> TokenStream {
    let MsgConverters {
        unions,
        adapters: conversions,
        decoders,
    } = parse_macro_input!(input as MsgConverters);

    // Collect generated enums
    let enums = unions.iter().map(generate_enum);

    // Adapters
    let adapters = conversions.iter().map(generate_adapters);

    // Adapters
    let decoders = decoders.iter().map(generate_decoders);

    // Combine them into one stream
    let output = quote! {
        #(#enums)*

        #(#adapters)*

        #(#decoders)*
    };

    let output = output.into();
    export_to_file("actor", "debug", &output);
    output
}

fn export_to_file(crate_name: &str, file_name: &str, item: &TokenStream) -> bool {
    use std::io::Write;

    if let Ok(var) = std::env::var("CARGO_MANIFEST_DIR") {
        let mut path = std::path::PathBuf::from(var);
        loop {
            {
                let mut path = path.clone();
                path.push("target");
                if path.exists() {
                    path.push("generated");
                    path.push(crate_name);
                    if std::fs::create_dir_all(&path).is_err() {
                        return false;
                    }
                    path.push(format!("{file_name}.rs"));
                    if let Ok(mut file) = std::fs::File::create(path) {
                        let _ = file.write_all(item.to_string().as_bytes());
                        return true;
                    }
                }
            }
            if let Some(parent) = path.parent() {
                path = parent.into();
            } else {
                break;
            }
        }
    }
    false
}

#[allow(dead_code)]
fn export_to_file2(crate_name: &str, file_name: &str, item: &str) -> bool {
    use std::io::Write;

    if let Ok(var) = std::env::var("CARGO_MANIFEST_DIR") {
        let mut path = std::path::PathBuf::from(var);
        loop {
            {
                let mut path = path.clone();
                path.push("target");
                if path.exists() {
                    path.push("generated");
                    path.push(crate_name);
                    if std::fs::create_dir_all(&path).is_err() {
                        return false;
                    }
                    path.push(format!("{file_name}.rs"));
                    if let Ok(mut file) = std::fs::File::create(path) {
                        let _ = file.write_all(item.to_string().as_bytes());
                        return true;
                    }
                }
            }
            if let Some(parent) = path.parent() {
                path = parent.into();
            } else {
                break;
            }
        }
    }
    false
}
