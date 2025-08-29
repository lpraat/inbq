use std::{collections::HashSet, fmt::Write};

use syn::GenericArgument;

#[derive(Debug, Clone)]
struct RsStructField {
    name: String,
    r#type: RsType,
}

#[derive(Debug, Clone)]
struct RsStruct {
    name: String,
    fields: Vec<RsStructField>,
}

#[derive(Debug, Clone)]
struct RsEnum {
    name: String,
    variants: Vec<RsEnumVariant>,
}

#[derive(Debug, Clone)]
enum RsEnumVariant {
    Unit(String),
    Struct(RsStructEnumVariant),
    Tuple(RsTupleEnumVariant),
}

#[derive(Debug, Clone)]
struct RsTupleEnumVariant {
    name: String,
    r#types: Vec<RsType>,
}

#[derive(Debug, Clone)]
struct RsStructEnumVariant {
    name: String,
    fields: Vec<RsStructField>,
}

#[derive(Debug, Clone)]
enum RsType {
    Vec(Box<RsType>),
    Option(Box<RsType>),
    Box(Box<RsType>),
    Basic(String),
}

impl RsType {
    #[inline]
    fn _py_type(&self) -> String {
        match self {
            RsType::Vec(rs_type) => {
                format!("list[{}]", rs_type._py_type())
            }
            RsType::Option(rs_type) => {
                format!("Optional[{}]", rs_type._py_type())
            }
            RsType::Box(rs_type) => rs_type._py_type(),
            RsType::Basic(s) => match s.as_str() {
                "String" => "str".to_owned(),
                "i8" | "u8" | "i16" | "u16" | "i32" | "u32" | "i64" | "u64" => "int".to_owned(),
                "f32 | f64" => "float".to_owned(),
                _ => s.clone(),
            },
        }
    }

    fn py_type(&self) -> String {
        format!("'{}'", self._py_type())
    }
}

impl From<&syn::TypePath> for RsType {
    fn from(value: &syn::TypePath) -> Self {
        let segments = &value.path.segments;

        let first = segments.first().unwrap().ident.to_string();

        let first_as_str = first.as_str();
        match first_as_str {
            type_name @ ("Option" | "Vec" | "Box") => {
                let args = &value.path.segments.first().unwrap().arguments;
                let inner_type: RsType = match args {
                    syn::PathArguments::AngleBracketed(angle_bracketed_generic_arguments) => {
                        match angle_bracketed_generic_arguments.args.first() {
                            Some(GenericArgument::Type(syn::Type::Path(type_path))) => {
                                type_path.into()
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };
                match type_name {
                    "Option" => Self::Option(Box::new(inner_type)),
                    "Vec" => Self::Vec(Box::new(inner_type)),
                    "Box" => Self::Box(Box::new(inner_type)),
                    _ => unreachable!(),
                }
            }
            _ => Self::Basic(segments.first().unwrap().ident.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PyCodeGenerator {
    rs_structs: Vec<RsStruct>,
    rs_enums: Vec<RsEnum>,
}

impl PyCodeGenerator {
    #[inline]
    fn normalize_reserved_attribute_name(attr_name: &str) -> String {
        let py_reserved = HashSet::from(["type", "except", "from", "with", "else", "case"]);
        let attr_name = if attr_name.starts_with("r#") {
            attr_name.get(2..).unwrap()
        } else {
            attr_name
        };
        if py_reserved.contains(&attr_name) {
            format!("{}_", attr_name)
        } else {
            attr_name.to_owned()
        }
    }

    pub fn generate_ast_nodes_file_str(&self) -> anyhow::Result<String> {
        let mut upper_part = String::new();
        let mut lower_part = String::new();

        self.gen_module_docstrings(&mut upper_part)?;
        self.gen_imports(&mut upper_part)?;
        self.gen_struts(&mut lower_part)?;
        let enum_decoder = self.gen_enums(&mut lower_part)?;
        self.gen_ast_node_base_class(&mut upper_part, &enum_decoder)?;

        upper_part.push_str(&lower_part);
        Ok(upper_part)
    }

    pub fn new(ast_nodes_file_path: &std::path::Path) -> anyhow::Result<Self> {
        let rust_code = std::fs::read_to_string(ast_nodes_file_path)?;
        let parsed = syn::parse_file(&rust_code)?;

        let mut rs_structs = vec![];
        let mut rs_enums = vec![];

        for item in &parsed.items {
            match item {
                syn::Item::Enum(item_enum) => {
                    rs_enums.push(RsEnum {
                        name: item_enum.ident.to_string(),
                        variants: item_enum
                            .variants
                            .iter()
                            .cloned()
                            .map(|variant| match variant.fields {
                                syn::Fields::Named(fields_named) => {
                                    RsEnumVariant::Struct(RsStructEnumVariant {
                                        name: variant.ident.to_string(),
                                        fields: fields_named
                                            .named
                                            .iter()
                                            .map(|f| RsStructField {
                                                name: f.ident.as_ref().unwrap().to_string(),
                                                r#type: match &f.ty {
                                                    syn::Type::Path(type_path) => type_path.into(),
                                                    _ => unreachable!(),
                                                },
                                            })
                                            .collect(),
                                    })
                                }
                                syn::Fields::Unnamed(fields_unnamed) => {
                                    RsEnumVariant::Tuple(RsTupleEnumVariant {
                                        name: variant.ident.to_string(),
                                        types: fields_unnamed
                                            .unnamed
                                            .iter()
                                            .map(|f| match &f.ty {
                                                syn::Type::Path(type_path) => type_path.into(),
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    })
                                }
                                syn::Fields::Unit => RsEnumVariant::Unit(variant.ident.to_string()),
                            })
                            .collect(),
                    });
                }
                syn::Item::Struct(item_struct) => {
                    rs_structs.push(RsStruct {
                        name: item_struct.ident.to_string(),
                        fields: item_struct
                            .fields
                            .iter()
                            .cloned()
                            .map(|f| RsStructField {
                                name: f.ident.expect("Found unexpected tuple struct").to_string(),
                                r#type: match &f.ty {
                                    syn::Type::Path(type_path) => type_path.into(),
                                    _ => unreachable!(),
                                },
                            })
                            .collect(),
                    });
                }
                _ => {}
            }
        }

        Ok(Self {
            rs_structs,
            rs_enums,
        })
    }

    fn gen_struts(&self, mut buffer: &mut String) -> anyhow::Result<()> {
        for s in &self.rs_structs {
            let mut py_fields = String::from("");
            for field in &s.fields {
                let field_name = Self::normalize_reserved_attribute_name(&field.name);
                py_fields.push_str(&format!("\t{}: {}\n", field_name, field.r#type.py_type()));
            }
            writeln!(
                &mut buffer,
                r#"
@dataclass
class {}(AstNode):
{}
            "#,
                s.name, py_fields
            )?
        }
        Ok(())
    }

    fn gen_enums(&self, buffer: &mut String) -> anyhow::Result<String> {
        let mut enum_decoder = String::new();

        for rs_enum in &self.rs_enums {
            self.gen_variants(buffer, &mut enum_decoder, rs_enum)?;
        }

        Ok(format!("{{{}}}", enum_decoder))
    }

    fn gen_variants(
        &self,
        mut buffer: &mut String,
        enum_decoder: &mut String,
        rs_enum: &RsEnum,
    ) -> anyhow::Result<()> {
        if !enum_decoder.is_empty() && !enum_decoder.ends_with(',') {
            enum_decoder.push(',');
        }
        enum_decoder.push_str(&format!("\"{}\":{{", rs_enum.name));

        let mut enum_py_variants = vec![];

        for variant in &rs_enum.variants {
            match variant {
                RsEnumVariant::Unit(name) => {
                    let py_variant = format!("{}_{}", rs_enum.name, name);
                    writeln!(
                        &mut buffer,
                        r#"
@dataclass
class {}(AstNode): ...
"#,
                        py_variant
                    )?;
                    enum_decoder.push_str(&format!("\"{}\":\"{}\",", name, py_variant));
                    enum_py_variants.push(py_variant);
                }
                RsEnumVariant::Struct(rs_struct_enum_variant) => {
                    let py_variant = format!("{}_{}", rs_enum.name, rs_struct_enum_variant.name);

                    let mut py_fields = String::new();
                    for field in &rs_struct_enum_variant.fields {
                        let field_name = Self::normalize_reserved_attribute_name(&field.name);
                        py_fields.push_str(&format!(
                            "\t{}: {}\n",
                            field_name,
                            field.r#type.py_type()
                        ));
                    }
                    writeln!(
                        &mut buffer,
                        r#"
@dataclass
class {}(AstNode):
{}
                    "#,
                        py_variant, py_fields
                    )?;
                    enum_decoder.push_str(&format!(
                        "\"{}\":\"{}\",",
                        rs_struct_enum_variant.name, py_variant
                    ));
                    enum_py_variants.push(py_variant.clone());
                }

                RsEnumVariant::Tuple(rs_tuple_enum_variant) => {
                    if rs_tuple_enum_variant.types.len() > 1 {
                        unimplemented!();
                    }
                    let tuple_ty = &rs_tuple_enum_variant.types[0];
                    let py_variant = format!("{}_{}", rs_enum.name, rs_tuple_enum_variant.name);

                    writeln!(
                        &mut buffer,
                        r#"
@dataclass
class {}(AstNode):
    value: {}
                    "#,
                        py_variant,
                        tuple_ty.py_type()
                    )?;
                    enum_decoder.push_str(&format!(
                        "\"{}\":\"{}\",",
                        rs_tuple_enum_variant.name, py_variant
                    ));
                    enum_py_variants.push(py_variant);
                }
            }
        }

        writeln!(
            &mut buffer,
            "{}: TypeAlias = '{}'",
            rs_enum.name,
            enum_py_variants.join(" | ")
        )?;

        enum_decoder.push('}');

        Ok(())
    }

    fn gen_module_docstrings(&self, mut buffer: &mut String) -> anyhow::Result<()> {
        write!(
            &mut buffer,
            r#""""
This file is autogenerated via `cargo run --bin inbq_genpy`.
"""
"#
        )?;
        Ok(())
    }

    fn gen_imports(&self, mut buffer: &mut String) -> anyhow::Result<()> {
        writeln!(
            &mut buffer,
            r#"import typing
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Optional,
    Self,
    TypeAlias,
    get_args,
    get_origin,
    get_type_hints,
)
"#
        )?;
        Ok(())
    }

    /// Generate the base `AstNode` python class
    ///
    /// The class provides the factory `from_json` to instantiate any kind of node
    /// from json data returned by the parser.
    fn gen_ast_node_base_class(
        &self,
        mut buffer: &mut String,
        enum_decoder: &str,
    ) -> anyhow::Result<()> {
        writeln!(
            &mut buffer,
            r#"
class AstNode:
    _PRIMITIVE_TYPES: ClassVar[frozenset[typing.Type[Any]]] = frozenset(
        (bool, str, int, float)
    )
    _GLOBALS: ClassVar[dict[str, Any]] = globals()
    _ENUM_DECODER: dict[str, dict[str, str]] = {}

    @classmethod
    def _instantiate_typed_field(cls, json: dict, ty: typing.Type[Any]):
        args = get_args(ty)

        if not args:
            # Initialize this specific class
            return ty.from_json(json)

        if "_" in args[0].__name__:
            # Find the py_class of the enum variant
            # from args which contains all the variants

            enum_name = args[0].__name__.split("_")[0]

            if type(json) is str and json in cls._ENUM_DECODER[enum_name]:
                # This is a unit variant (whose py_class does not have any attribute, e.g., TokenType_Minus)
                return cls._GLOBALS[cls._ENUM_DECODER[enum_name][json]]()

            variant_in_json = list(json)[0]
            clz = cls._GLOBALS[cls._ENUM_DECODER[enum_name][variant_in_json]]
            return clz.from_json(json[variant_in_json])

        # At this point, args is always type | None (Optional[type])
        # first element is the class, second element is NoneType
        if args[0] in cls._PRIMITIVE_TYPES:
            return args[0](json)
        return args[0].from_json(json)

    @classmethod
    def from_json(cls, json: dict) -> Self:
        cls_dict = dict()
        ty_hints = get_type_hints(cls)
        for field, ty in ty_hints.items():
            if field.startswith("_"):
                # Ignore private attributes
                continue

            if "_" in cls.__name__ and field == "value" and field not in json:
                # Tuple Variant
                ty = ty_hints["value"]
                if ty in cls._PRIMITIVE_TYPES:
                    cls_dict["value"] = ty(json)
                else:
                    cls_dict["value"] = cls._instantiate_typed_field(json, ty)

                break

            # Handle reserved keywords (e.g., from_)
            original_field = field
            if field.endswith("_"):
                field = field[:-1]

            if ty in cls._PRIMITIVE_TYPES:
                cls_dict[original_field] = ty(json[field])

            elif get_origin(ty) is list:
                field_json = json[field]
                li = []
                for el in field_json:
                    li.append(cls._instantiate_typed_field(el, get_args(ty)[0]))

                cls_dict[original_field] = li
            else:
                field_json = json[field]
                if field_json is None:
                    # Optional field is None
                    cls_dict[original_field] = None
                else:
                    cls_dict[original_field] = cls._instantiate_typed_field(
                        field_json, ty
                    )

        return cls(**cls_dict)
        "#,
            enum_decoder
        )?;
        Ok(())
    }
}
