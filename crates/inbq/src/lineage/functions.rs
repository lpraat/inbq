use crate::arena::ArenaIndex;

use super::{ArrayNodeType, NodeType, StructNodeFieldType, StructNodeType};

pub(crate) struct FunctionDefinition {
    #[allow(dead_code)]
    pub(crate) name: String,
    #[allow(clippy::type_complexity)]
    pub(crate) compute_return_type: Box<dyn Fn(&[&NodeType], &[ArenaIndex]) -> NodeType>,
}

fn array_type_with_unkown_type() -> NodeType {
    NodeType::Array(Box::new(ArrayNodeType {
        r#type: NodeType::Unknown,
        input: vec![],
    }))
}

pub(crate) fn find_mathching_function(name: &str) -> Option<FunctionDefinition> {
    match name.to_lowercase().as_str() {
        "abs" => Some(FunctionDefinition {
            name: "abs".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::Float64] => NodeType::Float64,
                [t] => {
                    log::warn!("Found unexpected input type {} in abs function.", t);
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("abs expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        // AEAD encryption functions
        "aead.decrypt_bytes" => Some(FunctionDefinition {
            name: "aead.decrypt_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::Bytes,
                    NodeType::String | NodeType::Bytes,
                ] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in aead.decrypt_bytes function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "aead.decrypt_bytes expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "aead.decrypt_string" => Some(FunctionDefinition {
            name: "aead.decrypt_string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::Bytes,
                    NodeType::String,
                ] => NodeType::String,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in aead.decrypt_string function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "aead.decrypt_string expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "aead.encrypt" => Some(FunctionDefinition {
            name: "aead.encrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::String | NodeType::Bytes,
                    NodeType::String | NodeType::Bytes,
                ] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in aead.encrypt function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!("aead.encrypt expects 3 arguments, but got {}", tys.len());
                    NodeType::Bytes
                }
            }),
        }),
        "deterministic_decrypt_bytes" => Some(FunctionDefinition {
            name: "deterministic_decrypt_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::Bytes,
                    NodeType::String | NodeType::Bytes,
                ] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in deterministic_decrypt_bytes function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "deterministic_decrypt_bytes expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "deterministic_decrypt_string" => Some(FunctionDefinition {
            name: "deterministic_decrypt_string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::String,
                    NodeType::String | NodeType::Bytes,
                ] => NodeType::String,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in deterministic_decrypt_string function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "deterministic_decrypt_string expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "deterministic_encrypt" => Some(FunctionDefinition {
            name: "deterministic_encrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Bytes | NodeType::Struct(_),
                    NodeType::String | NodeType::Bytes,
                    NodeType::String | NodeType::Bytes,
                ] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in deterministic_encrypt function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "deterministic_encrypt expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.add_key_from_raw_bytes" => Some(FunctionDefinition {
            name: "keys.add_key_from_raw_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Bytes] => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in keys.add_key_from_raw_bytes function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.add_key_from_raw_bytes expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.keyset_chain" => Some(FunctionDefinition {
            name: "keys.keyset_chain".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![StructNodeFieldType::new("key", NodeType::Bytes, vec![])],
                });
                match tys {
                    [NodeType::String, NodeType::Bytes] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in keys.keyset_chain function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "keys.keyset_chain expects 2 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "keys.keyset_from_json" => Some(FunctionDefinition {
            name: "keys.keyset_from_json".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::Bytes,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in keys.keyset_from_json function.",
                        t
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.keyset_from_json expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.keyset_length" => Some(FunctionDefinition {
            name: "keys.keyset_length".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes | NodeType::Struct(_)] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in keys.keyset_length function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "keys.keyset_length expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "keys.keyset_to_json" => Some(FunctionDefinition {
            name: "keys.keyset_to_json".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes | NodeType::Struct(_)] => NodeType::String,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in keys.keyset_to_json function.",
                        t
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "keys.keyset_to_json expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "keys.new_keyset" => Some(FunctionDefinition {
            name: "keys.new_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::Bytes,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in keys.new_keyset function.",
                        t
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!("keys.new_keyset expects 1 argument, but got {}", tys.len());
                    NodeType::Bytes
                }
            }),
        }),
        "keys.new_wrapped_keyset" => Some(FunctionDefinition {
            name: "keys.new_wrapped_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String] => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in keys.new_wrapped_keyset function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.new_wrapped_keyset expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.rewrap_keyset" => Some(FunctionDefinition {
            name: "keys.rewrap_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String, NodeType::Bytes] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in keys.rewrap_keyset function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.rewrap_keyset expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.rotate_keyset" => Some(FunctionDefinition {
            name: "keys.rotate_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes | NodeType::Struct(_), NodeType::String] => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in keys.rotate_keyset function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.rotate_keyset expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "keys.rotate_wrapped_keyset" => Some(FunctionDefinition {
            name: "keys.rotate_wrapped_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::Bytes, NodeType::String] => NodeType::Bytes,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in keys.rotate_wrapped_keyset function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "keys.rotate_wrapped_keyset expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        // Aggregate functions
        "any_value" => Some(FunctionDefinition {
            name: "any_value".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [t] => (*t).clone(),
                _ => {
                    log::warn!("any_value expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "array_concat_agg" => Some(FunctionDefinition {
            name: "array_concat_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [t @ NodeType::Array(_)] => (*t).clone(),
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in array_concat_agg function.",
                        t
                    );
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("array_concat_agg expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "avg" => Some(FunctionDefinition {
            name: "avg".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64 | NodeType::Float64] => NodeType::Float64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::Interval] => NodeType::Interval,
                [t] => {
                    log::warn!("Found unexpected input type {} in avg function.", t);
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("avg expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "bit_and" => Some(FunctionDefinition {
            name: "bit_and".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in bit_and function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("bit_and expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "bit_or" => Some(FunctionDefinition {
            name: "bit_or".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in bit_or function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("bit_or expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "bit_xor" => Some(FunctionDefinition {
            name: "bit_xor".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in bit_xor function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("bit_xor expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "count" => Some(FunctionDefinition {
            name: "count".to_owned(),
            compute_return_type: Box::new(|_, _| NodeType::Int64),
        }),
        "countif" => Some(FunctionDefinition {
            name: "countif".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Boolean] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in countif function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("countif expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "grouping" => Some(FunctionDefinition {
            name: "grouping".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [_] => NodeType::Int64,
                _ => {
                    log::warn!("grouping expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "logical_and" => Some(FunctionDefinition {
            name: "logical_and".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Boolean] => NodeType::Boolean,
                [t] => {
                    log::warn!("Found unexpected input type {} in logical_and function.", t);
                    NodeType::Boolean
                }
                _ => {
                    log::warn!("logical_and expects 1 argument, but got {}", tys.len());
                    NodeType::Boolean
                }
            }),
        }),
        "logical_or" => Some(FunctionDefinition {
            name: "logical_or".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Boolean] => NodeType::Boolean,
                [t] => {
                    log::warn!("Found unexpected input type {} in logical_or function.", t);
                    NodeType::Boolean
                }
                _ => {
                    log::warn!("logical_or expects 1 argument, but got {}", tys.len());
                    NodeType::Boolean
                }
            }),
        }),
        "max" => Some(FunctionDefinition {
            name: "max".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Array(_) | NodeType::Struct(_) | NodeType::Geography | NodeType::Json,
                ] => {
                    log::warn!("Found unexpected input type {} in max function.", tys[0]);
                    tys[0].clone()
                }
                [t] => (*t).clone(),
                _ => {
                    log::warn!("max expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "max_by" => Some(FunctionDefinition {
            name: "max_by".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    val,
                    NodeType::Array(_) | NodeType::Struct(_) | NodeType::Geography | NodeType::Json,
                ] => {
                    log::warn!(
                        "Found unexpected input types ({},{}) in max_by function.",
                        val,
                        tys[1]
                    );
                    (*val).clone()
                }
                [val, _] => (*val).clone(),
                _ => {
                    log::warn!("max_by expects 2 arguments, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "min" => Some(FunctionDefinition {
            name: "min".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Array(_) | NodeType::Struct(_) | NodeType::Geography | NodeType::Json,
                ] => {
                    log::warn!("Found unexpected input type {} in min function.", tys[0]);
                    tys[0].clone()
                }
                [t] => (*t).clone(),
                _ => {
                    log::warn!("min expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "min_by" => Some(FunctionDefinition {
            name: "min_by".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    val,
                    NodeType::Array(_) | NodeType::Struct(_) | NodeType::Geography | NodeType::Json,
                ] => {
                    log::warn!(
                        "Found unexpected input types ({},{}) in min_by function.",
                        val,
                        tys[1]
                    );
                    (*val).clone()
                }
                [val, _] => (*val).clone(),
                _ => {
                    log::warn!("min_by expects 2 arguments, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "string_agg" => Some(FunctionDefinition {
            name: "string_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [t @ (NodeType::String | NodeType::Bytes)]
                | [
                    t @ (NodeType::String | NodeType::Bytes),
                    NodeType::String | NodeType::Bytes,
                ] => (*t).clone(),
                [t1, t2] => {
                    log::warn!(
                        "Unexpected types for arguments of string_agg: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Unknown
                }
                [t] => {
                    log::warn!("Unexpected type for first argument of string_agg: {}", t);
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("string_agg expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        "sum" => Some(FunctionDefinition {
            name: "sum".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [NodeType::Float64] => NodeType::Float64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::Interval] => NodeType::Interval,
                [t] => {
                    log::warn!("Found unexpected input type {} in sum function.", t);
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("sum expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),

        // Approximate aggregate functions
        "approx_count_distinct" => Some(FunctionDefinition {
            name: "approx_count_distinct".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_) | NodeType::Struct(_) | NodeType::Interval] => {
                    log::warn!(
                        "Found unexpected input type {} in approx_count_distinct function.",
                        tys[0]
                    );
                    NodeType::Int64
                }
                [_] => NodeType::Int64,
                _ => {
                    log::warn!(
                        "approx_count_distinct expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "approx_quantiles" => Some(FunctionDefinition {
            name: "approx_quantiles".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: tys[0].clone(),
                    input: indices.to_vec(),
                }));

                match tys {
                    [t, NodeType::Int64]
                        if !matches!(
                            t,
                            NodeType::Array(_) | NodeType::Struct(_) | NodeType::Interval
                        ) =>
                    {
                        return_type
                    }
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types {} {} in approx_quantiles function",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "approx_quantiles expects 2 arguments, but got {}",
                            tys.len()
                        );
                        NodeType::Unknown
                    }
                }
            }),
        }),
        "approx_top_count" => Some(FunctionDefinition {
            name: "approx_top_count".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("value", tys[0].clone(), vec![indices[0]]),
                        StructNodeFieldType::new("count", NodeType::Int64, indices.to_vec()),
                    ],
                });

                match tys {
                    [t, NodeType::Int64] if t.is_groupable() => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types {} {} in approx_top_count function",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "approx_top_count expects 2 arguments, but got {}",
                            tys.len()
                        );
                        NodeType::Unknown
                    }
                }
            }),
        }),
        "approx_top_sum" => Some(FunctionDefinition {
            name: "approx_top_sum".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Struct(StructNodeType {
                        fields: vec![
                            StructNodeFieldType::new("value", tys[0].clone(), vec![indices[0]]),
                            StructNodeFieldType::new("sum", NodeType::Int64, indices.to_vec()),
                        ],
                    }),
                    input: vec![],
                }));

                match tys {
                    [
                        t,
                        NodeType::Int64
                        | NodeType::Numeric
                        | NodeType::BigNumeric
                        | NodeType::Float64,
                        NodeType::Int64,
                    ] if t.is_groupable() => return_type,
                    [t1, t2, t3] => {
                        log::warn!(
                            "Unexpected input types {} {} {} in approx_top_sum function",
                            t1,
                            t2,
                            t3
                        );
                        return_type
                    }
                    _ => {
                        log::warn!("approx_top_sum expects 3 arguments, but got {}", tys.len());
                        NodeType::Unknown
                    }
                }
            }),
        }),
        //Array functions
        "array_concat" => Some(FunctionDefinition {
            name: "array_concat".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                if let Some(first_ty) = tys.first() {
                    (*first_ty).clone()
                } else {
                    log::warn!("array_concat expects at least 1 argument, but got 0");
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: NodeType::Unknown,
                        input: indices.to_vec(),
                    }))
                }
            }),
        }),
        "array_first" => Some(FunctionDefinition {
            name: "array_first".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(array_type)] => array_type.r#type.clone(),
                [t] => {
                    log::warn!("Found unexpected input type {} in array_first function.", t);
                    array_type_with_unkown_type()
                }
                _ => {
                    log::warn!("array_first expects 1 argument, but got {}", tys.len());
                    array_type_with_unkown_type()
                }
            }),
        }),
        "array_last" => Some(FunctionDefinition {
            name: "array_last".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(array_type)] => array_type.r#type.clone(),
                [t] => {
                    log::warn!("Found unexpected input type {} in array_last function.", t);
                    array_type_with_unkown_type()
                }
                _ => {
                    log::warn!("array_last expects 1 argument, but got {}", tys.len());
                    array_type_with_unkown_type()
                }
            }),
        }),
        "array_length" => Some(FunctionDefinition {
            name: "array_length".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_)] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in array_length function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("array_length expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "array_reverse" => Some(FunctionDefinition {
            name: "array_reverse".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [t @ NodeType::Array(_)] => (*t).clone(),
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in array_reverse function.",
                        t
                    );
                    array_type_with_unkown_type()
                }
                _ => {
                    log::warn!("array_reverse expects 1 argument, but got {}", tys.len());
                    array_type_with_unkown_type()
                }
            }),
        }),
        "array_slice" => Some(FunctionDefinition {
            name: "array_slice".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [t @ NodeType::Array(_), NodeType::Int64] => (*t).clone(),
                [t @ NodeType::Array(_), NodeType::Int64, NodeType::Int64] => (*t).clone(),
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in array_slice function: ({}, {})",
                        t1,
                        t2
                    );
                    array_type_with_unkown_type()
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in array_slice function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    array_type_with_unkown_type()
                }
                _ => {
                    log::warn!(
                        "array_slice expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    array_type_with_unkown_type()
                }
            }),
        }),
        "array_to_string" => Some(FunctionDefinition {
            name: "array_to_string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(array_node_type), NodeType::String]
                | [
                    NodeType::Array(array_node_type),
                    NodeType::String,
                    NodeType::String,
                ] if matches!(array_node_type.r#type, NodeType::String) => NodeType::String,
                [NodeType::Array(array_node_type), NodeType::Bytes]
                | [
                    NodeType::Array(array_node_type),
                    NodeType::Bytes,
                    NodeType::Bytes,
                ] if matches!(array_node_type.r#type, NodeType::Bytes) => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in array_to_string function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in array_to_string function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "array_to_string expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "generate_array" => Some(FunctionDefinition {
            name: "generate_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| match tys {
                [start, end] if start.is_number() && end.is_number() => {
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: (*start).clone(),
                        input: indices.to_vec(),
                    }))
                }
                [start, end, step] if start.is_number() && end.is_number() && step.is_number() => {
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: (*start).clone(),
                        input: indices.to_vec(),
                    }))
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in generate_array function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: (*t1).clone(),
                        input: indices.to_vec(),
                    }))
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in generate_array function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: (*t1).clone(),
                        input: indices.to_vec(),
                    }))
                }
                _ => {
                    log::warn!(
                        "generate_array expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Unknown
                }
            }),
        }),
        "generate_date_array" => Some(FunctionDefinition {
            name: "generate_date_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Date,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Date, NodeType::Date]
                    | [NodeType::Date, NodeType::Date, NodeType::Interval] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in generate_date_array function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    [t1, t2, t3] => {
                        log::warn!(
                            "Found unexpected input types in generate_date_array function: ({}, {}, {})",
                            t1,
                            t2,
                            t3
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "generate_date_array expects 2 or 3 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "generate_timestamp_array" => Some(FunctionDefinition {
            name: "generate_timestamp_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Timestamp,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Timestamp, NodeType::Timestamp, NodeType::Interval] => return_type,
                    [t1, t2, t3] => {
                        log::warn!(
                            "Found unexpected input types in generate_timestamp_array function: ({}, {}, {})",
                            t1,
                            t2,
                            t3
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "generate_timestamp_array expects 3 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        // Bit functions
        "bit_count" => Some(FunctionDefinition {
            name: "bit_count".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] | [NodeType::Bytes] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in bit_count function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("bit_count expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        // Date functions
        "date" => Some(FunctionDefinition {
            name: "date".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64, NodeType::Int64, NodeType::Int64]
                | [NodeType::Timestamp]
                | [NodeType::Timestamp, NodeType::String]
                | [NodeType::Datetime] => NodeType::Date,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in date function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Date
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in date function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Date
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in date function.", t);
                    NodeType::Date
                }
                _ => {
                    log::warn!("date expects 1, 2 or 3 arguments, but got {}", tys.len());
                    NodeType::Date
                }
            }),
        }),
        "date_add" => Some(FunctionDefinition {
            name: "date_add".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Date, NodeType::Interval] => NodeType::Date,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in date_add function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Date
                }
                _ => {
                    log::warn!("date_add expects 2 arguments, but got {}", tys.len());
                    NodeType::Date
                }
            }),
        }),

        "date_from_unix_date" => Some(FunctionDefinition {
            name: "date_from_unix_date".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Date,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in date_from_unix_date function.",
                        t
                    );
                    NodeType::Date
                }
                _ => {
                    log::warn!(
                        "date_from_unix_date expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Date
                }
            }),
        }),
        "date_sub" => Some(FunctionDefinition {
            name: "date_sub".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Date, NodeType::Interval] => NodeType::Date,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in date_sub function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Date
                }
                _ => {
                    log::warn!("date_sub expects 2 arguments, but got {}", tys.len());
                    NodeType::Date
                }
            }),
        }),
        "format_date" => Some(FunctionDefinition {
            name: "format_date".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::Date] => NodeType::String,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in format_date function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("format_date expects 2 arguments, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "parse_date" => Some(FunctionDefinition {
            name: "parse_date".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String] => NodeType::Date,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in parse_date function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Date
                }
                _ => {
                    log::warn!("parse_date expects 2 arguments, but got {}", tys.len());
                    NodeType::Date
                }
            }),
        }),
        "unix_date" => Some(FunctionDefinition {
            name: "unix_date".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Date] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in unix_date function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("unix_date expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        // Conversion functions
        "parse_bignumeric" => Some(FunctionDefinition {
            name: "parse_bignumeric".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::BigNumeric,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in parse_bignumeric function.",
                        t
                    );
                    NodeType::BigNumeric
                }
                _ => {
                    log::warn!("parse_bignumeric expects 1 argument, but got {}", tys.len());
                    NodeType::BigNumeric
                }
            }),
        }),
        "parse_numeric" => Some(FunctionDefinition {
            name: "parse_numeric".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::Numeric,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in parse_numeric function.",
                        t
                    );
                    NodeType::Numeric
                }
                _ => {
                    log::warn!("parse_numeric expects 1 argument, but got {}", tys.len());
                    NodeType::Numeric
                }
            }),
        }),
        // Datetime functions
        "datetime" => Some(FunctionDefinition {
            name: "datetime".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Int64,
                    NodeType::Int64,
                    NodeType::Int64,
                    NodeType::Int64,
                    NodeType::Int64,
                    NodeType::Int64,
                ]
                | [NodeType::Date, NodeType::Time]
                | [NodeType::Timestamp]
                | [NodeType::Timestamp, NodeType::String] => NodeType::Datetime,
                [t1, t2, t3, t4, t5, t6] => {
                    log::warn!(
                        "Found unexpected input types in datetime function: ({}, {}, {}, {}, {}, {})",
                        t1,
                        t2,
                        t3,
                        t4,
                        t5,
                        t6
                    );
                    NodeType::Datetime
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in datetime function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Datetime
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in datetime function.", t);
                    NodeType::Datetime
                }
                _ => {
                    log::warn!(
                        "datetime has no matching signature for {} arguments",
                        tys.len()
                    );
                    NodeType::Datetime
                }
            }),
        }),
        "datetime_add" => Some(FunctionDefinition {
            name: "datetime_add".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Datetime, NodeType::Interval] => NodeType::Datetime,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in datetime_add function: ({}, {})",
                        t1,
                        t2,
                    );
                    NodeType::Datetime
                }
                _ => {
                    log::warn!("datetime_add expects 2 arguments, but got {}", tys.len());
                    NodeType::Datetime
                }
            }),
        }),
        "datetime_sub" => Some(FunctionDefinition {
            name: "datetime_sub".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Datetime, NodeType::Interval] => NodeType::Datetime,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in datetime_sub function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Datetime
                }
                _ => {
                    log::warn!("datetime_sub expects 2 arguments, but got {}", tys.len());
                    NodeType::Datetime
                }
            }),
        }),
        "format_datetime" => Some(FunctionDefinition {
            name: "format_datetime".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::Datetime] => NodeType::String,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in format_datetime function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("format_datetime expects 2 arguments, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "parse_datetime" => Some(FunctionDefinition {
            name: "parse_datetime".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String] => NodeType::Datetime,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in parse_datetime function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Datetime
                }
                _ => {
                    log::warn!("parse_datetime expects 2 arguments, but got {}", tys.len());
                    NodeType::Datetime
                }
            }),
        }),
        // Time functions
        "time" => Some(FunctionDefinition {
            name: "time".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64, NodeType::Int64, NodeType::Int64]
                | [NodeType::Timestamp]
                | [NodeType::Timestamp, NodeType::String]
                | [NodeType::Datetime] => NodeType::Time,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in time function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Time
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in time function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Time
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in time function.", t);
                    NodeType::Time
                }
                _ => {
                    log::warn!("time has no matching signature for {} arguments", tys.len());
                    NodeType::Time
                }
            }),
        }),
        "time_add" => Some(FunctionDefinition {
            name: "time_add".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Time, NodeType::Interval] => NodeType::Time,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in time_add function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Time
                }
                _ => {
                    log::warn!("time_add expects 2 arguments, but got {}", tys.len());
                    NodeType::Time
                }
            }),
        }),
        "time_sub" => Some(FunctionDefinition {
            name: "time_sub".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Time, NodeType::Interval] => NodeType::Time,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in time_sub function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Time
                }
                _ => {
                    log::warn!("time_sub expects 2 arguments, but got {}", tys.len());
                    NodeType::Time
                }
            }),
        }),
        "format_time" => Some(FunctionDefinition {
            name: "format_time".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::Time] => NodeType::String,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in format_time function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("format_time expects 2 arguments, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "parse_time" => Some(FunctionDefinition {
            name: "parse_time".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String] => NodeType::Time,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in parse_time function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Time
                }
                _ => {
                    log::warn!("parse_time expects 2 arguments, but got {}", tys.len());
                    NodeType::Time
                }
            }),
        }),
        // Timestamp functions
        "format_timestamp" => Some(FunctionDefinition {
            name: "format_timestamp".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::Timestamp]
                | [NodeType::String, NodeType::Timestamp, NodeType::String] => NodeType::String,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in format_timestamp function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in format_timestamp function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "format_timestamp expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "parse_timestamp" => Some(FunctionDefinition {
            name: "parse_timestamp".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String, NodeType::String]
                | [NodeType::String, NodeType::String, NodeType::String] => NodeType::Timestamp,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in parse_timestamp function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Timestamp
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in parse_timestamp function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!(
                        "parse_timestamp expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Timestamp
                }
            }),
        }),
        "string" => Some(FunctionDefinition {
            name: "string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp] | [NodeType::Timestamp, NodeType::String] => NodeType::String,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in string function for timestamp.",
                        t
                    );
                    NodeType::String
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in string function for timestamp: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "string for timestamp expects 1 or 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "timestamp" => Some(FunctionDefinition {
            name: "timestamp".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String]
                | [NodeType::String, NodeType::String]
                | [NodeType::Date]
                | [NodeType::Date, NodeType::String]
                | [NodeType::Datetime]
                | [NodeType::Datetime, NodeType::String] => NodeType::Timestamp,
                [t] => {
                    log::warn!("Found unexpected input type {} in timestamp function.", t);
                    NodeType::Timestamp
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in timestamp function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!("timestamp expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::Timestamp
                }
            }),
        }),
        "timestamp_add" => Some(FunctionDefinition {
            name: "timestamp_add".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp, NodeType::Interval] => NodeType::Timestamp,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in timestamp_add function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!("timestamp_add expects 2 arguments, but got {}", tys.len());
                    NodeType::Timestamp
                }
            }),
        }),
        "timestamp_sub" => Some(FunctionDefinition {
            name: "timestamp_sub".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp, NodeType::Interval] => NodeType::Timestamp,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in timestamp_sub function: ({}, {})",
                        t1,
                        t2,
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!("timestamp_sub expects 2 arguments, but got {}", tys.len());
                    NodeType::Timestamp
                }
            }),
        }),
        "timestamp_micros" => Some(FunctionDefinition {
            name: "timestamp_micros".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Timestamp,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in timestamp_micros function.",
                        t
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!("timestamp_micros expects 1 argument, but got {}", tys.len());
                    NodeType::Timestamp
                }
            }),
        }),
        "timestamp_millis" => Some(FunctionDefinition {
            name: "timestamp_millis".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Timestamp,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in timestamp_millis function.",
                        t
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!("timestamp_millis expects 1 argument, but got {}", tys.len());
                    NodeType::Timestamp
                }
            }),
        }),
        "timestamp_seconds" => Some(FunctionDefinition {
            name: "timestamp_seconds".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Timestamp,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in timestamp_seconds function.",
                        t
                    );
                    NodeType::Timestamp
                }
                _ => {
                    log::warn!(
                        "timestamp_seconds expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Timestamp
                }
            }),
        }),
        "unix_micros" => Some(FunctionDefinition {
            name: "unix_micros".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in unix_micros function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("unix_micros expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "unix_millis" => Some(FunctionDefinition {
            name: "unix_millis".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in unix_millis function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("unix_millis expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "unix_seconds" => Some(FunctionDefinition {
            name: "unix_seconds".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in unix_seconds function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("unix_seconds expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        _ => None,
    }
}
