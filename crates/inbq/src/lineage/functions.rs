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
        // Generic functions
        "string" => Some(FunctionDefinition {
            name: "string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Timestamp] | [NodeType::Timestamp, NodeType::String] => NodeType::String,
                [NodeType::Json] => NodeType::String,
                [t] => {
                    log::warn!("Found unexpected input type {} in string function.", t);
                    NodeType::String
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in string function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("string expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::String
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
        // Debugging functions
        "error" => Some(FunctionDefinition {
            name: "error".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::Unknown,
                [t] => {
                    log::warn!("Found unexpected input type {} in error function.", t);
                    NodeType::Unknown
                }
                _ => {
                    log::warn!("error expects 1 argument, but got {}", tys.len());
                    NodeType::Unknown
                }
            }),
        }),
        // DLP encryption functions
        "dlp_deterministic_encrypt" => Some(FunctionDefinition {
            name: "dlp_deterministic_encrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::String, NodeType::String]
                | [
                    NodeType::Bytes,
                    NodeType::String,
                    NodeType::String,
                    NodeType::String,
                ] => NodeType::String,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in dlp_deterministic_encrypt function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                [t1, t2, t3, t4] => {
                    log::warn!(
                        "Found unexpected input types in dlp_deterministic_encrypt function: ({}, {}, {}, {})",
                        t1,
                        t2,
                        t3,
                        t4
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "dlp_deterministic_encrypt expects 3 or 4 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "dlp_deterministic_decrypt" => Some(FunctionDefinition {
            name: "dlp_deterministic_decrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::String, NodeType::String]
                | [
                    NodeType::Bytes,
                    NodeType::String,
                    NodeType::String,
                    NodeType::String,
                ] => NodeType::String,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in dlp_deterministic_decrypt function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::String
                }
                [t1, t2, t3, t4] => {
                    log::warn!(
                        "Found unexpected input types in dlp_deterministic_decrypt function: ({}, {}, {}, {})",
                        t1,
                        t2,
                        t3,
                        t4
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "dlp_deterministic_decrypt expects 3 or 4 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "dlp_key_chain" => Some(FunctionDefinition {
            name: "dlp_key_chain".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Struct(StructNodeType {
                        fields: vec![StructNodeFieldType::new(
                            "key",
                            tys[0].clone(),
                            indices.to_vec(),
                        )],
                    }),
                    input: vec![],
                }));
                match tys {
                    [NodeType::String, NodeType::Bytes] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in dlp_key_chain function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!("dlp_key_chain expects 2 arguments, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        // Geography Functions
        "s2_cellidfrompoint" => Some(FunctionDefinition {
            name: "s2_cellidfrompoint".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `level`
                NodeType::Int64
            }),
        }),
        "s2_coveringcellids" => Some(FunctionDefinition {
            name: "s2_coveringcellids".to_owned(),
            compute_return_type: Box::new(|_, indices| {
                // TODO: contains named arguments `min_level`, `max_level`, `max_cells`
                NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Int64,
                    input: indices.to_vec(),
                }))
            }),
        }),

        "st_angle" => Some(FunctionDefinition {
            name: "st_angle".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Geography,
                    NodeType::Geography,
                    NodeType::Geography,
                ] => NodeType::Float64,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in st_angle function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!("st_angle expects 3 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "st_area" => Some(FunctionDefinition {
            name: "st_area".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `use_spheroid`
                NodeType::Float64
            }),
        }),
        "st_asbinary" => Some(FunctionDefinition {
            name: "st_asbinary".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Bytes,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_asbinary function.", t);
                    NodeType::Bytes
                }
                _ => {
                    log::warn!("st_asbinary expects 1 argument, but got {}", tys.len());
                    NodeType::Bytes
                }
            }),
        }),
        "st_asgeojson" => Some(FunctionDefinition {
            name: "st_asgeojson".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::String,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_asgeojson function.",
                        t
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("st_asgeojson expects 1 argument, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "st_astext" => Some(FunctionDefinition {
            name: "st_astext".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::String,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_astext function.", t);
                    NodeType::String
                }
                _ => {
                    log::warn!("st_astext expects 1 argument, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "st_azimuth" => Some(FunctionDefinition {
            name: "st_azimuth".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Geography] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_azimuth function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!("st_azimuth expects 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "st_boundary" => Some(FunctionDefinition {
            name: "st_boundary".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_boundary function.", t);
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_boundary expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_boundingbox" => Some(FunctionDefinition {
            name: "st_boundingbox".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("xmin", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("ymin", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("xmax", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("ymax", NodeType::Float64, indices.to_vec()),
                    ],
                });

                match tys {
                    [NodeType::Geography] => return_type,
                    [t] => {
                        log::warn!(
                            "Found unexpected input type {} in st_boundingbox function.",
                            t
                        );
                        return_type
                    }
                    _ => {
                        log::warn!("st_boundingbox expects 1 argument, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        "st_buffer" => Some(FunctionDefinition {
            name: "st_buffer".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named arguments `num_seg_quarter_circle`, `endcap`, `side`, `join`, `mitre_limit`
                NodeType::Geography
            }),
        }),
        "st_bufferwithtolerance" => Some(FunctionDefinition {
            name: "st_bufferwithtolerance".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named arguments `num_seg_quarter_circle`, `endcap`, `side`, `join`, `mitre_limit`
                NodeType::Geography
            }),
        }),
        "st_centroid" => Some(FunctionDefinition {
            name: "st_centroid".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_centroid function.", t);
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_centroid expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_centroid_agg" => Some(FunctionDefinition {
            name: "st_centroid_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_centroid_agg function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_centroid_agg expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_closestpoint" => Some(FunctionDefinition {
            name: "st_closestpoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Geography] => NodeType::Geography,
                [NodeType::Geography, NodeType::Geography, NodeType::Bytes] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_closestpoint function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in st_closestpoint function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_closestpoint expects 2 or 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_clusterdbscan" => Some(FunctionDefinition {
            name: "st_clusterdbscan".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Float64, NodeType::Int64] => NodeType::Int64,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in st_clusterdbscan function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "st_clusterdbscan expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "st_contains" | "st_coveredby" | "st_covers" | "st_disjoint" | "st_equals"
        | "st_intersects" | "st_touches" | "st_within" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: name.to_owned(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Geography, NodeType::Geography] => NodeType::Boolean,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in {} function: ({}, {})",
                            fn_name,
                            t1,
                            t2
                        );
                        NodeType::Boolean
                    }
                    _ => {
                        log::warn!("{} expects 2 arguments, but got {}", fn_name, tys.len());
                        NodeType::Boolean
                    }
                }),
            })
        }
        "st_convexhull" => Some(FunctionDefinition {
            name: "st_convexhull".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] | [NodeType::Array(_)] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_convexhull function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_convexhull expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_difference" => Some(FunctionDefinition {
            name: "st_difference".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Geography] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_difference function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_difference expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_dimension" => Some(FunctionDefinition {
            name: "st_dimension".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_dimension function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("st_dimension expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "st_distance" => Some(FunctionDefinition {
            name: "st_distance".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `use_spheroid`
                NodeType::Float64
            }),
        }),
        "st_dump" => Some(FunctionDefinition {
            name: "st_dump".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Geography,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Geography] => return_type,
                    [NodeType::Geography, NodeType::Int64] => return_type,
                    [t] => {
                        log::warn!("Found unexpected input type {} in st_dump function.", t);
                        return_type
                    }
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in st_dump function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!("st_dump expects 1 argument, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        "st_dwithin" => Some(FunctionDefinition {
            name: "st_dwithin".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `use_spheroid`
                NodeType::Boolean
            }),
        }),
        "st_endpoint" => Some(FunctionDefinition {
            name: "st_endpoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_endpoint function.", t);
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_endpoint expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_extent" => Some(FunctionDefinition {
            name: "st_extent".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("xmin", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("ymin", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("xmax", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("ymax", NodeType::Float64, indices.to_vec()),
                    ],
                });
                match tys {
                    [NodeType::Geography] => return_type,
                    [t] => {
                        log::warn!("Found unexpected input type {} in st_extent function.", t);
                        return_type
                    }
                    _ => {
                        log::warn!("st_extent expects 1 argument, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        "st_exteriorring" => Some(FunctionDefinition {
            name: "st_exteriorring".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_exteriorring function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_exteriorring expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_geogfrom" => Some(FunctionDefinition {
            name: "st_geogfrom".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String | NodeType::Bytes] => NodeType::Geography,
                [t] => {
                    log::warn!("Found unexpected input type {} in st_geogfrom function.", t);
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_geogfrom expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_geogfromgeojson" => Some(FunctionDefinition {
            name: "st_geogfromgeojson".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `make_valid`
                NodeType::Geography
            }),
        }),
        "st_geogfromtext" => Some(FunctionDefinition {
            name: "st_geogfromtext".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named arguments `oriented`, `planar`, `make_valid`
                NodeType::Geography
            }),
        }),
        "st_geogfromwkb" => Some(FunctionDefinition {
            name: "st_geogfromwkb".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named arguments `oriented`, `planar`, `make_valid`
                NodeType::Geography
            }),
        }),
        "st_geogpoint" => Some(FunctionDefinition {
            name: "st_geogpoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Float64, NodeType::Float64] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_geogpoint function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_geogpoint expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_geogpointfromgeohash" => Some(FunctionDefinition {
            name: "st_geogpointfromgeohash".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_geogpointfromgeohash function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_geogpointfromgeohash expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_geohash" => Some(FunctionDefinition {
            name: "st_geohash".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] | [NodeType::Geography, NodeType::Int64] => NodeType::String,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_geohash function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::String
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in st_geohash function.", t);
                    NodeType::String
                }
                _ => {
                    log::warn!("st_geohash expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "st_geometrytype" => Some(FunctionDefinition {
            name: "st_geometrytype".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::String,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_geometrytype function.",
                        t
                    );
                    NodeType::String
                }
                _ => {
                    log::warn!("st_geometrytype expects 1 argument, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "st_hausdorffdistance" => Some(FunctionDefinition {
            name: "st_hausdorffdistance".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `directed`
                NodeType::Float64
            }),
        }),
        "st_hausdorffdwithin" => Some(FunctionDefinition {
            name: "st_hausdorffdwithin".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `directed`
                NodeType::Boolean
            }),
        }),
        "st_interiorrings" => Some(FunctionDefinition {
            name: "st_interiorrings".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Geography,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Geography] => return_type,
                    [t] => {
                        log::warn!(
                            "Found unexpected input type {} in st_interiorrings function.",
                            t
                        );
                        return_type
                    }
                    _ => {
                        log::warn!("st_interiorrings expects 1 argument, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        "st_intersection" => Some(FunctionDefinition {
            name: "st_intersection".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Geography] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_intersection function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_intersection expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_intersectsbox" => Some(FunctionDefinition {
            name: "st_intersectsbox".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Geography,
                    NodeType::Float64,
                    NodeType::Float64,
                    NodeType::Float64,
                    NodeType::Float64,
                ] => NodeType::Boolean,
                [t1, t2, t3, t4, t5] => {
                    log::warn!(
                        "Found unexpected input types in st_intersectsbox function: ({}, {}, {}, {}, {})",
                        t1,
                        t2,
                        t3,
                        t4,
                        t5
                    );
                    NodeType::Boolean
                }
                _ => {
                    log::warn!(
                        "st_intersectsbox expects 5 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Boolean
                }
            }),
        }),
        "st_isclosed" | "st_iscollection" | "st_isempty" | "st_isring" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: name.to_owned(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Geography] => NodeType::Boolean,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Boolean
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Boolean
                    }
                }),
            })
        }
        "st_length" => Some(FunctionDefinition {
            name: "st_length".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `use_spheroid`
                NodeType::Float64
            }),
        }),
        "st_lineinterpolatepoint" => Some(FunctionDefinition {
            name: "st_lineinterpolatepoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Float64] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_lineinterpolatepoint function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_lineinterpolatepoint expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_linelocatepoint" => Some(FunctionDefinition {
            name: "st_linelocatepoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Geography] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_linelocatepoint function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "st_linelocatepoint expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Float64
                }
            }),
        }),
        "st_linesubstring" => Some(FunctionDefinition {
            name: "st_linesubstring".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Float64, NodeType::Float64] => NodeType::Geography,
                [t1, t2, t3] => {
                    log::warn!(
                        "Found unexpected input types in st_linesubstring function: ({}, {}, {})",
                        t1,
                        t2,
                        t3
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_linesubstring expects 3 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_makeline" => Some(FunctionDefinition {
            name: "st_makeline".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_)] | [NodeType::Geography, NodeType::Geography] => {
                    NodeType::Geography
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in st_makeline function.", t);
                    NodeType::Geography
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_makeline function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_makeline expects 1 or 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_makepolygon" => Some(FunctionDefinition {
            name: "st_makepolygon".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] | [NodeType::Geography, NodeType::Array(_)] => {
                    NodeType::Geography
                }
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_makepolygon function.",
                        t
                    );
                    NodeType::Geography
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_makepolygon function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_makepolygon expects 1 or 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_makepolygonoriented" => Some(FunctionDefinition {
            name: "st_makepolygonoriented".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_)] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_makepolygonoriented function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!(
                        "st_makepolygonoriented expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Geography
                }
            }),
        }),
        "st_maxdistance" => Some(FunctionDefinition {
            name: "st_maxdistance".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named argument `use_spheroid`
                NodeType::Float64
            }),
        }),
        "st_npoints" | "st_numpoints" => Some(FunctionDefinition {
            name: name.to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in point count function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "point count function expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "st_numgeometries" => Some(FunctionDefinition {
            name: "st_numgeometries".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_numgeometries function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("st_numgeometries expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "st_perimeter" => Some(FunctionDefinition {
            name: "st_perimeter".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] | [NodeType::Geography, NodeType::Boolean] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_perimeter function.",
                        t
                    );
                    NodeType::Int64
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_perimeter function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("st_perimeter expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "st_pointn" => Some(FunctionDefinition {
            name: "st_pointn".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Int64] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_pointn function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_pointn expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_regionstats" => Some(FunctionDefinition {
            name: "st_regionstats".to_owned(),
            compute_return_type: Box::new(|_, indices| {
                // TODO: contains named argument `band`, `include`, `options`
                NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("count", NodeType::Int64, indices.to_vec()),
                        StructNodeFieldType::new("min", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("max", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("stdDev", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("sum", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("mean", NodeType::Float64, indices.to_vec()),
                        StructNodeFieldType::new("area", NodeType::Float64, indices.to_vec()),
                    ],
                })
            }),
        }),
        "st_simplify" => Some(FunctionDefinition {
            name: "st_simplify".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Float64] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_simplify function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_simplify expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_snaptogrid" => Some(FunctionDefinition {
            name: "st_snaptogrid".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography, NodeType::Float64] => NodeType::Geography,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_snaptogrid function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_snaptogrid expects 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_startpoint" => Some(FunctionDefinition {
            name: "st_startpoint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_startpoint function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_startpoint expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_union" => Some(FunctionDefinition {
            name: "st_union".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_)] | [NodeType::Geography, NodeType::Geography] => {
                    NodeType::Geography
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in st_union function.", t);
                    NodeType::Geography
                }
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in st_union function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_union expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_union_agg" => Some(FunctionDefinition {
            name: "st_union_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Geography] => NodeType::Geography,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in st_union_agg function.",
                        t
                    );
                    NodeType::Geography
                }
                _ => {
                    log::warn!("st_union_agg expects 1 argument, but got {}", tys.len());
                    NodeType::Geography
                }
            }),
        }),
        "st_x" | "st_y" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: name.to_owned(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Geography] => NodeType::Float64,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        // Hash functions
        "farm_fingerprint" => Some(FunctionDefinition {
            name: "farm_fingerprint".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::String | NodeType::Bytes] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in farm_fingerprint function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("farm_fingerprint expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "md5" | "sha1" | "sha256" | "sha512" => {
            let name = name.to_lowercase();
            Some(FunctionDefinition {
                name: name.to_owned(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::String | NodeType::Bytes] => NodeType::Bytes,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, name);
                        NodeType::Bytes
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", name, tys.len());
                        NodeType::Bytes
                    }
                }),
            })
        }
        // HLL Functions
        "hll_count.extract" => Some(FunctionDefinition {
            name: "hll_count.extract".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in hll_count.extract function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "hll_count.extract expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "hll_count.init" => Some(FunctionDefinition {
            name: "hll_count.init".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::String
                    | NodeType::Bytes,
                ] => NodeType::Bytes,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in hll_count.init function.",
                        t
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!("hll_count.init expects 1 argument, but got {}", tys.len());
                    NodeType::Bytes
                }
            }),
        }),
        "hll_count.merge" => Some(FunctionDefinition {
            name: "hll_count.merge".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes] => NodeType::Int64,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in hll_count.merge function.",
                        t
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("hll_count.merge expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "hll_count.merge_partial" => Some(FunctionDefinition {
            name: "hll_count.merge_partial".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes] => NodeType::Bytes,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in hll_count.merge_partial function.",
                        t
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "hll_count.merge_partial expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        // Interval functions
        "make_interval" => Some(FunctionDefinition {
            name: "make_interval".to_owned(),
            compute_return_type: Box::new(|_, _| {
                // TODO: contains named arguments `year`, `month`, `day`, `hour`, `minute`, `second`
                NodeType::Interval
            }),
        }),
        "justify_days" => Some(FunctionDefinition {
            name: "justify_days".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Interval] => NodeType::Interval,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in justify_days function.",
                        t
                    );
                    NodeType::Interval
                }
                _ => {
                    log::warn!("justify_days expects 1 argument, but got {}", tys.len());
                    NodeType::Interval
                }
            }),
        }),
        "justify_hours" => Some(FunctionDefinition {
            name: "justify_hours".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Interval] => NodeType::Interval,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in justify_hours function.",
                        t
                    );
                    NodeType::Interval
                }
                _ => {
                    log::warn!("justify_hours expects 1 argument, but got {}", tys.len());
                    NodeType::Interval
                }
            }),
        }),
        "justify_interval" => Some(FunctionDefinition {
            name: "justify_interval".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Interval] => NodeType::Interval,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in justify_interval function.",
                        t
                    );
                    NodeType::Interval
                }
                _ => {
                    log::warn!("justify_interval expects 1 argument, but got {}", tys.len());
                    NodeType::Interval
                }
            }),
        }),
        // JSON Functions
        "bool" => Some(FunctionDefinition {
            name: "bool".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::Boolean,
                [t] => {
                    log::warn!("Found unexpected input type {} in bool function.", t);
                    NodeType::Boolean
                }
                _ => {
                    log::warn!("bool expects 1 argument, but got {}", tys.len());
                    NodeType::Boolean
                }
            }),
        }),
        "float64" => Some(FunctionDefinition {
            name: "float64".to_owned(),
            // TODO: contains named arguments `wide_number_mode`
            compute_return_type: Box::new(|_, _| NodeType::Float64),
        }),
        "int64" => Some(FunctionDefinition {
            name: "int64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::Int64,
                [t] => {
                    log::warn!("Found unexpected input type {} in int64 function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("int64 expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "json_array" => Some(FunctionDefinition {
            name: "json_array".to_owned(),
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_array_append" => Some(FunctionDefinition {
            name: "json_array_append".to_owned(),
            // TODO: contains named arguments `append_each_element`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_array_insert" => Some(FunctionDefinition {
            name: "json_array_insert".to_owned(),
            // TODO: contains named arguments `insert_each_element`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_extract" => Some(FunctionDefinition {
            name: "json_extract".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json, NodeType::String] => NodeType::Json,
                [NodeType::String, NodeType::String] => NodeType::String,
                [t1, t2] => {
                    log::warn!("Unexpected types in json_extract: ({}, {})", t1, t2);
                    if matches!(t1, NodeType::Json) {
                        NodeType::Json
                    } else {
                        NodeType::String
                    }
                }
                _ => {
                    log::warn!("json_extract expects 2 arguments, but got {}", tys.len());
                    NodeType::Json
                }
            }),
        }),
        "json_extract_array" => Some(FunctionDefinition {
            name: "json_extract_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let make_array = |elem_type: NodeType| {
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: elem_type,
                        input: indices.to_vec(),
                    }))
                };

                match tys {
                    [NodeType::Json] | [NodeType::Json, NodeType::String] => {
                        make_array(NodeType::Json)
                    }
                    [NodeType::String] | [NodeType::String, NodeType::String] => {
                        make_array(NodeType::String)
                    }
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types ({}, {}) in json_extract_array",
                            t1,
                            t2
                        );
                        if matches!(t1, NodeType::Json) {
                            make_array(NodeType::Json)
                        } else {
                            make_array(NodeType::String)
                        }
                    }
                    [t] => {
                        log::warn!("Unexpected input type {} in json_extract_array", t);
                        make_array(NodeType::String)
                    }
                    _ => {
                        log::warn!(
                            "json_extract_array expects 1 or 2 arguments, but got {}",
                            tys.len()
                        );
                        make_array(NodeType::String)
                    }
                }
            }),
        }),
        "json_extract_scalar" => Some(FunctionDefinition {
            name: "json_extract_scalar".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json | NodeType::String]
                | [NodeType::Json | NodeType::String, NodeType::String] => NodeType::String,
                [t, ..] => {
                    log::warn!("Unexpected input type {} in json_extract_scalar", t);
                    NodeType::String
                }
                _ => {
                    log::warn!(
                        "json_extract_scalar expects 1 or 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        "json_extract_string_array" => Some(FunctionDefinition {
            name: "json_extract_string_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::String,
                    input: indices.to_vec(),
                }));

                match tys {
                    [NodeType::Json] | [NodeType::Json, NodeType::String] => return_type,
                    [NodeType::String] | [NodeType::String, NodeType::String] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types ({}, {}) in json_extract_string_array",
                            t1,
                            t2
                        );
                        return_type
                    }
                    [t] => {
                        log::warn!("Unexpected input type {} in json_extract_string_array", t);
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "json_extract_string_array expects 1 or 2 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "json_flatten" => Some(FunctionDefinition {
            name: "json_flatten".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Json,
                    input: indices.to_vec(),
                }));

                match tys {
                    [NodeType::Json] => NodeType::Json,
                    [t] => {
                        log::warn!("Unexpected input type {} in json_flatten", t);
                        return_type
                    }
                    _ => {
                        log::warn!("json_flatten expects 1 argument, but got {}", tys.len());
                        return_type
                    }
                }
            }),
        }),
        "json_keys" => Some(FunctionDefinition {
            name: "json_keys".to_owned(),
            compute_return_type: Box::new(|_, indices| {
                // TODO: contains named arguments `max_depth`, `mode`
                NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::String,
                    input: indices.to_vec(),
                }))
            }),
        }),
        "json_object" => Some(FunctionDefinition {
            name: "json_object".to_owned(),
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_query" => Some(FunctionDefinition {
            name: "json_query".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json, NodeType::String] => NodeType::Json,
                [NodeType::String, NodeType::String] => NodeType::String,
                [t1, t2] => {
                    log::warn!("Unexpected types in json_query: ({}, {})", t1, t2);
                    if matches!(t1, NodeType::Json) {
                        NodeType::Json
                    } else {
                        NodeType::String
                    }
                }
                _ => {
                    log::warn!("json_query expects 2 arguments, but got {}", tys.len());
                    NodeType::Json
                }
            }),
        }),
        "json_query_array" => Some(FunctionDefinition {
            name: "json_query_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let make_array = |elem_type: NodeType| {
                    NodeType::Array(Box::new(ArrayNodeType {
                        r#type: elem_type,
                        input: indices.to_vec(),
                    }))
                };

                match tys {
                    [NodeType::Json] | [NodeType::Json, NodeType::String] => {
                        make_array(NodeType::Json)
                    }
                    [NodeType::String] | [NodeType::String, NodeType::String] => {
                        make_array(NodeType::String)
                    }
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types ({}, {}) in json_query_array",
                            t1,
                            t2
                        );
                        if matches!(t1, NodeType::Json) {
                            make_array(NodeType::Json)
                        } else {
                            make_array(NodeType::String)
                        }
                    }
                    [t] => {
                        log::warn!("Unexpected input type {} in json_query_array", t);
                        make_array(NodeType::String)
                    }
                    _ => {
                        log::warn!(
                            "json_query_array expects 1 or 2 arguments, but got {}",
                            tys.len()
                        );
                        make_array(NodeType::String)
                    }
                }
            }),
        }),
        "json_remove" => Some(FunctionDefinition {
            name: "json_remove".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json, ..] => NodeType::Json,
                [t, ..] => {
                    log::warn!("Unexpected input type {} in json_remove", t);
                    NodeType::Json
                }
                _ => {
                    log::warn!(
                        "json_remove expects at least 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Json
                }
            }),
        }),
        "json_set" => Some(FunctionDefinition {
            name: "json_set".to_owned(),
            // TODO: contains named arguments `create_if_missing`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_strip_nulls" => Some(FunctionDefinition {
            name: "json_strip_nulls".to_owned(),
            // TODO: contains named arguments `max_depth`, `mode`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "json_type" => Some(FunctionDefinition {
            name: "json_type".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::String,
                [t] => {
                    log::warn!("Unexpected input type {} in json_type", t);
                    NodeType::String
                }
                _ => {
                    log::warn!("json_type expects 1 argument, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "json_value" => Some(FunctionDefinition {
            name: "json_value".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json | NodeType::String]
                | [NodeType::Json | NodeType::String, NodeType::String] => NodeType::String,
                [t, ..] => {
                    log::warn!("Unexpected input type {} in json_value", t);
                    NodeType::String
                }
                _ => {
                    log::warn!("json_value expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "json_value_array" => Some(FunctionDefinition {
            name: "json_value_array".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::String,
                    input: indices.to_vec(),
                }));

                match tys {
                    [NodeType::Json] | [NodeType::Json, NodeType::String] => return_type,
                    [NodeType::String] | [NodeType::String, NodeType::String] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Unexpected input types ({}, {}) in json_value_array",
                            t1,
                            t2
                        );
                        return_type
                    }
                    [t] => {
                        log::warn!("Unexpected input type {} in json_value_array", t);
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "json_value_array expects 1 or 2 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "lax_bool" => Some(FunctionDefinition {
            name: "lax_bool".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::Boolean,
                [t] => {
                    log::warn!("Unexpected input type {} in lax_bool", t);
                    NodeType::Boolean
                }
                _ => {
                    log::warn!("lax_bool expects 1 argument, but got {}", tys.len());
                    NodeType::Boolean
                }
            }),
        }),
        "lax_float64" => Some(FunctionDefinition {
            name: "lax_float64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::Float64,
                [t] => {
                    log::warn!("Unexpected input type {} in lax_float64", t);
                    NodeType::Float64
                }
                _ => {
                    log::warn!("lax_float64 expects 1 argument, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "lax_int64" => Some(FunctionDefinition {
            name: "lax_int64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::Int64,
                [t] => {
                    log::warn!("Unexpected input type {} in lax_int64", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("lax_int64 expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "lax_string" => Some(FunctionDefinition {
            name: "lax_string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Json] => NodeType::String,
                [t] => {
                    log::warn!("Unexpected input type {} in lax_string", t);
                    NodeType::String
                }
                _ => {
                    log::warn!("lax_string expects 1 argument, but got {}", tys.len());
                    NodeType::String
                }
            }),
        }),
        "parse_json" => Some(FunctionDefinition {
            name: "parse_json".to_owned(),
            // TODO: contains named arguments `wide_number_mode`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "to_json" => Some(FunctionDefinition {
            name: "to_json".to_owned(),
            // TODO: contains named arguments `stringify_wide_numbers`
            compute_return_type: Box::new(|_, _| NodeType::Json),
        }),
        "to_json_string" => Some(FunctionDefinition {
            name: "to_json_string".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [_, NodeType::Boolean] | [_] => NodeType::String,
                _ => {
                    log::warn!(
                        "to_json_string expects 1 or 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::String
                }
            }),
        }),
        // KLL Quantiles functions
        "kll_quantiles.extract_int64" => Some(FunctionDefinition {
            name: "kll_quantiles.extract_int64".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Int64,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Bytes, NodeType::Int64] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in kll_quantiles.extract_int64 function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "kll_quantiles.extract_int64 expects 2 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "kll_quantiles.extract_float64" => Some(FunctionDefinition {
            name: "kll_quantiles.extract_float64".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Float64,
                    input: indices.to_vec(),
                }));
                match tys {
                    [NodeType::Bytes, NodeType::Int64] => return_type,
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in kll_quantiles.extract_float64 function: ({}, {})",
                            t1,
                            t2
                        );
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "kll_quantiles.extract_float64 expects 2 arguments, but got {}",
                            tys.len()
                        );
                        return_type
                    }
                }
            }),
        }),
        "kll_quantiles.extract_point_int64" => Some(FunctionDefinition {
            name: "kll_quantiles.extract_point_int64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Float64] => NodeType::Int64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.extract_point_int64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.extract_point_int64 expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "kll_quantiles.extract_point_float64" => Some(FunctionDefinition {
            name: "kll_quantiles.extract_point_float64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Float64] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.extract_point_float64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.extract_point_float64 expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Float64
                }
            }),
        }),
        "kll_quantiles.init_int64" => Some(FunctionDefinition {
            name: "kll_quantiles.init_int64".to_owned(),
            // TODO: contains named arguments `weight`
            compute_return_type: Box::new(|_, _| NodeType::Bytes),
        }),
        "kll_quantiles.init_float64" => Some(FunctionDefinition {
            name: "kll_quantiles.init_float64".to_owned(),
            // TODO: contains named arguments `weight`
            compute_return_type: Box::new(|_, _| NodeType::Bytes),
        }),
        "kll_quantiles.merge_int64" => Some(FunctionDefinition {
            name: "kll_quantiles.merge_int64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Int64] => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.merge_int64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.merge_int64 expects 2 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "kll_quantiles.merge_float64" => Some(FunctionDefinition {
            name: "kll_quantiles.merge_float64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Int64] => NodeType::Bytes,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.merge_float64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.merge_float64 expects 2 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "kll_quantiles.merge_partial" => Some(FunctionDefinition {
            name: "kll_quantiles.merge_partial".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes] => NodeType::Bytes,
                [t] => {
                    log::warn!(
                        "Found unexpected input type {} in kll_quantiles.merge_partial function.",
                        t
                    );
                    NodeType::Bytes
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.merge_partial expects 1 argument, but got {}",
                        tys.len()
                    );
                    NodeType::Bytes
                }
            }),
        }),
        "kll_quantiles.merge_point_int64" => Some(FunctionDefinition {
            name: "kll_quantiles.merge_point_int64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Float64] => NodeType::Int64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.merge_point_int64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.merge_point_int64 expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Int64
                }
            }),
        }),
        "kll_quantiles.merge_point_float64" => Some(FunctionDefinition {
            name: "kll_quantiles.merge_point_float64".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Bytes, NodeType::Float64] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in kll_quantiles.merge_point_float64 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "kll_quantiles.merge_point_float64 expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Float64
                }
            }),
        }),
        // Mathematical functions
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
        "acos" | "acosh" | "asin" | "asinh" | "atan" | "atanh" | "cbrt" | "cos" | "cosh"
        | "cot" | "coth" | "csc" | "csch" | "sec" | "sech" | "sin" | "sinh" | "tan" | "tanh" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [
                        NodeType::Int64
                        | NodeType::Float64
                        | NodeType::Numeric
                        | NodeType::BigNumeric,
                    ] => NodeType::Float64,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        "atan2" => Some(FunctionDefinition {
            name: "atan2".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                ] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in atan2 function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!("atan2 expects 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "ceil" | "ceiling" | "floor" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Float64] => NodeType::Float64,
                    [NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::Int64] => NodeType::Float64, // BigQuery promotes INT to FLOAT for these
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        "cosine_distance" => Some(FunctionDefinition {
            name: "cosine_distance".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_), NodeType::Array(_)] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in cosine_distance function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!("cosine_distance expects 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "euclidean_distance" => Some(FunctionDefinition {
            name: "euclidean_distance".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Array(_), NodeType::Array(_)] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in euclidean_distance function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!(
                        "euclidean_distance expects 2 arguments, but got {}",
                        tys.len()
                    );
                    NodeType::Float64
                }
            }),
        }),
        "trunc" => Some(FunctionDefinition {
            name: "trunc".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Float64] | [NodeType::Float64, NodeType::Int64] => NodeType::Float64,
                [NodeType::Numeric] | [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                [NodeType::BigNumeric] | [NodeType::BigNumeric, NodeType::Int64] => {
                    NodeType::BigNumeric
                }
                [NodeType::Int64] | [NodeType::Int64, NodeType::Int64] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in trunc function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                [t] => {
                    log::warn!("Found unexpected input type {} in trunc function.", t);
                    NodeType::Float64
                }
                _ => {
                    log::warn!("trunc expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "div" => Some(FunctionDefinition {
            name: "div".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64, NodeType::Int64] => NodeType::Int64,
                [NodeType::Numeric, NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::Int64, NodeType::Numeric] => NodeType::Numeric,
                [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                [NodeType::Int64, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::BigNumeric, NodeType::Int64] => NodeType::BigNumeric,
                [NodeType::Numeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::BigNumeric, NodeType::Numeric] => NodeType::BigNumeric,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in div function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("div expects 2 arguments, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "exp" | "ln" | "log10" | "sqrt" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Int64 | NodeType::Float64] => NodeType::Float64,
                    [NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::BigNumeric] => NodeType::BigNumeric,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        "greatest" | "least" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| {
                    tys.iter().fold(NodeType::Unknown, |acc, &e| {
                        acc.common_supertype_with(e).unwrap_or(NodeType::Unknown)
                    })
                }),
            })
        }
        "ieee_divide" => Some(FunctionDefinition {
            name: "ieee_divide".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                ] => NodeType::Float64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in ieee_divide function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Float64
                }
                _ => {
                    log::warn!("ieee_divide expects 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "is_inf" | "is_nan" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [
                        NodeType::Float64
                        | NodeType::Int64
                        | NodeType::Numeric
                        | NodeType::BigNumeric,
                    ] => NodeType::Boolean,
                    [t] => {
                        log::warn!("Found unexpected input type {} in {} function.", t, fn_name);
                        NodeType::Boolean
                    }
                    _ => {
                        log::warn!("{} expects 1 argument, but got {}", fn_name, tys.len());
                        NodeType::Boolean
                    }
                }),
            })
        }
        "log" => Some(FunctionDefinition {
            name: "log".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                ] => NodeType::Float64,
                [
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                    NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric,
                ] => NodeType::Float64,
                _ => {
                    log::warn!("log expects 1 or 2 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "mod" => Some(FunctionDefinition {
            name: "mod".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64, NodeType::Int64] => NodeType::Int64,
                [NodeType::Numeric, NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::Int64, NodeType::Numeric] => NodeType::Numeric,
                [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                [NodeType::Int64, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::BigNumeric, NodeType::Int64] => NodeType::BigNumeric,
                [NodeType::Numeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                [NodeType::BigNumeric, NodeType::Numeric] => NodeType::BigNumeric,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in mod function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("mod expects 2 arguments, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "pow" | "power" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    [NodeType::Int64, NodeType::Int64] => NodeType::Float64,
                    [NodeType::Int64, NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::Int64, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::Int64, NodeType::Float64] => NodeType::Float64,
                    [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                    [NodeType::Numeric, NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::Numeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::Numeric, NodeType::Float64] => NodeType::Float64,
                    [NodeType::BigNumeric, NodeType::Int64] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::Numeric] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::Float64] => NodeType::Float64,
                    [NodeType::Float64, NodeType::Int64] => NodeType::Float64,
                    [NodeType::Float64, NodeType::Numeric] => NodeType::Float64,
                    [NodeType::Float64, NodeType::BigNumeric] => NodeType::Float64,
                    [NodeType::Float64, NodeType::Float64] => NodeType::Float64,

                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in {} function: ({}, {})",
                            fn_name,
                            t1,
                            t2
                        );
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 2 arguments, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        "rand" => Some(FunctionDefinition {
            name: "rand".to_owned(),
            compute_return_type: Box::new(|_, _| NodeType::Float64),
        }),
        "range_bucket" => Some(FunctionDefinition {
            name: "range_bucket".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [_, NodeType::Array(_)] => NodeType::Int64,
                [t1, t2] => {
                    log::warn!(
                        "Found unexpected input types in range_bucket function: ({}, {})",
                        t1,
                        t2
                    );
                    NodeType::Int64
                }
                _ => {
                    log::warn!("range_bucket expects 2 arguments, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "round" => Some(FunctionDefinition {
            name: "round".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64 | NodeType::Float64] => NodeType::Float64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,

                [NodeType::Int64 | NodeType::Float64, NodeType::Int64] => NodeType::Float64,
                [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                [NodeType::BigNumeric, NodeType::Int64] => NodeType::BigNumeric,

                [
                    NodeType::Int64 | NodeType::Float64,
                    NodeType::Int64,
                    NodeType::String,
                ] => NodeType::Float64,
                [NodeType::Numeric, NodeType::Int64, NodeType::String] => NodeType::Numeric,
                [NodeType::BigNumeric, NodeType::Int64, NodeType::String] => NodeType::BigNumeric,

                [t, ..] => {
                    log::warn!(
                        "Found unexpected input types in round function. First argument type: {}",
                        t
                    );
                    match t {
                        NodeType::Numeric => NodeType::Numeric,
                        NodeType::BigNumeric => NodeType::BigNumeric,
                        _ => NodeType::Float64,
                    }
                }
                _ => {
                    log::warn!("round expects 1, 2, or 3 arguments, but got {}", tys.len());
                    NodeType::Float64
                }
            }),
        }),
        "safe_add" | "safe_subtract" | "safe_multiply" | "safe_divide" => {
            let fn_name = name.to_lowercase();
            Some(FunctionDefinition {
                name: fn_name.clone(),
                compute_return_type: Box::new(move |tys, _| match tys {
                    // Row 1: Left argument INT64
                    [NodeType::Int64, NodeType::Int64] => NodeType::Int64,
                    [NodeType::Int64, NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::Int64, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::Int64, NodeType::Float64] => NodeType::Float64,

                    // Row 2: Left argument NUMERIC
                    [NodeType::Numeric, NodeType::Int64] => NodeType::Numeric,
                    [NodeType::Numeric, NodeType::Numeric] => NodeType::Numeric,
                    [NodeType::Numeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::Numeric, NodeType::Float64] => NodeType::Float64,

                    // Row 3: Left argument BIGNUMERIC
                    [NodeType::BigNumeric, NodeType::Int64] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::Numeric] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::BigNumeric] => NodeType::BigNumeric,
                    [NodeType::BigNumeric, NodeType::Float64] => NodeType::Float64,

                    // Row 4: Left argument FLOAT64
                    [NodeType::Float64, NodeType::Int64] => NodeType::Float64,
                    [NodeType::Float64, NodeType::Numeric] => NodeType::Float64,
                    [NodeType::Float64, NodeType::BigNumeric] => NodeType::Float64,
                    [NodeType::Float64, NodeType::Float64] => NodeType::Float64,

                    // Fallback / Error logging
                    [t1, t2] => {
                        log::warn!(
                            "Found unexpected input types in {} function: ({}, {})",
                            fn_name,
                            t1,
                            t2
                        );
                        // Default fallback, likely Float64 or Unknown
                        NodeType::Float64
                    }
                    _ => {
                        log::warn!("{} expects 2 arguments, but got {}", fn_name, tys.len());
                        NodeType::Float64
                    }
                }),
            })
        }
        "safe_negate" => Some(FunctionDefinition {
            name: "safe_negate".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [NodeType::Float64] => NodeType::Float64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,
                [t] => {
                    log::warn!("Found unexpected input type {} in safe_negate function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("safe_negate expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        "sign" => Some(FunctionDefinition {
            name: "sign".to_owned(),
            compute_return_type: Box::new(|tys, _| match tys {
                [NodeType::Int64] => NodeType::Int64,
                [NodeType::Float64] => NodeType::Float64,
                [NodeType::Numeric] => NodeType::Numeric,
                [NodeType::BigNumeric] => NodeType::BigNumeric,
                [t] => {
                    log::warn!("Found unexpected input type {} in sign function.", t);
                    NodeType::Int64
                }
                _ => {
                    log::warn!("sign expects 1 argument, but got {}", tys.len());
                    NodeType::Int64
                }
            }),
        }),
        _ => None,
    }
}
