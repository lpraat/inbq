use crate::arena::ArenaIndex;

use super::{ArrayNodeType, NodeType, StructNodeFieldType, StructNodeType};

pub(crate) struct FunctionDefinition {
    #[allow(dead_code)]
    pub(crate) name: String,
    #[allow(clippy::type_complexity)]
    pub(crate) compute_return_type: Box<dyn Fn(&[&NodeType], &[ArenaIndex]) -> NodeType>,
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
        "array_agg" => Some(FunctionDefinition {
            name: "array_agg".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: tys[0].clone(),
                    input: vec![indices[0]],
                }));
                match tys {
                    [t @ NodeType::Array(_)] => {
                        log::warn!("Found unexpected input type {} in array_agg function.", t);
                        return_type
                    }
                    [_] => return_type,
                    _ => {
                        log::warn!("array_agg expects 1 argument, but got {}", tys.len());
                        NodeType::Unknown
                    }
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
            compute_return_type: Box::new(|tys, _| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: tys[0].clone(),
                    input: vec![],
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
            compute_return_type: Box::new(|tys, _| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("value", tys[0].clone(), vec![]),
                        StructNodeFieldType::new("count", NodeType::Int64, vec![]),
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
            compute_return_type: Box::new(|tys, _| {
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Struct(StructNodeType {
                        fields: vec![
                            StructNodeFieldType::new("value", tys[0].clone(), vec![]),
                            StructNodeFieldType::new("count", NodeType::Int64, vec![]),
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

        // Date functions
        "date" => Some(FunctionDefinition {
            name: "date".to_owned(),
            // todo
            compute_return_type: Box::new(|_, _| NodeType::Date),
        }),
        _ => None,
    }
}
