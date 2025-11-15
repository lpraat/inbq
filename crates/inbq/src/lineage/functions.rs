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
            compute_return_type: Box::new(|tys, _| match tys[0] {
                NodeType::Int64 => NodeType::Int64,
                NodeType::Numeric => NodeType::Numeric,
                NodeType::BigNumeric => NodeType::BigNumeric,
                NodeType::Float64 => NodeType::Float64,
                _ => {
                    log::warn!("Found unexpected input type {} in abs function.", tys[0]);
                    NodeType::Unknown
                }
            }),
        }),
        // AEAD encryption functions
        "aead.decrypt_bytes" => Some(FunctionDefinition {
            name: "aead.decrypt_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "aead.decrypt_bytes expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match tys {
                    [
                        NodeType::Bytes | NodeType::Struct(_),
                        NodeType::Bytes,
                        NodeType::String | NodeType::Bytes,
                    ] => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in aead.decrypt_bytes function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "aead.decrypt_string" => Some(FunctionDefinition {
            name: "aead.decrypt_string".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "aead.decrypt_string expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::String;
                }
                match (tys[0], tys[1], tys[2]) {
                    (NodeType::Bytes | NodeType::Struct(_), NodeType::Bytes, NodeType::String) => {
                        NodeType::String
                    }
                    _ => {
                        log::warn!(
                            "Found unexpected input types in aead.decrypt_string function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::String
                    }
                }
            }),
        }),
        "aead.encrypt" => Some(FunctionDefinition {
            name: "aead.encrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!("aead.encrypt expects 3 arguments, but got {}", tys.len());
                    return NodeType::Bytes;
                }
                match tys {
                    [
                        NodeType::Bytes | NodeType::Struct(_),
                        NodeType::String | NodeType::Bytes,
                        NodeType::String | NodeType::Bytes,
                    ] => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in aead.encrypt function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "deterministic_decrypt_bytes" => Some(FunctionDefinition {
            name: "deterministic_decrypt_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "deterministic_decrypt_bytes expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1], tys[2]) {
                    (
                        NodeType::Bytes | NodeType::Struct(_),
                        NodeType::Bytes,
                        NodeType::String | NodeType::Bytes,
                    ) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in deterministic_decrypt_bytes function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "deterministic_decrypt_string" => Some(FunctionDefinition {
            name: "deterministic_decrypt_string".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "deterministic_decrypt_string expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::String;
                }
                match (tys[0], tys[1], tys[2]) {
                    (
                        NodeType::Bytes | NodeType::Struct(_),
                        NodeType::String,
                        NodeType::String | NodeType::Bytes,
                    ) => NodeType::String,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in deterministic_decrypt_string function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::String
                    }
                }
            }),
        }),
        "deterministic_encrypt" => Some(FunctionDefinition {
            name: "deterministic_encrypt".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "deterministic_encrypt expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1], tys[2]) {
                    (
                        NodeType::Bytes | NodeType::Struct(_),
                        NodeType::String | NodeType::Bytes,
                        NodeType::String | NodeType::Bytes,
                    ) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in deterministic_encrypt function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.add_key_from_raw_bytes" => Some(FunctionDefinition {
            name: "keys.add_key_from_raw_bytes".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!(
                        "keys.add_key_from_raw_bytes expects 2 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1], tys[2]) {
                    (NodeType::Bytes, NodeType::String, NodeType::Bytes) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.add_key_from_raw_bytes function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.keyset_chain" => Some(FunctionDefinition {
            name: "keys.keyset_chain".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![StructNodeFieldType::new("key", NodeType::Bytes, vec![])],
                });
                if tys.len() != 2 {
                    log::warn!(
                        "keys.keyset_chain expects 2 arguments, but got {}",
                        tys.len()
                    );
                    return return_type;
                }
                match (tys[0], tys[1]) {
                    // todo: check the struct fields (they are not mentioned in the documentation, for now we assume there is a "key" field)
                    (NodeType::String, NodeType::Bytes) => return_type,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.keyset_chain function: ({}, {})",
                            tys[0],
                            tys[1]
                        );
                        return_type
                    }
                }
            }),
        }),
        "keys.keyset_from_json" => Some(FunctionDefinition {
            name: "keys.keyset_from_json".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!(
                        "keys.keyset_from_json expects 1 argument, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match tys[0] {
                    NodeType::String => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in keys.keyset_from_json function.",
                            tys[0]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.keyset_length" => Some(FunctionDefinition {
            name: "keys.keyset_length".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!(
                        "keys.keyset_length expects 1 argument, but got {}",
                        tys.len()
                    );
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Bytes | NodeType::Struct(_) => NodeType::Int64,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in keys.keyset_length function.",
                            tys[0]
                        );
                        NodeType::Int64
                    }
                }
            }),
        }),
        "keys.keyset_to_json" => Some(FunctionDefinition {
            name: "keys.keyset_to_json".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!(
                        "keys.keyset_to_json expects 1 argument, but got {}",
                        tys.len()
                    );
                    return NodeType::String;
                }
                match tys[0] {
                    NodeType::Bytes | NodeType::Struct(_) => NodeType::String,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in keys.keyset_to_json function.",
                            tys[0]
                        );
                        NodeType::String
                    }
                }
            }),
        }),
        "keys.new_keyset" => Some(FunctionDefinition {
            name: "keys.new_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("keys.new_keyset expects 1 argument, but got {}", tys.len());
                    return NodeType::Bytes;
                }
                match tys[0] {
                    NodeType::String => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in keys.new_keyset function.",
                            tys[0]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.new_wrapped_keyset" => Some(FunctionDefinition {
            name: "keys.new_wrapped_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!(
                        "keys.new_wrapped_keyset expects 2 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1]) {
                    (NodeType::String, NodeType::String) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.new_wrapped_keyset function: ({}, {})",
                            tys[0],
                            tys[1]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.rewrap_keyset" => Some(FunctionDefinition {
            name: "keys.rewrap_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "keys.rewrap_keyset expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1], tys[2]) {
                    (NodeType::String, NodeType::String, NodeType::Bytes) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.rewrap_keyset function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.rotate_keyset" => Some(FunctionDefinition {
            name: "keys.rotate_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!(
                        "keys.rotate_keyset expects 2 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1]) {
                    (NodeType::Bytes | NodeType::Struct(_), NodeType::String) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.rotate_keyset function: ({}, {})",
                            tys[0],
                            tys[1]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        "keys.rotate_wrapped_keyset" => Some(FunctionDefinition {
            name: "keys.rotate_wrapped_keyset".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!(
                        "keys.rotate_wrapped_keyset expects 3 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Bytes;
                }
                match (tys[0], tys[1], tys[2]) {
                    (NodeType::String, NodeType::Bytes, NodeType::String) => NodeType::Bytes,
                    _ => {
                        log::warn!(
                            "Found unexpected input types in keys.rotate_wrapped_keyset function: ({}, {}, {})",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        NodeType::Bytes
                    }
                }
            }),
        }),
        // Aggregate functions
        "any_value" => Some(FunctionDefinition {
            name: "any_value".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("any_value expects 1 argument, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                tys[0].clone()
            }),
        }),
        "array_agg" => Some(FunctionDefinition {
            name: "ARRAY_AGG".to_owned(),
            compute_return_type: Box::new(|tys, indices| {
                if tys.len() != 1 {
                    log::warn!("ARRAY_AGG expects 1 argument, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: tys[0].clone(),
                    input: vec![indices[0]],
                }));
                match tys[0] {
                    NodeType::Array(_) => {
                        log::warn!(
                            "Found unexpected input type {} in array_agg function.",
                            tys[0]
                        );
                        return_type
                    }
                    _ => return_type,
                }
            }),
        }),
        "array_concat_agg" => Some(FunctionDefinition {
            name: "array_concat_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("array_concat_agg expects 1 argument, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                match &tys[0] {
                    NodeType::Array(_) => tys[0].clone(),
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in array_concat_agg function.",
                            tys[0]
                        );
                        NodeType::Unknown
                    }
                }
            }),
        }),
        "avg" => Some(FunctionDefinition {
            name: "avg".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("avg expects 1 argument, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                match tys[0] {
                    NodeType::Int64 | NodeType::Float64 => NodeType::Float64,
                    NodeType::Numeric => NodeType::Numeric,
                    NodeType::BigNumeric => NodeType::BigNumeric,
                    NodeType::Interval => NodeType::Interval,
                    _ => {
                        log::warn!("Found unexpected input type {} in avg function.", tys[0]);
                        NodeType::Unknown
                    }
                }
            }),
        }),
        "bit_and" => Some(FunctionDefinition {
            name: "bit_and".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("bit_and expects 1 argument, but got {}", tys.len());
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Int64 => NodeType::Int64,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in bit_and function.",
                            tys[0]
                        );
                        NodeType::Int64
                    }
                }
            }),
        }),
        "bit_or" => Some(FunctionDefinition {
            name: "bit_or".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("bit_or expects 1 argument, but got {}", tys.len());
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Int64 => NodeType::Int64,
                    _ => {
                        log::warn!("Found unexpected input type {} in bit_or function.", tys[0]);
                        NodeType::Int64
                    }
                }
            }),
        }),
        "bit_xor" => Some(FunctionDefinition {
            name: "bit_xor".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("bit_xor expects 1 argument, but got {}", tys.len());
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Int64 => NodeType::Int64,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in bit_xor function.",
                            tys[0]
                        );
                        NodeType::Int64
                    }
                }
            }),
        }),
        "count" => Some(FunctionDefinition {
            name: "count".to_owned(),
            compute_return_type: Box::new(|_, _| NodeType::Int64),
        }),
        "countif" => Some(FunctionDefinition {
            name: "countif".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("countif expects 1 argument, but got {}", tys.len());
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Boolean => NodeType::Int64,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in countif function.",
                            tys[0]
                        );
                        NodeType::Int64
                    }
                }
            }),
        }),
        "grouping" => Some(FunctionDefinition {
            name: "grouping".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("grouping expects 1 argument, but got {}", tys.len());
                    return NodeType::Int64;
                }
                NodeType::Int64
            }),
        }),
        "logical_and" => Some(FunctionDefinition {
            name: "logical_and".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("logical_and expects 1 argument, but got {}", tys.len());
                    return NodeType::Boolean;
                }
                match tys[0] {
                    NodeType::Boolean => NodeType::Boolean,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in logical_and function.",
                            tys[0]
                        );
                        NodeType::Boolean
                    }
                }
            }),
        }),
        "logical_or" => Some(FunctionDefinition {
            name: "logical_or".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("logical_or expects 1 argument, but got {}", tys.len());
                    return NodeType::Boolean;
                }
                match tys[0] {
                    NodeType::Boolean => NodeType::Boolean,
                    _ => {
                        log::warn!(
                            "Found unexpected input type {} in logical_or function.",
                            tys[0]
                        );
                        NodeType::Boolean
                    }
                }
            }),
        }),
        "max" => Some(FunctionDefinition {
            name: "MAX".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("MAX expects 1 argument, but got {}", tys.len());
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }

                let return_type = tys[0].clone();
                match tys[0] {
                    NodeType::Array(_)
                    | NodeType::Struct(_)
                    | NodeType::Geography
                    | NodeType::Json => {
                        log::warn!("Found unexpected input type {} in max function.", tys[0]);
                        return_type
                    }
                    _ => return_type,
                }
            }),
        }),
        "max_by" => Some(FunctionDefinition {
            name: "max_by".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!("max_by expects 2 arguments, but got {}", tys.len());
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }
                let return_type = tys[0].clone();
                match (tys[0], tys[1]) {
                    (
                        _,
                        NodeType::Array(_)
                        | NodeType::Struct(_)
                        | NodeType::Geography
                        | NodeType::Json,
                    ) => {
                        log::warn!(
                            "Found unexpected input types ({},{}) in max_by function.",
                            tys[0],
                            tys[1]
                        );
                        return_type
                    }
                    _ => return_type,
                }
            }),
        }),
        "min" => Some(FunctionDefinition {
            name: "min".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("min expects 1 argument, but got {}", tys.len());
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }
                let return_type = tys[0].clone();
                match tys[0] {
                    NodeType::Array(_)
                    | NodeType::Struct(_)
                    | NodeType::Geography
                    | NodeType::Json => {
                        log::warn!("Found unexpected input type {} in min function.", tys[0]);
                        return_type
                    }
                    _ => return_type,
                }
            }),
        }),
        "min_by" => Some(FunctionDefinition {
            name: "min_by".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!("min_by expects 2 arguments, but got {}", tys.len());
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }
                let return_type = tys[0].clone();
                match (tys[0], tys[1]) {
                    (
                        _,
                        NodeType::Array(_)
                        | NodeType::Struct(_)
                        | NodeType::Geography
                        | NodeType::Json,
                    ) => {
                        log::warn!(
                            "Found unexpected input types ({},{}) in min_by function.",
                            tys[0],
                            tys[1]
                        );
                        return_type
                    }
                    _ => return_type,
                }
            }),
        }),
        "string_agg" => Some(FunctionDefinition {
            name: "string_agg".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.is_empty() || tys.len() > 2 {
                    log::warn!("string_agg expects 1 or 2 arguments, but got {}", tys.len());
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }

                if !matches!(tys[0], NodeType::String | NodeType::Bytes) {
                    log::warn!(
                        "Unexpected type for first argument of string_agg: {}",
                        tys[0]
                    );
                    return NodeType::Unknown;
                }
                if tys.len() == 2 && !matches!(tys[1], NodeType::String | NodeType::Bytes) {
                    log::warn!(
                        "Unexpected type for second argument of string_agg: {}",
                        tys[1]
                    );
                    return tys[0].clone();
                }
                tys[0].clone()
            }),
        }),
        "sum" => Some(FunctionDefinition {
            name: "sum".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!("sum expects 1 argument, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                match tys[0] {
                    NodeType::Int64 => NodeType::Int64,
                    NodeType::Float64 => NodeType::Float64,
                    NodeType::Numeric => NodeType::Numeric,
                    NodeType::BigNumeric => NodeType::BigNumeric,
                    NodeType::Interval => NodeType::Interval,
                    _ => {
                        log::warn!("Found unexpected input type {} in sum function.", tys[0]);
                        NodeType::Unknown
                    }
                }
            }),
        }),

        // Approximate aggregate functions
        "approx_count_distinct" => Some(FunctionDefinition {
            name: "approx_count_distinct".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 1 {
                    log::warn!(
                        "approx_count_distinct expects 1 argument, but got {}",
                        tys.len()
                    );
                    return NodeType::Int64;
                }
                match tys[0] {
                    NodeType::Array(_) | NodeType::Struct(_) | NodeType::Interval => {
                        log::warn!(
                            "Found unexpected input type {} in approx_count_distinct function.",
                            tys[0]
                        );
                        NodeType::Int64
                    }
                    _ => NodeType::Int64,
                }
            }),
        }),
        "approx_quantiles" => Some(FunctionDefinition {
            name: "approx_quantiles".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!(
                        "approx_quantiles expects 2 arguments, but got {}",
                        tys.len()
                    );
                    if tys.is_empty() {
                        return NodeType::Unknown;
                    }
                    return tys[0].clone();
                }

                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: tys[0].clone(),
                    input: vec![],
                }));
                match (tys[0], tys[1]) {
                    (t1, NodeType::Int64)
                        if !matches!(
                            t1,
                            NodeType::Array(_) | NodeType::Struct(_) | NodeType::Interval
                        ) =>
                    {
                        return_type
                    }
                    _ => {
                        log::warn!(
                            "Unexpected input types {} {} in approx_quantiles function",
                            tys[0],
                            tys[1]
                        );
                        return_type
                    }
                }
            }),
        }),
        "approx_top_count" => Some(FunctionDefinition {
            name: "approx_top_count".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 2 {
                    log::warn!(
                        "approx_top_count expects 2 arguments, but got {}",
                        tys.len()
                    );
                    return NodeType::Unknown;
                }
                let return_type = NodeType::Struct(StructNodeType {
                    fields: vec![
                        StructNodeFieldType::new("value", tys[0].clone(), vec![]),
                        StructNodeFieldType::new("count", NodeType::Int64, vec![]),
                    ],
                });
                match (tys[0], tys[1]) {
                    (t1, NodeType::Int64) if t1.is_groupable() => return_type,
                    _ => {
                        log::warn!(
                            "Unexpected input types {} {} in approx_top_count function",
                            tys[0],
                            tys[1]
                        );
                        return_type
                    }
                }
            }),
        }),
        "approx_top_sum" => Some(FunctionDefinition {
            name: "APPROX_TOP_SUM".to_owned(),
            compute_return_type: Box::new(|tys, _| {
                if tys.len() != 3 {
                    log::warn!("APPROX_TOP_SUM expects 3 arguments, but got {}", tys.len());
                    return NodeType::Unknown;
                }
                let return_type = NodeType::Array(Box::new(ArrayNodeType {
                    r#type: NodeType::Struct(StructNodeType {
                        fields: vec![
                            StructNodeFieldType::new("value", tys[0].clone(), vec![]),
                            StructNodeFieldType::new("count", NodeType::Int64, vec![]),
                        ],
                    }),
                    input: vec![],
                }));
                match (tys[0], tys[1], tys[2]) {
                    (
                        t1,
                        NodeType::Int64
                        | NodeType::Numeric
                        | NodeType::BigNumeric
                        | NodeType::Float64,
                        NodeType::Int64,
                    ) if t1.is_groupable() => return_type,
                    _ => {
                        log::warn!(
                            "Unexpected input types {} {} {} in approx_top_sum function",
                            tys[0],
                            tys[1],
                            tys[2]
                        );
                        return_type
                    }
                }
            }),
        }),

        // Date functions
        "date" => Some(FunctionDefinition {
            name: "date".to_owned(),
            compute_return_type: Box::new(|tys, _| NodeType::Date),
        }),
        //
        _ => None,
    }
}
