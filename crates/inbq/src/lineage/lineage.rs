use crate::{
    arena::{Arena, ArenaIndex},
    ast::{
        ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, BinaryOperator,
        CreateJsFunctionStatement, CreateSqlFunctionStatement, CreateTableStatement, Cte,
        DeclareVarStatement, DeleteStatement, DropFunctionStatement, DropTableStatement, Expr,
        ForInStatement, FromExpr, FromPathExpr, FunctionExpr, GenericFunctionExpr, GroupByExpr,
        GroupingQueryExpr, Identifier, InsertStatement, IntervalExpr, JoinCondition, JoinExpr,
        Merge, MergeInsert, MergeSource, MergeStatement, MergeUpdate, NamedWindowExpr,
        ParameterizedType, PivotColumn, QuantifiedLikeExprPattern, QueryExpr, QueryStatement,
        QuotedIdentifier, SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr,
        SelectQueryExpr, SelectTableValue, SetSelectQueryExpr, SetVarStatement, SetVariable,
        Statement, StatementsBlock, StructExpr, TableOperator, Type, UnaryExpr, UnaryOperator,
        UnpivotKind, UpdateStatement, When, With,
    },
    parser::Parser,
    scanner::Scanner,
};

use anyhow::anyhow;
use bitflags::{Flags, bitflags};
use indexmap::{IndexMap, IndexSet};
use rayon::prelude::*;
use serde::Serialize;
use std::{
    collections::HashSet,
    fmt::{Debug, Display},
};
use std::{hash::Hash, result::Result::Ok};

use super::{
    catalog::{Catalog, Column, SchemaObjectKind},
    find_matching_function,
};

#[macro_export]
macro_rules! routine_name {
    ($name:expr) => {
        format!("{}(*)", $name)
    };
}

#[derive(Debug, Clone)]
struct LineageNode {
    name: NodeName,
    r#type: NodeType,
    origin: NodeOrigin,
    source_obj: ArenaIndex,
    input: Vec<ArenaIndex>,
    nested_nodes: IndexMap<String, ArenaIndex>,
}

impl LineageNode {
    fn access(&self, path: &AccessPath) -> anyhow::Result<ArenaIndex> {
        self.nested_nodes
            .get(&path.nested_path())
            .ok_or_else(|| {
                anyhow!(
                    "Cannot find nested node {:?} in {:?} in table {:?}",
                    path,
                    self,
                    &self.name
                )
            })
            .copied()
    }

    fn pretty_log_lineage_node(node_idx: ArenaIndex, ctx: &LineageContext) {
        let node = &ctx.arena_lineage_nodes[node_idx];
        let node_source_name = &ctx.arena_objects[node.source_obj].name;
        let in_str = node
            .input
            .iter()
            .map(|idx| {
                let in_node = &ctx.arena_lineage_nodes[*idx];
                format!(
                    "[{}]{}->{}${}",
                    in_node.source_obj.index,
                    ctx.arena_objects[in_node.source_obj].name,
                    in_node.name.nested_path(),
                    <NodeOrigin as Into<String>>::into(in_node.origin),
                )
            })
            .fold((0, String::from("")), |acc, el| {
                if acc.0 == 0 {
                    (acc.0 + 1, el.to_string())
                } else {
                    (acc.0 + 1, format!("{}, {}", acc.1, el))
                }
            })
            .1;
        log::debug!(
            "[{}]{}->{}${} <-[{}]",
            node.source_obj.index,
            node_source_name,
            node.name.nested_path(),
            <NodeOrigin as Into<String>>::into(node.origin),
            in_str
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessOp {
    Field(String),
    FieldStar,
    Index,
}

#[derive(Debug, Clone, Default)]
struct AccessPath {
    path: Vec<AccessOp>,
}

impl AccessPath {
    fn nested_path(&self) -> String {
        let acc = match &self.path[0] {
            AccessOp::Field(s) => s.to_lowercase(),
            AccessOp::FieldStar => "*".to_owned(),
            AccessOp::Index => "[]".to_owned(),
        };
        self.path.iter().skip(1).fold(acc, |acc, op| match op {
            AccessOp::Field(f) => format!("{}.{}", acc, f.to_lowercase()),
            AccessOp::FieldStar => format!("{}.{}", acc, "*"),
            AccessOp::Index => format!("{}[]", acc),
        })
    }
}

#[derive(Debug, Clone)]
enum NodeName {
    Anonymous,
    Defined(String),
    Nested(NestedNodeName),
}

impl Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string())
    }
}

impl From<NodeName> for String {
    fn from(val: NodeName) -> Self {
        val.string().into()
    }
}

impl NodeName {
    fn string(&self) -> &str {
        match self {
            NodeName::Anonymous => "!anonymous",
            NodeName::Defined(s) => s,
            NodeName::Nested(nested) => match nested.access_path.path.last().unwrap() {
                AccessOp::Field(s) => s,
                _ => "!anonymous",
            },
        }
    }

    fn nested_path(&self) -> String {
        match self {
            NodeName::Anonymous => self.string().to_owned(),
            NodeName::Defined(s) => s.to_owned(),
            NodeName::Nested(nested) => nested.nested_path(),
        }
    }
}

#[derive(Debug, Clone)]
struct NestedNodeName {
    parent: String,
    access_path: AccessPath,
}

impl NestedNodeName {
    fn nested_path(&self) -> String {
        self.access_path
            .path
            .iter()
            .fold(self.parent.clone(), |acc, op| match op {
                AccessOp::Field(f) => format!("{}.{}", acc, f),
                AccessOp::FieldStar => format!("{}.{}", acc, "*"),
                AccessOp::Index => format!("{}[]", acc),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType {
    Unknown,
    BigNumeric,
    Boolean,
    Bytes,
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Json,
    Numeric,
    Range(Box<NodeType>),
    String,
    Time,
    Timestamp,
    Struct(StructNodeType),
    Array(Box<ArrayNodeType>),
}

impl NodeType {
    pub(crate) fn is_groupable(&self) -> bool {
        !matches!(self, NodeType::Geography | NodeType::Json)
    }

    pub(crate) fn is_number(&self) -> bool {
        matches!(
            self,
            NodeType::Int64 | NodeType::Float64 | NodeType::Numeric | NodeType::BigNumeric
        )
    }

    pub(crate) fn can_be_cast_to(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                NodeType::Int64,
                NodeType::Boolean
                    | NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::Float64
                    | NodeType::String
            ) | (
                NodeType::Numeric,
                NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::Float64
                    | NodeType::String
            ) | (
                NodeType::BigNumeric,
                NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::Float64
                    | NodeType::String
            ) | (
                NodeType::Float64,
                NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::Float64
                    | NodeType::String
            ) | (
                NodeType::Boolean,
                NodeType::Boolean | NodeType::Int64 | NodeType::String
            ) | (
                NodeType::String,
                NodeType::Boolean
                    | NodeType::Int64
                    | NodeType::Numeric
                    | NodeType::BigNumeric
                    | NodeType::Float64
                    | NodeType::String
                    | NodeType::Bytes
                    | NodeType::Date
                    | NodeType::Datetime
                    | NodeType::Time
                    | NodeType::Timestamp
                    | NodeType::Range(_)
            ) | (NodeType::Bytes, NodeType::String | NodeType::Bytes)
                | (
                    NodeType::Date,
                    NodeType::String | NodeType::Date | NodeType::Datetime | NodeType::Timestamp
                )
                | (
                    NodeType::Datetime,
                    NodeType::String
                        | NodeType::Date
                        | NodeType::Datetime
                        | NodeType::Time
                        | NodeType::Timestamp
                )
                | (NodeType::Time, NodeType::String | NodeType::Time)
                | (
                    NodeType::Timestamp,
                    NodeType::String
                        | NodeType::Date
                        | NodeType::Datetime
                        | NodeType::Time
                        | NodeType::Timestamp
                )
                | (NodeType::Array(_), NodeType::Array(_))
                | (NodeType::Struct(_), NodeType::Struct(_))
                | (NodeType::Range(_), NodeType::Range(_) | NodeType::String)
        )
    }

    pub(crate) fn common_supertype_with(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (NodeType::Int64, NodeType::Numeric) => Some(NodeType::Numeric),
            (NodeType::Numeric, NodeType::Int64) => Some(NodeType::Numeric),
            (NodeType::Int64, NodeType::BigNumeric) => Some(NodeType::BigNumeric),
            (NodeType::BigNumeric, NodeType::Int64) => Some(NodeType::BigNumeric),
            (NodeType::Int64, NodeType::Float64) => Some(NodeType::Float64),
            (NodeType::Float64, NodeType::Int64) => Some(NodeType::Float64),
            (NodeType::Float64, NodeType::Numeric) => Some(NodeType::Float64),
            (NodeType::Numeric, NodeType::Float64) => Some(NodeType::Float64),
            (NodeType::Float64, NodeType::BigNumeric) => Some(NodeType::Float64),
            (NodeType::BigNumeric, NodeType::Float64) => Some(NodeType::Float64),

            // todo: this should be valid only if date/datetime/time/timestamp is an expr and string is a literal
            // https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#supertypes_and_literals
            (NodeType::Timestamp, NodeType::String) => Some(NodeType::Timestamp),
            (NodeType::String, NodeType::Timestamp) => Some(NodeType::Timestamp),
            (NodeType::Date, NodeType::String) => Some(NodeType::Date),
            (NodeType::String, NodeType::Date) => Some(NodeType::Date),
            (NodeType::Datetime, NodeType::String) => Some(NodeType::Datetime),
            (NodeType::String, NodeType::Datetime) => Some(NodeType::Datetime),
            (NodeType::Time, NodeType::String) => Some(NodeType::Time),
            (NodeType::String, NodeType::Time) => Some(NodeType::Time),

            // array<f64> and array<int64> are not coerceable to a common supertype
            // but struct<x f64> and struct<x int64> are
            // select if(true, struct(1.5 as x), struct(1 as x)).x -> ok
            // select if(true, [1.5], [1])[0] -> ko
            // (NodeType::Array(t1), NodeType::Array(t2)) => {
            //     if let Some(super_type) = t1.r#type.common_supertype_with(&t2.r#type) {
            //         let mut super_input = t1.input.clone();
            //         super_input.extend(&t2.input);
            //         Some(NodeType::Array(Box::new(ArrayNodeType {
            //             r#type: super_type,
            //             input: super_input,
            //         })))
            //     } else {
            //         None
            //     }
            // }
            (a @ NodeType::Array(_), NodeType::Array(t)) if t.r#type == NodeType::Unknown => {
                Some(a.clone())
            }
            (NodeType::Array(t), a @ NodeType::Array(_)) if t.r#type == NodeType::Unknown => {
                Some(a.clone())
            }

            (NodeType::Struct(s1), NodeType::Struct(s2)) => {
                let mut fields = vec![];
                for (f1, f2) in s1.fields.iter().zip(&s2.fields) {
                    if !(f1.name.eq_ignore_ascii_case(&f2.name)) {
                        return None;
                    }

                    if let Some(super_type) = f1.r#type.common_supertype_with(&f2.r#type) {
                        let mut super_input = f1.input.clone();
                        super_input.extend(&f2.input);
                        fields.push(StructNodeFieldType::new(&f1.name, super_type, super_input));
                    } else {
                        return None;
                    }
                }
                Some(NodeType::Struct(StructNodeType { fields }))
            }
            (NodeType::Unknown, t2) => Some(t2.clone()),
            (t1, NodeType::Unknown) => Some(t1.clone()),
            (t1, t2) if t1 == t2 => Some(t1.clone()),
            _ => None,
        }
    }

    fn from_parser_type(param_type: &Type) -> Self {
        match param_type {
            Type::Array { r#type } => NodeType::Array(Box::new(ArrayNodeType {
                r#type: Self::from_parser_type(r#type),
                input: vec![],
            })),
            Type::BigNumeric => NodeType::BigNumeric,
            Type::Bool => NodeType::Boolean,
            Type::Bytes => NodeType::Bytes,
            Type::Date => NodeType::Date,
            Type::Datetime => NodeType::Datetime,
            Type::Float64 => NodeType::Float64,
            Type::Geography => NodeType::Geography,
            Type::Int64 => NodeType::Int64,
            Type::Interval => NodeType::Interval,
            Type::Json => NodeType::Json,
            Type::Numeric => NodeType::Numeric,
            Type::Range { r#type: ty } => NodeType::Range(Box::new(Self::from_parser_type(ty))),
            Type::String => NodeType::String,
            Type::Struct { fields } => NodeType::Struct(StructNodeType {
                fields: fields
                    .iter()
                    .map(|field| {
                        StructNodeFieldType::new(
                            field
                                .name
                                .as_ref()
                                .map_or("anonymous", |name| name.as_str()),
                            Self::from_parser_type(&field.r#type),
                            vec![],
                        )
                    })
                    .collect(),
            }),
            Type::Time => NodeType::Time,
            Type::Timestamp => NodeType::Timestamp,
        }
    }

    fn from_parser_parameterized_type(param_type: &ParameterizedType) -> NodeType {
        match param_type {
            ParameterizedType::Array {
                r#type: parameterized_type,
            } => NodeType::Array(Box::new(ArrayNodeType {
                r#type: Self::from_parser_parameterized_type(parameterized_type),
                input: vec![],
            })),
            ParameterizedType::BigNumeric {
                precision: _,
                scale: _,
            } => NodeType::BigNumeric,
            ParameterizedType::Bool => NodeType::Boolean,
            ParameterizedType::Bytes { max_length: _ } => NodeType::Bytes,
            ParameterizedType::Date => NodeType::Date,
            ParameterizedType::Datetime => NodeType::Datetime,
            ParameterizedType::Float64 => NodeType::Float64,
            ParameterizedType::Geography => NodeType::Geography,
            ParameterizedType::Int64 => NodeType::Int64,
            ParameterizedType::Interval => NodeType::Interval,
            ParameterizedType::Json => NodeType::Json,
            ParameterizedType::Numeric {
                precision: _,
                scale: _,
            } => NodeType::Numeric,
            ParameterizedType::Range { r#type: ty } => {
                NodeType::Range(Box::new(Self::from_parser_parameterized_type(ty)))
            }
            ParameterizedType::String { max_length: _ } => NodeType::String,
            ParameterizedType::Struct { fields } => NodeType::Struct(StructNodeType {
                fields: fields
                    .iter()
                    .map(|field| {
                        StructNodeFieldType::new(
                            field.name.as_str(),
                            Self::from_parser_parameterized_type(&field.r#type),
                            vec![],
                        )
                    })
                    .collect(),
            }),
            ParameterizedType::Time => NodeType::Time,
            ParameterizedType::Timestamp => NodeType::Timestamp,
        }
    }
}

impl Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Unknown => write!(f, "UNKNOWN"),
            NodeType::BigNumeric => write!(f, "BIGNUMERIC"),
            NodeType::Boolean => write!(f, "BOOLEAN"),
            NodeType::Bytes => write!(f, "BYTES"),
            NodeType::Date => write!(f, "DATE"),
            NodeType::Datetime => write!(f, "DATETIME"),
            NodeType::Float64 => write!(f, "FLOAT64"),
            NodeType::Geography => write!(f, "GEOGRAPHY"),
            NodeType::Int64 => write!(f, "INT64"),
            NodeType::Interval => write!(f, "INTERVAL"),
            NodeType::Json => write!(f, "JSON"),
            NodeType::Numeric => write!(f, "NUMERIC"),
            NodeType::Range(range_node_type) => write!(f, "RANGE<{}>", range_node_type),
            NodeType::String => write!(f, "STRING"),
            NodeType::Time => write!(f, "TIME"),
            NodeType::Timestamp => write!(f, "TIMESTAMP"),
            NodeType::Struct(struct_node_type) => {
                let struct_types = struct_node_type
                    .fields
                    .iter()
                    .map(|f| format!("{} {}", f.name, f.r#type))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "STRUCT<{}>", struct_types)
            }
            NodeType::Array(array_node_type) => write!(f, "ARRAY<{}>", array_node_type.r#type),
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct ArrayNodeType {
    pub r#type: NodeType,
    pub(crate) input: Vec<ArenaIndex>,
}

impl PartialEq for ArrayNodeType {
    fn eq(&self, other: &Self) -> bool {
        self.r#type == other.r#type
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructNodeType {
    pub fields: Vec<StructNodeFieldType>,
}

impl StructNodeType {
    /// `struct<a float64, s struct<x int64, ss struct<z int64, w int64>, y string>`
    /// -> `a, x, z, w, ss, y, s`
    fn flatten_fields(&self) -> Vec<StructNodeFieldType> {
        let mut nodes = vec![];
        for field in &self.fields {
            if let NodeType::Struct(struct_node_type) = &field.r#type {
                let sub_nodes = struct_node_type.flatten_fields();
                nodes.extend(sub_nodes);
                nodes.push(field.clone());
            } else {
                nodes.push(field.clone());
            }
        }
        nodes
    }
}

#[derive(Debug, Clone, Eq)]
pub struct StructNodeFieldType {
    pub name: String,
    pub r#type: NodeType,
    pub(crate) input: Vec<ArenaIndex>,
}

impl PartialEq for StructNodeFieldType {
    fn eq(&self, other: &Self) -> bool {
        self.r#type == other.r#type && self.name.eq(&other.name)
    }
}

impl StructNodeFieldType {
    pub(crate) fn new(name: &str, r#type: NodeType, input: Vec<ArenaIndex>) -> Self {
        Self {
            name: name.to_lowercase(),
            r#type,
            input,
        }
    }
}

#[derive(Debug, Clone)]
struct ContextObject {
    name: String,
    lineage_nodes: Vec<ArenaIndex>,
    kind: ContextObjectKind,
}

bitflags! {
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
    pub struct NodeOrigin: u64 {
        const Source = 1 << 0;
        const Select = 1 << 1;
        const Join = 1 << 2;
        const Where = 1 << 3;
        const GroupBy = 1 << 4;
        const Having = 1 << 5;
        const Qualify = 1 << 6;
        const Window = 1 << 7;
        const OrderBy = 1 << 8;
        const Unnest = 1 << 9;
        const Insert = 1 << 10;
        const Update = 1 << 11;
        const MergeJoin = 1 << 12;
        const MergeCond = 1 << 13;
        const MergeUpdate = 1 << 14;
        const MergeInsert = 1 << 15;
        const Var = 1 << 16;
        const DefaultVar = 1 << 17;
        const SetVar = 1 << 18;
        const UserSqlFunction = 1 << 19;
        const PivotAggregate = 1 << 20;
        const PivotColumn = 1 << 21;
        const UnpivotColumn = 1 << 22;
        const Argument = 1 << 23;
    }
}

impl NodeOrigin {
    const NODE_ORIGIN_NAMES: [&'static str; NodeOrigin::FLAGS.len()] = [
        "source",
        "select",
        "join",
        "where",
        "group_by",
        "having",
        "qualify",
        "window",
        "order_by",
        "unnest",
        "insert",
        "update",
        "merge_join",
        "merge_cond",
        "merge_update",
        "merge_insert",
        "var",
        "default_var",
        "set_var",
        "user_sql_function",
        "pivot_aggregate",
        "pivot_column",
        "unpivot_column",
        "argument",
    ];

    fn single_bit_snake_case(&self) -> &'static str {
        debug_assert!(self.bits().count_ones() == 1);
        Self::NODE_ORIGIN_NAMES[self.bits().trailing_zeros() as usize]
    }

    fn is_output(&self) -> bool {
        self.contains(NodeOrigin::Source)
            && (self.bits()
                & (NodeOrigin::Source
                    | NodeOrigin::Select
                    | NodeOrigin::Unnest
                    | NodeOrigin::MergeUpdate
                    | NodeOrigin::MergeInsert
                    | NodeOrigin::Insert
                    | NodeOrigin::Update
                    | NodeOrigin::Var
                    | NodeOrigin::DefaultVar
                    | NodeOrigin::SetVar
                    | NodeOrigin::UserSqlFunction
                    | NodeOrigin::PivotAggregate)
                    .complement()
                    .bits()
                == 0)
    }
}

impl From<NodeOrigin> for String {
    fn from(value: NodeOrigin) -> Self {
        value
            .iter()
            .map(|n| n.single_bit_snake_case())
            .collect::<Vec<&'static str>>()
            .join(", ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ContextObjectKind {
    TempTable,
    Table,
    Cte,
    Query,
    JoinTable,
    TableAlias,
    UsingTable,
    PivotTable,
    UnpivotTable,
    AnonymousQuery,
    AnonymousStruct,
    AnonymousArray,
    AnonymousExpr,
    Unnest,
    Var,
    Arg,
    UserJsFunction,
    UserSqlFunction,
    TempUserJsFunction,
    TempUserSqlFunction,
    TableFunction,
}

impl From<ContextObjectKind> for String {
    fn from(val: ContextObjectKind) -> Self {
        match val {
            ContextObjectKind::TempTable => "temp_table".to_owned(),
            ContextObjectKind::Table => "table".to_owned(),
            ContextObjectKind::Cte => "cte".to_owned(),
            ContextObjectKind::Query => "query".to_owned(),
            ContextObjectKind::TableAlias => "table_alias".to_owned(),
            ContextObjectKind::JoinTable => "join_table".to_owned(),
            ContextObjectKind::PivotTable => "pivot_table".to_owned(),
            ContextObjectKind::UnpivotTable => "unpivot_table".to_owned(),
            ContextObjectKind::UsingTable => "using_table".to_owned(),
            ContextObjectKind::AnonymousQuery => "anonymous_query".to_owned(),
            ContextObjectKind::AnonymousExpr => "anonymous_expr".to_owned(),
            ContextObjectKind::AnonymousStruct => "anonymous_struct".to_owned(),
            ContextObjectKind::AnonymousArray => "anonymous_array".to_owned(),
            ContextObjectKind::Unnest => "unnest".to_owned(),
            ContextObjectKind::Var => "var".to_owned(),
            ContextObjectKind::Arg => "arg".to_owned(),
            ContextObjectKind::TableFunction => "table_function".to_owned(),
            ContextObjectKind::UserSqlFunction => "user_sql_function".to_owned(),
            ContextObjectKind::TempUserSqlFunction => "temp_user_sql_function".to_owned(),
            ContextObjectKind::UserJsFunction => "user_js_function".to_owned(),
            ContextObjectKind::TempUserJsFunction => "temp_user_js_function".to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
enum GetColumnError {
    NotFound(String),
    Ambiguous(String),
}

impl Display for GetColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetColumnError::NotFound(msg) | GetColumnError::Ambiguous(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

#[derive(Debug)]
struct ColumnsReferenced {
    map: IndexMap<ArenaIndex, NodeOrigin>,
}

impl ColumnsReferenced {
    fn new() -> Self {
        Self {
            map: IndexMap::new(),
        }
    }
}

impl Default for ColumnsReferenced {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnsReferenced {
    fn add_column(&mut self, node_idx: ArenaIndex, origin: NodeOrigin) {
        self.map.insert(node_idx, origin);
    }
}

impl std::error::Error for GetColumnError {}

#[derive(Debug, Clone)]
enum UserSqlFunctionArgType {
    Standard(NodeType),
    AnyType,
}

#[derive(Debug, Clone)]
struct UserSqlFunctionArg {
    name: String,
    r#type: UserSqlFunctionArgType,
}

impl UserSqlFunctionArg {
    fn node_type(&self) -> NodeType {
        match &self.r#type {
            UserSqlFunctionArgType::Standard(node_type) => node_type.clone(),
            UserSqlFunctionArgType::AnyType => NodeType::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
struct UserSqlFunction {
    name: String,
    arguments: Vec<UserSqlFunctionArg>,
    returns: Option<NodeType>,
    body: Expr,
    is_temporary: bool,
}

impl UserSqlFunction {
    #[allow(dead_code)]
    fn is_templated(&self) -> bool {
        self.returns.is_none()
            || self
                .arguments
                .iter()
                .any(|el| matches!(el.r#type, UserSqlFunctionArgType::AnyType))
    }
    fn from_create_statement(statement: &CreateSqlFunctionStatement) -> Self {
        let arguments = statement
            .arguments
            .iter()
            .map(|arg| {
                let r#type = match &arg.r#type {
                    crate::ast::FunctionArgumentType::Standard(ty) => {
                        UserSqlFunctionArgType::Standard(NodeType::from_parser_type(ty))
                    }
                    crate::ast::FunctionArgumentType::AnyType => UserSqlFunctionArgType::AnyType,
                };
                let name = arg.name.as_str().to_lowercase();
                UserSqlFunctionArg { name, r#type }
            })
            .collect();
        let returns = statement.returns.as_ref().map(NodeType::from_parser_type);
        Self {
            name: statement.name.name.clone(),
            arguments,
            returns,
            body: statement.body.clone(),
            is_temporary: statement.is_temporary,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Vars {
    vars: IndexMap<String, ArenaIndex>,
}
impl Vars {
    /// Get the node index of a variable.
    ///
    /// If not found, an `Err` is returned.
    fn get(&self, var_name: &str) -> anyhow::Result<ArenaIndex> {
        self.vars
            .get(&var_name.to_lowercase())
            .ok_or_else(|| anyhow!("Variable `{}` not found.", var_name))
            .copied()
    }

    fn add(&mut self, name: String, object_idx: ArenaIndex) {
        self.vars.insert(name, object_idx);
    }

    fn remove(&mut self, name: &str) -> Option<ArenaIndex> {
        self.vars.swap_remove(name)
    }

    fn clear(&mut self) {
        self.vars.clear()
    }
}

#[derive(Debug, Clone, Default)]
struct Args {
    args: IndexMap<String, ArenaIndex>,
}
impl Args {
    /// Get the node index of an arg.
    ///
    /// If not found, an `Err` is returned.
    fn get(&self, arg_name: &str) -> anyhow::Result<ArenaIndex> {
        self.args
            .get(&arg_name.to_lowercase())
            .ok_or_else(|| anyhow!("Argument `{}` not found.", arg_name))
            .copied()
    }

    fn add(&mut self, name: String, object_idx: ArenaIndex) {
        self.args.insert(name, object_idx);
    }

    fn remove(&mut self, name: &str) -> Option<ArenaIndex> {
        self.args.swap_remove(name)
    }

    fn clear(&mut self) {
        self.args.clear()
    }
}

#[derive(Debug, Default)]
struct LineageContext {
    anon_id: u64,

    // Arena
    arena_objects: Arena<ContextObject>,
    arena_lineage_nodes: Arena<LineageNode>,

    // Tables
    script_tables: Vec<ArenaIndex>,

    // Routines
    script_routines: Vec<ArenaIndex>,

    vars: Vars,
    args: Args,

    // Query context
    query_tables: Vec<IndexMap<String, IndexDepth>>,
    query_columns: Vec<IndexMap<String, Vec<IndexDepth>>>,
    query_depth: u16,
    query_joined_ambiguous_columns: Vec<HashSet<String>>,
    query_windows: Vec<IndexMap<String, ArenaIndex>>,
    query_selected_columns: Vec<IndexMap<String, ArenaIndex>>,

    typed_struct_fields: Vec<StructNodeFieldType>,

    // Output
    output: Vec<ArenaIndex>,
    last_select_statement: Option<String>,
    columns_referenced: ColumnsReferenced,
}

#[derive(Debug, Clone, Copy)]
struct IndexDepth {
    arena_index: ArenaIndex,
    depth: u16,
}

impl LineageContext {
    fn reset(&mut self) {
        // Keep the same arena and clear the rest
        self.script_tables.clear();
        self.script_routines.clear();
        self.query_tables.clear();
        self.query_columns.clear();
        self.query_joined_ambiguous_columns.clear();
        self.query_depth = 0;
        self.query_windows.clear();
        self.args.clear();
        self.vars.clear();
        self.typed_struct_fields.clear();
        self.output.clear();
        self.last_select_statement = None;
        self.columns_referenced = ColumnsReferenced::default();
    }

    fn get_anon_obj_id(&mut self) -> u64 {
        let curr = self.anon_id;
        self.anon_id += 1;
        curr
    }

    fn get_next_anon_obj_name(&mut self, name: &str) -> String {
        format!("!{}_{}", name, self.anon_id)
    }

    fn get_anon_obj_name(&mut self, name: &str) -> String {
        format!("!{}_{}", name, self.get_anon_obj_id())
    }

    fn allocate_object(
        &mut self,
        name: &str,
        kind: ContextObjectKind,
        nodes: Vec<(NodeName, NodeType, NodeOrigin, Vec<ArenaIndex>)>,
    ) -> ArenaIndex {
        let new_obj = ContextObject {
            name: name.to_owned(),
            lineage_nodes: vec![],
            kind,
        };
        let new_id = self.arena_objects.allocate(new_obj);

        let mut new_lineage_nodes = Vec::with_capacity(nodes.len());
        for (node_name, node_type, node_origin, items) in nodes.into_iter() {
            let lin = LineageNode {
                name: node_name,
                r#type: node_type,
                origin: node_origin,
                source_obj: new_id,
                input: items.clone(),
                nested_nodes: IndexMap::new(),
            };
            let lin_idx = self.arena_lineage_nodes.allocate(lin);
            self.add_nested_nodes_from_input_nodes(lin_idx, &items);
            new_lineage_nodes.push(lin_idx);
        }

        let new_obj = &mut self.arena_objects[new_id];
        new_obj.lineage_nodes = new_lineage_nodes;
        new_id
    }

    fn allocate_lineage_node(
        &mut self,
        name: NodeName,
        r#type: NodeType,
        origin: NodeOrigin,
        source_obj: ArenaIndex,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let idx = self.arena_lineage_nodes.allocate(LineageNode {
            name,
            r#type,
            origin,
            source_obj,
            input: input.clone(),
            nested_nodes: IndexMap::new(),
        });
        self.add_nested_nodes_from_input_nodes(idx, &input);
        idx
    }

    fn add_inputs_to_node(&mut self, node_idx: ArenaIndex, input: &[ArenaIndex]) {
        let node = &mut self.arena_lineage_nodes[node_idx];
        node.input.extend(input);
        self.add_nested_nodes_from_input_nodes(node_idx, input);
    }

    fn add_side_inputs_to_node(&mut self, node_idx: ArenaIndex, side_inputs: &[ArenaIndex]) {
        let node = &mut self.arena_lineage_nodes[node_idx];
        node.input.extend(side_inputs);
        self.add_side_inputs_to_nested_nodes(node_idx, side_inputs);
    }

    fn add_side_inputs_to_nested_nodes(
        &mut self,
        node_idx: ArenaIndex,
        side_inputs: &[ArenaIndex],
    ) {
        for nested_node_idx in self.arena_lineage_nodes[node_idx]
            .nested_nodes
            .values()
            .copied()
            .collect::<Vec<_>>()
        {
            self.arena_lineage_nodes[nested_node_idx]
                .input
                .extend(side_inputs);
            self.add_side_inputs_to_nested_nodes(nested_node_idx, side_inputs);
        }
    }

    fn add_nested_nodes_from_input_nodes(
        &mut self,
        node_idx: ArenaIndex,
        input_node_indices: &[ArenaIndex],
    ) {
        self._add_nested_nodes_from_input_nodes(
            AccessPath::default(),
            node_idx,
            &self.arena_lineage_nodes[node_idx].r#type.clone(),
            input_node_indices,
        );
    }

    fn nested_inputs(
        &self,
        access_path: &AccessPath,
        input_node_idx: ArenaIndex,
    ) -> Vec<ArenaIndex> {
        let input_node = &self.arena_lineage_nodes[input_node_idx];
        input_node
            .nested_nodes
            .get(&access_path.nested_path())
            .map_or(vec![], |el| vec![*el])
    }

    fn gather_node_output_indices(
        &mut self,
        access_path: AccessPath,
        output_idx: ArenaIndex,
        ty: &NodeType,
        indices: &mut Vec<ArenaIndex>,
    ) {
        match ty {
            NodeType::Struct(struct_node_type) => {
                for field in &struct_node_type.fields {
                    let mut field_access_path = access_path.clone();
                    field_access_path
                        .path
                        .push(AccessOp::Field(field.name.clone()));

                    let nested_node_idx = self.allocate_node_with_nested_input(
                        output_idx,
                        &field_access_path,
                        &field.r#type,
                        &[],
                    );
                    indices.push(nested_node_idx);
                    self.add_node_to_output_lineage(nested_node_idx);

                    let node = &mut self.arena_lineage_nodes[output_idx];
                    node.nested_nodes
                        .insert(field_access_path.nested_path(), nested_node_idx);

                    self.gather_node_output_indices(
                        field_access_path.clone(),
                        output_idx,
                        &field.r#type,
                        indices,
                    );
                }
            }
            NodeType::Array(array_node_type) => {
                let mut array_access_path = access_path;
                array_access_path.path.push(AccessOp::Index);

                let nested_node_idx = self.allocate_node_with_nested_input(
                    output_idx,
                    &array_access_path,
                    &array_node_type.r#type,
                    &[],
                );
                indices.push(nested_node_idx);
                self.add_node_to_output_lineage(nested_node_idx);

                let node = &mut self.arena_lineage_nodes[output_idx];
                node.nested_nodes
                    .insert(array_access_path.nested_path(), nested_node_idx);

                self.gather_node_output_indices(
                    array_access_path.clone(),
                    output_idx,
                    &array_node_type.r#type,
                    indices,
                );
            }
            _ => {}
        }
    }

    fn gather_node_input_indices(
        &mut self,
        access_path: AccessPath,
        input_idx: ArenaIndex,
        ty: &NodeType,
        indices: &mut Vec<ArenaIndex>,
    ) {
        match ty {
            NodeType::Struct(struct_node_type) => {
                for field in &struct_node_type.fields {
                    let mut field_access_path = access_path.clone();
                    field_access_path
                        .path
                        .push(AccessOp::Field(field.name.clone()));

                    let node = &self.arena_lineage_nodes[input_idx];
                    let nested_node_idx = node
                        .nested_nodes
                        .get(&field_access_path.nested_path())
                        .unwrap();
                    indices.push(*nested_node_idx);

                    self.gather_node_input_indices(
                        field_access_path.clone(),
                        input_idx,
                        &field.r#type,
                        indices,
                    );
                }
            }
            NodeType::Array(array_node_type) => {
                let mut array_access_path = access_path;
                array_access_path.path.push(AccessOp::Index);

                let node = &mut self.arena_lineage_nodes[input_idx];
                let nested_node_idx = node
                    .nested_nodes
                    .get(&array_access_path.nested_path())
                    .unwrap();
                indices.push(*nested_node_idx);

                self.gather_node_input_indices(
                    array_access_path.clone(),
                    input_idx,
                    &array_node_type.r#type,
                    indices,
                );
            }
            _ => {}
        }
    }

    fn build_output_node(&mut self, output_idx: ArenaIndex, input_idx: ArenaIndex) {
        let input_node_ty = &self.arena_lineage_nodes[input_idx].r#type.clone();
        let mut input_indices = vec![];
        self.gather_node_input_indices(
            AccessPath::default(),
            input_idx,
            input_node_ty,
            &mut input_indices,
        );

        let output_node_ty = &self.arena_lineage_nodes[output_idx].r#type.clone();
        let mut output_indices = vec![];
        self.gather_node_output_indices(
            AccessPath::default(),
            output_idx,
            output_node_ty,
            &mut output_indices,
        );

        self.arena_lineage_nodes[output_idx].input.push(input_idx);
        for (out_idx, inp_idx) in output_indices.iter().zip(input_indices.iter()) {
            self.arena_lineage_nodes[*out_idx].input.push(*inp_idx);
        }
    }

    fn _add_nested_nodes_from_input_nodes(
        &mut self,
        access_path: AccessPath,
        node_idx: ArenaIndex,
        node_type: &NodeType,
        input_node_indices: &[ArenaIndex],
    ) -> Vec<ArenaIndex> {
        match node_type {
            NodeType::Struct(struct_node_type) => {
                let mut added_nested_nodes = vec![];
                let mut star_indices = vec![];

                for field in &struct_node_type.fields {
                    let mut field_access_path = access_path.clone();
                    field_access_path
                        .path
                        .push(AccessOp::Field(field.name.clone()));

                    let nested_node_idx = if !input_node_indices.is_empty() {
                        let mut input = vec![];
                        for input_node_idx in input_node_indices {
                            input.extend(self.nested_inputs(&field_access_path, *input_node_idx));
                        }

                        self.allocate_node_with_nested_input(
                            node_idx,
                            &field_access_path,
                            &field.r#type,
                            &input,
                        )
                    } else {
                        self.allocate_node_with_nested_input(
                            node_idx,
                            &field_access_path,
                            &field.r#type,
                            &field.input,
                        )
                    };

                    self.add_node_to_output_lineage(nested_node_idx);
                    added_nested_nodes.push(nested_node_idx);

                    let node = &mut self.arena_lineage_nodes[node_idx];
                    node.nested_nodes
                        .insert(field_access_path.nested_path(), nested_node_idx);

                    star_indices.push(nested_node_idx);

                    let inner_nested_nodes = self._add_nested_nodes_from_input_nodes(
                        field_access_path.clone(),
                        node_idx,
                        &field.r#type,
                        input_node_indices,
                    );
                    self.add_inner_nested_nodes(
                        &field_access_path,
                        nested_node_idx,
                        &inner_nested_nodes,
                    );

                    added_nested_nodes.extend(&inner_nested_nodes);
                }

                let mut star_access_path = access_path;
                star_access_path.path.push(AccessOp::FieldStar);

                let nested_node_idx = self.allocate_node_with_nested_input(
                    node_idx,
                    &star_access_path,
                    &NodeType::Unknown,
                    &star_indices,
                );

                let node = &mut self.arena_lineage_nodes[node_idx];

                node.nested_nodes
                    .insert(star_access_path.nested_path(), nested_node_idx);

                added_nested_nodes.push(nested_node_idx);
                added_nested_nodes
            }
            NodeType::Array(array_node_type) => {
                let mut array_access_path = access_path;
                array_access_path.path.push(AccessOp::Index);

                let mut added_nested_nodes = vec![];

                let nested_node_idx = if !input_node_indices.is_empty() {
                    let mut input = vec![];
                    for input_node_idx in input_node_indices {
                        input.extend(self.nested_inputs(&array_access_path, *input_node_idx));
                    }

                    self.allocate_node_with_nested_input(
                        node_idx,
                        &array_access_path,
                        &array_node_type.r#type,
                        &input,
                    )
                } else {
                    self.allocate_node_with_nested_input(
                        node_idx,
                        &array_access_path,
                        &array_node_type.r#type,
                        &array_node_type.input,
                    )
                };

                self.add_node_to_output_lineage(nested_node_idx);
                added_nested_nodes.push(nested_node_idx);

                let node = &mut self.arena_lineage_nodes[node_idx];
                node.nested_nodes
                    .insert(array_access_path.nested_path(), nested_node_idx);

                let inner_nested_nodes = self._add_nested_nodes_from_input_nodes(
                    array_access_path.clone(),
                    node_idx,
                    &array_node_type.r#type,
                    input_node_indices,
                );
                self.add_inner_nested_nodes(
                    &array_access_path,
                    nested_node_idx,
                    &inner_nested_nodes,
                );

                added_nested_nodes.extend(&inner_nested_nodes);
                added_nested_nodes
            }
            _ => vec![],
        }
    }

    fn add_nested_nodes(&mut self, node_idx: ArenaIndex) {
        let node_type = &self.arena_lineage_nodes[node_idx].r#type.clone();
        self._add_nested_nodes(AccessPath::default(), node_idx, node_type, &[]);
    }

    fn _add_nested_nodes(
        &mut self,
        access_path: AccessPath,
        node_idx: ArenaIndex,
        node_type: &NodeType,
        curr_input: &[ArenaIndex],
    ) -> Vec<ArenaIndex> {
        match node_type {
            NodeType::Struct(struct_node_type) => {
                let mut added_nested_nodes = vec![];

                let mut star_indices = vec![];
                for field in &struct_node_type.fields {
                    let mut field_access_path = access_path.clone();
                    field_access_path
                        .path
                        .push(AccessOp::Field(field.name.clone()));

                    let local_access_path = AccessPath {
                        path: vec![AccessOp::Field(field.name.clone())],
                    };

                    let mut input_indices =
                        self.local_nested_inputs(&local_access_path, curr_input);
                    input_indices.extend(field.input.clone());

                    let lin_idx = self.allocate_node_with_nested_input(
                        node_idx,
                        &field_access_path,
                        &field.r#type,
                        &input_indices,
                    );
                    self.add_nested_nodes(lin_idx);
                    added_nested_nodes.push(lin_idx);

                    self.add_node_to_output_lineage(lin_idx);
                    star_indices.push(lin_idx);

                    let node = &mut self.arena_lineage_nodes[node_idx];
                    node.nested_nodes
                        .insert(field_access_path.nested_path(), lin_idx);

                    let inner_nested_nodes = self._add_nested_nodes(
                        field_access_path.clone(),
                        node_idx,
                        &field.r#type,
                        &input_indices,
                    );

                    added_nested_nodes.extend(&inner_nested_nodes);
                    self.add_inner_nested_nodes(&field_access_path, lin_idx, &inner_nested_nodes);
                }

                let mut star_access_path = access_path.clone();
                star_access_path.path.push(AccessOp::FieldStar);

                let lin_idx = self.allocate_node_with_nested_input(
                    node_idx,
                    &star_access_path,
                    &NodeType::Unknown,
                    &star_indices,
                );
                let node = &mut self.arena_lineage_nodes[node_idx];
                node.nested_nodes
                    .insert(star_access_path.nested_path(), lin_idx);

                added_nested_nodes.push(lin_idx);

                added_nested_nodes
            }
            NodeType::Array(array_node_type) => {
                let mut added_nested_nodes = vec![];

                let mut array_access_path = access_path;
                array_access_path.path.push(AccessOp::Index);

                let local_access_path = AccessPath {
                    path: vec![AccessOp::Index],
                };

                let mut input_indices = self.local_nested_inputs(&local_access_path, curr_input);
                input_indices.extend(array_node_type.input.clone());

                let lin_idx = self.allocate_node_with_nested_input(
                    node_idx,
                    &array_access_path,
                    &array_node_type.r#type,
                    &input_indices,
                );
                self.add_nested_nodes(lin_idx);
                self.add_node_to_output_lineage(lin_idx);
                added_nested_nodes.push(lin_idx);

                let node = &mut self.arena_lineage_nodes[node_idx];
                node.nested_nodes
                    .insert(array_access_path.nested_path(), lin_idx);
                let inner_added_nested_nodes = self._add_nested_nodes(
                    array_access_path.clone(),
                    node_idx,
                    &array_node_type.r#type,
                    &input_indices,
                );

                added_nested_nodes.extend(&inner_added_nested_nodes);
                self.add_inner_nested_nodes(&array_access_path, lin_idx, &inner_added_nested_nodes);

                added_nested_nodes
            }
            _ => vec![],
        }
    }

    fn add_inner_nested_nodes(
        &mut self,
        access_path: &AccessPath,
        node_idx: ArenaIndex,
        inner_added_nested_nodes: &Vec<ArenaIndex>,
    ) {
        for added_node_idx in inner_added_nested_nodes {
            let added_node = &self.arena_lineage_nodes[*added_node_idx];
            let inner_path = match &added_node.name {
                NodeName::Nested(nested_node_name) => &nested_node_name.access_path,
                _ => unreachable!(),
            };

            let common_path = AccessPath {
                path: inner_path
                    .path
                    .iter()
                    .skip(access_path.path.len())
                    .cloned()
                    .collect::<Vec<_>>(),
            };
            let node = &mut self.arena_lineage_nodes[node_idx];
            node.nested_nodes
                .insert(common_path.nested_path(), *added_node_idx);
        }
    }

    fn local_nested_inputs(
        &self,
        local_access_path: &AccessPath,
        curr_input: &[ArenaIndex],
    ) -> Vec<ArenaIndex> {
        let mut input_indices = vec![];
        for inp_idx in curr_input {
            let inp_node = &self.arena_lineage_nodes[*inp_idx];
            if let Some(inp_index) = inp_node.nested_nodes.get(&local_access_path.nested_path()) {
                input_indices.push(*inp_index);
            }
        }
        input_indices
    }

    fn allocate_node_with_nested_input(
        &mut self,
        node_idx: ArenaIndex,
        access_path: &AccessPath,
        node_type: &NodeType,
        nested_input: &[ArenaIndex],
    ) -> ArenaIndex {
        let node = &self.arena_lineage_nodes[node_idx];
        self.arena_lineage_nodes.allocate(LineageNode {
            name: NodeName::Nested(NestedNodeName {
                parent: node.name.to_string(),
                access_path: access_path.clone(),
            }),
            r#type: node_type.clone(),
            origin: node.origin,
            source_obj: node.source_obj,
            input: nested_input.to_vec(),
            nested_nodes: IndexMap::new(),
        })
    }

    fn curr_query_tables(&self) -> Option<&IndexMap<String, IndexDepth>> {
        self.query_tables.last()
    }

    fn curr_query_columns(&self) -> Option<&IndexMap<String, Vec<IndexDepth>>> {
        self.query_columns.last()
    }

    fn curr_query_joined_ambiguous_columns(&self) -> Option<&HashSet<String>> {
        self.query_joined_ambiguous_columns.last()
    }

    fn push_empty_query_ctx(&mut self, include_outer: bool) {
        self.push_query_ctx(IndexMap::new(), HashSet::new(), include_outer)
    }

    fn push_query_ctx(
        &mut self,
        query_tables: IndexMap<String, ArenaIndex>,
        ambiguous_columns: HashSet<String>,
        include_outer: bool,
    ) {
        self.query_depth += 1;
        let mut new_tables: IndexMap<String, IndexDepth> = query_tables
            .into_iter()
            .map(|(k, v)| {
                (
                    k.to_lowercase(),
                    IndexDepth {
                        arena_index: v,
                        depth: self.query_depth,
                    },
                )
            })
            .collect();
        let mut new_columns: IndexMap<String, Vec<IndexDepth>> = IndexMap::new();
        let mut new_ambiguous_columns: HashSet<String> = ambiguous_columns;

        for key in new_tables.keys() {
            let query_table = &self.arena_objects[new_tables[key].arena_index];
            for node_idx in &query_table.lineage_nodes {
                let node = &self.arena_lineage_nodes[*node_idx];
                new_columns
                    .entry(node.name.string().to_lowercase())
                    .or_default()
                    .push(IndexDepth {
                        arena_index: new_tables[key].arena_index,
                        depth: self.query_depth,
                    });
            }
        }

        if include_outer {
            if let Some(outer_tables) = self.query_tables.last() {
                for key in outer_tables.keys() {
                    if !new_tables.contains_key(key) {
                        new_tables.insert(key.clone(), outer_tables[key]);
                        let query_table = &self.arena_objects[outer_tables[key].arena_index];
                        for node_idx in &query_table.lineage_nodes {
                            let node = &self.arena_lineage_nodes[*node_idx];
                            new_columns
                                .entry(node.name.string().to_lowercase())
                                .or_default()
                                .push(outer_tables[key]);
                        }
                    }
                }
            }

            if let Some(prev_ambiguous_cols) = self.query_joined_ambiguous_columns.last() {
                for col in prev_ambiguous_cols {
                    new_ambiguous_columns.insert(col.clone());
                }
            }
        }

        self.query_tables.push(new_tables);
        self.query_columns.push(new_columns);
        self.query_joined_ambiguous_columns
            .push(new_ambiguous_columns);
    }

    fn pop_curr_query_ctx(&mut self) {
        self.query_tables.pop();
        self.query_columns.pop();
        self.query_joined_ambiguous_columns.pop();
        self.query_depth -= 1;
    }

    fn push_query_windows(&mut self, query_windows: IndexMap<String, ArenaIndex>) {
        self.query_windows.push(query_windows)
    }

    fn pop_query_windows(&mut self) {
        self.query_windows.pop();
    }

    fn curr_query_windows(&self) -> Option<&IndexMap<String, ArenaIndex>> {
        self.query_windows.last()
    }

    fn push_selected_columns(&mut self, selected_columns: IndexMap<String, ArenaIndex>) {
        self.query_selected_columns.push(selected_columns)
    }

    fn pop_selected_columns(&mut self) {
        self.query_selected_columns.pop();
    }

    fn curr_selected_columns(&self) -> Option<&IndexMap<String, ArenaIndex>> {
        self.query_selected_columns.last()
    }

    /// Get the node index of a column or variable or arg from the current query context.
    /// Priority order: columns are checked first, then variables, then args.
    ///
    /// If not found, an `Err` is returned.
    fn get_query_col_or_var_or_arg(&mut self, name: &str) -> anyhow::Result<ArenaIndex> {
        match self.get_column(None, name) {
            Ok(col_idx) => Ok(col_idx),
            Err(col_err) => match col_err.downcast_ref::<GetColumnError>() {
                Some(GetColumnError::Ambiguous(_)) => Err(col_err),
                Some(GetColumnError::NotFound(_)) | None => {
                    self.vars.get(name).or(self.args.get(name)).map_err(|_| {
                        anyhow!(
                            "Could not get column `{name:?}` \
                        and could not find a variable with that name either."
                        )
                    })
                }
            },
        }
    }

    /// Get the object index of a table from the current query context.
    ///
    /// If not found, an `Err` is returned
    fn get_query_table(&self, name: &str) -> anyhow::Result<ArenaIndex> {
        let curr_stack = self
            .curr_query_tables()
            .ok_or_else(|| anyhow!("Table `{}` not found in context.", name))?;

        if let Some(ctx_table_idx) = curr_stack.get(&name.to_lowercase()) {
            return Ok(ctx_table_idx.arena_index);
        }

        Err(anyhow!("Table `{}` not found in context.", name))
    }

    /// Get the node index of a column from the current query context.
    ///
    /// If not found, an `Err` is returned
    fn get_column(&mut self, table: Option<&str>, column: &str) -> anyhow::Result<ArenaIndex> {
        // column names are case insensitive
        let column = column.to_lowercase();

        if let Some(table) = table {
            let ctx_table_idx = self.get_query_table(table)?;
            let ctx_table = &self.arena_objects[ctx_table_idx];
            let col_in_schema = ctx_table
                .lineage_nodes
                .iter()
                .map(|n_idx| (&self.arena_lineage_nodes[*n_idx], *n_idx))
                .find(|(n, _)| n.name.string().eq_ignore_ascii_case(&column));
            if let Some((_, col_idx)) = col_in_schema {
                return Ok(col_idx);
            }
            Err(anyhow!(GetColumnError::NotFound(format!(
                "Column `{}` not found in the schema of table `{}`",
                column, table
            ))))
        } else if let Some(target_tables) =
            self.curr_query_columns().and_then(|map| map.get(&column))
        {
            if let Some(selected_columns) = self.curr_selected_columns() {
                if let Some(node_idx) = selected_columns.get(&column) {
                    return Ok(*node_idx);
                }
            }

            if self
                .curr_query_joined_ambiguous_columns()
                .unwrap()
                .contains(&column)
            {
                return Err(anyhow!(GetColumnError::Ambiguous(format!(
                    "Joined column `{}` is ambiguous.",
                    column
                ))));
            }

            let target_table_idx = if target_tables.len() > 1 {
                if let Some(using_idx) = target_tables.iter().find(|&idx| {
                    matches!(
                        &self.arena_objects[idx.arena_index].kind,
                        ContextObjectKind::UsingTable
                    )
                }) {
                    *using_idx
                } else {
                    // if there are atleast two table (not joined with using) at the same maximum depth -> ambiguous
                    let is_ambiguous = target_tables
                        .iter()
                        .filter(|&idx| idx.depth == self.query_depth)
                        .count()
                        > 1;

                    if is_ambiguous {
                        return Err(anyhow!(GetColumnError::Ambiguous(format!(
                            "Column `{}` is ambiguous. It is contained in more than one table: {:?}.",
                            column,
                            target_tables
                                .iter()
                                .map(|source_idx| &self.arena_objects[source_idx.arena_index].name)
                                .collect::<Vec<_>>()
                        ))));
                    }

                    target_tables[0]
                }
            } else {
                target_tables[0]
            };

            let target_table_name = &self.arena_objects[target_table_idx.arena_index].name;
            let ctx_table = self
                .curr_query_tables()
                .unwrap()
                .get(&target_table_name.to_lowercase())
                .map(|idx| &self.arena_objects[idx.arena_index])
                .unwrap();

            let node_idx = ctx_table
                .lineage_nodes
                .iter()
                .map(|n_idx| (&self.arena_lineage_nodes[*n_idx], *n_idx))
                .find(|(n, _)| n.name.string().eq_ignore_ascii_case(&column))
                .unwrap()
                .1;
            return Ok(node_idx);
        } else {
            return Err(anyhow!(GetColumnError::NotFound(format!(
                "Column `{}` not found in context.",
                column
            ))));
        }
    }

    /// Get the object index for the table-like obj named `table_name`.
    /// Script tables are checked first, then source tables.
    fn get_table(&self, catalog: &LineageCatalog, table_name: &str) -> Option<ArenaIndex> {
        let table_idx = {
            for obj_idx in self.script_tables.iter().rev() {
                let obj = &self.arena_objects[*obj_idx];

                if matches!(obj.kind, ContextObjectKind::Cte)
                    && obj.name.eq_ignore_ascii_case(table_name)
                {
                    return Some(*obj_idx);
                }

                if obj.name == *table_name {
                    return Some(*obj_idx);
                }
            }
            None
        };

        table_idx.map_or(catalog.source_tables.get(table_name).copied(), Some)
    }

    fn add_script_table(&mut self, object_idx: ArenaIndex) {
        self.script_tables.push(object_idx);
    }

    fn pop_script_table(&mut self) {
        self.script_tables.pop();
    }

    /// Get the object index for the routine-like obj named `routine_name`.
    /// Script routines are checked first, then source routines.
    fn get_routine(&self, catalog: &LineageCatalog, routine_name: &str) -> Option<ArenaIndex> {
        let routine_idx = {
            for obj_idx in self.script_routines.iter().rev() {
                if self.arena_objects[*obj_idx].name == *routine_name {
                    return Some(*obj_idx);
                }
            }
            None
        };

        routine_idx.map_or(catalog.source_routines.get(routine_name).copied(), Some)
    }

    fn add_script_routine(&mut self, object_idx: ArenaIndex) {
        self.script_routines.push(object_idx);
    }

    fn add_node_to_output_lineage(&mut self, node_idx: ArenaIndex) {
        self.output.push(node_idx);
    }

    fn add_object_nodes_to_output_lineage(&mut self, obj_idx: ArenaIndex) {
        let obj = &self.arena_objects[obj_idx];
        let nodes = &obj.lineage_nodes;
        self.output.extend(nodes);
    }

    fn add_referenced_column(&mut self, node_idx: ArenaIndex, origin: NodeOrigin) {
        self.columns_referenced.add_column(node_idx, origin);
    }

    fn cte_lin(&mut self, catalog: &LineageCatalog, cte: &Cte) -> anyhow::Result<()> {
        match cte {
            Cte::NonRecursive(non_recursive_cte) => {
                let cte_name = &non_recursive_cte.name;

                let obj_idx = self.query_expr_lin(catalog, &non_recursive_cte.query, true)?;
                let obj = &self.arena_objects[obj_idx];

                let cte_idx = self.allocate_object(
                    &cte_name.as_str().to_lowercase(),
                    ContextObjectKind::Cte,
                    obj.lineage_nodes
                        .iter()
                        .map(|idx| {
                            let node = &self.arena_lineage_nodes[*idx];
                            (
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*idx],
                            )
                        })
                        .collect(),
                );
                self.add_script_table(cte_idx);
                self.add_object_nodes_to_output_lineage(cte_idx);
            }
            Cte::Recursive(_) => {
                // TODO
                return Err(anyhow!(
                    "Cannot extract lineage from recursive cte (still a todo)."
                ));
            }
        }
        Ok(())
    }

    fn with_lin(&mut self, catalog: &LineageCatalog, with: &With) -> anyhow::Result<()> {
        // We push an empty context since a CTE on a subquery may not reference correlated columns from the outer query
        self.push_query_ctx(IndexMap::new(), HashSet::new(), false);

        for cte in &with.ctes {
            self.cte_lin(catalog, cte)?;
        }

        self.pop_curr_query_ctx();
        Ok(())
    }

    fn nested_node_lin(
        &mut self,
        access_path: &AccessPath,
        nested_node_idx: ArenaIndex,
        origin: NodeOrigin,
        input: &[ArenaIndex],
    ) -> ArenaIndex {
        let path_len = access_path.path.len();
        if matches!(access_path.path[path_len - 1], AccessOp::FieldStar) {
            let nested_node = &self.arena_lineage_nodes[nested_node_idx];
            self.allocate_expr_node("star", NodeType::Unknown, origin, nested_node.input.clone())
        } else {
            let nested_node = &self.arena_lineage_nodes[nested_node_idx];

            let node_idx = self.allocate_expr_node_with_name(
                nested_node.name.clone(),
                "nested",
                nested_node.r#type.clone(),
                origin,
                vec![nested_node_idx],
            );
            let node = &mut self.arena_lineage_nodes[node_idx];
            node.input.extend_from_slice(input);
            node_idx
        }
    }

    fn binary_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        expr: &BinaryExpr,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        let mut b = expr;
        let mut access_path = AccessPath::default();
        loop {
            let left = &*b.left;
            let right = &*b.right;
            let op = &b.operator;

            // TODO: add field element access in structs via literal integer positions

            if matches!(op, BinaryOperator::FieldAccess) || matches!(op, BinaryOperator::ArrayIndex)
            {
                match (left, right) {
                    (
                        Expr::Identifier(Identifier { name: left_ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: left_ident }),
                        Expr::Identifier(Identifier { name: right_ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: right_ident }),
                    ) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));

                        if access_path.path.is_empty() {
                            if self.get_query_table(left_ident).ok().is_some() {
                                // table.col
                                let col_source_idx =
                                    self.get_column(Some(left_ident), right_ident)?;
                                let col = &self.arena_lineage_nodes[col_source_idx];
                                let col_name = col.name.clone();
                                let allocated_node_idx = self.allocate_expr_node(
                                    "col",
                                    col.r#type.clone(),
                                    node_origin,
                                    vec![col_source_idx],
                                );
                                let allocated_node =
                                    &mut self.arena_lineage_nodes[allocated_node_idx];
                                allocated_node.name = col_name;
                                return Ok(allocated_node_idx);
                            } else {
                                // col_struct.field (or var_struct.field)
                                let col_or_node_idx =
                                    self.get_query_col_or_var_or_arg(left_ident)?;
                                access_path.path.push(AccessOp::Field(right_ident.clone()));
                                let node = &self.arena_lineage_nodes[col_or_node_idx];
                                let nested_node_idx = node.access(&access_path)?;
                                return Ok(self.nested_node_lin(
                                    &access_path,
                                    nested_node_idx,
                                    node_origin,
                                    &[],
                                ));
                            }
                        } else {
                            let col_or_var_source_idx =
                                if self.get_query_table(left_ident).ok().is_some() {
                                    // table.col
                                    let col_name = right_ident.clone();
                                    self.get_column(Some(left_ident), &col_name)?
                                } else {
                                    // col_struct.field (or var_struct.field)
                                    let col_or_node_idx =
                                        self.get_query_col_or_var_or_arg(left_ident)?;
                                    access_path.path.push(AccessOp::Field(right_ident.clone()));
                                    col_or_node_idx
                                };
                            access_path.path.reverse();
                            let node = &self.arena_lineage_nodes[col_or_var_source_idx];
                            let nested_node_idx = node.access(&access_path)?;
                            return Ok(self.nested_node_lin(
                                &access_path,
                                nested_node_idx,
                                node_origin,
                                &[],
                            ));
                        }
                    }
                    (Expr::Binary(left), right) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        match right {
                            Expr::Binary(binary_expr)
                                if matches!(binary_expr.operator, BinaryOperator::ArrayIndex) =>
                            {
                                let field_name = match &binary_expr.left.as_ref() {
                                    Expr::Identifier(Identifier { name: ident })
                                    | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => {
                                        ident
                                    }
                                    _ => unreachable!(),
                                };
                                self.expr_lin(catalog, &binary_expr.right, false, node_origin)?;
                                access_path.path.extend_from_slice(&[
                                    AccessOp::Index,
                                    AccessOp::Field(field_name.clone()),
                                ]);
                            }
                            Expr::Identifier(Identifier { name: ident })
                            | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => {
                                access_path.path.push(AccessOp::Field(ident.clone()));
                            }
                            Expr::Star => {
                                access_path.path.push(AccessOp::FieldStar);
                            }
                            _ => {
                                unreachable!()
                            }
                        }
                        b = left;
                    }
                    (Expr::Function(function_expr), _)
                        if (matches!(op, BinaryOperator::ArrayIndex))
                            && matches!(
                                function_expr.as_ref(),
                                FunctionExpr::Array(_) | FunctionExpr::ArrayAgg(_)
                            ) =>
                    {
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();

                        let node_idx = match function_expr.as_ref() {
                            FunctionExpr::Array(array_function_expr) => self
                                .array_function_expr_lin(
                                    catalog,
                                    array_function_expr,
                                    node_origin,
                                )?,
                            FunctionExpr::ArrayAgg(array_agg_function_expr) => self
                                .array_agg_function_expr_lin(
                                    catalog,
                                    array_agg_function_expr,
                                    false,
                                    node_origin,
                                )?,
                            _ => unreachable!(),
                        };
                        self.add_node_to_output_lineage(node_idx);
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (Expr::Array(array_expr), _) => {
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();
                        let node_idx = self.array_expr_lin(catalog, array_expr, node_origin)?;
                        self.add_node_to_output_lineage(node_idx);
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        let index_node_idx = self.expr_lin(catalog, right, false, node_origin)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[index_node_idx],
                        ));
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Binary(bin_expr),
                    ) => {
                        let array_field = match bin_expr.left.as_ref() {
                            Expr::Identifier(Identifier { name: ident })
                            | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => ident,
                            _ => unreachable!(),
                        };

                        if self.get_query_table(ident).ok().is_some() {
                            // table.array_field
                            let col_source_idx = self.get_column(Some(ident), array_field)?;
                            return Ok(col_source_idx);
                        } else {
                            // struct_col.array_field (or struct_var.array_field)
                            let col_or_var_idx = self.get_query_col_or_var_or_arg(ident)?;
                            access_path.path.extend_from_slice(&[
                                AccessOp::Index,
                                AccessOp::Field(array_field.clone()),
                            ]);
                            access_path.path.reverse();

                            let node = &self.arena_lineage_nodes[col_or_var_idx];
                            let nested_node_idx = node.access(&access_path)?;
                            return Ok(self.nested_node_lin(
                                &access_path,
                                nested_node_idx,
                                node_origin,
                                &[],
                            ));
                        }
                    }
                    (
                        Expr::Struct(struct_expr),
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                    ) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        access_path.path.push(AccessOp::Field(ident.clone()));
                        access_path.path.reverse();
                        let node_idx = self.struct_expr_lin(catalog, struct_expr, node_origin)?;
                        self.add_node_to_output_lineage(node_idx);
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (Expr::Struct(struct_expr), Expr::Star) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        access_path.path.push(AccessOp::FieldStar);
                        access_path.path.reverse();
                        let node_idx = self.struct_expr_lin(catalog, struct_expr, node_origin)?;
                        self.add_node_to_output_lineage(node_idx);
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Star,
                    ) => {
                        if let Ok(source_obj_idx) = self.get_query_table(ident) {
                            // table.*
                            let source_obj = &self.arena_objects[source_obj_idx];
                            return Ok(self.allocate_expr_node(
                                "table_star",
                                NodeType::Unknown,
                                node_origin,
                                source_obj.lineage_nodes.clone(),
                            ));
                        } else {
                            // col_struct.*
                            let col_or_var_idx = self.get_query_col_or_var_or_arg(ident)?;
                            let node = &self.arena_lineage_nodes[col_or_var_idx];
                            let access_path = AccessPath {
                                path: vec![AccessOp::FieldStar],
                            };
                            let nested_node_idx = node.access(&access_path)?;
                            return Ok(self.nested_node_lin(
                                &access_path,
                                nested_node_idx,
                                node_origin,
                                &[],
                            ));
                        }
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Number(_),
                    ) => {
                        // array[]
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();
                        let col_or_var_idx = self.get_query_col_or_var_or_arg(ident)?;
                        let node = &self.arena_lineage_nodes[col_or_var_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (
                        Expr::QueryNamedParameter(_)
                        | Expr::QueryPositionalParameter
                        | Expr::SystemVariable(_)
                        | Expr::Query(_)
                        | Expr::Grouping(_)
                        | Expr::Function(_)
                        | Expr::GenericFunction(_),
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                    ) => {
                        // select if(true, struct(3 as X), struct(10 as x)).x
                        // select (select struct(1 as a, 2 as b)).a
                        // todo: handle named/positional parameters and sysvars from schema
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        access_path.path.push(AccessOp::Field(ident.clone()));
                        access_path.path.reverse();
                        let node_idx = self.expr_lin(catalog, left, false, node_origin)?;
                        let node = &self.arena_lineage_nodes[node_idx];
                        debug_assert!(matches!(node.r#type, NodeType::Struct(_)));
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (
                        Expr::QueryNamedParameter(_)
                        | Expr::QueryPositionalParameter
                        | Expr::SystemVariable(_)
                        | Expr::Query(_)
                        | Expr::Grouping(_)
                        | Expr::Function(_)
                        | Expr::GenericFunction(_),
                        Expr::Number(_),
                    ) => {
                        // select (select [1,2,3])[0]
                        // todo: handle named/positional parameters and sysvars from schema
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();
                        let node_idx = self.expr_lin(catalog, left, false, node_origin)?;
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[],
                        ));
                    }
                    (
                        Expr::QueryNamedParameter(_)
                        | Expr::QueryPositionalParameter
                        | Expr::SystemVariable(_)
                        | Expr::Query(_)
                        | Expr::Grouping(_)
                        | Expr::Function(_)
                        | Expr::GenericFunction(_),
                        Expr::QueryNamedParameter(_)
                        | Expr::QueryPositionalParameter
                        | Expr::SystemVariable(_)
                        | Expr::Query(_)
                        | Expr::Grouping(_)
                        | Expr::Function(_)
                        | Expr::GenericFunction(_),
                    ) => {
                        // select (select [1,2,3])[fn()]
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();

                        let node_idx = self.expr_lin(catalog, left, false, node_origin)?;
                        let node = &self.arena_lineage_nodes[node_idx];
                        let nested_node_idx = node.access(&access_path)?;

                        let index_node_idx = self.expr_lin(catalog, right, false, node_origin)?;
                        let index_node = &self.arena_lineage_nodes[index_node_idx];

                        if !matches!(index_node.r#type, NodeType::Int64) {
                            return Err(anyhow!(
                                "Found unexpected type `{}` in indexing expr `{:?}`. Must be `{}`.",
                                index_node.r#type,
                                right,
                                NodeType::Int64
                            ));
                        }
                        return Ok(self.nested_node_lin(
                            &access_path,
                            nested_node_idx,
                            node_origin,
                            &[node_idx, index_node_idx],
                        ));
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::GenericFunction(function_expr),
                    ) => {
                        if ident.eq_ignore_ascii_case("safe") {
                            return self.expr_lin(catalog, right, false, node_origin);
                        }
                        let node_idx = self.generic_function_expr_lin(
                            catalog,
                            &format!("{}.{}", ident, function_expr.name.as_str()),
                            function_expr,
                            false,
                            node_origin,
                        )?;
                        self.add_node_to_output_lineage(node_idx);
                        return Ok(node_idx);
                    }

                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Function(_),
                    ) => {
                        if ident.eq_ignore_ascii_case("safe") {
                            return self.expr_lin(catalog, right, false, node_origin);
                        }
                        return Err(anyhow!(
                            "Found unexpected binary expr with left: {:?} and right {:?}.",
                            left,
                            right
                        ));
                    }

                    _ => {
                        return Err(anyhow!(
                            "Found unexpected binary expr with left: {:?} and right {:?} with op {:?}.",
                            left,
                            right,
                            op
                        ));
                    }
                }
            } else {
                return self.binary_expr_type(catalog, left, right, op, node_origin);
            }
        }
    }

    fn binary_expr_type(
        &mut self,
        catalog: &LineageCatalog,
        left_expr: &Expr,
        right_expr: &Expr,
        op: &BinaryOperator,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        let left_idx = self.expr_lin(catalog, left_expr, false, node_origin)?;
        let right_idx = self.expr_lin(catalog, right_expr, false, node_origin)?;

        let left_node = &self.arena_lineage_nodes[left_idx];
        let right_node = &self.arena_lineage_nodes[right_idx];

        let left_type = &left_node.r#type;
        let right_type = &right_node.r#type;

        let node_type = match op {
            BinaryOperator::Star | BinaryOperator::Slash => match (left_type, right_type) {
                (NodeType::Float64, NodeType::Float64) => NodeType::Float64,
                (NodeType::Int64, NodeType::Float64) | (NodeType::Float64, NodeType::Int64) => {
                    NodeType::Float64
                }
                (NodeType::Int64, NodeType::Numeric) | (NodeType::Numeric, NodeType::Int64) => {
                    NodeType::Numeric
                }
                (NodeType::Int64, NodeType::BigNumeric)
                | (NodeType::BigNumeric, NodeType::Int64) => NodeType::BigNumeric,
                (NodeType::Numeric, NodeType::BigNumeric)
                | (NodeType::BigNumeric, NodeType::Numeric) => NodeType::BigNumeric,
                (NodeType::Numeric, NodeType::Float64) | (NodeType::Float64, NodeType::Numeric) => {
                    NodeType::Float64
                }
                (NodeType::BigNumeric, NodeType::Float64)
                | (NodeType::Float64, NodeType::BigNumeric) => NodeType::Float64,
                (NodeType::Int64, NodeType::Int64) => {
                    if *op == BinaryOperator::Star {
                        NodeType::Int64
                    } else {
                        NodeType::Float64
                    }
                }
                (NodeType::Numeric, NodeType::Numeric) => NodeType::Numeric,
                (NodeType::BigNumeric, NodeType::BigNumeric) => NodeType::BigNumeric,
                (known, NodeType::Unknown) | (NodeType::Unknown, known) => {
                    if *known == NodeType::Int64 {
                        NodeType::Float64
                    } else {
                        known.clone()
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "Cannot apply BinaryOperator::{} with type `{}` from expr `{:?}` with type `{}` from expr `{:?}`",
                        op,
                        left_type,
                        left_expr,
                        right_type,
                        right_expr
                    ));
                }
            },
            BinaryOperator::Plus | BinaryOperator::Minus => match (left_type, right_type) {
                (left, right) if left == right => left.clone(),
                (NodeType::Int64, NodeType::Float64) | (NodeType::Float64, NodeType::Int64) => {
                    NodeType::Float64
                }
                (NodeType::Int64, NodeType::Numeric) | (NodeType::Numeric, NodeType::Int64) => {
                    NodeType::Numeric
                }
                (NodeType::Int64, NodeType::BigNumeric)
                | (NodeType::BigNumeric, NodeType::Int64) => NodeType::BigNumeric,
                (NodeType::Numeric, NodeType::BigNumeric)
                | (NodeType::BigNumeric, NodeType::Numeric) => NodeType::BigNumeric,
                (NodeType::Numeric, NodeType::Float64) | (NodeType::Float64, NodeType::Numeric) => {
                    NodeType::Float64
                }
                (NodeType::BigNumeric, NodeType::Float64)
                | (NodeType::Float64, NodeType::BigNumeric) => NodeType::Float64,
                (left, NodeType::Unknown) => left.clone(),
                (NodeType::Unknown, right) => right.clone(),
                (NodeType::Date, NodeType::Int64) | (NodeType::Int64, NodeType::Date) => {
                    NodeType::Date
                }
                _ => {
                    return Err(anyhow!(
                        "Cannot apply BinaryOperator::{} with type `{}` from expr `{:?}` and type `{}` from expr `{:?}`.",
                        op,
                        left_type,
                        left_expr,
                        right_type,
                        right_expr
                    ));
                }
            },
            BinaryOperator::Concat => match (left_type, right_type) {
                (left, right)
                    if left == right
                        && matches!(
                            left_type,
                            NodeType::Bytes | NodeType::String | NodeType::Array(_)
                        ) =>
                {
                    left.clone()
                }
                _ => {
                    return Err(anyhow!(
                        "Cannot apply BinaryOperator::{} with type `{}` from expr `{:?}` and type `{}` from expr `{:?}`.",
                        op,
                        left_type,
                        left_expr,
                        right_type,
                        right_expr
                    ));
                }
            },
            BinaryOperator::BitwiseLeftShift | BinaryOperator::BitwiseRightShift => {
                match (left_type, right_type) {
                    (NodeType::Bytes, NodeType::Int64) => NodeType::Bytes,
                    (NodeType::Int64, NodeType::Int64) => NodeType::Int64,
                    _ => {
                        return Err(anyhow!(
                            "Cannot apply BinaryOperator::{} with type `{}` from expr `{:?}` and type `{}` from expr `{:?}`",
                            op,
                            left_type,
                            left_expr,
                            right_type,
                            right_expr
                        ));
                    }
                }
            }
            BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseOr | BinaryOperator::BitwiseXor => {
                match (left_type, right_type) {
                    (NodeType::Int64, NodeType::Int64) => NodeType::Int64,
                    (NodeType::Bytes, NodeType::Bytes) => NodeType::Bytes,
                    _ => {
                        return Err(anyhow!(
                            "Cannot apply BinaryOperator::{} with type `{}` from expr `{:?}` and type `{}` from expr `{:?}`.",
                            op,
                            left_type,
                            left_expr,
                            right_type,
                            right_expr
                        ));
                    }
                }
            }
            BinaryOperator::Equal
            | BinaryOperator::LessThan
            | BinaryOperator::GreaterThan
            | BinaryOperator::LessThanOrEqualTo
            | BinaryOperator::GreaterThanOrEqualTo
            | BinaryOperator::NotEqual
            | BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::QuantifiedLike
            | BinaryOperator::QuantifiedNotLike
            | BinaryOperator::Between
            | BinaryOperator::NotBetween
            | BinaryOperator::In
            | BinaryOperator::NotIn
            | BinaryOperator::IsDistinctFrom
            | BinaryOperator::IsNotDistinctFrom
            | BinaryOperator::And
            | BinaryOperator::Or => NodeType::Boolean,
            _ => unreachable!(),
        };
        let node_idx = self.allocate_expr_node(
            &format!("bin_expr_{}", op),
            node_type,
            node_origin,
            vec![left_idx, right_idx],
        );
        Ok(node_idx)
    }

    fn add_typed_struct_fields_to_context(&mut self, typ: &Type) {
        let struct_node_type = NodeType::from_parser_type(typ);

        match struct_node_type {
            NodeType::Struct(struct_node_type) => {
                let mut fields = struct_node_type.flatten_fields();
                fields.reverse();
                self.typed_struct_fields = fields;
            }
            NodeType::Array(array_ty) => {
                if let NodeType::Struct(struct_node_type) = array_ty.r#type {
                    let mut fields = struct_node_type.flatten_fields();
                    fields.reverse();
                    self.typed_struct_fields = fields;
                }
            }
            _ => {}
        }
    }

    fn create_struct_node(
        &mut self,
        name: NodeName,
        fields: Vec<StructNodeFieldType>,
        origin: NodeOrigin,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let obj_name = self.get_anon_obj_name("anon_struct");
        let obj_idx = self.allocate_object(&obj_name, ContextObjectKind::AnonymousStruct, vec![]);

        let node = LineageNode {
            name,
            r#type: NodeType::Struct(StructNodeType { fields }),
            origin,
            source_obj: obj_idx,
            input,
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.arena_lineage_nodes.allocate(node);
        self.add_nested_nodes(node_idx);

        let obj = &mut self.arena_objects[obj_idx];
        obj.lineage_nodes.push(node_idx);

        node_idx
    }

    fn struct_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        struct_expr: &StructExpr,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        if let Some(typ) = &struct_expr.r#type {
            // Typed struct syntax
            self.add_typed_struct_fields_to_context(typ);
        };

        let mut fields = Vec::with_capacity(struct_expr.fields.len());
        let mut input = Vec::with_capacity(struct_expr.fields.len());
        for field in struct_expr.fields.iter() {
            let name = field.alias.as_ref().map(|tok| tok.as_str().to_owned());

            let node_field_idx = self.expr_lin(catalog, &field.expr, false, node_origin)?;
            let node_field = &self.arena_lineage_nodes[node_field_idx];

            let mut name_from_col = None;
            if let NodeName::Defined(ref name) = node_field.name {
                name_from_col = Some(name);
            }

            input.push(node_field_idx);

            let struct_node_field_type = self.typed_struct_fields.pop();

            let field_name = if let Some(name) = name.as_ref() {
                name
            } else if let Some(struct_node_field_type) = struct_node_field_type.as_ref() {
                &struct_node_field_type.name
            } else if let Some(name_from_col) = name_from_col {
                name_from_col
            } else {
                "!anonymous"
            };

            let struct_node_field_type = StructNodeFieldType::new(
                field_name,
                node_field.r#type.clone(),
                vec![node_field_idx],
            );
            fields.push(struct_node_field_type);
        }

        Ok(self.create_struct_node(NodeName::Anonymous, fields, node_origin, input))
    }

    fn create_anon_struct_from_table_nodes(
        &mut self,
        name: &str,
        node_origin: NodeOrigin,
        nodes: &[ArenaIndex],
    ) -> ArenaIndex {
        let mut fields = Vec::with_capacity(nodes.len());
        let mut input = vec![];
        for node_idx in nodes {
            let node = &self.arena_lineage_nodes[*node_idx];
            fields.push(StructNodeFieldType::new(
                node.name.string(),
                node.r#type.clone(),
                node.input.clone(),
            ));
            input.extend(&node.input);
        }
        let node_idx = self.create_struct_node(
            NodeName::Defined(name.to_owned()),
            fields,
            node_origin,
            input,
        );
        self.add_node_to_output_lineage(node_idx);
        node_idx
    }

    fn create_array_node(
        &mut self,
        array_type: NodeType,
        node_origin: NodeOrigin,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let obj_name = self.get_anon_obj_name("anon_array");
        let obj_idx = self.allocate_object(&obj_name, ContextObjectKind::AnonymousArray, vec![]);

        let node = LineageNode {
            name: NodeName::Anonymous,
            r#type: NodeType::Array(Box::new(ArrayNodeType {
                r#type: array_type,
                input: input.clone(),
            })),
            origin: node_origin,
            source_obj: obj_idx,
            input,
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.arena_lineage_nodes.allocate(node);
        self.add_nested_nodes(node_idx);

        let obj = &mut self.arena_objects[obj_idx];
        obj.lineage_nodes.push(node_idx);
        node_idx
    }

    fn array_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        array_expr: &ArrayExpr,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        if let Some(typ) = &array_expr.r#type {
            // Typed struct syntax
            self.add_typed_struct_fields_to_context(typ);
        }

        let (mut array_type, strict) = if let Some(typ) = &array_expr.r#type {
            let ty = match typ {
                Type::Array { r#type } => NodeType::from_parser_type(r#type),
                _ => NodeType::Unknown,
            };
            (ty, true)
        } else {
            (NodeType::Unknown, false)
        };

        let mut input = Vec::with_capacity(array_expr.exprs.len());
        for expr in &array_expr.exprs {
            let element_node_idx = self.expr_lin(catalog, expr, false, node_origin)?;
            let element = &self.arena_lineage_nodes[element_node_idx];

            if strict {
                if element.r#type != array_type {
                    return Err(anyhow!(
                        "Array element type `{}` is not equal to type `{}`",
                        element.r#type,
                        array_type
                    ));
                }
            } else if let Some(super_type) = element.r#type.common_supertype_with(&array_type) {
                array_type = super_type;
            } else {
                return Err(anyhow!(
                    "Array element type `{}` does not coerce to type `{}`",
                    element.r#type,
                    array_type
                ));
            }
            input.push(element_node_idx)
        }

        Ok(self.create_array_node(array_type, node_origin, input))
    }

    fn array_function_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        array_function_expr: &ArrayFunctionExpr,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        self.array_expr_lin(
            catalog,
            &ArrayExpr {
                r#type: None,
                exprs: vec![Expr::Query(Box::new(array_function_expr.query.clone()))],
            },
            node_origin,
        )
    }

    fn array_agg_function_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        array_agg_function_expr: &ArrayAggFunctionExpr,
        expand_value_table: bool,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        let mut input = vec![];
        let node_idx = self.expr_lin(
            catalog,
            &array_agg_function_expr.arg.expr,
            expand_value_table,
            node_origin,
        )?;
        input.push(node_idx);

        if let Some(named_window_expr) = &array_agg_function_expr.over {
            input.push(self.named_window_expr_lin(
                catalog,
                named_window_expr,
                expand_value_table,
            )?);
        }

        Ok(self.create_array_node(
            self.arena_lineage_nodes[node_idx].r#type.clone(),
            node_origin,
            vec![node_idx],
        ))
    }

    fn generic_function_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        name: &str,
        function_expr: &GenericFunctionExpr,
        expand_value_table: bool,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        let mut input = vec![];
        let mut side_inputs = vec![];

        for arg in &function_expr.arguments {
            let node_idx = self.expr_lin(catalog, &arg.expr, expand_value_table, node_origin)?;
            input.push(node_idx);
        }

        let args = input
            .iter()
            .map(|node_idx| &self.arena_lineage_nodes[*node_idx].r#type)
            .collect::<Vec<_>>();

        let routine_name = routine_name!(name);
        let (name, return_type) = match self.get_routine(catalog, &routine_name) {
            Some(routine_idx)
                if self.arena_objects[routine_idx].kind == ContextObjectKind::UserSqlFunction =>
            {
                let routine = &self.arena_objects[routine_idx];
                if let Some(sql_function) = catalog.user_sql_functions.get(&routine.name) {
                    // Evaluate body of templated function
                    debug_assert!(sql_function.is_templated());
                    let args = args.iter().map(|&n| n.clone()).collect::<Vec<_>>();
                    let return_type = sql_function.returns.clone();
                    let return_node_idx = self.arena_objects[routine_idx].lineage_nodes[0];
                    let node_idx = self.evaluate_user_sql_function_body(
                        catalog,
                        sql_function,
                        &args,
                        &mut input,
                        expand_value_table,
                    )?;
                    input.push(return_node_idx);
                    (
                        routine_name,
                        return_type.unwrap_or(self.arena_lineage_nodes[node_idx].r#type.clone()),
                    )
                } else {
                    let return_node_idx = self.arena_objects[routine_idx].lineage_nodes[0];
                    input.push(return_node_idx);
                    (
                        routine_name,
                        self.arena_lineage_nodes[return_node_idx].r#type.clone(),
                    )
                }
            }
            Some(routine_idx)
                if self.arena_objects[routine_idx].kind
                    == ContextObjectKind::TempUserSqlFunction =>
            {
                // We evaluate the body of temp functions
                let args = args.iter().map(|&n| n.clone()).collect::<Vec<_>>();
                let sql_function = catalog.user_sql_functions.get(&routine_name).unwrap();
                let return_type = sql_function.returns.clone();
                let return_node_idx = self.arena_objects[routine_idx].lineage_nodes[0];
                let node_idx = self.evaluate_user_sql_function_body(
                    catalog,
                    sql_function,
                    &args,
                    &mut input,
                    expand_value_table,
                )?;
                input.push(return_node_idx);
                (
                    routine_name,
                    return_type.unwrap_or(self.arena_lineage_nodes[node_idx].r#type.clone()),
                )
            }
            Some(routine_idx)
                if matches!(
                    self.arena_objects[routine_idx].kind,
                    ContextObjectKind::UserJsFunction | ContextObjectKind::TempUserJsFunction
                ) =>
            {
                let return_node_idx = self.arena_objects[routine_idx].lineage_nodes[0];
                input.push(return_node_idx);
                (
                    routine_name,
                    self.arena_lineage_nodes[return_node_idx].r#type.clone(),
                )
            }
            _ => {
                let func = find_matching_function(name);
                let return_type = if let Some(func_def) = func {
                    (func_def.compute_return_type)(&args, &input)
                } else {
                    NodeType::Unknown
                };
                (name.to_lowercase(), return_type)
            }
        };

        if let Some(named_window_expr) = &function_expr.over {
            side_inputs.push(self.named_window_expr_lin(
                catalog,
                named_window_expr,
                expand_value_table,
            )?);
        }

        let node_idx =
            self.allocate_expr_node(&name.to_lowercase(), return_type, node_origin, input);
        self.add_side_inputs_to_node(node_idx, &side_inputs);
        Ok(node_idx)
    }

    fn evaluate_user_sql_function_body(
        &mut self,
        catalog: &LineageCatalog,
        user_sql_function: &UserSqlFunction,
        arg_types: &[NodeType],
        input: &mut Vec<ArenaIndex>,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let arguments = user_sql_function
            .arguments
            .iter()
            .zip(arg_types)
            .map(|(sql_arg, arg_ty)| {
                let name = sql_arg.name.clone();
                let ty = match sql_arg.node_type() {
                    NodeType::Unknown => arg_ty.clone(),
                    t => t,
                };
                (name, ty)
            })
            .collect::<Vec<_>>();

        // Evaluate body
        for (arg_name, arg_ty) in &arguments {
            self.create_new_arg(arg_name, arg_ty.clone(), &[]);
        }
        let node_idx = self.expr_lin(
            catalog,
            &user_sql_function.body,
            expand_value_table,
            NodeOrigin::UserSqlFunction,
        )?;
        for (arg_name, _) in &arguments {
            self.args.remove(arg_name);
        }

        input.push(node_idx);
        Ok(node_idx)
    }

    fn create_new_arg(
        &mut self,
        name: &str,
        node_type: NodeType,
        input_lineage_nodes: &[ArenaIndex],
    ) -> ArenaIndex {
        // Arg names are case insensitive (like column names)
        let arg_ident = name.to_lowercase();
        let obj_name = format!("!arg_{}", arg_ident);

        let object_idx = self.allocate_object(
            &obj_name,
            ContextObjectKind::Arg,
            vec![(
                NodeName::Defined(arg_ident.clone()),
                node_type,
                NodeOrigin::Argument,
                input_lineage_nodes.into(),
            )],
        );
        let arg_node_idx = self.arena_objects[object_idx].lineage_nodes[0];
        self.args.add(arg_ident, arg_node_idx);
        self.add_node_to_output_lineage(arg_node_idx);
        object_idx
    }

    fn common_supertype_error_msg(
        &self,
        t1: &NodeType,
        t2: &NodeType,
        expr_name: &str,
        expr: &Expr,
    ) -> String {
        format!(
            "Cannot find common supertype for type `{}` and type `{}` in {} expr `{:?}`",
            t1, t2, expr_name, expr
        )
    }

    fn track_usage_expr_node(
        &mut self,
        source_name: &str,
        r#type: NodeType,
        origin: NodeOrigin,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let node_idx = self.allocate_expr_node(source_name, r#type, origin, input);
        self.add_node_to_output_lineage(node_idx);
        self.columns_referenced.add_column(node_idx, origin);
        node_idx
    }

    fn allocate_expr_node_with_name(
        &mut self,
        node_name: NodeName,
        source_name: &str,
        r#type: NodeType,
        origin: NodeOrigin,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let obj_name = self.get_anon_obj_name(source_name);
        let obj_idx = self.allocate_object(&obj_name, ContextObjectKind::AnonymousExpr, vec![]);
        self.allocate_lineage_node(node_name, r#type, origin, obj_idx, input)
    }

    fn allocate_expr_node(
        &mut self,
        source_name: &str,
        r#type: NodeType,
        origin: NodeOrigin,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let obj_name = self.get_anon_obj_name(source_name);
        let obj_idx = self.allocate_object(&obj_name, ContextObjectKind::AnonymousExpr, vec![]);
        self.allocate_lineage_node(NodeName::Anonymous, r#type, origin, obj_idx, input)
    }

    fn expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        expr: &Expr,
        expand_value_table: bool,
        node_origin: NodeOrigin,
    ) -> anyhow::Result<ArenaIndex> {
        let node_idx = match expr {
            // todo: retrieve type from schema for query named/positional
            Expr::QueryNamedParameter(_) => {
                self.allocate_expr_node("literal", NodeType::Unknown, node_origin, vec![])
            }
            Expr::QueryPositionalParameter => {
                self.allocate_expr_node("literal", NodeType::Unknown, node_origin, vec![])
            }

            // todo: retrieve type from sysvars
            Expr::SystemVariable(_) => {
                self.allocate_expr_node("literal", NodeType::Unknown, node_origin, vec![])
            }

            Expr::Null => {
                self.allocate_expr_node("literal", NodeType::Unknown, node_origin, vec![])
            }
            Expr::Default => {
                self.allocate_expr_node("literal", NodeType::Unknown, node_origin, vec![])
            }
            Expr::RawBytes(_) | Expr::Bytes(_) | Expr::BytesConcat(_) => {
                self.allocate_expr_node("literal", NodeType::Bytes, node_origin, vec![])
            }
            Expr::String(_) | Expr::RawString(_) | Expr::StringConcat(_) => {
                self.allocate_expr_node("literal", NodeType::String, node_origin, vec![])
            }
            Expr::Bool(_) => {
                self.allocate_expr_node("literal", NodeType::Boolean, node_origin, vec![])
            }
            Expr::Number(num_expr) => {
                let r#type = if num_expr.value.parse::<i64>().is_ok() {
                    NodeType::Int64
                } else {
                    NodeType::Float64
                };
                self.allocate_expr_node("constant", r#type, node_origin, vec![])
            }
            Expr::Numeric(_) => {
                self.allocate_expr_node("constant", NodeType::Numeric, node_origin, vec![])
            }
            Expr::BigNumeric(_) => {
                self.allocate_expr_node("constant", NodeType::BigNumeric, node_origin, vec![])
            }
            Expr::Range(range_expr) => self.allocate_expr_node(
                "constant",
                NodeType::Range(Box::new(NodeType::from_parser_type(&range_expr.r#type))),
                node_origin,
                vec![],
            ),
            Expr::Date(_) => {
                self.allocate_expr_node("constant", NodeType::Date, node_origin, vec![])
            }
            Expr::Timestamp(_) => {
                self.allocate_expr_node("constant", NodeType::Timestamp, node_origin, vec![])
            }
            Expr::Datetime(_) => {
                self.allocate_expr_node("constant", NodeType::Datetime, node_origin, vec![])
            }
            Expr::Time(_) => {
                self.allocate_expr_node("constant", NodeType::Time, node_origin, vec![])
            }
            Expr::Json(_) => {
                self.allocate_expr_node("constant", NodeType::Json, node_origin, vec![])
            }
            Expr::Binary(binary_expr) => self.binary_expr_lin(catalog, binary_expr, node_origin)?,
            Expr::Unary(unary_expr) => {
                let node_idx =
                    self.expr_lin(catalog, &unary_expr.right, expand_value_table, node_origin)?;
                let node_type = match unary_expr.operator {
                    UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::BitwiseNot => {
                        let node = &self.arena_lineage_nodes[node_idx];
                        node.r#type.clone()
                    }
                    UnaryOperator::IsNull
                    | UnaryOperator::IsNotNull
                    | UnaryOperator::IsTrue
                    | UnaryOperator::IsNotTrue
                    | UnaryOperator::IsFalse
                    | UnaryOperator::IsNotFalse
                    | UnaryOperator::Not => NodeType::Boolean,
                };

                self.allocate_expr_node("unary", node_type, node_origin, vec![node_idx])
            }
            Expr::Grouping(grouping_expr) => {
                let node_idx = self.expr_lin(
                    catalog,
                    &grouping_expr.expr,
                    expand_value_table,
                    node_origin,
                )?;
                let node = &self.arena_lineage_nodes[node_idx];
                let allocated_node_idx = self.allocate_expr_node(
                    "grouping",
                    node.r#type.clone(),
                    node_origin,
                    vec![node_idx],
                );
                if let Expr::Identifier(Identifier { name })
                | Expr::QuotedIdentifier(QuotedIdentifier { name }) = &*grouping_expr.expr
                {
                    let allocated_node = &mut self.arena_lineage_nodes[allocated_node_idx];
                    allocated_node.name = NodeName::Defined(name.clone());
                    allocated_node_idx
                } else {
                    allocated_node_idx
                }
            }
            Expr::Identifier(Identifier { name: ident })
            | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => {
                if let Ok(source_obj_idx) = self.get_query_table(ident) {
                    // table (can be unnest)
                    let source_obj = &self.arena_objects[source_obj_idx];
                    if source_obj.kind == ContextObjectKind::Unnest {
                        if source_obj.lineage_nodes.len() > 1 {
                            // unnest is an array of structs
                            self.create_anon_struct_from_table_nodes(
                                ident,
                                node_origin,
                                &source_obj.lineage_nodes.clone(),
                            )
                        } else {
                            source_obj.lineage_nodes[0]
                        }
                    } else {
                        self.create_anon_struct_from_table_nodes(
                            ident,
                            node_origin,
                            &source_obj.lineage_nodes.clone(),
                        )
                    }
                } else {
                    // col
                    let node_idx = self.get_query_col_or_var_or_arg(ident)?;
                    let node = &self.arena_lineage_nodes[node_idx];
                    let allocated_node_idx = self.allocate_expr_node(
                        "col",
                        node.r#type.clone(),
                        node_origin,
                        vec![node_idx],
                    );
                    let allocated_node = &mut self.arena_lineage_nodes[allocated_node_idx];
                    allocated_node.name = NodeName::Defined(ident.clone());
                    allocated_node_idx
                }
            }
            Expr::Interval(interval_expr) => match interval_expr {
                IntervalExpr::Interval { value, .. } => {
                    let node_idx =
                        self.expr_lin(catalog, value, expand_value_table, node_origin)?;
                    self.allocate_expr_node(
                        "interval",
                        NodeType::Interval,
                        node_origin,
                        vec![node_idx],
                    )
                }
                IntervalExpr::IntervalRange { .. } => {
                    self.allocate_expr_node("constant", NodeType::Interval, node_origin, vec![])
                }
            },
            Expr::Array(array_expr) => self.array_expr_lin(catalog, array_expr, node_origin)?,
            Expr::Unnest(unnext_expr) => {
                let node_idx =
                    self.expr_lin(catalog, &unnext_expr.array, expand_value_table, node_origin)?;
                let node = &self.arena_lineage_nodes[node_idx];
                self.allocate_expr_node("unnest", node.r#type.clone(), node_origin, vec![node_idx])
            }
            Expr::Struct(struct_expr) => self.struct_expr_lin(catalog, struct_expr, node_origin)?,
            Expr::Query(query_expr) => {
                let obj_idx = self.query_expr_lin(catalog, query_expr, expand_value_table)?;
                let obj = &self.arena_objects[obj_idx];
                debug_assert!(obj.lineage_nodes.len() == 1);
                let node_idx = obj.lineage_nodes[0];
                let node = &self.arena_lineage_nodes[node_idx];
                self.allocate_expr_node(
                    "subquery",
                    node.r#type.clone(),
                    node_origin,
                    vec![node_idx],
                )
            }
            Expr::Exists(query_expr) => {
                let obj_idx = self.query_expr_lin(catalog, query_expr, expand_value_table)?;
                let obj = &self.arena_objects[obj_idx];
                debug_assert!(obj.lineage_nodes.len() == 1);
                let node_idx = obj.lineage_nodes[0];
                let node = &self.arena_lineage_nodes[node_idx];
                self.allocate_expr_node(
                    "exists_subquery",
                    node.r#type.clone(),
                    node_origin,
                    vec![node_idx],
                )
            }
            Expr::Case(case_expr) => {
                let mut when_super_type = NodeType::Unknown;
                let mut result_super_type = NodeType::Unknown;
                let mut input = vec![];

                if let Some(case) = &case_expr.case {
                    let node_idx = self.expr_lin(catalog, case, expand_value_table, node_origin)?;
                    let node = &self.arena_lineage_nodes[node_idx];
                    when_super_type = when_super_type.common_supertype_with(&node.r#type).unwrap();
                    input.push(node_idx);
                }

                for when_then in &case_expr.when_thens {
                    let when_idx =
                        self.expr_lin(catalog, &when_then.when, expand_value_table, node_origin)?;
                    let when_node = &self.arena_lineage_nodes[when_idx];
                    if let Some(new_super_type) =
                        when_super_type.common_supertype_with(&when_node.r#type)
                    {
                        when_super_type = new_super_type;
                    } else {
                        return Err(anyhow!(self.common_supertype_error_msg(
                            &when_super_type,
                            &when_node.r#type,
                            "case",
                            expr
                        )));
                    }
                    input.push(when_idx);

                    let then_idx =
                        self.expr_lin(catalog, &when_then.then, expand_value_table, node_origin)?;
                    let then_node = &self.arena_lineage_nodes[then_idx];
                    if let Some(new_super_type) =
                        result_super_type.common_supertype_with(&then_node.r#type)
                    {
                        result_super_type = new_super_type;
                    } else {
                        return Err(anyhow!(self.common_supertype_error_msg(
                            &result_super_type,
                            &then_node.r#type,
                            "case",
                            expr
                        )));
                    }
                    input.push(then_idx);
                }

                if let Some(else_expr) = &case_expr.r#else {
                    let else_idx =
                        self.expr_lin(catalog, else_expr, expand_value_table, node_origin)?;
                    let else_node = &self.arena_lineage_nodes[else_idx];
                    if let Some(new_super_type) =
                        result_super_type.common_supertype_with(&else_node.r#type)
                    {
                        result_super_type = new_super_type;
                    } else {
                        return Err(anyhow!(self.common_supertype_error_msg(
                            &result_super_type,
                            &else_node.r#type,
                            "case",
                            expr
                        )));
                    }
                    input.push(else_idx);
                }

                self.allocate_expr_node("case", result_super_type, node_origin, input)
            }
            Expr::GenericFunction(function_expr) => self.generic_function_expr_lin(
                catalog,
                function_expr.name.as_str(),
                function_expr,
                expand_value_table,
                node_origin,
            )?,
            Expr::Function(function_expr) => match function_expr.as_ref() {
                FunctionExpr::Concat(concat_fn_expr) => {
                    let mut return_type = NodeType::Unknown;
                    let mut input = vec![];
                    for expr in &concat_fn_expr.values {
                        let node_idx =
                            self.expr_lin(catalog, expr, expand_value_table, node_origin)?;
                        let node = &self.arena_lineage_nodes[node_idx];
                        input.push(node_idx);

                        if return_type == NodeType::Unknown {
                            #[allow(clippy::collapsible_if)]
                            if !matches!(node.r#type, NodeType::String | NodeType::Bytes) {
                                if node.r#type.can_be_cast_to(&NodeType::String) {
                                    return_type = NodeType::String;
                                } else if node.r#type.can_be_cast_to(&NodeType::Bytes) {
                                    return_type = NodeType::Bytes;
                                } else {
                                    return Err(anyhow!(
                                        "Found unexpected type `{}` in concat function that cannot be cast neither to `{}` nor to `{}`.",
                                        node.r#type,
                                        NodeType::String,
                                        NodeType::Bytes
                                    ));
                                }
                            } else {
                                return_type = node.r#type.clone();
                            }
                        } else if node.r#type != return_type {
                            if return_type == NodeType::String
                                && !(node.r#type.can_be_cast_to(&NodeType::String))
                            {
                                return Err(anyhow!(
                                    "Found unexpected type `{}` in concat function that cannot be cast to the return type `{}`.",
                                    node.r#type,
                                    NodeType::String
                                ));
                            }
                            if return_type == NodeType::Bytes
                                && !(node.r#type.can_be_cast_to(&NodeType::Bytes))
                            {
                                return Err(anyhow!(
                                    "Found unexpected type `{}` in concat function that cannot be cast to the return type `{}`.",
                                    node.r#type,
                                    NodeType::Bytes
                                ));
                            }
                            if (return_type == NodeType::String && node.r#type == NodeType::Bytes)
                                || (return_type == NodeType::Bytes
                                    && node.r#type == NodeType::String)
                            {
                                return Err(anyhow!(
                                    "Concat type changed from `{}` to `{}`.",
                                    return_type,
                                    node.r#type
                                ));
                            }
                        }
                    }
                    self.allocate_expr_node("fn_concat", return_type, node_origin, input)
                }
                FunctionExpr::Coalesce(coalesce_fn_expr) => {
                    let mut input = Vec::with_capacity(coalesce_fn_expr.exprs.len());
                    let mut return_type = NodeType::Unknown;
                    for coalesce_expr in &coalesce_fn_expr.exprs {
                        let node_idx =
                            self.expr_lin(catalog, coalesce_expr, expand_value_table, node_origin)?;
                        input.push(node_idx);
                        let node = &self.arena_lineage_nodes[node_idx];

                        if let Some(super_type) = node.r#type.common_supertype_with(&return_type) {
                            return_type = super_type;
                        } else {
                            return Err(anyhow!(self.common_supertype_error_msg(
                                &return_type,
                                &node.r#type,
                                "coalesce",
                                expr
                            )));
                        }
                    }
                    self.allocate_expr_node("coalesce", return_type, node_origin, input)
                }
                FunctionExpr::Cast(cast_fn_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &cast_fn_expr.expr,
                        expand_value_table,
                        node_origin,
                    )?;
                    let cast_type = NodeType::from_parser_parameterized_type(&cast_fn_expr.r#type);
                    self.allocate_expr_node("cast", cast_type, node_origin, vec![node_idx])
                }
                FunctionExpr::SafeCast(safe_cast_fn_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &safe_cast_fn_expr.expr,
                        expand_value_table,
                        node_origin,
                    )?;
                    let safe_cast_type =
                        NodeType::from_parser_parameterized_type(&safe_cast_fn_expr.r#type);
                    self.allocate_expr_node(
                        "safe_cast",
                        safe_cast_type,
                        node_origin,
                        vec![node_idx],
                    )
                }
                FunctionExpr::Array(array_function_expr) => {
                    self.array_function_expr_lin(catalog, array_function_expr, node_origin)?
                }
                FunctionExpr::ArrayAgg(array_agg_function_expr) => self
                    .array_agg_function_expr_lin(
                        catalog,
                        array_agg_function_expr,
                        expand_value_table,
                        node_origin,
                    )?,
                FunctionExpr::If(if_function_expr) => {
                    let cond_node_idx = self.expr_lin(
                        catalog,
                        &if_function_expr.condition,
                        expand_value_table,
                        node_origin,
                    )?;
                    let true_result_node_idx = self.expr_lin(
                        catalog,
                        &if_function_expr.true_result,
                        expand_value_table,
                        node_origin,
                    )?;

                    let false_result_node_idx = self.expr_lin(
                        catalog,
                        &if_function_expr.false_result,
                        expand_value_table,
                        node_origin,
                    )?;

                    let true_result_node = &self.arena_lineage_nodes[true_result_node_idx];
                    let false_result_node = &self.arena_lineage_nodes[false_result_node_idx];

                    let if_type = if let Some(if_type) = true_result_node
                        .r#type
                        .common_supertype_with(&false_result_node.r#type)
                    {
                        if_type
                    } else {
                        return Err(anyhow!(self.common_supertype_error_msg(
                            &true_result_node.r#type,
                            &false_result_node.r#type,
                            "if",
                            expr
                        )));
                    };

                    self.allocate_expr_node(
                        "if",
                        if_type,
                        node_origin,
                        vec![cond_node_idx, true_result_node_idx, false_result_node_idx],
                    )
                }
                FunctionExpr::Normalize(normalize_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &normalize_function_expr.value,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "normalize",
                        NodeType::String,
                        node_origin,
                        vec![node_idx],
                    )
                }
                FunctionExpr::NormalizeAndCasefold(normalize_and_casefold_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &normalize_and_casefold_function_expr.value,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "normalize_and_casefold",
                        NodeType::String,
                        node_origin,
                        vec![node_idx],
                    )
                }
                FunctionExpr::Left(left_function_expr) => {
                    let value_idx = self.expr_lin(
                        catalog,
                        &left_function_expr.value,
                        expand_value_table,
                        node_origin,
                    )?;
                    let len_idx = self.expr_lin(
                        catalog,
                        &left_function_expr.length,
                        expand_value_table,
                        node_origin,
                    )?;

                    let value = &self.arena_lineage_nodes[value_idx];

                    self.allocate_expr_node(
                        "left",
                        value.r#type.clone(),
                        node_origin,
                        vec![value_idx, len_idx],
                    )
                }
                FunctionExpr::Right(right_function_expr) => {
                    let value_idx = self.expr_lin(
                        catalog,
                        &right_function_expr.value,
                        expand_value_table,
                        node_origin,
                    )?;
                    let len_idx = self.expr_lin(
                        catalog,
                        &right_function_expr.length,
                        expand_value_table,
                        node_origin,
                    )?;

                    let value = &self.arena_lineage_nodes[value_idx];

                    self.allocate_expr_node(
                        "right",
                        value.r#type.clone(),
                        node_origin,
                        vec![value_idx, len_idx],
                    )
                }
                FunctionExpr::CurrentDate(current_date_function_expr) => {
                    let mut input = vec![];
                    if let Some(timezone_expr) = &current_date_function_expr.timezone {
                        let node_idx =
                            self.expr_lin(catalog, timezone_expr, expand_value_table, node_origin)?;
                        input.push(node_idx);
                    }
                    self.allocate_expr_node("current_date", NodeType::Date, node_origin, input)
                }
                FunctionExpr::CurrentDatetime(current_datetime_function_expr) => {
                    let mut input = vec![];
                    if let Some(timezone_expr) = &current_datetime_function_expr.timezone {
                        let node_idx =
                            self.expr_lin(catalog, timezone_expr, expand_value_table, node_origin)?;
                        input.push(node_idx);
                    }
                    self.allocate_expr_node(
                        "current_datetime",
                        NodeType::Datetime,
                        node_origin,
                        input,
                    )
                }
                FunctionExpr::CurrentTime(current_time_function_expr) => {
                    let mut input = vec![];
                    if let Some(timezone_expr) = &current_time_function_expr.timezone {
                        let node_idx =
                            self.expr_lin(catalog, timezone_expr, expand_value_table, node_origin)?;
                        input.push(node_idx);
                    }
                    self.allocate_expr_node("current_time", NodeType::Time, node_origin, input)
                }
                FunctionExpr::CurrentTimestamp => self.allocate_expr_node(
                    "current_timestamp",
                    NodeType::Timestamp,
                    node_origin,
                    vec![],
                ),
                FunctionExpr::Extract(extract_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &extract_function_expr.expr,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node("extract", NodeType::Int64, node_origin, vec![node_idx])
                }
                FunctionExpr::DateDiff(date_diff_function_expr) => {
                    let start_date_idx = self.expr_lin(
                        catalog,
                        &date_diff_function_expr.start_date,
                        expand_value_table,
                        node_origin,
                    )?;
                    let end_date_idx = self.expr_lin(
                        catalog,
                        &date_diff_function_expr.end_date,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "date_diff",
                        NodeType::Int64,
                        node_origin,
                        vec![start_date_idx, end_date_idx],
                    )
                }
                FunctionExpr::DatetimeDiff(datetime_diff_function_expr) => {
                    let start_datetime_idx = self.expr_lin(
                        catalog,
                        &datetime_diff_function_expr.start_datetime,
                        expand_value_table,
                        node_origin,
                    )?;
                    let end_datetime_idx = self.expr_lin(
                        catalog,
                        &datetime_diff_function_expr.end_datetime,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "datetime_diff",
                        NodeType::Int64,
                        node_origin,
                        vec![start_datetime_idx, end_datetime_idx],
                    )
                }
                FunctionExpr::TimestampDiff(timestamp_diff_function_expr) => {
                    let start_timestamp_idx = self.expr_lin(
                        catalog,
                        &timestamp_diff_function_expr.start_timestamp,
                        expand_value_table,
                        node_origin,
                    )?;
                    let end_timestamp_idx = self.expr_lin(
                        catalog,
                        &timestamp_diff_function_expr.end_timestamp,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "timestamp_diff",
                        NodeType::Int64,
                        node_origin,
                        vec![start_timestamp_idx, end_timestamp_idx],
                    )
                }
                FunctionExpr::TimeDiff(time_diff_function_expr) => {
                    let start_time_idx = self.expr_lin(
                        catalog,
                        &time_diff_function_expr.start_time,
                        expand_value_table,
                        node_origin,
                    )?;
                    let end_time_idx = self.expr_lin(
                        catalog,
                        &time_diff_function_expr.end_time,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "time_diff",
                        NodeType::Int64,
                        node_origin,
                        vec![start_time_idx, end_time_idx],
                    )
                }
                FunctionExpr::DateTrunc(date_trunc_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &date_trunc_function_expr.date,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "date_trunc",
                        NodeType::Date,
                        node_origin,
                        vec![node_idx],
                    )
                }
                FunctionExpr::DatetimeTrunc(datetime_trunc_function_expr) => {
                    let mut input = vec![];
                    let datetime_idx = self.expr_lin(
                        catalog,
                        &datetime_trunc_function_expr.datetime,
                        expand_value_table,
                        node_origin,
                    )?;
                    input.push(datetime_idx);
                    if let Some(timezone) = &datetime_trunc_function_expr.timezone {
                        let timezone_idx =
                            self.expr_lin(catalog, timezone, expand_value_table, node_origin)?;
                        input.push(timezone_idx)
                    }
                    self.allocate_expr_node(
                        "datetime_trunc",
                        NodeType::Datetime,
                        node_origin,
                        input,
                    )
                }
                FunctionExpr::TimestampTrunc(timestamp_trunc_function_expr) => {
                    let mut input = vec![];
                    let timestamp_idx = self.expr_lin(
                        catalog,
                        &timestamp_trunc_function_expr.timestamp,
                        expand_value_table,
                        node_origin,
                    )?;
                    input.push(timestamp_idx);
                    if let Some(timezone) = &timestamp_trunc_function_expr.timezone {
                        let timezone_idx =
                            self.expr_lin(catalog, timezone, expand_value_table, node_origin)?;
                        input.push(timezone_idx)
                    }
                    self.allocate_expr_node(
                        "timestamp_trunc",
                        NodeType::Timestamp,
                        node_origin,
                        input,
                    )
                }
                FunctionExpr::TimeTrunc(time_trunc_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &time_trunc_function_expr.time,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node(
                        "time_trunc",
                        NodeType::Time,
                        node_origin,
                        vec![node_idx],
                    )
                }
                FunctionExpr::LastDay(last_day_function_expr) => {
                    let node_idx = self.expr_lin(
                        catalog,
                        &last_day_function_expr.expr,
                        expand_value_table,
                        node_origin,
                    )?;
                    self.allocate_expr_node("last_day", NodeType::Date, node_origin, vec![node_idx])
                }
            },
            Expr::Star => {
                // NOTE: we can enter here for a COUNT(*)
                self.allocate_expr_node("star", NodeType::Int64, node_origin, vec![])
            }
            Expr::QuantifiedLike(quantified_like_expr) => match &quantified_like_expr.pattern {
                QuantifiedLikeExprPattern::ExprList { exprs } => {
                    let mut input = vec![];
                    for expr in exprs {
                        let node_idx =
                            self.expr_lin(catalog, expr, expand_value_table, node_origin)?;
                        input.push(node_idx);
                    }
                    self.allocate_expr_node(
                        "quantified_like_expr",
                        NodeType::Boolean,
                        node_origin,
                        input,
                    )
                }
                QuantifiedLikeExprPattern::ArrayUnnest { expr } => {
                    let node_idx = self.expr_lin(catalog, expr, expand_value_table, node_origin)?;
                    self.allocate_expr_node(
                        "quantified_like_array_unnest",
                        NodeType::Boolean,
                        node_origin,
                        vec![node_idx],
                    )
                }
            },
            Expr::With(with_expr) => {
                let mut input = vec![];
                for with_var in &with_expr.vars {
                    let node_idx =
                        self.expr_lin(catalog, &with_var.value, expand_value_table, node_origin)?;
                    input.push(node_idx)
                }
                let result_idx =
                    self.expr_lin(catalog, &with_expr.result, expand_value_table, node_origin)?;
                let result = &self.arena_lineage_nodes[result_idx];
                input.push(result_idx);
                self.allocate_expr_node("with", result.r#type.clone(), node_origin, input)
            }
        };

        self.add_node_to_output_lineage(node_idx);
        self.add_referenced_column(node_idx, node_origin);
        Ok(node_idx)
    }

    fn select_expr_all_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        select_expr: &SelectAllExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let mut new_lineage_nodes = vec![];
        let except_columns = select_expr
            .except
            .as_ref()
            .map_or(HashSet::default(), |cols| {
                cols.iter()
                    .map(|c| c.as_str().to_lowercase())
                    .collect::<HashSet<String>>()
            });

        for (col_name, sources) in self.curr_query_columns().unwrap_or(&IndexMap::new()).iter() {
            if except_columns.contains(col_name) {
                continue;
            }

            let source_indices = if let Some(using_idx) = sources.iter().find(|&idx| {
                idx.depth == self.query_depth
                    && matches!(
                        &self.arena_objects[idx.arena_index].kind,
                        ContextObjectKind::UsingTable
                    )
            }) {
                vec![using_idx.arena_index]
            } else {
                sources
                    .iter()
                    .filter(|&idx| idx.depth == self.query_depth)
                    .map(|idx| idx.arena_index)
                    .collect()
            };

            for source_idx in source_indices {
                let source = &self.arena_objects[source_idx];
                let col_in_table_idx = source
                    .lineage_nodes
                    .iter()
                    .map(|&n_idx| (&self.arena_lineage_nodes[n_idx], n_idx))
                    .filter(|(n, _)| n.name.string().eq_ignore_ascii_case(col_name))
                    .collect::<Vec<_>>()[0]
                    .1;
                new_lineage_nodes.push((
                    NodeName::Defined(col_name.clone()),
                    self.arena_lineage_nodes[col_in_table_idx].r#type.clone(),
                    NodeOrigin::Select,
                    anon_obj_idx,
                    vec![col_in_table_idx],
                ));
            }
        }

        for tup in new_lineage_nodes {
            let lineage_node_idx = self.allocate_lineage_node(tup.0, tup.1, tup.2, tup.3, tup.4);
            lineage_nodes.push(lineage_node_idx);
            self.arena_objects[anon_obj_idx]
                .lineage_nodes
                .push(lineage_node_idx);
        }

        Ok(())
    }

    fn select_expr_col_all_lin(
        &mut self,
        catalog: &LineageCatalog,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColAllExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let except_columns = col_expr.except.as_ref().map_or(HashSet::default(), |cols| {
            cols.iter()
                .map(|c| c.as_str().to_lowercase())
                .collect::<HashSet<String>>()
        });

        let node_idx = self.expr_lin(catalog, &col_expr.expr, false, NodeOrigin::Select)?;
        let node = &self.arena_lineage_nodes[node_idx];

        let mut new_lineage_nodes = Vec::with_capacity(node.input.len());
        for node_idx in &node.input {
            let node = &self.arena_lineage_nodes[*node_idx];
            if except_columns.contains(&node.name.string().to_lowercase()) {
                continue;
            }

            new_lineage_nodes.push((
                node.name.clone(),
                node.r#type.clone(),
                NodeOrigin::Select,
                anon_obj_idx,
                vec![*node_idx],
            ))
        }

        for tup in new_lineage_nodes {
            let lineage_node_idx = self.allocate_lineage_node(tup.0, tup.1, tup.2, tup.3, tup.4);
            lineage_nodes.push(lineage_node_idx);
            self.arena_objects[anon_obj_idx]
                .lineage_nodes
                .push(lineage_node_idx);
        }

        Ok(())
    }

    fn select_expr_col_lin(
        &mut self,
        catalog: &LineageCatalog,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let pending_node_idx = self.allocate_lineage_node(
            NodeName::Anonymous,
            NodeType::Unknown,
            NodeOrigin::Select,
            anon_obj_idx,
            vec![],
        );
        self.arena_objects[anon_obj_idx]
            .lineage_nodes
            .push(pending_node_idx);

        let node_idx = self.expr_lin(catalog, &col_expr.expr, false, NodeOrigin::Select)?;
        let node = &self.arena_lineage_nodes[node_idx];
        let node_name = node.name.clone();
        let node_type = node.r#type.clone();

        let pending_node = &mut self.arena_lineage_nodes[pending_node_idx];
        pending_node.input.push(node_idx);

        if let Some(alias) = &col_expr.alias {
            pending_node.name = NodeName::Defined(alias.as_str().to_lowercase());
        } else if matches!(node_name, NodeName::Defined(_) | NodeName::Nested(_)) {
            pending_node.name = NodeName::Defined(node_name.nested_path().to_owned());
        }

        pending_node.r#type = node_type;

        self.add_nested_nodes_from_input_nodes(pending_node_idx, &[node_idx]);

        lineage_nodes.push(pending_node_idx);
        Ok(())
    }

    fn add_new_from_table(
        &self,
        from_tables: &mut Vec<ArenaIndex>,
        new_table_idx: ArenaIndex,
    ) -> anyhow::Result<()> {
        let new_table = &self.arena_objects[new_table_idx];
        if from_tables
            .iter()
            .map(|idx| &self.arena_objects[*idx])
            .any(|obj| obj.name == new_table.name)
        {
            return Err(anyhow!(
                "Found duplicate table object in from with name {}",
                new_table.name
            ));
        }
        from_tables.push(new_table_idx);
        Ok(())
    }

    fn create_table_alias_from_table(&mut self, alias: &str, obj_idx: ArenaIndex) -> ArenaIndex {
        // If aliased, we create a new object
        let table_like_obj = &self.arena_objects[obj_idx];

        let mut new_lineage_nodes = Vec::with_capacity(table_like_obj.lineage_nodes.len());
        for el in &table_like_obj.lineage_nodes {
            let ln = &self.arena_lineage_nodes[*el];
            new_lineage_nodes.push((
                ln.name.clone(),
                ln.r#type.clone(),
                NodeOrigin::Source,
                vec![*el],
            ))
        }

        self.allocate_object(
            &alias.to_lowercase(),
            ContextObjectKind::TableAlias,
            new_lineage_nodes,
        )
    }

    fn named_window_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        named_window_expr: &NamedWindowExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let mut node_indices = vec![];
        match named_window_expr {
            NamedWindowExpr::Reference(name) => {
                node_indices.push(
                    *self
                        .curr_query_windows()
                        .unwrap()
                        .get(&name.as_str().to_lowercase())
                        .ok_or_else(|| anyhow!("Expected window with name: {}.", name.as_str()))?,
                );
            }
            NamedWindowExpr::WindowSpec(window_spec) => {
                if let Some(window_name) = &window_spec.window_name {
                    node_indices.push(
                        *self
                            .curr_query_windows()
                            .unwrap()
                            .get(&window_name.as_str().to_lowercase())
                            .ok_or_else(|| {
                                anyhow!("Expected window with name: {}.", window_name.as_str())
                            })?,
                    );
                }

                if let Some(order_bys) = &window_spec.order_by {
                    for order_by in order_bys {
                        node_indices.push(self.expr_lin(
                            catalog,
                            &order_by.expr,
                            expand_value_table,
                            NodeOrigin::Window,
                        )?);
                    }
                }

                if let Some(partition_by_exprs) = &window_spec.partition_by {
                    for partition_by_expr in partition_by_exprs {
                        node_indices.push(self.expr_lin(
                            catalog,
                            partition_by_expr,
                            expand_value_table,
                            NodeOrigin::Window,
                        )?);
                    }
                }
            }
        };
        let new_node_idx = self.allocate_expr_node(
            "window",
            NodeType::Unknown,
            NodeOrigin::Window,
            node_indices,
        );
        self.add_node_to_output_lineage(new_node_idx);
        Ok(new_node_idx)
    }

    fn query_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        query: &QueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        Ok(match query {
            QueryExpr::Grouping(grouping_query_expr) => {
                self.grouping_query_expr_lin(catalog, grouping_query_expr, expand_value_table)?
            }
            QueryExpr::Select(select_query_expr) => {
                self.select_query_expr_lin(catalog, select_query_expr, expand_value_table)?
            }
            QueryExpr::SetSelect(set_select_query_expr) => {
                self.set_select_query_expr_lin(catalog, set_select_query_expr, expand_value_table)?
            }
        })
    }

    fn pivot_column_alias(pivot_col: &PivotColumn, expr: &Expr) -> anyhow::Result<String> {
        Ok(match expr {
            Expr::Null => "NULL".to_owned(),
            Expr::Number(number) => number.value.clone(),
            Expr::Numeric(num_str) | Expr::BigNumeric(num_str) => num_str.clone(),
            Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Minus,
                right,
            }) => match &(**right) {
                Expr::Number(number) => number.value.clone(),
                _ => {
                    return Err(anyhow!(
                        "An alias must be provided for pivot ovlumn {:?}",
                        pivot_col
                    ));
                }
            },
            Expr::String(s) | Expr::RawString(s) => s.clone(),
            Expr::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_owned(),
            Expr::Date(date) => date.replace("-", "_"),
            Expr::Struct(s) => {
                let mut parts = vec![];
                for field in &s.fields {
                    if let Some(alias) = &field.alias {
                        parts.push(format!(
                            "{}_{}",
                            alias.as_str(),
                            Self::pivot_column_alias(pivot_col, &field.expr)?
                        ))
                    } else {
                        parts.push(Self::pivot_column_alias(pivot_col, &field.expr)?)
                    }
                }
                parts.join("_")
            }
            _ => {
                return Err(anyhow!(
                    "An alias must be provided for pivot ovlumn {:?}",
                    pivot_col
                ));
            }
        })
    }

    fn table_operator_lin(
        &mut self,
        catalog: &LineageCatalog,
        obj_idx: ArenaIndex,
        table_operator: &Option<TableOperator>,
    ) -> anyhow::Result<ArenaIndex> {
        if table_operator.is_none() {
            return Ok(obj_idx);
        }

        let table_operator = table_operator.as_ref().unwrap();

        let table_name = &self.arena_objects[obj_idx].name;
        self.push_query_ctx(
            IndexMap::from([(table_name.to_owned(), obj_idx)]),
            HashSet::new(),
            false,
        );

        let obj_idx = match table_operator {
            TableOperator::Pivot(pivot) => {
                if pivot.aggregates.len() > 1
                    && pivot.aggregates.len()
                        != pivot
                            .aggregates
                            .iter()
                            .filter(|agg| agg.alias.is_some())
                            .count()
                {
                    return Err(anyhow!(
                        "Found multiple aggregate function calls. For each aggregate expression an alias must be provided."
                    ));
                }

                let mut cols_used_in_aggregates = HashSet::new();
                let mut types_aggregate = vec![];
                let mut input = vec![];
                for agg in &pivot.aggregates {
                    let node_idx =
                        self.expr_lin(catalog, &agg.expr, false, NodeOrigin::PivotAggregate)?;
                    types_aggregate.push(self.arena_lineage_nodes[node_idx].r#type.clone());
                    input.push(node_idx);

                    let mut agg_nodes = vec![node_idx];
                    while let Some(agg_node_idx) = agg_nodes.pop() {
                        let curr = &self.arena_lineage_nodes[agg_node_idx];

                        for inp in &curr.input {
                            if self.arena_lineage_nodes[*inp].source_obj == obj_idx {
                                cols_used_in_aggregates.insert(*inp);
                            } else {
                                agg_nodes.push(*inp);
                            }
                        }
                    }
                }

                let mut new_nodes = vec![];
                for pivot_col in &pivot.pivot_columns {
                    let pivot_col_name = if let Some(alias) = &pivot_col.alias {
                        alias.as_str().to_owned()
                    } else {
                        Self::pivot_column_alias(pivot_col, &pivot_col.expr)?
                    };

                    for ((aggregate, input), ty) in
                        pivot.aggregates.iter().zip(&input).zip(&types_aggregate)
                    {
                        let new_column_name = if let Some(alias) = &aggregate.alias {
                            format!("{}_{}", alias.as_str(), pivot_col_name)
                        } else {
                            pivot_col_name.to_owned()
                        };

                        new_nodes.push((
                            NodeName::Defined(new_column_name),
                            ty.clone(),
                            NodeOrigin::Select,
                            vec![*input],
                        ));
                    }
                }

                let mut new_lineage_nodes = vec![];
                let mut pivot_column_idx = None;
                let obj = &self.arena_objects[obj_idx];
                for node_idx in &obj.lineage_nodes {
                    if self.arena_lineage_nodes[*node_idx]
                        .name
                        .string()
                        .eq_ignore_ascii_case(pivot.input_column.as_str())
                    {
                        pivot_column_idx = Some(*node_idx);
                    } else if !cols_used_in_aggregates.contains(node_idx) {
                        let node = &self.arena_lineage_nodes[*node_idx];
                        new_lineage_nodes.push((
                            node.name.clone(),
                            node.r#type.clone(),
                            node.origin,
                            vec![*node_idx],
                        ));
                    }
                }
                new_lineage_nodes.extend(new_nodes);

                let pivot_column_idx = pivot_column_idx
                    .expect("Pivot column not found in table. This should not happen.");
                let pivot_column = &self.arena_lineage_nodes[pivot_column_idx];
                self.track_usage_expr_node(
                    "pivot_column",
                    pivot_column.r#type.clone(),
                    NodeOrigin::PivotColumn,
                    vec![pivot_column_idx],
                );
                self.add_referenced_column(pivot_column_idx, NodeOrigin::PivotColumn);

                let pivot_obj_name = if let Some(alias) = &pivot.alias {
                    alias.as_str()
                } else {
                    &self.get_anon_obj_name("pivot")
                };
                self.allocate_object(
                    pivot_obj_name,
                    ContextObjectKind::PivotTable,
                    new_lineage_nodes,
                )
            }
            TableOperator::Unpivot(unpivot) => {
                let unpivot_obj_name = if let Some(alias) = &unpivot.alias {
                    alias.as_str()
                } else {
                    &self.get_anon_obj_name("unpivot")
                };

                let mut new_lineage_nodes = vec![];

                match &unpivot.kind {
                    UnpivotKind::SingleColumn(single_column_unpivot) => {
                        let unpivot_column_names = single_column_unpivot
                            .columns_to_unpivot
                            .iter()
                            .map(|col| col.name.as_str().to_lowercase())
                            .collect::<HashSet<_>>();
                        let unpivot_column_indices = self.arena_objects[obj_idx]
                            .lineage_nodes
                            .iter()
                            .filter(|&node_idx| {
                                unpivot_column_names.contains(
                                    &self.arena_lineage_nodes[*node_idx]
                                        .name
                                        .string()
                                        .to_lowercase(),
                                )
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        for node_idx in &self.arena_objects[obj_idx].lineage_nodes {
                            let node = &self.arena_lineage_nodes[*node_idx];
                            if !unpivot_column_names.contains(&node.name.string().to_lowercase()) {
                                new_lineage_nodes.push((
                                    node.name.clone(),
                                    node.r#type.clone(),
                                    node.origin,
                                    vec![*node_idx],
                                ));
                            }
                        }

                        unpivot_column_indices.iter().for_each(|idx| {
                            let node = &self.arena_lineage_nodes[*idx];
                            self.track_usage_expr_node(
                                "unpivot_column",
                                node.r#type.clone(),
                                NodeOrigin::UnpivotColumn,
                                vec![*idx],
                            );
                        });

                        let unpivot_col_ty = self.arena_lineage_nodes[unpivot_column_indices[0]]
                            .r#type
                            .clone();

                        new_lineage_nodes.push((
                            NodeName::Defined(
                                single_column_unpivot.values_column.as_str().to_owned(),
                            ),
                            unpivot_col_ty,
                            NodeOrigin::Select,
                            unpivot_column_indices,
                        ));
                        new_lineage_nodes.push((
                            NodeName::Defined(
                                single_column_unpivot.name_column.as_str().to_owned(),
                            ),
                            NodeType::String,
                            NodeOrigin::Select,
                            vec![],
                        ));
                    }
                    UnpivotKind::MultiColumn(multi_column_unpivot) => {
                        let mut new_unpivot_nodes = vec![];
                        let mut names_with_count: IndexMap<String, u16> = IndexMap::new();
                        let mut all_unpivot_column_names = HashSet::new();
                        for (value_column, column_set) in multi_column_unpivot
                            .values_columns
                            .iter()
                            .zip(multi_column_unpivot.column_sets_to_unpivot.iter())
                        {
                            let unpivot_column_names = column_set
                                .names
                                .iter()
                                .map(|col| col.as_str().to_lowercase())
                                .collect::<HashSet<_>>();
                            let unpivot_column_indices = self.arena_objects[obj_idx]
                                .lineage_nodes
                                .iter()
                                .filter(|&node_idx| {
                                    unpivot_column_names.contains(
                                        self.arena_lineage_nodes[*node_idx]
                                            .name
                                            .string()
                                            .to_lowercase()
                                            .as_str(),
                                    )
                                })
                                .cloned()
                                .collect::<Vec<_>>();

                            unpivot_column_indices.iter().for_each(|idx| {
                                let node = &self.arena_lineage_nodes[*idx];
                                self.track_usage_expr_node(
                                    "unpivot_column",
                                    node.r#type.clone(),
                                    NodeOrigin::UnpivotColumn,
                                    vec![*idx],
                                );
                            });

                            let unpivot_col_ty = self.arena_lineage_nodes
                                [unpivot_column_indices[0]]
                                .r#type
                                .clone();

                            let entry = names_with_count
                                .entry(value_column.as_str().to_owned())
                                .or_insert(0);
                            let col_name = if *entry == 0 {
                                value_column.as_str().to_owned()
                            } else {
                                format!("{}_{}", value_column.as_str(), *entry)
                            };
                            *entry += 1;
                            new_unpivot_nodes.push((
                                NodeName::Defined(col_name),
                                unpivot_col_ty,
                                NodeOrigin::Select,
                                unpivot_column_indices,
                            ));
                            all_unpivot_column_names.extend(unpivot_column_names);
                        }

                        new_unpivot_nodes.push((
                            NodeName::Defined(multi_column_unpivot.name_column.as_str().to_owned()),
                            NodeType::String,
                            NodeOrigin::Select,
                            vec![],
                        ));

                        for node_idx in &self.arena_objects[obj_idx].lineage_nodes {
                            let node = &self.arena_lineage_nodes[*node_idx];
                            if !all_unpivot_column_names
                                .contains(&node.name.string().to_lowercase())
                            {
                                new_lineage_nodes.push((
                                    node.name.clone(),
                                    node.r#type.clone(),
                                    node.origin,
                                    vec![*node_idx],
                                ));
                            }
                        }
                        new_lineage_nodes.extend(new_unpivot_nodes);
                    }
                }

                self.allocate_object(
                    unpivot_obj_name,
                    ContextObjectKind::UnpivotTable,
                    new_lineage_nodes,
                )
            }
        };

        self.pop_curr_query_ctx();
        self.add_object_nodes_to_output_lineage(obj_idx);

        Ok(obj_idx)
    }

    fn select_query_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        select_query_expr: &SelectQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let script_tables_start_size = self.script_tables.len();
        let anon_obj_name = self.get_anon_obj_name("anon");

        if let Some(with) = select_query_expr.with.as_ref() {
            self.with_lin(catalog, with)?;
        }

        let mut side_inputs = vec![];

        if let Some(from) = select_query_expr.select.from.as_ref() {
            self.from_lin(catalog, from, &mut side_inputs)?;
        } else {
            self.push_empty_query_ctx(true);
        };

        let anon_obj_idx =
            self.allocate_object(&anon_obj_name, ContextObjectKind::AnonymousQuery, vec![]);

        if let Some(window) = &select_query_expr.select.window {
            let mut windows: IndexMap<String, ArenaIndex> = IndexMap::new();
            for named_window in &window.named_windows {
                match &named_window.window {
                    NamedWindowExpr::Reference(name) => {
                        let node_idx =
                            windows.get(&name.as_str().to_lowercase()).ok_or_else(|| {
                                anyhow!("Expected window with name: {}.", name.as_str())
                            })?;
                        windows.insert(named_window.name.as_str().to_lowercase(), *node_idx);
                    }
                    NamedWindowExpr::WindowSpec(window_spec) => {
                        let mut node_indices = vec![];

                        if let Some(window_name) = &window_spec.window_name {
                            node_indices.push(
                                *windows
                                    .get(&window_name.as_str().to_lowercase())
                                    .ok_or_else(|| {
                                        anyhow!(
                                            "Expected window with name: {}.",
                                            window_name.as_str()
                                        )
                                    })?,
                            );
                        }

                        if let Some(order_bys) = &window_spec.order_by {
                            for order_by in order_bys {
                                node_indices.push(self.expr_lin(
                                    catalog,
                                    &order_by.expr,
                                    expand_value_table,
                                    NodeOrigin::Window,
                                )?);
                            }
                        }

                        if let Some(partition_by_exprs) = &window_spec.partition_by {
                            for partition_by_expr in partition_by_exprs {
                                node_indices.push(self.expr_lin(
                                    catalog,
                                    partition_by_expr,
                                    expand_value_table,
                                    NodeOrigin::Window,
                                )?);
                            }
                        }

                        let new_node_idx = self.allocate_expr_node(
                            "window_spec",
                            NodeType::Unknown,
                            NodeOrigin::Window,
                            node_indices,
                        );
                        self.add_node_to_output_lineage(new_node_idx);
                        windows.insert(named_window.name.as_str().to_lowercase(), new_node_idx);
                    }
                }
            }
            self.push_query_windows(windows);
        } else {
            self.push_query_windows(IndexMap::new());
        };

        let mut lineage_nodes = vec![];
        for expr in &select_query_expr.select.exprs {
            match expr {
                SelectExpr::Col(col_expr) => {
                    self.select_expr_col_lin(catalog, anon_obj_idx, col_expr, &mut lineage_nodes)?;
                }
                SelectExpr::All(all_expr) => {
                    self.select_expr_all_lin(anon_obj_idx, all_expr, &mut lineage_nodes)?
                }
                SelectExpr::ColAll(col_all_expr) => self.select_expr_col_all_lin(
                    catalog,
                    anon_obj_idx,
                    col_all_expr,
                    &mut lineage_nodes,
                )?,
            }
        }

        if let Some(table_value) = &select_query_expr.select.table_value {
            match table_value {
                SelectTableValue::Struct if !expand_value_table => {
                    let mut struct_node_types = Vec::with_capacity(lineage_nodes.len());
                    let mut input = Vec::with_capacity(lineage_nodes.len());
                    for node_idx in &lineage_nodes {
                        let node = &self.arena_lineage_nodes[*node_idx];
                        struct_node_types.push(StructNodeFieldType::new(
                            node.name.string(),
                            node.r#type.clone(),
                            vec![*node_idx],
                        ));
                        input.extend(&node.input);
                    }
                    lineage_nodes.drain(..);

                    let struct_node = NodeType::Struct(StructNodeType {
                        fields: struct_node_types,
                    });

                    let struct_node_idx = self.allocate_lineage_node(
                        NodeName::Anonymous,
                        struct_node,
                        NodeOrigin::Select,
                        anon_obj_idx,
                        input,
                    );

                    lineage_nodes.push(struct_node_idx);
                }
                SelectTableValue::Value if expand_value_table => {
                    let struct_node = &self.arena_lineage_nodes[lineage_nodes[0]].clone();
                    lineage_nodes.pop();
                    match &struct_node.r#type {
                        NodeType::Struct(struct_node_type) => {
                            for field in &struct_node_type.fields {
                                let access_path = AccessPath {
                                    path: vec![AccessOp::Field(field.name.clone())],
                                };
                                let nested_field_idx = struct_node.access(&access_path)?;
                                let nested_field = &self.arena_lineage_nodes[nested_field_idx];

                                let new_node_idx = self.allocate_lineage_node(
                                    NodeName::Defined(nested_field.name.string().to_owned()),
                                    nested_field.r#type.clone(),
                                    NodeOrigin::Select,
                                    anon_obj_idx,
                                    vec![nested_field_idx],
                                );

                                lineage_nodes.push(new_node_idx);
                            }
                        }
                        _ => return Err(anyhow!("Expected struct type in select table value.")),
                    }
                }
                _ => {}
            }
        }

        if let Some(r#where) = &select_query_expr.select.r#where {
            let node_idx = self.expr_lin(
                catalog,
                &r#where.expr,
                expand_value_table,
                NodeOrigin::Where,
            )?;
            side_inputs.push(node_idx);
        }

        // Push query ctx with select columns (they can be referenced in group_by, having, qualify, and order_by clauses)
        self.push_query_ctx(
            IndexMap::from([(anon_obj_name, anon_obj_idx)]),
            HashSet::new(),
            true,
        );

        // Push select columns (these are never ambiguous)
        let obj = &self.arena_objects[anon_obj_idx];
        self.push_selected_columns(
            obj.lineage_nodes
                .iter()
                .map(|idx| {
                    let node = &self.arena_lineage_nodes[*idx];
                    (node.name.string().to_lowercase(), *idx)
                })
                .collect::<IndexMap<_, _>>(),
        );

        if let Some(group_by) = &select_query_expr.select.group_by {
            match &group_by.expr {
                GroupByExpr::Items { exprs } => {
                    for expr in exprs {
                        match expr {
                            Expr::Number(number) => {
                                let col_idx: u32 = number.value.parse::<u32>()?;
                                let col_node_idx = lineage_nodes[col_idx as usize - 1];
                                let col = &self.arena_lineage_nodes[col_node_idx];
                                let node_idx = self.track_usage_expr_node(
                                    "group_by",
                                    col.r#type.clone(),
                                    NodeOrigin::GroupBy,
                                    vec![col_node_idx],
                                );
                                side_inputs.push(node_idx)
                            }
                            _ => {
                                let node_idx = self.expr_lin(
                                    catalog,
                                    expr,
                                    expand_value_table,
                                    NodeOrigin::GroupBy,
                                )?;
                                side_inputs.push(node_idx);
                            }
                        }
                    }
                }
                GroupByExpr::All => {
                    // TODO: inferred grouping keys are used in "select"
                    // but we should also add them here as "groupby"
                    // https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_all
                }
            }
        }

        if let Some(having) = &select_query_expr.select.having {
            let node_idx = self.expr_lin(
                catalog,
                &having.expr,
                expand_value_table,
                NodeOrigin::Having,
            )?;
            side_inputs.push(node_idx);
        }

        if let Some(qualify) = &select_query_expr.select.qualify {
            let node_idx = self.expr_lin(
                catalog,
                &qualify.expr,
                expand_value_table,
                NodeOrigin::Qualify,
            )?;
            side_inputs.push(node_idx);
        }

        if let Some(order_by) = &select_query_expr.order_by {
            for order_by_expr in &order_by.exprs {
                match &order_by_expr.expr {
                    Expr::Number(number) => {
                        let col_idx: u32 = number.value.parse::<u32>()?;
                        let col_node_idx = lineage_nodes[col_idx as usize - 1];
                        let col = &self.arena_lineage_nodes[col_node_idx];
                        let node_idx = self.track_usage_expr_node(
                            "order_by",
                            col.r#type.clone(),
                            NodeOrigin::OrderBy,
                            vec![col_node_idx],
                        );
                        side_inputs.push(node_idx)
                    }
                    _ => {
                        let node_idx = self.expr_lin(
                            catalog,
                            &order_by_expr.expr,
                            expand_value_table,
                            NodeOrigin::OrderBy,
                        )?;
                        side_inputs.push(node_idx);
                    }
                }
            }
        }

        lineage_nodes.iter().for_each(|lin_node_idx| {
            self.add_side_inputs_to_node(*lin_node_idx, &side_inputs);
        });

        self.pop_query_windows();

        self.pop_selected_columns();

        self.pop_curr_query_ctx();

        let anon_obj = &mut self.arena_objects[anon_obj_idx];
        anon_obj.lineage_nodes = lineage_nodes;

        self.add_object_nodes_to_output_lineage(anon_obj_idx);

        let script_tables_curr_size = self.script_tables.len();
        for _ in 0..script_tables_curr_size - script_tables_start_size {
            self.pop_script_table();
        }

        self.pop_curr_query_ctx();

        Ok(anon_obj_idx)
    }

    fn grouping_query_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        grouping_query_expr: &GroupingQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let script_tables_start_size = self.script_tables.len();
        if let Some(with) = grouping_query_expr.with.as_ref() {
            self.with_lin(catalog, with)?;
        }
        let obj_idx =
            self.query_expr_lin(catalog, &grouping_query_expr.query, expand_value_table)?;
        let script_tables_curr_size = self.script_tables.len();
        for _ in 0..script_tables_curr_size - script_tables_start_size {
            self.pop_script_table();
        }
        Ok(obj_idx)
    }

    fn set_select_query_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        set_select_query_expr: &SetSelectQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let script_tables_start_size = self.script_tables.len();
        let anon_obj_name = self.get_anon_obj_name("anon");

        if let Some(with) = set_select_query_expr.with.as_ref() {
            self.with_lin(catalog, with)?;
        }

        let set_obj_idx =
            self.allocate_object(&anon_obj_name, ContextObjectKind::AnonymousQuery, vec![]);

        let left_obj_idx = self.query_expr_lin(
            catalog,
            &set_select_query_expr.left_query,
            expand_value_table,
        )?;
        let right_obj_idx = self.query_expr_lin(
            catalog,
            &set_select_query_expr.right_query,
            expand_value_table,
        )?;

        let left_obj = &self.arena_objects[left_obj_idx];
        let right_obj = &self.arena_objects[right_obj_idx];

        let nodes_elems = left_obj
            .lineage_nodes
            .iter()
            .zip(right_obj.lineage_nodes.iter())
            .map(|(l_node_idx, r_node_idx)| {
                let l_node = &self.arena_lineage_nodes[*l_node_idx];
                (
                    l_node.name.clone(),
                    l_node.r#type.clone(),
                    set_obj_idx,
                    vec![*l_node_idx, *r_node_idx],
                )
            })
            .collect::<Vec<_>>();

        let mut set_nodes = Vec::with_capacity(nodes_elems.len());
        for (name, r#type, source_obj, input) in nodes_elems.into_iter() {
            let node_idx =
                self.allocate_lineage_node(name, r#type, NodeOrigin::Select, source_obj, input);
            set_nodes.push(node_idx);
        }

        let set_obj = &mut self.arena_objects[set_obj_idx];
        set_obj.lineage_nodes = set_nodes;

        self.add_object_nodes_to_output_lineage(set_obj_idx);

        let script_tables_curr_size = self.script_tables.len();
        for _ in 0..script_tables_curr_size - script_tables_start_size {
            self.pop_script_table();
        }

        Ok(set_obj_idx)
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_lin(
        &mut self,
        catalog: &LineageCatalog,
        from: &crate::ast::From,
        side_inputs: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let mut from_tables: Vec<ArenaIndex> = Vec::new();
        let mut joined_tables: Vec<ArenaIndex> = Vec::new();
        let mut joined_ambiguous_columns: HashSet<String> = HashSet::new();

        self.from_expr_lin(
            catalog,
            &from.expr,
            &mut from_tables,
            &mut joined_tables,
            &mut joined_ambiguous_columns,
            side_inputs,
        )?;

        self.push_from_context(&from_tables, &joined_tables, joined_ambiguous_columns);
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        from_expr: &FromExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &mut Vec<ArenaIndex>,
        joined_ambiguous_columns: &mut HashSet<String>,
        side_inputs: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        match from_expr {
            FromExpr::Join(join_expr)
            | FromExpr::LeftJoin(join_expr)
            | FromExpr::RightJoin(join_expr)
            | FromExpr::FullJoin(join_expr) => self.join_expr_lineage(
                catalog,
                join_expr,
                from_tables,
                joined_tables,
                joined_ambiguous_columns,
                side_inputs,
            )?,
            FromExpr::CrossJoin(cross_join_expr) => {
                self.from_expr_lin(
                    catalog,
                    &cross_join_expr.left,
                    from_tables,
                    joined_tables,
                    joined_ambiguous_columns,
                    side_inputs,
                )?;
                match cross_join_expr.right.as_ref() {
                    FromExpr::Path(from_path_expr) => {
                        // Implicit unnest
                        self.from_path_expr_lin(
                            catalog,
                            from_path_expr,
                            from_tables,
                            joined_tables,
                            true,
                        )?
                    }
                    _ => self.from_expr_lin(
                        catalog,
                        &cross_join_expr.right,
                        from_tables,
                        joined_tables,
                        joined_ambiguous_columns,
                        side_inputs,
                    )?,
                }

                let from_tables_len = from_tables.len();
                let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
                let left_join_table = if !joined_tables.is_empty() {
                    // We have already joined two tables
                    &self.arena_objects[*joined_tables.last().unwrap()]
                } else {
                    // This is the first join, which corresponds to index -2 in the original from_tables
                    &self.arena_objects[*from_tables_split.0.last().unwrap()]
                };
                let right_join_table =
                    &self.arena_objects[*from_tables_split.1.last_mut().unwrap()];
                let mut lineage_nodes = vec![];
                let mut joined_columns = HashSet::new();
                let mut add_lineage_nodes = |table: &ContextObject| {
                    for node_idx in &table.lineage_nodes {
                        let node = &self.arena_lineage_nodes[*node_idx];
                        let newly_inserted = joined_columns.insert(node.name.string());
                        if newly_inserted {
                            lineage_nodes.push((
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*node_idx],
                            ))
                        }
                    }
                };
                add_lineage_nodes(left_join_table);
                add_lineage_nodes(right_join_table);

                let joined_table_name = format!(
                    // Create a new name for the join_table.
                    // This name is not a valid bq table name (we use {})
                    "{{{}}}",
                    [&left_join_table.name, &right_join_table.name]
                        .into_iter()
                        .fold(String::from("join"), |acc, name| {
                            format!("{}_{}", acc, name)
                        })
                );

                let table_like_idx = self.allocate_object(
                    &joined_table_name,
                    ContextObjectKind::JoinTable,
                    lineage_nodes,
                );
                self.add_object_nodes_to_output_lineage(table_like_idx);
                joined_tables.push(table_like_idx);
            }
            FromExpr::Unnest(unnest_expr) => {
                // Push new context just for unnest expr
                self.push_from_context(from_tables, joined_tables, HashSet::new());

                let node_idx = self.expr_lin(
                    catalog,
                    unnest_expr.array.as_ref(),
                    false,
                    NodeOrigin::Unnest,
                )?;

                let name = unnest_expr
                    .alias
                    .as_ref()
                    .map_or(self.get_anon_obj_name("unnest"), |alias| {
                        alias.as_str().to_owned()
                    });

                let col_name = unnest_expr
                    .alias
                    .as_ref()
                    .map_or(NodeName::Anonymous, |alias| {
                        NodeName::Defined(alias.as_str().to_owned())
                    });

                let node = &self.arena_lineage_nodes[node_idx];

                let nested_node_idx = node.access(&AccessPath {
                    path: vec![AccessOp::Index],
                })?;
                let nested_node = &self.arena_lineage_nodes[nested_node_idx];

                let unnest_nodes = match &nested_node.r#type {
                    NodeType::Struct(struct_node_type) => {
                        let mut nodes = Vec::with_capacity(struct_node_type.fields.len());
                        for field in &struct_node_type.fields {
                            let inner_node_idx = nested_node.access(&AccessPath {
                                path: vec![AccessOp::Field(field.name.clone())],
                            })?;
                            nodes.push((
                                NodeName::Defined(field.name.clone()),
                                field.r#type.clone(),
                                NodeOrigin::Unnest,
                                vec![inner_node_idx],
                            ));
                        }
                        nodes
                    }
                    _ => vec![(
                        col_name,
                        nested_node.r#type.clone(),
                        NodeOrigin::Unnest,
                        vec![nested_node_idx],
                    )],
                };

                let unnest_idx =
                    self.allocate_object(&name, ContextObjectKind::Unnest, unnest_nodes);

                self.pop_curr_query_ctx();

                self.add_object_nodes_to_output_lineage(unnest_idx);
                self.add_new_from_table(from_tables, unnest_idx)?;
            }
            FromExpr::Path(from_path_expr) => {
                self.from_path_expr_lin(catalog, from_path_expr, from_tables, joined_tables, false)?
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let source_name = &from_grouping_query_expr
                    .alias
                    .as_ref()
                    .map(|alias| alias.as_str().to_owned());

                let new_source_name = if let Some(name) = source_name {
                    name
                } else {
                    &self.get_anon_obj_name("query")
                };

                let obj_idx =
                    self.query_expr_lin(catalog, &from_grouping_query_expr.query, true)?;
                let obj = &self.arena_objects[obj_idx];

                let table_idx = self.allocate_object(
                    new_source_name,
                    ContextObjectKind::Query,
                    obj.lineage_nodes
                        .iter()
                        .map(|idx| {
                            let node = &self.arena_lineage_nodes[*idx];
                            (
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*idx],
                            )
                        })
                        .collect(),
                );
                self.add_object_nodes_to_output_lineage(table_idx);

                let table_idx = if let Some(alias) = source_name {
                    let table_alias_idx =
                        self.create_table_alias_from_table(alias.as_str(), table_idx);
                    self.add_object_nodes_to_output_lineage(table_alias_idx);
                    table_alias_idx
                } else {
                    table_idx
                };

                let table_idx = self.table_operator_lin(
                    catalog,
                    table_idx,
                    &from_grouping_query_expr.table_operator,
                )?;
                self.add_new_from_table(from_tables, table_idx)?;
            }
            FromExpr::GroupingFrom(grouping_from_expr) => self.from_expr_lin(
                catalog,
                &grouping_from_expr.query,
                from_tables,
                joined_tables,
                joined_ambiguous_columns,
                side_inputs,
            )?,
            FromExpr::TableFunction(table_function_expr) => {
                // TODO
                // - https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/range-functions#range_sessionize
                // - https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/search_functions#vector_search
                // - https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/vectorindex_functions
                // - https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/federated_query_functions#external_query
                // - https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/table-functions-built-in#external_object_transform

                let routine_name = routine_name!(table_function_expr.name.name);
                let routine_idx =
                    if let Some(routine_idx) = self.get_routine(catalog, &routine_name) {
                        routine_idx
                    } else {
                        return Err(anyhow!(
                            "Table valued function `{}` not in context.",
                            routine_name
                        ));
                    };
                let table_idx = if let Some(alias) = &table_function_expr.alias {
                    let table_alias_idx =
                        self.create_table_alias_from_table(alias.as_str(), routine_idx);
                    self.add_object_nodes_to_output_lineage(table_alias_idx);
                    table_alias_idx
                } else {
                    routine_idx
                };

                let table_idx = self.table_operator_lin(
                    catalog,
                    table_idx,
                    &table_function_expr.table_operator,
                )?;
                self.add_new_from_table(from_tables, table_idx)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_path_expr_lin(
        &mut self,
        catalog: &LineageCatalog,
        from_path_expr: &FromPathExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &[ArenaIndex],
        check_unnest: bool,
    ) -> anyhow::Result<()> {
        let table_name = &from_path_expr.path.name;
        let table_idx = self.get_table(catalog, table_name);
        let path_identifiers = from_path_expr.path.identifiers();

        if table_idx.is_none() {
            if check_unnest {
                if path_identifiers.len() > 1 {
                    let table = path_identifiers[0];
                    let column = path_identifiers[1];
                    let mut access_path = AccessPath {
                        path: path_identifiers
                            .into_iter()
                            .skip(2)
                            .map(|f| AccessOp::Field(f.to_owned()))
                            .collect::<Vec<AccessOp>>(),
                    };
                    access_path.path.push(AccessOp::Index);

                    // Push new context just for unnest expr
                    self.push_from_context(from_tables, joined_tables, HashSet::new());

                    let col_source_idx = self.get_column(Some(table), column)?;

                    self.track_usage_expr_node(
                        "unnest",
                        self.arena_lineage_nodes[col_source_idx].r#type.clone(),
                        NodeOrigin::Unnest,
                        vec![col_source_idx],
                    );

                    let col_node = &self.arena_lineage_nodes[col_source_idx];
                    let nested_node_idx = col_node.access(&access_path)?;

                    let name = from_path_expr
                        .alias
                        .as_ref()
                        .map_or(self.get_anon_obj_name("unnest"), |alias| {
                            alias.as_str().to_owned()
                        });

                    let nested_node = &self.arena_lineage_nodes[nested_node_idx];
                    let col_name = from_path_expr
                        .alias
                        .as_ref()
                        .map_or(NodeName::Anonymous, |alias| {
                            NodeName::Defined(alias.as_str().to_owned())
                        });

                    let unnest_nodes = match &nested_node.r#type {
                        NodeType::Struct(struct_node_type) => {
                            let mut nodes = Vec::with_capacity(struct_node_type.fields.len());
                            for field in &struct_node_type.fields {
                                let inner_node_idx = nested_node.access(&AccessPath {
                                    path: vec![AccessOp::Field(field.name.clone())],
                                })?;
                                nodes.push((
                                    NodeName::Defined(field.name.clone()),
                                    field.r#type.clone(),
                                    NodeOrigin::Unnest,
                                    vec![inner_node_idx],
                                ));
                            }
                            nodes
                        }
                        _ => vec![(
                            col_name,
                            nested_node.r#type.clone(),
                            NodeOrigin::Unnest,
                            vec![nested_node_idx],
                        )],
                    };

                    let unnest_idx =
                        self.allocate_object(&name, ContextObjectKind::Unnest, unnest_nodes);

                    self.pop_curr_query_ctx();

                    self.add_object_nodes_to_output_lineage(unnest_idx);
                    self.add_new_from_table(from_tables, unnest_idx)?;
                }

                return Ok(());
            }

            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                table_name
            ));
        }

        let table_idx = table_idx.unwrap();
        let table_idx = if let Some(alias) = &from_path_expr.alias {
            // If aliased, we create a new object
            let table_alias_idx = self.create_table_alias_from_table(alias.as_str(), table_idx);
            self.add_object_nodes_to_output_lineage(table_alias_idx);
            table_alias_idx
        } else {
            table_idx
        };

        let table_idx =
            self.table_operator_lin(catalog, table_idx, &from_path_expr.table_operator)?;
        self.add_new_from_table(from_tables, table_idx)?;

        Ok(())
    }

    fn push_from_context(
        &mut self,
        from_tables: &[ArenaIndex],
        joined_tables: &[ArenaIndex],
        joined_ambiguous_columns: HashSet<String>,
    ) {
        let mut clean_joined_tables: Vec<ArenaIndex> = vec![];
        let mut last_using_idx: Option<ArenaIndex> = None;
        for jt in joined_tables {
            let jt_obj = &self.arena_objects[*jt];
            match jt_obj.kind {
                ContextObjectKind::UsingTable => {
                    last_using_idx = Some(*jt);
                }
                ContextObjectKind::JoinTable => {
                    // dont push
                }
                _ => clean_joined_tables.push(*jt),
            }
        }
        if let Some(last_using_idx) = last_using_idx {
            clean_joined_tables.push(last_using_idx);
        }

        let joined_tables = clean_joined_tables;

        let mut query_tables = from_tables
            .iter()
            .map(|idx| (self.arena_objects[*idx].name.clone(), *idx))
            .collect::<IndexMap<String, ArenaIndex>>();
        query_tables.extend(
            joined_tables
                .into_iter()
                .map(|idx| (self.arena_objects[idx].name.clone(), idx)),
        );
        self.push_query_ctx(query_tables, joined_ambiguous_columns, true);
    }

    fn join_expr_lineage(
        &mut self,
        catalog: &LineageCatalog,
        join_expr: &JoinExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &mut Vec<ArenaIndex>,
        joined_ambiguous_columns: &mut HashSet<String>,
        side_inputs: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        self.from_expr_lin(
            catalog,
            &join_expr.left,
            from_tables,
            joined_tables,
            joined_ambiguous_columns,
            side_inputs,
        )?;
        self.from_expr_lin(
            catalog,
            &join_expr.right,
            from_tables,
            joined_tables,
            joined_ambiguous_columns,
            side_inputs,
        )?;
        let from_tables_len = from_tables.len();

        let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
        let left_join_table_idx = if !joined_tables.is_empty() {
            // We have already joined two tables
            *joined_tables.last().unwrap()
        } else {
            // This is the first join, which corresponds to index -2 in the original from_tables
            *from_tables_split.0.last().unwrap()
        };

        let right_join_table_idx = *from_tables_split.1.last_mut().unwrap();

        match &join_expr.cond {
            JoinCondition::Using {
                columns: using_columns,
            } => {
                let mut lineage_nodes = vec![];
                let mut using_columns_added = HashSet::new();
                for col in using_columns {
                    let col_name = col.as_str().to_lowercase();
                    let left_lineage_node_idx = self.arena_objects[left_join_table_idx]
                        .lineage_nodes
                        .iter()
                        .map(|&idx| (&self.arena_lineage_nodes[idx], idx))
                        .find(|(n, _)| n.name.string().eq_ignore_ascii_case(&col_name))
                        .ok_or_else(|| {
                            anyhow!(
                                "Cannot find column {:?} in table {:?}.",
                                col_name,
                                &self.arena_objects[left_join_table_idx].name
                            )
                        })?
                        .1;

                    let right_lineage_node_idx = self.arena_objects[right_join_table_idx]
                        .lineage_nodes
                        .iter()
                        .map(|&idx| (&self.arena_lineage_nodes[idx], idx))
                        .find(|(n, _)| n.name.string().eq_ignore_ascii_case(&col_name))
                        .ok_or_else(|| {
                            anyhow!(
                                "Cannot find column {:?} in table {:?}.",
                                col_name,
                                &self.arena_objects[right_join_table_idx].name
                            )
                        })?
                        .1;

                    lineage_nodes.push((
                        NodeName::Defined(col_name.clone()),
                        self.arena_lineage_nodes[left_lineage_node_idx]
                            .r#type
                            .clone(),
                        NodeOrigin::Source,
                        vec![left_lineage_node_idx, right_lineage_node_idx],
                    ));
                    using_columns_added.insert(col_name);

                    side_inputs.push(
                        self.track_usage_expr_node(
                            "using_column",
                            self.arena_lineage_nodes[left_lineage_node_idx]
                                .r#type
                                .clone(),
                            NodeOrigin::Join,
                            vec![left_lineage_node_idx],
                        ),
                    );
                    side_inputs.push(
                        self.track_usage_expr_node(
                            "using_column",
                            self.arena_lineage_nodes[right_lineage_node_idx]
                                .r#type
                                .clone(),
                            NodeOrigin::Join,
                            vec![right_lineage_node_idx],
                        ),
                    );
                }

                for using_col in &using_columns_added {
                    joined_ambiguous_columns.remove(using_col);
                }

                let left_columns: HashSet<&str> = self.arena_objects[left_join_table_idx]
                    .lineage_nodes
                    .iter()
                    .map(|&idx| self.arena_lineage_nodes[idx].name.string())
                    .collect();
                let right_columns: HashSet<&str> = self.arena_objects[right_join_table_idx]
                    .lineage_nodes
                    .iter()
                    .map(|&idx| self.arena_lineage_nodes[idx].name.string())
                    .collect();

                for col in left_columns.intersection(&right_columns) {
                    if !using_columns_added.contains(*col) {
                        joined_ambiguous_columns.insert(col.to_string());
                    }
                }

                // Add remaning columns not in using clause
                lineage_nodes.extend(
                    self.arena_objects[left_join_table_idx]
                        .lineage_nodes
                        .iter()
                        .map(|idx| (&self.arena_lineage_nodes[*idx], idx))
                        .filter(|(node, _)| !using_columns_added.contains(node.name.string()))
                        .map(|(node, idx)| {
                            (
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*idx],
                            )
                        }),
                );

                lineage_nodes.extend(
                    self.arena_objects[right_join_table_idx]
                        .lineage_nodes
                        .iter()
                        .map(|idx| (&self.arena_lineage_nodes[*idx], idx))
                        .filter(|(node, _)| !using_columns_added.contains(node.name.string()))
                        .map(|(node, idx)| {
                            (
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*idx],
                            )
                        }),
                );

                let joined_table_name = format!(
                    // Create a new name for the using_table.
                    // This name is not a valid bq table name (we use {})
                    "{{{}}}",
                    [
                        &self.arena_objects[left_join_table_idx].name,
                        &self.arena_objects[right_join_table_idx].name
                    ]
                    .into_iter()
                    .fold(String::from("join"), |acc, name| {
                        format!("{}_{}", acc, name)
                    })
                );

                let table_like_idx = self.allocate_object(
                    &joined_table_name,
                    ContextObjectKind::UsingTable,
                    lineage_nodes,
                );
                self.add_object_nodes_to_output_lineage(table_like_idx);
                joined_tables.push(table_like_idx);
            }
            JoinCondition::On(expr) => {
                let mut lineage_nodes = vec![];
                let mut joined_columns = HashSet::new();
                let mut add_lineage_nodes = |table: &ContextObject| {
                    for node_idx in &table.lineage_nodes {
                        let node = &self.arena_lineage_nodes[*node_idx];
                        let newly_inserted = joined_columns.insert(node.name.string());
                        if newly_inserted {
                            lineage_nodes.push((
                                node.name.clone(),
                                node.r#type.clone(),
                                NodeOrigin::Source,
                                vec![*node_idx],
                            ))
                        }
                    }
                };

                add_lineage_nodes(&self.arena_objects[left_join_table_idx]);
                add_lineage_nodes(&self.arena_objects[right_join_table_idx]);
                let joined_table_name = format!(
                    // Create a new name for the join_table.
                    // This name is not a valid bq table name (we use {})
                    "{{{}}}",
                    [
                        &self.arena_objects[left_join_table_idx].name,
                        &self.arena_objects[right_join_table_idx].name
                    ]
                    .into_iter()
                    .fold(String::from("join"), |acc, name| {
                        format!("{}_{}", acc, name)
                    })
                );

                let table_like_idx = self.allocate_object(
                    &joined_table_name,
                    ContextObjectKind::JoinTable,
                    lineage_nodes,
                );
                self.add_object_nodes_to_output_lineage(table_like_idx);
                joined_tables.push(table_like_idx);

                self.push_from_context(from_tables, joined_tables, HashSet::new());
                side_inputs.push(self.expr_lin(catalog, expr, false, NodeOrigin::Join)?);
                self.pop_curr_query_ctx();
            }
        }

        Ok(())
    }

    fn query_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        query_statement: &QueryStatement,
    ) -> anyhow::Result<()> {
        self.last_select_statement = Some(self.get_next_anon_obj_name("anon"));
        self.query_expr_lin(catalog, &query_statement.query, false)?;
        Ok(())
    }

    fn create_table_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        create_table_statement: &CreateTableStatement,
    ) -> anyhow::Result<()> {
        let table_kind = if create_table_statement.is_temporary {
            ContextObjectKind::TempTable
        } else {
            ContextObjectKind::Table
        };

        if table_kind == ContextObjectKind::Table {
            //todo: handle non temp table creation
            return Ok(());
        }

        let table_name = &create_table_statement.name.name;

        let temp_table_idx = if let Some(ref query) = create_table_statement.query {
            // Extract the schema from the query lineage?;
            let obj_idx = self.query_expr_lin(catalog, query, false)?;
            let obj = &self.arena_objects[obj_idx];

            self.allocate_object(
                table_name,
                table_kind,
                obj.lineage_nodes
                    .iter()
                    .map(|idx| {
                        let node = &self.arena_lineage_nodes[*idx];
                        (
                            node.name.clone(),
                            node.r#type.clone(),
                            NodeOrigin::Source,
                            vec![*idx],
                        )
                    })
                    .collect(),
            )
        } else {
            let schema = create_table_statement
                .schema
                .as_ref()
                .ok_or_else(|| anyhow!("Schema not found for table: `{:?}`.", table_name))?;
            self.allocate_object(
                table_name,
                table_kind,
                schema
                    .iter()
                    .map(|col_schema| {
                        (
                            NodeName::Defined(col_schema.name.as_str().to_owned()),
                            NodeType::from_parser_parameterized_type(&col_schema.r#type),
                            NodeOrigin::Source,
                            vec![],
                        )
                    })
                    .collect(),
            )
        };
        self.add_script_table(temp_table_idx);
        self.add_object_nodes_to_output_lineage(temp_table_idx);
        Ok(())
    }

    /// Build a mapping col_name -> node_idx for each column/node in `table_obj`
    #[inline]
    fn target_table_nodes_map(&self, table_obj: &ContextObject) -> IndexMap<String, ArenaIndex> {
        table_obj
            .lineage_nodes
            .iter()
            .map(|idx| {
                (
                    self.arena_lineage_nodes[*idx].name.string().to_lowercase(),
                    *idx,
                )
            })
            .collect::<IndexMap<String, ArenaIndex>>()
    }

    fn delete_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        delete_statement: &DeleteStatement,
    ) -> anyhow::Result<()> {
        let target_table = &delete_statement.table.name;
        let target_table_alias = if let Some(ref alias) = delete_statement.alias {
            alias.as_str()
        } else {
            target_table
        };

        let target_table_id = self.get_table(catalog, target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        self.push_query_ctx(
            IndexMap::from([(target_table_alias.to_owned(), target_table_id.unwrap())]),
            HashSet::new(),
            true,
        );

        self.expr_lin(catalog, &delete_statement.cond, false, NodeOrigin::Where)?;

        self.pop_curr_query_ctx();
        Ok(())
    }

    fn update_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        update_statement: &UpdateStatement,
    ) -> anyhow::Result<()> {
        let target_table = &update_statement.table.name;
        let target_table_alias = if let Some(ref alias) = update_statement.alias {
            alias.as_str()
        } else {
            target_table
        };

        let target_table_id = self.get_table(catalog, target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        let mut side_inputs = vec![];

        if let Some(ref from) = update_statement.from {
            self.from_lin(catalog, from, &mut side_inputs)?;
        } else {
            self.push_empty_query_ctx(true);
        };

        let target_table_obj = &self.arena_objects[target_table_id.unwrap()];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        // NOTE: we push the target table after pushing the from context
        self.push_query_ctx(
            IndexMap::from([(target_table_alias.to_owned(), target_table_id.unwrap())]),
            HashSet::new(),
            true,
        );

        side_inputs.push(self.expr_lin(
            catalog,
            &update_statement.r#where.expr,
            false,
            NodeOrigin::Where,
        )?);

        for update_item in &update_statement.update_items {
            let column = match &update_item.column {
                // col = ...
                Expr::Identifier(Identifier { name })
                | Expr::QuotedIdentifier(QuotedIdentifier { name }) => name,
                // table.col = ...
                Expr::Binary(binary_expr) => match binary_expr.right.as_ref() {
                    Expr::Identifier(Identifier { name })
                    | Expr::QuotedIdentifier(QuotedIdentifier { name }) => name,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
            .to_lowercase();

            let col_source_idx = target_table_nodes.get(&column).ok_or_else(|| {
                anyhow!(
                    "Cannot find column {} in table {}",
                    column,
                    target_table_alias
                )
            })?;

            let node_idx = self.expr_lin(catalog, &update_item.expr, false, NodeOrigin::Update)?;
            self.build_output_node(*col_source_idx, node_idx);
            self.add_side_inputs_to_node(*col_source_idx, &side_inputs);
            self.add_node_to_output_lineage(*col_source_idx);
        }

        self.pop_curr_query_ctx();

        self.pop_curr_query_ctx();
        Ok(())
    }

    fn insert_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        insert_statement: &InsertStatement,
    ) -> anyhow::Result<()> {
        let target_table = &insert_statement.table.name;

        let target_table_id = self.get_table(catalog, target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        let target_table_obj = &self.arena_objects[target_table_id.unwrap()];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        let target_columns = if let Some(columns) = &insert_statement.columns {
            let mut filtered_columns = Vec::with_capacity(columns.len());
            for col in columns {
                let col_name = col.as_str().to_lowercase();
                let col_idx = target_table_nodes.get(&col_name).ok_or_else(|| {
                    anyhow!("Cannot find column {} in table {}", col_name, target_table)
                })?;
                filtered_columns.push(*col_idx);
            }
            filtered_columns
        } else {
            target_table_obj.lineage_nodes.clone()
        };

        if let Some(query_expr) = &insert_statement.query {
            let obj_idx = self.query_expr_lin(catalog, query_expr, false)?;
            let obj_lineage_nodes = self.arena_objects[obj_idx].lineage_nodes.clone();

            if obj_lineage_nodes.len() != target_columns.len() {
                return Err(anyhow!(
                    "The number of insert columns is not equal to the number of insert values."
                ));
            }

            target_columns
                .iter()
                .zip(obj_lineage_nodes)
                .for_each(|(target_col, value)| {
                    self.build_output_node(*target_col, value);
                    self.add_node_to_output_lineage(*target_col);
                });
        } else {
            for (target_col, value) in target_columns
                .iter()
                .zip(insert_statement.values.as_ref().unwrap())
            {
                let node_idx = self.expr_lin(catalog, value, false, NodeOrigin::Insert)?;
                self.build_output_node(*target_col, node_idx);
                self.add_node_to_output_lineage(*target_col);
            }
        }

        Ok(())
    }

    fn merge_insert(
        &mut self,
        catalog: &LineageCatalog,
        target_table_id: ArenaIndex,
        merge_insert: &MergeInsert,
        side_inputs: &[ArenaIndex],
    ) -> anyhow::Result<()> {
        let target_table_obj = &self.arena_objects[target_table_id];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        let target_columns = if let Some(columns) = &merge_insert.columns {
            let mut filtered_columns = Vec::with_capacity(columns.len());
            for col in columns {
                let col_name = col.as_str().to_lowercase();
                let col_idx = target_table_nodes.get(&col_name).ok_or_else(|| {
                    anyhow!(
                        "Cannot find column {} in table {}",
                        col_name,
                        target_table_obj.name
                    )
                })?;
                filtered_columns.push(*col_idx);
            }
            filtered_columns
        } else {
            target_table_obj.lineage_nodes.clone()
        };
        for (target_col, value) in target_columns.iter().zip(&merge_insert.values) {
            let node_idx = self.expr_lin(catalog, value, false, NodeOrigin::MergeInsert)?;
            self.build_output_node(*target_col, node_idx);
            self.add_side_inputs_to_node(*target_col, side_inputs);
            self.add_node_to_output_lineage(*target_col);
        }

        Ok(())
    }

    fn merge_update(
        &mut self,
        catalog: &LineageCatalog,
        target_table_name: &str,
        target_table_id: ArenaIndex,
        merge_update: &MergeUpdate,
        side_inputs: &[ArenaIndex],
    ) -> anyhow::Result<()> {
        let target_table_obj = &self.arena_objects[target_table_id];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        for update_item in &merge_update.update_items {
            let column = match &update_item.column {
                // col = ...
                Expr::Identifier(Identifier { name })
                | Expr::QuotedIdentifier(QuotedIdentifier { name }) => name,
                // table.col = ...
                Expr::Binary(binary_expr) => match binary_expr.right.as_ref() {
                    Expr::Identifier(Identifier { name })
                    | Expr::QuotedIdentifier(QuotedIdentifier { name }) => name,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
            .to_lowercase();

            let col_source_idx = target_table_nodes.get(&column).ok_or_else(|| {
                anyhow!(
                    "Cannot find column {} in table {}",
                    column,
                    target_table_name
                )
            })?;

            let node_idx =
                self.expr_lin(catalog, &update_item.expr, false, NodeOrigin::MergeUpdate)?;
            self.build_output_node(*col_source_idx, node_idx);
            self.add_side_inputs_to_node(*col_source_idx, side_inputs);
            self.add_node_to_output_lineage(*col_source_idx);
        }
        Ok(())
    }

    fn merge_insert_row(
        &mut self,
        target_table_id: ArenaIndex,
        source_table_id: Option<ArenaIndex>,
        subquery_nodes: &Option<Vec<ArenaIndex>>,
        side_inputs: &[ArenaIndex],
    ) -> anyhow::Result<()> {
        let nodes = if let Some(source_idx) = source_table_id {
            let source_obj = &self.arena_objects[source_idx];
            source_obj.lineage_nodes.clone()
        } else {
            subquery_nodes.as_ref().unwrap().clone()
        };

        let target_table = &self.arena_objects[target_table_id];
        let target_nodes = target_table.lineage_nodes.clone();

        if target_nodes.len() != nodes.len() {
            return Err(anyhow!(
                "The number of merge insert columns is not equal to the number of columns in target table `{}`.",
                target_table.name
            ));
        }

        for (target_node, source_node) in target_nodes.into_iter().zip(nodes) {
            self.build_output_node(target_node, source_node);
            self.add_side_inputs_to_node(target_node, side_inputs);
            self.add_node_to_output_lineage(target_node);
        }

        Ok(())
    }

    fn merge_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        merge_statement: &MergeStatement,
    ) -> anyhow::Result<()> {
        let target_table_name = &merge_statement.target_table.name;
        let target_table_id = match self.get_table(catalog, target_table_name) {
            Some(target_table_id) => target_table_id,
            None => {
                return Err(anyhow!(
                    "Table like obj name `{}` not in context.",
                    target_table_name
                ));
            }
        };

        let source_table_id = if let MergeSource::Table(path_name) = &merge_statement.source {
            let source_table = &path_name.name;
            let source_table_id = self.get_table(catalog, source_table);
            if source_table_id.is_none() {
                return Err(anyhow!(
                    "Table like obj name `{}` not in context.",
                    source_table
                ));
            }
            let source_table_id = source_table_id.unwrap();
            Some(source_table_id)
        } else {
            None
        };

        let (subquery_table_id, subquery_nodes) =
            if let MergeSource::Subquery(query_expr) = &merge_statement.source {
                let obj_idx = self.query_expr_lin(catalog, query_expr, false)?;
                let obj = &self.arena_objects[obj_idx];
                let input = obj.lineage_nodes.clone();

                if let Some(alias) = &merge_statement.source_alias {
                    let new_source_name = alias.as_str();
                    let source_idx = self.allocate_object(
                        new_source_name,
                        ContextObjectKind::Query,
                        input
                            .iter()
                            .map(|idx| {
                                let node = &self.arena_lineage_nodes[*idx];
                                (
                                    node.name.clone(),
                                    node.r#type.clone(),
                                    node.origin,
                                    vec![*idx],
                                )
                            })
                            .collect(),
                    );
                    self.add_object_nodes_to_output_lineage(source_idx);
                    (Some(source_idx), Some(input))
                } else {
                    (None, Some(input))
                }
            } else {
                (None, None)
            };

        let source_table_id = source_table_id.or(subquery_table_id);

        let mut full_tables: IndexMap<String, ArenaIndex> = IndexMap::new();
        let mut source_tables: IndexMap<String, ArenaIndex> = IndexMap::new();
        let mut target_tables: IndexMap<String, ArenaIndex> = IndexMap::new();
        if let Some(alias) = &merge_statement.source_alias {
            let alias_idx =
                self.create_table_alias_from_table(alias.as_str(), source_table_id.unwrap());
            self.add_object_nodes_to_output_lineage(alias_idx);
            let source_table = &self.arena_objects[alias_idx];
            full_tables.insert(source_table.name.clone(), alias_idx);
            source_tables.insert(source_table.name.clone(), alias_idx);
        } else if let Some(source_table_id) = source_table_id {
            let source_table = &self.arena_objects[source_table_id];
            full_tables.insert(source_table.name.clone(), source_table_id);
            source_tables.insert(source_table.name.clone().to_owned(), source_table_id);
        }
        if let Some(alias) = &merge_statement.target_alias {
            let alias_idx = self.create_table_alias_from_table(alias.as_str(), target_table_id);
            self.add_object_nodes_to_output_lineage(alias_idx);
            let target_table = &self.arena_objects[alias_idx];
            full_tables.insert(target_table.name.clone(), alias_idx);
            target_tables.insert(target_table.name.clone(), alias_idx);
        } else {
            let target_table = &self.arena_objects[target_table_id];
            full_tables.insert(target_table.name.clone(), target_table_id);
            target_tables.insert(target_table.name.clone(), target_table_id);
        }

        self.push_query_ctx(full_tables.clone(), HashSet::new(), true);
        let join_idx = self.expr_lin(
            catalog,
            &merge_statement.condition,
            false,
            NodeOrigin::MergeJoin,
        )?;
        self.pop_curr_query_ctx();

        for when in &merge_statement.whens {
            match when {
                When::Matched(when_matched) => {
                    let mut side_inputs = vec![join_idx];

                    self.push_query_ctx(full_tables.clone(), HashSet::new(), true);

                    if let Some(search_condition) = &when_matched.search_condition {
                        side_inputs.push(self.expr_lin(
                            catalog,
                            search_condition,
                            false,
                            NodeOrigin::MergeCond,
                        )?);
                    }

                    match &when_matched.merge {
                        Merge::Update(merge_update) => self.merge_update(
                            catalog,
                            target_table_name,
                            target_table_id,
                            merge_update,
                            &side_inputs,
                        )?,
                        Merge::Delete => {}
                        _ => unreachable!(),
                    }

                    self.pop_curr_query_ctx();
                }
                When::NotMatchedByTarget(when_not_matched_by_target) => {
                    let mut side_inputs = vec![join_idx];

                    self.push_query_ctx(source_tables.clone(), HashSet::new(), true);

                    if let Some(search_condition) = &when_not_matched_by_target.search_condition {
                        side_inputs.push(self.expr_lin(
                            catalog,
                            search_condition,
                            false,
                            NodeOrigin::MergeCond,
                        )?);
                    }

                    match &when_not_matched_by_target.merge {
                        Merge::Insert(merge_insert) => {
                            self.merge_insert(catalog, target_table_id, merge_insert, &side_inputs)?
                        }
                        Merge::InsertRow => self.merge_insert_row(
                            target_table_id,
                            source_table_id,
                            &subquery_nodes,
                            &side_inputs,
                        )?,
                        _ => unreachable!(),
                    }

                    self.pop_curr_query_ctx();
                }
                When::NotMatchedBySource(when_not_matched_by_source) => {
                    let mut side_inputs = vec![join_idx];

                    self.push_query_ctx(target_tables.clone(), HashSet::new(), true);

                    if let Some(search_condition) = &when_not_matched_by_source.search_condition {
                        side_inputs.push(self.expr_lin(
                            catalog,
                            search_condition,
                            false,
                            NodeOrigin::MergeCond,
                        )?);
                    }

                    match &when_not_matched_by_source.merge {
                        Merge::Update(merge_update) => self.merge_update(
                            catalog,
                            target_table_name,
                            target_table_id,
                            merge_update,
                            &side_inputs,
                        )?,
                        Merge::Delete => {}
                        _ => unreachable!(),
                    }

                    self.pop_curr_query_ctx();
                }
            }
        }

        Ok(())
    }

    fn create_new_var(
        &mut self,
        name: &str,
        node_type: NodeType,
        input_lineage_nodes: &[ArenaIndex],
    ) -> ArenaIndex {
        // Var names are case insensitive (like column names)
        let var_ident = name.to_lowercase();
        let obj_name = format!("!var_{}", var_ident);

        let object_idx = self.allocate_object(
            &obj_name,
            ContextObjectKind::Var,
            vec![(
                NodeName::Defined(var_ident.clone()),
                node_type,
                NodeOrigin::Var,
                input_lineage_nodes.into(),
            )],
        );
        let var_node_idx = self.arena_objects[object_idx].lineage_nodes[0];
        self.vars.add(var_ident, var_node_idx);
        self.add_node_to_output_lineage(var_node_idx);
        object_idx
    }

    fn declare_var_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        declare_var_statement: &DeclareVarStatement,
    ) -> anyhow::Result<Vec<ArenaIndex>> {
        let declared_vars = Vec::with_capacity(declare_var_statement.vars.len());

        let input_lineage_nodes = if let Some(default_expr) = &declare_var_statement.default {
            let node_idx = self.expr_lin(catalog, default_expr, false, NodeOrigin::DefaultVar)?;
            vec![node_idx]
        } else {
            vec![]
        };

        for var in &declare_var_statement.vars {
            self.create_new_var(
                var.as_str(),
                declare_var_statement.r#type.as_ref().map_or_else(
                    || NodeType::Unknown,
                    NodeType::from_parser_parameterized_type,
                ),
                &input_lineage_nodes,
            );
        }

        Ok(declared_vars)
    }

    fn set_var_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        set_var_statement: &SetVarStatement,
    ) -> anyhow::Result<()> {
        if set_var_statement.vars.len() > 1 && set_var_statement.exprs.len() == 1 {
            let node_idx = self.expr_lin(
                catalog,
                &set_var_statement.exprs[0],
                false,
                NodeOrigin::SetVar,
            )?;
            let node = &self.arena_lineage_nodes[node_idx];
            let mut inputs = vec![];
            match &node.r#type {
                NodeType::Struct(struct_node_type) => struct_node_type
                    .fields
                    .iter()
                    .for_each(|f| inputs.push(f.input.clone())),
                _ => return Err(anyhow!("Expected struct node.")),
            }
            for (i, var) in set_var_statement.vars.iter().enumerate() {
                match var {
                    SetVariable::UserVariable(var_name) => {
                        let var_node_idx = self.vars.get(var_name.as_str())?;
                        self.add_inputs_to_node(var_node_idx, &inputs[i]);
                    }
                    SetVariable::SystemVariable(_) => {}
                }
            }
        } else {
            debug_assert!(set_var_statement.vars.len() == set_var_statement.exprs.len());
            for (var, expr) in set_var_statement.vars.iter().zip(&set_var_statement.exprs) {
                match var {
                    SetVariable::UserVariable(var_name) => {
                        let node_idx = self.expr_lin(catalog, expr, false, NodeOrigin::SetVar)?;
                        let var_node_idx = self.vars.get(var_name.as_str())?;
                        self.add_inputs_to_node(var_node_idx, &[node_idx]);
                    }
                    SetVariable::SystemVariable(_) => {}
                }
            }
        }

        Ok(())
    }

    /// Drop tables (just consider temp tables)
    fn drop_table_statement_lin(
        &mut self,
        catalog: &LineageCatalog,
        drop_table_statement: &DropTableStatement,
    ) -> anyhow::Result<()> {
        let table_name = &drop_table_statement.name.name;
        let is_source_table = catalog.source_tables.contains_key(table_name);

        if !is_source_table {
            // Not a source table, it is a table created in this script
            self.script_tables.retain(|obj_idx| {
                let obj = &self.arena_objects[*obj_idx];
                // todo: remove also non temp tables created in this script (need to adapt create table code)
                !(obj.name == *table_name && obj.kind == ContextObjectKind::TempTable)
            });
        }
        Ok(())
    }

    /// Drop functions
    fn drop_function_statement_lin(
        &mut self,
        catalog: &mut LineageCatalog,
        drop_table_statement: &DropFunctionStatement,
    ) -> anyhow::Result<()> {
        let routine_name = routine_name!(drop_table_statement.name.name);
        let is_source_routine = catalog.source_tables.contains_key(&routine_name);

        if !is_source_routine {
            // Not a source routine, it is a function created in this script
            self.script_routines
                .retain(|obj_idx| self.arena_objects[*obj_idx].name != routine_name);
            catalog.user_sql_functions.swap_remove(&routine_name);
        }
        Ok(())
    }

    /// Remove vars from current scope
    fn remove_scope_vars(&mut self, vars: &[ArenaIndex]) {
        vars.iter().for_each(|obj_idx| {
            let var_obj = &self.arena_objects[*obj_idx];
            let var_node_name = self.arena_lineage_nodes[var_obj.lineage_nodes[0]]
                .name
                .string();
            self.vars.remove(var_node_name);
        });
    }

    fn statements_block_lin(
        &mut self,
        catalog: &mut LineageCatalog,
        statements_block: &StatementsBlock,
    ) -> anyhow::Result<()> {
        // We use a vec since a variable cannot be redeclared in an inner scope with the same name
        let mut declared_vars = vec![];

        for statement in &statements_block.statements {
            match statement {
                Statement::DeclareVar(declare_var_statement) => {
                    declared_vars
                        .extend(self.declare_var_statement_lin(catalog, declare_var_statement)?);
                }
                _ => self.statement_lin(catalog, statement)?,
            }
        }

        if let Some(exception_statements) = &statements_block.exception_statements {
            for statement in exception_statements {
                // A declare statement can't be here as variable declarations
                // must appear at the start of a block
                self.statement_lin(catalog, statement)?;
            }
        }

        self.remove_scope_vars(&declared_vars);
        Ok(())
    }

    fn for_in_statement_lin(
        &mut self,
        catalog: &mut LineageCatalog,
        for_in_statement: &ForInStatement,
    ) -> anyhow::Result<()> {
        let obj_idx = self.query_expr_lin(catalog, &for_in_statement.table_expr, false)?;
        let obj = &self.arena_objects[obj_idx];

        let mut struct_node_types = Vec::with_capacity(obj.lineage_nodes.len());
        let mut input = Vec::with_capacity(obj.lineage_nodes.len());
        for node_idx in &obj.lineage_nodes {
            let node = &self.arena_lineage_nodes[*node_idx];
            struct_node_types.push(StructNodeFieldType::new(
                node.name.string(),
                node.r#type.clone(),
                vec![*node_idx],
            ));
            input.extend(&node.input);
        }
        let struct_node = NodeType::Struct(StructNodeType {
            fields: struct_node_types,
        });

        let obj_name = self.get_anon_obj_name("anon_struct");
        let obj_idx = self.allocate_object(&obj_name, ContextObjectKind::AnonymousStruct, vec![]);

        let node = LineageNode {
            name: NodeName::Anonymous,
            r#type: struct_node.clone(),
            origin: NodeOrigin::Select,
            source_obj: obj_idx,
            input: input.clone(),
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.arena_lineage_nodes.allocate(node);
        self.add_nested_nodes(node_idx);

        self.add_node_to_output_lineage(node_idx);
        let var_idx =
            self.create_new_var(for_in_statement.var_name.as_str(), struct_node, &[node_idx]);

        for statement in &for_in_statement.statements {
            self.statement_lin(catalog, statement)?
        }

        self.remove_scope_vars(&[var_idx]);

        Ok(())
    }

    fn create_sql_function_statement_lin(
        &mut self,
        catalog: &mut LineageCatalog,
        create_sql_function_statement: &CreateSqlFunctionStatement,
    ) -> anyhow::Result<()> {
        let sql_function = UserSqlFunction::from_create_statement(create_sql_function_statement);
        let routine_name = routine_name!(sql_function.name);
        let routine_nodes = vec![(
            NodeName::Anonymous,
            sql_function.returns.clone().unwrap_or(NodeType::Unknown),
            NodeOrigin::Source,
            vec![],
        )];
        let kind = if sql_function.is_temporary {
            ContextObjectKind::TempUserSqlFunction
        } else {
            ContextObjectKind::UserSqlFunction
        };
        let routine_idx = self.allocate_object(&routine_name, kind, routine_nodes);
        catalog
            .user_sql_functions
            .insert(routine_name, sql_function);
        self.add_script_routine(routine_idx);
        Ok(())
    }

    fn create_js_function_statement_lin(
        &mut self,
        create_js_function_statement: &CreateJsFunctionStatement,
    ) -> anyhow::Result<()> {
        let routine_name = routine_name!(create_js_function_statement.name.name);
        let routine_nodes = vec![(
            NodeName::Anonymous,
            NodeType::from_parser_type(&create_js_function_statement.returns),
            NodeOrigin::Source,
            vec![],
        )];
        let kind = if create_js_function_statement.is_temporary {
            ContextObjectKind::TempUserJsFunction
        } else {
            ContextObjectKind::UserJsFunction
        };
        let routine_idx = self.allocate_object(&routine_name, kind, routine_nodes);
        self.add_script_routine(routine_idx);
        Ok(())
    }

    fn statement_lin(
        &mut self,
        catalog: &mut LineageCatalog,
        statement: &Statement,
    ) -> anyhow::Result<()> {
        match statement {
            Statement::Labeled(labeled_statement) => {
                self.statement_lin(catalog, &labeled_statement.statement)?
            }
            Statement::Query(query_statement) => {
                self.query_statement_lin(catalog, query_statement)?
            }
            Statement::Update(update_statement) => {
                self.update_statement_lin(catalog, update_statement)?
            }
            Statement::While(while_statement) => {
                for statement in &while_statement.statements {
                    self.statement_lin(catalog, statement)?
                }
            }
            Statement::ForIn(for_in_statement) => {
                self.for_in_statement_lin(catalog, for_in_statement)?
            }
            Statement::Repeat(repeat_statement) => {
                for statement in &repeat_statement.statements {
                    self.statement_lin(catalog, statement)?
                }
            }
            Statement::Insert(insert_statement) => {
                self.insert_statement_lin(catalog, insert_statement)?
            }
            Statement::Merge(merge_statement) => {
                self.merge_statement_lin(catalog, merge_statement)?
            }
            Statement::CreateTable(create_table_statement) => {
                // To handle temp tables
                self.create_table_statement_lin(catalog, create_table_statement)?
            }
            Statement::CreateSqlFunction(create_sql_function_statement) => {
                self.create_sql_function_statement_lin(catalog, create_sql_function_statement)?
            }
            Statement::CreateJsFunction(create_js_function_statement) => {
                self.create_js_function_statement_lin(create_js_function_statement)?
            }
            Statement::DeclareVar(declare_var_statement) => {
                self.declare_var_statement_lin(catalog, declare_var_statement)?;
            }
            Statement::SetVar(set_var_statement) => {
                self.set_var_statement_lin(catalog, set_var_statement)?
            }
            Statement::Block(statements_block) => {
                self.statements_block_lin(catalog, statements_block)?
            }
            Statement::Loop(loop_statement) => {
                for statement in &loop_statement.statements {
                    self.statement_lin(catalog, statement)?
                }
            }
            Statement::DropTable(drop_table_statement) => {
                // To handle drop of temp tables
                self.drop_table_statement_lin(catalog, drop_table_statement)?
            }
            Statement::DropFunction(drop_function_statement) => {
                self.drop_function_statement_lin(catalog, drop_function_statement)?
            }
            Statement::If(if_statement) => {
                for statement in &if_statement.r#if.statements {
                    self.statement_lin(catalog, statement)?;
                }
                if let Some(ref else_ifs) = if_statement.else_ifs {
                    for else_if in else_ifs {
                        for statement in &else_if.statements {
                            self.statement_lin(catalog, statement)?;
                        }
                    }
                }
                if let Some(ref else_statements) = if_statement.r#else {
                    for statement in else_statements {
                        self.statement_lin(catalog, statement)?;
                    }
                }
            }
            Statement::Case(case_statement) => {
                for when_then in &case_statement.when_thens {
                    for statement in &when_then.then {
                        self.statement_lin(catalog, statement)?;
                    }
                }
                if let Some(ref else_statements) = case_statement.r#else {
                    for statement in else_statements {
                        self.statement_lin(catalog, statement)?;
                    }
                }
            }
            Statement::Delete(delete_statement) => {
                self.delete_statement_lin(catalog, delete_statement)?;
            }
            Statement::Truncate(_)
            | Statement::BeginTransaction
            | Statement::CommitTransaction
            | Statement::RollbackTransaction
            | Statement::Raise(_)
            | Statement::Call(_)
            | Statement::Return
            | Statement::Break
            | Statement::Leave
            | Statement::Continue
            | Statement::Iterate => {}
            Statement::ExecuteImmediate(_) => {
                // Execute immediate runs an arbitrary sql string
                // which may be known only at runtime
            }
            Statement::CreateSchema(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::DropSchema(_)
            | Statement::UndropSchema(_) => {
                // Ignored DDLs
            }
        }
        Ok(())
    }

    fn ast_lin(&mut self, catalog: &mut LineageCatalog, query: &Ast) -> anyhow::Result<()> {
        let mut declared_vars = vec![];
        for statement in &query.statements {
            match statement {
                Statement::DeclareVar(declare_var_statement) => {
                    declared_vars
                        .extend(self.declare_var_statement_lin(catalog, declare_var_statement)?);
                }
                _ => self.statement_lin(catalog, statement)?,
            }
        }
        self.remove_scope_vars(&declared_vars);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct LineageCatalog {
    source_tables: IndexMap<String, ArenaIndex>,
    source_routines: IndexMap<String, ArenaIndex>,
    /// Currently used to store templated functions
    user_sql_functions: IndexMap<String, UserSqlFunction>,
}

impl LineageCatalog {
    fn reset(&mut self) {
        self.user_sql_functions
            .retain(|_, sql_func| !sql_func.is_temporary);
    }
    fn is_source_obj(&self, name: &str) -> bool {
        self.source_tables.contains_key(name) || self.source_routines.contains_key(name)
    }
}

#[derive(Debug, Default)]
struct LineageExtractor {
    context: LineageContext,
    catalog: LineageCatalog,
}

impl LineageExtractor {
    fn reset(&mut self) {
        self.context.reset();
        self.catalog.reset();
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct RawLineageObject {
    pub id: usize,
    pub name: String,
    pub kind: String,
    pub nodes: Vec<usize>,
}

#[derive(Serialize, Debug, Clone)]
pub struct RawLineageNode {
    pub id: usize,
    pub name: String,
    pub r#type: String,
    pub source_object: usize,
    pub inputs: Vec<usize>,
}

#[derive(Serialize, Debug, Clone)]
pub struct RawLineage {
    pub objects: Vec<RawLineageObject>,
    pub lineage_nodes: Vec<RawLineageNode>,
    pub output_lineage: Vec<usize>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageNodeSideInput {
    pub obj_name: String,
    pub obj_kind: String,
    pub node_name: String,
    pub sides: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageNodeInput {
    pub obj_name: String,
    pub obj_kind: String,
    pub node_name: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageNode {
    pub name: String,
    pub r#type: String,
    pub inputs: Vec<ReadyLineageNodeInput>,
    pub side_inputs: Vec<ReadyLineageNodeSideInput>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageObject {
    pub name: String,
    pub kind: String,
    pub nodes: Vec<ReadyLineageNode>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineage {
    pub objects: Vec<ReadyLineageObject>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferencedNode {
    pub name: String,
    pub referenced_in: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferencedObject {
    pub name: String,
    pub kind: String,
    pub nodes: Vec<ReferencedNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReferencedColumns {
    pub objects: Vec<ReferencedObject>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Lineage {
    pub raw_lineage: Option<RawLineage>,
    pub lineage: ReadyLineage,
    pub referenced_columns: ReferencedColumns,
}

fn parse_parameterized_dtype(dtype: &str) -> anyhow::Result<NodeType> {
    let mut scanner = Scanner::new(dtype);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let r#type = parser.parse_parameterized_bq_type()?;
    Ok(NodeType::from_parser_parameterized_type(&r#type))
}

fn parse_dtype(dtype: &str) -> anyhow::Result<NodeType> {
    let mut scanner = Scanner::new(dtype);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let r#type = parser.parse_bq_type()?;
    Ok(NodeType::from_parser_type(&r#type))
}

fn parse_expr(s: &str) -> anyhow::Result<Expr> {
    let mut scanner = Scanner::new(s);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    parser.parse_expr()
}

fn columns_to_nodes(
    schema_object_name: &str,
    columns: &[Column],
) -> Vec<(NodeName, NodeType, NodeOrigin, Vec<ArenaIndex>)> {
    let mut unique_columns = HashSet::new();
    let mut duplicate_columns = vec![];
    let are_columns_unique = columns.iter().all(|col| {
        let is_unique = unique_columns.insert(&col.name);
        if !is_unique {
            duplicate_columns.push(&col.name);
        }
        is_unique
    });
    if !are_columns_unique {
        panic!(
            "Found duplicate columns in schema object `{}`: `{:?}`.",
            schema_object_name, duplicate_columns
        );
    }

    let mut nodes = Vec::with_capacity(columns.len());

    for col in columns {
        let node_type = match parse_parameterized_dtype(&col.dtype) {
            Err(err) => {
                panic!(
                    "Cannot retrieve node type from column {:?} due to: {}",
                    col, err
                );
            }
            Ok(node_type) => node_type,
        };

        // Check that struct field names are unique
        if let NodeType::Struct(ref struct_node_type) = node_type {
            let mut set = HashSet::new();

            for field in &struct_node_type.fields {
                if set.contains(&field.name) {
                    panic!(
                        "Struct column `{}` in schema object `{}` contains duplicate field with name `{}`.",
                        &col.name, schema_object_name, &field.name
                    );
                }
                set.insert(&field.name);
            }
        }

        nodes.push((
            NodeName::Defined(col.name.to_lowercase()),
            node_type,
            NodeOrigin::Source,
            vec![],
        ));
    }
    nodes
}

pub fn extract_lineage(
    asts: &[&Ast],
    catalog: &Catalog,
    include_raw: bool,
    parallel: bool,
) -> Vec<anyhow::Result<Lineage>> {
    if parallel {
        let n_chunks = std::cmp::max(
            1,
            asts.len() / std::thread::available_parallelism().unwrap().get(),
        );
        let lineages: Vec<anyhow::Result<Lineage>> = asts
            .par_chunks(n_chunks)
            .flat_map(|asts| _extract_lineage(asts, catalog, include_raw))
            .collect();
        lineages
    } else {
        _extract_lineage(asts, catalog, include_raw)
    }
}

fn _extract_lineage(
    asts: &[&Ast],
    catalog: &Catalog,
    include_raw: bool,
) -> Vec<anyhow::Result<Lineage>> {
    let mut context = LineageContext::default();
    let mut source_tables = IndexMap::new();
    let mut source_routines = IndexMap::new();
    let mut user_sql_functions = IndexMap::new();

    for schema_object in &catalog.schema_objects {
        match &schema_object.kind {
            SchemaObjectKind::Table { columns } | SchemaObjectKind::View { columns } => {
                if source_tables.contains_key(&schema_object.name) {
                    panic!(
                        "Found duplicate definition of table schema object `{}`.",
                        schema_object.name
                    );
                }
                let table_nodes = columns_to_nodes(&schema_object.name, columns);
                let table_idx = context.allocate_object(
                    &schema_object.name,
                    ContextObjectKind::Table,
                    table_nodes,
                );
                source_tables.insert(schema_object.name.clone(), table_idx);
            }
            SchemaObjectKind::UserJsFunction {
                arguments: _,
                returns,
            } => {
                if source_routines.contains_key(&schema_object.name) {
                    panic!(
                        "Found duplicate definition of routine schema object `{}`.",
                        schema_object.name
                    );
                }

                let returns = match parse_dtype(returns) {
                    Err(err) => {
                        panic!(
                            "Cannot retrieve return type {} of js function schema object {} due to: {}",
                            returns, schema_object.name, err
                        );
                    }
                    Ok(return_type) => return_type,
                };

                let routine_name = routine_name!(schema_object.name);
                let routine_nodes =
                    vec![(NodeName::Anonymous, returns, NodeOrigin::Source, vec![])];
                let routine_idx = context.allocate_object(
                    &routine_name,
                    ContextObjectKind::UserJsFunction,
                    routine_nodes,
                );
                source_routines.insert(routine_name.clone(), routine_idx);
            }
            SchemaObjectKind::UserSqlFunction {
                arguments,
                returns,
                body,
            } => {
                if source_routines.contains_key(&schema_object.name) {
                    panic!(
                        "Found duplicate definition of routine schema object `{}`.",
                        schema_object.name
                    );
                }

                let (arguments, found_any_type) = {
                    let mut unique_arguments = HashSet::new();
                    let mut duplicate_arguments = vec![];

                    let are_args_unique = arguments.iter().all(|arg| {
                        let is_unique = unique_arguments.insert(&arg.name);
                        if !is_unique {
                            duplicate_arguments.push(&arg.name);
                        }
                        is_unique
                    });
                    if !are_args_unique {
                        panic!(
                            "Found duplicate arguments in schema object `{}`: `{:?}`.",
                            &schema_object.name, duplicate_arguments
                        );
                    }

                    let mut found_any_type = false;
                    let mut sql_args = Vec::with_capacity(arguments.len());
                    for arg in arguments {
                        if arg.dtype.eq_ignore_ascii_case("any type") {
                            sql_args.push(UserSqlFunctionArg {
                                name: arg.name.to_lowercase(),
                                r#type: UserSqlFunctionArgType::AnyType,
                            });
                            found_any_type = true;
                        } else {
                            let arg_type = match parse_dtype(&arg.dtype) {
                                Err(err) => {
                                    panic!(
                                        "Cannot retrieve node type from argument {:?} due to: {}",
                                        arg, err
                                    );
                                }
                                Ok(node_type) => node_type,
                            };
                            sql_args.push(UserSqlFunctionArg {
                                name: arg.name.to_lowercase(),
                                r#type: UserSqlFunctionArgType::Standard(arg_type),
                            })
                        }
                    }
                    (sql_args, found_any_type)
                };

                let returns = match returns {
                    Some(returns) => match parse_dtype(returns) {
                        Err(err) => {
                            panic!(
                                "Cannot retrieve return type {} of sql function schema object {} due to: {}",
                                returns, schema_object.name, err
                            );
                        }
                        Ok(return_type) => Some(return_type),
                    },
                    None => None,
                };

                let is_templated = returns.is_none() || found_any_type;

                let body = if is_templated {
                    let body_expr = match body {
                        Some(body) => match parse_expr(body) {
                            Ok(body) => body,
                            Err(err) => panic!(
                                "Cannot parse body expression of sql function schema object {} due to: {}",
                                schema_object.name, err
                            ),
                        },
                        None => panic!(
                            "Schema object `{}` is a templated SQL function. The definition of the function body is needed.",
                            schema_object.name
                        ),
                    };
                    Some(body_expr)
                } else {
                    None
                };

                let routine_name = routine_name!(schema_object.name);
                let routine_nodes = vec![(
                    NodeName::Anonymous,
                    returns.clone().unwrap_or(NodeType::Unknown),
                    NodeOrigin::Source,
                    vec![],
                )];
                let routine_idx = context.allocate_object(
                    &routine_name,
                    ContextObjectKind::UserSqlFunction,
                    routine_nodes,
                );
                source_routines.insert(routine_name.clone(), routine_idx);

                if is_templated {
                    // We only add templated functions because we need to evaluate
                    // their body (note: we could also add non-templated ones)
                    let sql_function = UserSqlFunction {
                        name: schema_object.name.clone(),
                        arguments,
                        returns,
                        body: body.unwrap(),
                        is_temporary: false,
                    };

                    user_sql_functions.insert(routine_name, sql_function);
                }
            }
            SchemaObjectKind::TableFunction {
                arguments: _,
                returns: columns,
            } => {
                if source_routines.contains_key(&schema_object.name) {
                    panic!(
                        "Found duplicate definition of routine schema object `{}`.",
                        schema_object.name
                    );
                }
                let routine_name = routine_name!(schema_object.name);
                let routine_nodes = columns_to_nodes(&schema_object.name, columns);
                let routine_idx = context.allocate_object(
                    &routine_name,
                    ContextObjectKind::TableFunction,
                    routine_nodes,
                );
                source_routines.insert(routine_name.clone(), routine_idx);
            }
        }
    }

    let mut lineages = vec![];
    let mut lineage_extractor = LineageExtractor {
        context,
        catalog: LineageCatalog {
            source_tables,
            source_routines,
            user_sql_functions,
        },
    };

    for (i, &ast) in asts.iter().enumerate() {
        if i > 0 {
            lineage_extractor.reset();
        }
        if let Err(err) = lineage_extractor
            .context
            .ast_lin(&mut lineage_extractor.catalog, ast)
        {
            lineages.push(Err(err));
            continue;
        };

        log::debug!("Output Lineage Nodes:");
        for pending_node in &lineage_extractor.context.output {
            LineageNode::pretty_log_lineage_node(*pending_node, &lineage_extractor.context);
        }

        let mut objects: IndexMap<
            ArenaIndex,
            IndexMap<ArenaIndex, IndexSet<(ArenaIndex, ArenaIndex, NodeOrigin)>>,
        > = IndexMap::new();

        for output_node_idx in &lineage_extractor.context.output {
            let output_node = &lineage_extractor.context.arena_lineage_nodes[*output_node_idx];
            let output_source_idx = output_node.source_obj;

            let mut visited = HashSet::new();

            let mut stack: Vec<(ArenaIndex, NodeOrigin)> = output_node
                .input
                .iter()
                .map(|node_idx| {
                    (
                        *node_idx,
                        lineage_extractor.context.arena_lineage_nodes[*node_idx].origin
                            | output_node.origin,
                    )
                })
                .collect();
            loop {
                if stack.is_empty() {
                    break;
                }
                let (curr_node_idx, curr_origin) = stack.pop().unwrap();
                if visited.contains(&(curr_node_idx, curr_origin)) {
                    continue;
                }
                visited.insert((curr_node_idx, curr_origin));
                let node = &lineage_extractor.context.arena_lineage_nodes[curr_node_idx];

                let source_obj_idx = node.source_obj;
                let source_obj = &lineage_extractor.context.arena_objects[source_obj_idx];

                if lineage_extractor.catalog.is_source_obj(&source_obj.name) {
                    objects
                        .entry(output_source_idx)
                        .or_default()
                        .entry(*output_node_idx)
                        .and_modify(|s| {
                            s.insert((source_obj_idx, curr_node_idx, curr_origin));
                        })
                        .or_insert(IndexSet::from([(
                            source_obj_idx,
                            curr_node_idx,
                            curr_origin,
                        )]));
                } else {
                    stack.extend(
                        &node
                            .input
                            .iter()
                            .map(|node_idx| {
                                (
                                    *node_idx,
                                    lineage_extractor.context.arena_lineage_nodes[*node_idx].origin
                                        | curr_origin,
                                )
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }

        let just_include_source_objects = true;

        let mut ready_lineage = ReadyLineage { objects: vec![] };
        for (obj_idx, obj_map) in &objects {
            let obj = &lineage_extractor.context.arena_objects[*obj_idx];
            let obj_name = obj.name.clone();
            let obj_kind = obj.kind;
            if just_include_source_objects
                && (!lineage_extractor.catalog.is_source_obj(&obj_name)
                    && !lineage_extractor
                        .context
                        .last_select_statement
                        .as_ref()
                        .is_some_and(|anon_name| *anon_name == *obj_name))
            {
                continue;
            }

            let mut obj_nodes = vec![];

            for (node_idx, input) in obj_map {
                let node = &lineage_extractor.context.arena_lineage_nodes[*node_idx];
                let node_name = node.name.clone();
                let node_type = node.r#type.clone();

                let mut node_input = vec![];
                let mut node_side_input = vec![];
                let mut side_inputs = IndexMap::new();

                for (inp_obj_idx, inp_node_idx, inp_node_origin) in input {
                    let inp_obj = &lineage_extractor.context.arena_objects[*inp_obj_idx];
                    if just_include_source_objects
                        && !lineage_extractor.catalog.is_source_obj(&inp_obj.name)
                    {
                        continue;
                    }

                    let inp_node = &lineage_extractor.context.arena_lineage_nodes[*inp_node_idx];

                    if inp_node_origin.is_output() {
                        node_input.push(ReadyLineageNodeInput {
                            obj_name: inp_obj.name.clone(),
                            obj_kind: inp_obj.kind.into(),
                            node_name: inp_node.name.nested_path().to_owned(),
                        });
                        // Add in nodes from output lineage to referenced columns
                        lineage_extractor
                            .context
                            .add_referenced_column(*inp_node_idx, NodeOrigin::Select);
                    } else {
                        side_inputs
                            .entry(*inp_node_idx)
                            .and_modify(|origin| *origin |= *inp_node_origin)
                            .or_insert(*inp_node_origin);
                    }
                }

                for (inp_node_idx, origin) in side_inputs {
                    let sides = (origin
                        & !(NodeOrigin::Select
                            | NodeOrigin::Source
                            | NodeOrigin::Unnest
                            | NodeOrigin::UserSqlFunction))
                        .iter()
                        .map(|n| n.into())
                        .collect::<Vec<_>>();
                    let inp_node = &lineage_extractor.context.arena_lineage_nodes[inp_node_idx];
                    let inp_obj = &lineage_extractor.context.arena_objects[inp_node.source_obj];
                    node_side_input.push(ReadyLineageNodeSideInput {
                        obj_name: inp_obj.name.clone(),
                        obj_kind: inp_obj.kind.into(),
                        node_name: inp_node.name.nested_path().to_owned(),
                        sides,
                    });
                }

                obj_nodes.push(ReadyLineageNode {
                    name: node_name.nested_path().to_owned(),
                    r#type: node_type.to_string(),
                    inputs: node_input,
                    side_inputs: node_side_input,
                });
            }
            ready_lineage.objects.push(ReadyLineageObject {
                name: obj_name,
                kind: obj_kind.into(),
                nodes: obj_nodes,
            });
        }

        let raw_lineage = if include_raw {
            let arena_objects = &lineage_extractor.context.arena_objects;
            let raw_lineage_objects = arena_objects
                .into_iter()
                .enumerate()
                .map(|(idx, obj)| RawLineageObject {
                    id: idx,
                    name: obj.name.clone(),
                    kind: obj.kind.into(),
                    nodes: obj.lineage_nodes.iter().map(|aidx| aidx.index).collect(),
                })
                .collect();

            let arena_lineage_nodes = &lineage_extractor.context.arena_lineage_nodes;
            let lineage_nodes = arena_lineage_nodes
                .into_iter()
                .enumerate()
                .map(|(idx, node)| RawLineageNode {
                    id: idx,
                    name: node.name.clone().into(),
                    r#type: node.r#type.to_string(),
                    source_object: node.source_obj.index,
                    inputs: node.input.iter().map(|aidx| aidx.index).collect(),
                })
                .collect();

            let output_lineage = lineage_extractor
                .context
                .output
                .iter()
                .map(|aidx| aidx.index)
                .collect();

            Some(RawLineage {
                objects: raw_lineage_objects,
                lineage_nodes,
                output_lineage,
            })
        } else {
            None
        };

        let referenced_columns = {
            let mut referenced_columns: IndexMap<
                ArenaIndex,
                IndexMap<ArenaIndex, IndexSet<String>>,
            > = IndexMap::new();

            for (col_idx, origin) in &lineage_extractor.context.columns_referenced.map {
                let col_node_idx = col_idx;
                let mut visited = HashSet::new();
                let mut stack = vec![col_node_idx];
                loop {
                    if stack.is_empty() {
                        break;
                    }
                    let node_idx = stack.pop().unwrap();
                    if visited.contains(node_idx) {
                        continue;
                    }
                    visited.insert(node_idx);

                    let node = &lineage_extractor.context.arena_lineage_nodes[*node_idx];

                    let source_obj_idx = node.source_obj;
                    let source_obj = &lineage_extractor.context.arena_objects[source_obj_idx];

                    if lineage_extractor.catalog.is_source_obj(&source_obj.name) {
                        referenced_columns
                            .entry(source_obj_idx)
                            .or_default()
                            .entry(*node_idx)
                            .and_modify(|s| {
                                for (_, o) in origin.iter_names() {
                                    s.insert((o).into());
                                }
                            })
                            .or_insert_with(|| {
                                origin.iter_names().map(|(_, o)| o.into()).collect()
                            });
                    } else {
                        for inp in &node.input {
                            let found_source_stop =
                                lineage_extractor.context.arena_lineage_nodes[*node_idx].origin
                                    == NodeOrigin::Source
                                    && (!lineage_extractor.catalog.is_source_obj(&source_obj.name)
                                        && !matches!(
                                            source_obj.kind,
                                            ContextObjectKind::TableAlias
                                                | ContextObjectKind::Unnest
                                                | ContextObjectKind::JoinTable
                                                | ContextObjectKind::UsingTable
                                        ));

                            // It is ok to move thorugh selects and unnest
                            let moving_through_selects_and_unnest =
                                lineage_extractor.context.arena_lineage_nodes[*inp]
                                    .origin
                                    .intersects(
                                        *origin
                                            | NodeOrigin::Source
                                            | NodeOrigin::Select
                                            | NodeOrigin::Unnest,
                                    );

                            if !found_source_stop && moving_through_selects_and_unnest {
                                stack.push(inp)
                            }
                        }
                    }
                }
            }

            let mut referenced_objects = vec![];

            for (table_idx, referenced_cols) in referenced_columns {
                let table = &lineage_extractor.context.arena_objects[table_idx];

                let mut nodes = vec![];
                for (col_idx, kinds) in referenced_cols {
                    let mut sorted_kinds: Vec<_> = kinds.into_iter().collect();
                    sorted_kinds.sort();

                    let node = &lineage_extractor.context.arena_lineage_nodes[col_idx];
                    nodes.push(ReferencedNode {
                        name: node.name.nested_path().to_owned(),
                        referenced_in: sorted_kinds,
                    });
                }

                referenced_objects.push(ReferencedObject {
                    name: table.name.clone(),
                    kind: table.kind.into(),
                    nodes,
                });
            }

            ReferencedColumns {
                objects: referenced_objects,
            }
        };

        let lineage = Lineage {
            raw_lineage,
            lineage: ready_lineage,
            referenced_columns,
        };
        lineages.push(Ok(lineage));
    }

    lineages
}
