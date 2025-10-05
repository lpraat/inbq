use crate::{
    arena::{Arena, ArenaIndex},
    ast::{
        ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, BinaryOperator,
        CreateTableStatement, Cte, DeclareVarStatement, DropTableStatement, Expr, ForInStatement,
        FromExpr, FromPathExpr, FunctionExpr, GroupingQueryExpr, Identifier, InsertStatement,
        IntervalExpr, JoinCondition, JoinExpr, Merge, MergeInsert, MergeSource, MergeStatement,
        MergeUpdate, ParameterizedType, QuantifiedLikeExprPattern, QueryExpr, QueryStatement,
        QuotedIdentifier, SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr,
        SelectQueryExpr, SelectTableValue, SetSelectQueryExpr, SetVarStatement, SetVariable,
        Statement, StatementsBlock, StructExpr, Type, UpdateStatement, When, With,
    },
    parser::Parser,
    scanner::Scanner,
};
use anyhow::anyhow;
use indexmap::IndexMap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::result::Result::Ok;
use std::{
    collections::HashSet,
    fmt::{Debug, Display},
};

#[derive(Debug, Clone)]
struct LineageNode {
    name: NodeName,
    r#type: NodeType,
    source_obj: ArenaIndex,
    input: Vec<ArenaIndex>,
    nested_nodes: IndexMap<String, ArenaIndex>,
}

impl LineageNode {
    fn access(&self, path: &AccessPath) -> anyhow::Result<ArenaIndex> {
        self.nested_nodes
            .get(&path.nested_path())
            .ok_or(anyhow!(
                "Cannot find nested node {:?} in {:?} in table {:?}",
                path,
                self,
                &self.name
            ))
            .cloned()
    }

    fn pretty_log_lineage_node(node_idx: ArenaIndex, ctx: &Context) {
        let node = &ctx.arena_lineage_nodes[node_idx];
        let node_source_name = &ctx.arena_objects[node.source_obj].name;
        let in_str = node
            .input
            .iter()
            .map(|idx| {
                let in_node = &ctx.arena_lineage_nodes[*idx];
                format!(
                    "[{}]{}->{}",
                    in_node.source_obj.index,
                    ctx.arena_objects[in_node.source_obj].name,
                    in_node.name.nested_path()
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
            "[{}]{}->{} <-[{}]",
            node.source_obj.index,
            node_source_name,
            node.name.nested_path(),
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
            AccessOp::Field(s) => s.clone(),
            AccessOp::FieldStar => "*".to_owned(),
            AccessOp::Index => "[]".to_owned(),
        };
        self.path.iter().skip(1).fold(acc, |acc, op| match op {
            AccessOp::Field(f) => format!("{}.{}", acc, f),
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

#[derive(Debug, Clone)]
enum NodeType {
    Unknown,
    #[allow(dead_code)]
    Base(String),
    Struct(StructNodeType),
    Array(Box<ArrayNodeType>),
}

#[derive(Debug, Clone)]
struct ArrayNodeType {
    r#type: NodeType,
    input: Vec<ArenaIndex>,
}

#[derive(Debug, Clone)]
struct StructNodeType {
    fields: Vec<StructNodeFieldType>,
}

#[derive(Debug, Clone)]
struct StructNodeFieldType {
    name: String,
    r#type: NodeType,
    input: Vec<ArenaIndex>,
}

#[derive(Debug, Clone)]
struct ContextObject {
    name: String,
    lineage_nodes: Vec<ArenaIndex>,
    kind: ContextObjectKind,
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
    Anonymous, // TODO: anonymous_subquery?
    AnonymousStruct,
    AnonymousArray,
    Unnest,
    Var,
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
            ContextObjectKind::UsingTable => "using_table".to_owned(),
            ContextObjectKind::Anonymous => "anonymous".to_owned(),
            ContextObjectKind::AnonymousStruct => "anonymous_struct".to_owned(),
            ContextObjectKind::AnonymousArray => "anonymous_array".to_owned(),
            ContextObjectKind::Unnest => "unnest".to_owned(),
            ContextObjectKind::Var => "var".to_owned(),
        }
    }
}

#[derive(Debug, Default)]
struct Context {
    arena_objects: Arena<ContextObject>,
    arena_lineage_nodes: Arena<LineageNode>,
    source_objects: IndexMap<String, ArenaIndex>,
    objects_stack: Vec<ArenaIndex>,
    stack: Vec<IndexMap<String, ArenaIndex>>,
    columns_stack: Vec<IndexMap<String, Vec<ArenaIndex>>>,
    vars: IndexMap<String, ArenaIndex>,
    lineage_stack: Vec<ArenaIndex>,
    struct_node_field_types_stack: Vec<StructNodeFieldType>,
    output: Vec<ArenaIndex>,
}

impl Context {
    fn reset(&mut self) {
        // Keep the same arena and clear the rest
        self.objects_stack.clear();
        self.stack.clear();
        self.columns_stack.clear();
        self.vars.clear();
        self.lineage_stack.clear();
        self.struct_node_field_types_stack.clear();
        self.output.clear();
    }

    fn allocate_new_ctx_object(
        &mut self,
        name: &str,
        kind: ContextObjectKind,
        nodes: Vec<(NodeName, NodeType, Vec<ArenaIndex>)>,
    ) -> ArenaIndex {
        let new_obj = ContextObject {
            name: name.to_owned(),
            lineage_nodes: vec![],
            kind,
        };
        let new_id = self.arena_objects.allocate(new_obj);

        let mut new_lineage_nodes = vec![];
        for (node_name, node_type, items) in nodes.into_iter() {
            let lin = LineageNode {
                name: node_name,
                r#type: node_type,
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

    fn allocate_new_lineage_node(
        &mut self,
        name: NodeName,
        r#type: NodeType,
        source_obj: ArenaIndex,
        input: Vec<ArenaIndex>,
    ) -> anyhow::Result<ArenaIndex> {
        let idx = self.arena_lineage_nodes.allocate(LineageNode {
            name,
            r#type,
            source_obj,
            input: input.clone(),
            nested_nodes: IndexMap::new(),
        });
        self.add_nested_nodes_from_input_nodes(idx, &input);
        Ok(idx)
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
            .cloned()
            .map_or(vec![], |el| vec![el])
    }

    fn _add_nested_nodes_from_input_nodes(
        &mut self,
        access_path: AccessPath,
        node_idx: ArenaIndex,
        node_type: &NodeType,
        input_node_indices: &[ArenaIndex],
    ) -> Vec<ArenaIndex> {
        match node_type {
            NodeType::Unknown | NodeType::Base(_) => {
                vec![]
            }
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

                    self.output.push(nested_node_idx);
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

                self.output.push(nested_node_idx);
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
            NodeType::Unknown | NodeType::Base(_) => {
                vec![]
            }
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

                    self.output.push(lin_idx);
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
                self.output.push(lin_idx);
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
                NodeName::Nested(nested_node_name) => nested_node_name.access_path.clone(),
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
            source_obj: node.source_obj,
            input: nested_input.to_vec(),
            nested_nodes: IndexMap::new(),
        })
    }

    fn curr_stack(&self) -> Option<&IndexMap<String, ArenaIndex>> {
        self.stack.last()
    }

    fn curr_columns_stack(&self) -> Option<&IndexMap<String, Vec<ArenaIndex>>> {
        self.columns_stack.last()
    }

    fn push_new_ctx(
        &mut self,
        ctx_objects: IndexMap<String, ArenaIndex>,
        include_outer_context: bool,
    ) {
        let mut new_ctx: IndexMap<String, ArenaIndex> = ctx_objects;
        let mut new_columns: IndexMap<String, Vec<ArenaIndex>> = IndexMap::new();

        for key in new_ctx.keys() {
            let ctx_obj = &self.arena_objects[new_ctx[key]];
            for node_idx in &ctx_obj.lineage_nodes {
                let node = &self.arena_lineage_nodes[*node_idx];
                new_columns
                    .entry(node.name.string().to_lowercase())
                    .or_default()
                    .push(new_ctx[key]);
            }
        }

        if include_outer_context && !self.stack.is_empty() {
            let prev_ctx = self.stack.last().unwrap();
            for key in prev_ctx.keys() {
                if !new_ctx.contains_key(key) {
                    new_ctx.insert(key.clone(), prev_ctx[key]);
                    let ctx_obj = &self.arena_objects[prev_ctx[key]];
                    for node_idx in &ctx_obj.lineage_nodes {
                        let node = &self.arena_lineage_nodes[*node_idx];
                        new_columns
                            .entry(node.name.string().to_lowercase())
                            .or_default()
                            .push(prev_ctx[key]);
                    }
                }
            }
        }

        self.stack.push(new_ctx);
        self.columns_stack.push(new_columns);
    }

    fn pop_curr_ctx(&mut self) {
        self.stack.pop();
        self.columns_stack.pop();
    }

    fn get_object(&self, key: &str) -> Option<ArenaIndex> {
        for i in (0..self.objects_stack.len()).rev() {
            let obj = &self.arena_objects[self.objects_stack[i]];

            if matches!(obj.kind, ContextObjectKind::Cte) && obj.name.eq_ignore_ascii_case(key) {
                return Some(self.objects_stack[i]);
            }

            if obj.name == *key {
                return Some(self.objects_stack[i]);
            }
        }
        None
    }

    fn add_object(&mut self, object_idx: ArenaIndex) {
        self.objects_stack.push(object_idx);
    }

    fn pop_object(&mut self) {
        self.objects_stack.pop();
    }

    fn update_output_lineage_with_object_nodes(&mut self, obj_idx: ArenaIndex) {
        let obj = &self.arena_objects[obj_idx];
        let nodes = &obj.lineage_nodes;
        self.output.extend(nodes);
    }

    fn update_output_lineage_from_nodes(&mut self, new_lineage_nodes_indices: &Vec<ArenaIndex>) {
        self.output.extend(new_lineage_nodes_indices);
    }
}

#[derive(Debug, Default)]
struct LineageExtractor {
    anon_id: u64,
    context: Context,
}

impl LineageExtractor {
    fn get_anon_obj_id(&mut self) -> u64 {
        let curr = self.anon_id;
        self.anon_id += 1;
        curr
    }

    fn get_anon_obj_name(&mut self, name: &str) -> String {
        format!("!{}_{}", name, self.get_anon_obj_id())
    }

    fn call_func_and_consume_lineage_nodes<T>(
        &mut self,
        func: impl Fn(&mut LineageExtractor) -> std::result::Result<T, anyhow::Error>,
    ) -> anyhow::Result<Vec<ArenaIndex>> {
        let initial_stack_size = self.context.lineage_stack.len();
        func(self)?;
        let curr_lineage_len = self.context.lineage_stack.len();
        Ok(self.consume_lineage_nodes(initial_stack_size, curr_lineage_len))
    }

    fn consume_lineage_nodes(
        &mut self,
        initial_stack_size: usize,
        final_stack_size: usize,
    ) -> Vec<ArenaIndex> {
        let n_nodes = final_stack_size - initial_stack_size;
        let mut lineage_nodes = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            let node_idx = self.context.lineage_stack.pop().unwrap();
            lineage_nodes.push(node_idx)
        }
        lineage_nodes.reverse();
        lineage_nodes
    }

    fn cte_lin(&mut self, cte: &Cte) -> anyhow::Result<()> {
        match cte {
            Cte::NonRecursive(non_recursive_cte) => {
                let cte_name = &non_recursive_cte.name;

                let consumed_lineage_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                    this.query_expr_lin(&non_recursive_cte.query, true)
                })?;

                let cte_idx = self.context.allocate_new_ctx_object(
                    cte_name.as_str(),
                    ContextObjectKind::Cte,
                    consumed_lineage_nodes
                        .into_iter()
                        .map(|idx| {
                            let node = &self.context.arena_lineage_nodes[idx];
                            (node.name.clone(), node.r#type.clone(), vec![idx])
                        })
                        .collect(),
                );
                self.context.add_object(cte_idx);
                self.context
                    .update_output_lineage_with_object_nodes(cte_idx);
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

    fn with_lin(&mut self, with: &With) -> anyhow::Result<()> {
        for cte in &with.ctes {
            self.cte_lin(cte)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn is_var_in_context(&self, var_name: &str) -> bool {
        self.context.vars.contains_key(var_name)
    }

    #[allow(dead_code)]
    fn is_column_in_context(&self, col_name: &str) -> bool {
        self.context
            .curr_columns_stack()
            .is_some_and(|map| map.contains_key(&col_name.to_lowercase()))
    }

    /// Get the node index of a column or variable from the current context.
    /// Priority order: columns are checked first, then variables.
    ///
    /// If not found, an `Err` is returned.
    fn get_col_or_var(&self, name: &str) -> anyhow::Result<ArenaIndex> {
        self.get_column(None, name).or_else(|_| self.get_var(name))
    }

    /// Get the node index of a variable from the current context.
    ///
    /// If not found, an `Err` is returned.
    fn get_var(&self, var_name: &str) -> anyhow::Result<ArenaIndex> {
        self.context
            .vars
            .get(&var_name.to_lowercase())
            .ok_or_else(|| anyhow!("Variable `{}` not found in context.", var_name))
            .copied()
    }

    /// Get the object index of a tble from the current index.
    ///
    /// If not found, an `Err` is returned
    fn get_table(&self, name: &str) -> anyhow::Result<ArenaIndex> {
        let curr_stack = self
            .context
            .curr_stack()
            .ok_or(anyhow!("Table `{}` not found in context.", name))?;

        if let Some(ctx_table_idx) = curr_stack.get(name) {
            return Ok(*ctx_table_idx);
        }
        if let Some(ctx_table_idx) = curr_stack
            .get(&name.to_lowercase())
            .map_or(curr_stack.get(&name.to_uppercase()), Some)
        {
            // Ctes are case insensitive
            let obj = &self.context.arena_objects[*ctx_table_idx];
            if matches!(obj.kind, ContextObjectKind::TableAlias) {
                return Ok(*ctx_table_idx);
            } else {
                return Err(anyhow!(
                    "Found matching table name {} by ignoring case but it is not an alias.",
                    name,
                ));
            }
        }

        Err(anyhow!("Table `{}` not found in contrext.", name))
    }

    fn get_column(&self, table: Option<&str>, column: &str) -> anyhow::Result<ArenaIndex> {
        // column names are case insensitive
        let column = column.to_lowercase();

        if let Some(table) = table {
            let ctx_table_idx = self.get_table(table)?;
            let ctx_table = &self.context.arena_objects[ctx_table_idx];
            let col_in_schema = ctx_table
                .lineage_nodes
                .iter()
                .map(|n_idx| (&self.context.arena_lineage_nodes[*n_idx], *n_idx))
                .find(|(n, _)| n.name.string().eq_ignore_ascii_case(&column));
            if let Some((_, col_idx)) = col_in_schema {
                return Ok(col_idx);
            }
            Err(anyhow!(
                "Column `{}` not found in the schema of table `{}`",
                column,
                table
            ))
        } else if let Some(target_tables) = self
            .context
            .curr_columns_stack()
            .and_then(|map| map.get(&column))
        {
            let unnest_idx = target_tables.iter().find(|&idx| {
                matches!(
                    &self.context.arena_objects[*idx].kind,
                    ContextObjectKind::Unnest
                )
            });
            let last_using_idx = target_tables
                .iter()
                .filter(|&idx| {
                    matches!(
                        &self.context.arena_objects[*idx].kind,
                        ContextObjectKind::UsingTable
                    )
                })
                .next_back();

            if target_tables.len() > 1 && unnest_idx.is_none() && last_using_idx.is_none() {
                return Err(anyhow!(
                    "Column `{}` is ambiguous. It is contained in more than one table: {:?}.",
                    column,
                    target_tables
                        .iter()
                        .map(|source_idx| self.context.arena_objects[*source_idx].name.clone())
                        .collect::<Vec<String>>()
                ));
            }
            let target_table_idx = if target_tables.len() > 1 {
                *unnest_idx.or(last_using_idx).unwrap()
            } else {
                target_tables[0]
            };
            let target_table_name = &self.context.arena_objects[target_table_idx].name;
            let ctx_table = self
                .context
                .curr_stack()
                .unwrap()
                .get(target_table_name)
                .map(|idx| &self.context.arena_objects[*idx])
                .unwrap();

            return Ok(ctx_table
                .lineage_nodes
                .iter()
                .map(|n_idx| (&self.context.arena_lineage_nodes[*n_idx], *n_idx))
                .find(|(n, _)| n.name.string() == column)
                .unwrap()
                .1);
        } else {
            return Err(anyhow!("Column `{}` not found in context.", column));
        }
    }

    fn nested_node_lin(&mut self, access_path: &AccessPath, nested_node_idx: ArenaIndex) {
        let path_len = access_path.path.len();
        if matches!(access_path.path[path_len - 1], AccessOp::FieldStar) {
            let node = &self.context.arena_lineage_nodes[nested_node_idx];
            node.input
                .iter()
                .for_each(|inp| self.context.lineage_stack.push(*inp));
        } else {
            self.context.lineage_stack.push(nested_node_idx);
        }
    }

    fn binary_col_expr_lin(&mut self, expr: &BinaryExpr) -> anyhow::Result<()> {
        let mut b = expr;
        let mut access_path = AccessPath::default();
        loop {
            let left = &*b.left;
            let right = &*b.right;
            let op = &b.operator;

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
                            if self.get_table(left_ident).ok().is_some() {
                                // table.col
                                let col_source_idx =
                                    self.get_column(Some(left_ident), right_ident)?;
                                self.context.lineage_stack.push(col_source_idx);
                            } else {
                                // col_struct.field (or var_struct.field)
                                let col_or_node_idx = self.get_col_or_var(left_ident)?;
                                access_path.path.push(AccessOp::Field(right_ident.clone()));
                                let node = &self.context.arena_lineage_nodes[col_or_node_idx];
                                let nested_node_idx = node.access(&access_path)?;
                                self.nested_node_lin(&access_path, nested_node_idx);
                            }
                            break;
                        } else {
                            let col_or_var_source_idx = if self.get_table(left_ident).ok().is_some()
                            {
                                // table.col
                                let col_name = right_ident.clone();
                                self.get_column(Some(left_ident), &col_name)?
                            } else {
                                // col_struct.field (or var_struct.field)
                                let col_or_node_idx = self.get_col_or_var(left_ident)?;
                                access_path.path.push(AccessOp::Field(right_ident.clone()));
                                col_or_node_idx
                            };
                            access_path.path.reverse();
                            let node = &self.context.arena_lineage_nodes[col_or_var_source_idx];
                            let nested_node_idx = node.access(&access_path)?;
                            self.nested_node_lin(&access_path, nested_node_idx);
                        }
                        break;
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
                                        ident.clone()
                                    }
                                    _ => unreachable!(),
                                };
                                self.select_expr_col_expr_lin(&binary_expr.right, false)?;
                                access_path.path.extend_from_slice(&[
                                    AccessOp::Index,
                                    AccessOp::Field(field_name),
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
                            FunctionExpr::Array(array_function_expr) => {
                                self.array_function_expr_lin(array_function_expr)?
                            }
                            FunctionExpr::ArrayAgg(array_agg_function_expr) => {
                                self.array_agg_function_expr_lin(array_agg_function_expr, false)?
                            }
                            _ => unreachable!(),
                        };
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (Expr::Array(array_expr), _) => {
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        access_path.path.push(AccessOp::Index);
                        access_path.path.reverse();
                        let node_idx = self.array_expr_lin(array_expr)?;
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Binary(bin_expr),
                    ) => {
                        let array_field = match bin_expr.left.as_ref() {
                            Expr::Identifier(Identifier { name: ident })
                            | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => {
                                ident.clone()
                            }
                            _ => unreachable!(),
                        };

                        if self.get_table(ident).ok().is_some() {
                            // table.array_field
                            let col_source_idx = self.get_column(Some(ident), &array_field)?;
                            self.context.lineage_stack.push(col_source_idx);
                        } else {
                            // struct_col.array_field (or struct_var.array_field)
                            let col_or_var_idx = self.get_col_or_var(ident)?;
                            access_path.path.extend_from_slice(&[
                                AccessOp::Index,
                                AccessOp::Field(array_field),
                            ]);
                            access_path.path.reverse();

                            let node = &self.context.arena_lineage_nodes[col_or_var_idx];
                            let nested_node_idx = node.access(&access_path)?;
                            self.nested_node_lin(&access_path, nested_node_idx);
                        }
                        break;
                    }
                    (
                        Expr::Struct(struct_expr),
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                    ) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        access_path.path.push(AccessOp::Field(ident.clone()));
                        access_path.path.reverse();
                        let node_idx = self.struct_expr_lin(struct_expr)?;
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (Expr::Struct(struct_expr), Expr::Star) => {
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        access_path.path.push(AccessOp::FieldStar);
                        access_path.path.reverse();
                        let node_idx = self.struct_expr_lin(struct_expr)?;
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Star,
                    ) => {
                        if let Ok(source_obj_idx) = self.get_table(ident) {
                            // table.*
                            let source_obj = &self.context.arena_objects[source_obj_idx];
                            self.context
                                .lineage_stack
                                .extend(source_obj.lineage_nodes.clone());
                        } else {
                            // col_struct.*
                            let col_or_var_idx = self.get_col_or_var(ident)?;
                            let node = &self.context.arena_lineage_nodes[col_or_var_idx];
                            let access_path = AccessPath {
                                path: vec![AccessOp::FieldStar],
                            };
                            let nested_node_idx = node.access(&access_path)?;
                            self.nested_node_lin(&access_path, nested_node_idx);
                        }
                        break;
                    }
                    (
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                        Expr::Number(_),
                    ) => {
                        // array[]
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        let col_or_var_idx = self.get_col_or_var(ident)?;
                        let node = &self.context.arena_lineage_nodes[col_or_var_idx];
                        let access_path = AccessPath {
                            path: vec![AccessOp::Index],
                        };
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (
                        Expr::Grouping(_) | Expr::Query(_),
                        Expr::Identifier(Identifier { name: ident })
                        | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }),
                    ) => {
                        // select (select struct(1 as a, 2 as b)).a
                        debug_assert!(matches!(op, BinaryOperator::FieldAccess));
                        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                            this.select_expr_col_expr_lin(left, false)
                        })?;
                        debug_assert!(consumed_nodes.len() == 1);
                        let access_path = AccessPath {
                            path: vec![AccessOp::Field(ident.clone())],
                        };
                        let node_idx = consumed_nodes[0];
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }
                    (Expr::Grouping(_) | Expr::Query(_), Expr::Number(_)) => {
                        // select (select [1,2,3])[0]
                        debug_assert!(matches!(op, BinaryOperator::ArrayIndex));
                        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                            this.select_expr_col_expr_lin(left, false)
                        })?;
                        debug_assert!(consumed_nodes.len() == 1);
                        let access_path = AccessPath {
                            path: vec![AccessOp::Index],
                        };
                        let node_idx = consumed_nodes[0];
                        let node = &self.context.arena_lineage_nodes[node_idx];
                        self.context.lineage_stack.pop();
                        let nested_node_idx = node.access(&access_path)?;
                        self.nested_node_lin(&access_path, nested_node_idx);
                        break;
                    }

                    (Expr::QueryNamedParameter(_), _)
                    | (Expr::SystemVariable(_), _)
                    | (Expr::QueryPositionalParameter, _) => {
                        break;
                    }

                    _ => {
                        return Err(anyhow!(
                            "Found unexpected binary expr with left: {:?} and right {:?}.",
                            left,
                            right
                        ));
                    }
                }
            } else {
                self.select_expr_col_expr_lin(left, false)?;
                self.select_expr_col_expr_lin(right, false)?;
                break;
            }
        }

        Ok(())
    }

    fn add_struct_node_field_types_from_type(&mut self, typ: &Type) {
        let mut node_types = vec![];
        node_type_from_parser_type(typ, &mut node_types);
        let mut struct_node_field_types = vec![];
        node_types.iter().for_each(|node_type| {
            if let NodeType::Struct(struct_node_type) = node_type {
                struct_node_field_types.extend(struct_node_type.fields.clone());
            }
        });
        struct_node_field_types.reverse();
        self.context.struct_node_field_types_stack = struct_node_field_types;
    }

    fn struct_expr_lin(&mut self, struct_expr: &StructExpr) -> anyhow::Result<ArenaIndex> {
        if let Some(typ) = &struct_expr.r#type {
            // Typed struct syntax
            self.add_struct_node_field_types_from_type(typ);
        };

        let mut fields = vec![];
        let mut input = vec![];
        for field in struct_expr.fields.iter() {
            let name = field.alias.as_ref().map(|tok| tok.as_str().to_owned());

            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(&field.expr, false)
            })?;

            let mut name_from_col = None;
            if consumed_nodes.len() == 1 {
                if let NodeName::Defined(name) =
                    &self.context.arena_lineage_nodes[consumed_nodes[0]].name
                {
                    name_from_col = Some(name);
                }
            };

            let node_type = consumed_nodes
                .iter()
                .find_map(|idx| {
                    let node = &self.context.arena_lineage_nodes[*idx];
                    if !matches!(node.r#type, NodeType::Unknown) {
                        Some(node.r#type.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or(NodeType::Unknown);

            input.extend(consumed_nodes.clone());

            let struct_node_field_type = self.context.struct_node_field_types_stack.pop();
            let struct_node_field_type = StructNodeFieldType {
                name: name
                    .or(struct_node_field_type.as_ref().map(|f| f.name.clone()))
                    .or(name_from_col.cloned())
                    .unwrap_or("!anonymous".to_owned()),
                r#type: node_type,
                input: consumed_nodes,
            };
            fields.push(struct_node_field_type);
        }

        let node_type = NodeType::Struct(StructNodeType {
            fields: fields.clone(),
        });

        let obj_name = self.get_anon_obj_name("anon_struct");
        let obj_idx = self.context.allocate_new_ctx_object(
            &obj_name,
            ContextObjectKind::AnonymousStruct,
            vec![],
        );

        let node = LineageNode {
            name: NodeName::Anonymous,
            r#type: node_type.clone(),
            source_obj: obj_idx,
            input: input.clone(),
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.context.arena_lineage_nodes.allocate(node);
        self.context.add_nested_nodes(node_idx);

        let obj = &mut self.context.arena_objects[obj_idx];
        obj.lineage_nodes.push(node_idx);

        self.context.lineage_stack.push(node_idx);
        self.context.output.push(node_idx);

        Ok(node_idx)
    }

    fn array_expr_lin(&mut self, array_expr: &ArrayExpr) -> anyhow::Result<ArenaIndex> {
        if let Some(typ) = &array_expr.r#type {
            // Typed struct syntax
            self.add_struct_node_field_types_from_type(typ);
        }

        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            for expr in array_expr.exprs.iter() {
                this.select_expr_col_expr_lin(expr, false)?;
            }
            Ok(())
        })?;

        let node_type = consumed_nodes
            .iter()
            .find_map(|idx| {
                let node = &self.context.arena_lineage_nodes[*idx];
                if !matches!(node.r#type, NodeType::Unknown) {
                    Some(node.r#type.clone())
                } else {
                    None
                }
            })
            .unwrap_or(NodeType::Unknown);

        let obj_name = self.get_anon_obj_name("anon_array");
        let obj_idx = self.context.allocate_new_ctx_object(
            &obj_name,
            ContextObjectKind::AnonymousArray,
            vec![],
        );

        let arr_node_type = NodeType::Array(Box::new(ArrayNodeType {
            r#type: node_type,
            input: consumed_nodes.clone(),
        }));

        let node = LineageNode {
            name: NodeName::Anonymous,
            r#type: arr_node_type,
            source_obj: obj_idx,
            input: consumed_nodes,
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.context.arena_lineage_nodes.allocate(node);
        self.context.add_nested_nodes(node_idx);

        let obj = &mut self.context.arena_objects[obj_idx];
        obj.lineage_nodes.push(node_idx);

        self.context.lineage_stack.push(node_idx);
        self.context.output.push(node_idx);
        Ok(node_idx)
    }

    fn select_expr_col_expr_lin(
        &mut self,
        expr: &Expr,
        expand_value_table: bool,
    ) -> anyhow::Result<()> {
        match expr {
            Expr::String(_)
            | Expr::RawString(_)
            | Expr::RawBytes(_)
            | Expr::Bytes(_)
            | Expr::Number(_)
            | Expr::Numeric(_)
            | Expr::BigNumeric(_)
            | Expr::Range(_)
            | Expr::Date(_)
            | Expr::Timestamp(_)
            | Expr::Datetime(_)
            | Expr::Time(_)
            | Expr::Json(_)
            | Expr::Bool(_)
            | Expr::Null
            | Expr::Default
            | Expr::QueryNamedParameter(_)
            | Expr::QueryPositionalParameter
            | Expr::SystemVariable(_)
            | Expr::StringConcat(_)
            | Expr::BytesConcat(_) => {}
            Expr::Binary(binary_expr) => self.binary_col_expr_lin(binary_expr)?,
            Expr::Unary(unary_expr) => {
                self.select_expr_col_expr_lin(&unary_expr.right, expand_value_table)?
            }
            Expr::Grouping(grouping_expr) => {
                self.select_expr_col_expr_lin(&grouping_expr.expr, expand_value_table)?
            }
            Expr::Identifier(Identifier { name: ident })
            | Expr::QuotedIdentifier(QuotedIdentifier { name: ident }) => {
                let col_or_var_idx = self.get_col_or_var(ident)?;
                self.context.lineage_stack.push(col_or_var_idx);
            }
            Expr::Interval(interval_expr) => match interval_expr {
                IntervalExpr::Interval { value, .. } => {
                    self.select_expr_col_expr_lin(value, expand_value_table)?
                }
                IntervalExpr::IntervalRange { .. } => {}
            },
            Expr::Array(array_expr) => {
                self.array_expr_lin(array_expr)?;
            }
            Expr::Unnest(unnext_expr) => {
                self.select_expr_col_expr_lin(&unnext_expr.array, expand_value_table)?;
            }
            Expr::Struct(struct_expr) => {
                self.struct_expr_lin(struct_expr)?;
            }
            Expr::Query(query_expr) => self.query_expr_lin(query_expr, expand_value_table)?,
            Expr::Exists(query_expr) => self.query_expr_lin(query_expr, expand_value_table)?,
            Expr::Case(case_expr) => {
                if let Some(case) = &case_expr.case {
                    self.select_expr_col_expr_lin(case, expand_value_table)?;
                }
                for when_then in &case_expr.when_thens {
                    self.select_expr_col_expr_lin(&when_then.when, expand_value_table)?;
                    self.select_expr_col_expr_lin(&when_then.then, expand_value_table)?;
                }
                if let Some(else_expr) = &case_expr.r#else {
                    self.select_expr_col_expr_lin(else_expr, expand_value_table)?;
                }
            }
            Expr::GenericFunction(function_expr) => {
                for arg in &function_expr.arguments {
                    self.select_expr_col_expr_lin(&arg.expr, expand_value_table)?
                }
            }
            Expr::Function(function_expr) => match function_expr.as_ref() {
                FunctionExpr::Concat(concat_fn_expr) => {
                    for expr in &concat_fn_expr.values {
                        self.select_expr_col_expr_lin(expr, expand_value_table)?;
                    }
                }
                FunctionExpr::Cast(cast_fn_expr) => {
                    self.select_expr_col_expr_lin(&cast_fn_expr.expr, expand_value_table)?
                }
                FunctionExpr::SafeCast(safe_cast_fn_expr) => {
                    self.select_expr_col_expr_lin(&safe_cast_fn_expr.expr, expand_value_table)?
                }
                FunctionExpr::Array(array_function_expr) => {
                    self.array_function_expr_lin(array_function_expr)?;
                }
                FunctionExpr::ArrayAgg(array_agg_function_expr) => {
                    self.array_agg_function_expr_lin(array_agg_function_expr, expand_value_table)?;
                }
                FunctionExpr::CurrentDate(current_date_function_expr) => {
                    if let Some(timezone_expr) = &current_date_function_expr.timezone {
                        self.select_expr_col_expr_lin(timezone_expr, expand_value_table)?;
                    }
                }
                FunctionExpr::If(if_function_expr) => {
                    self.select_expr_col_expr_lin(&if_function_expr.condition, expand_value_table)?;
                    self.select_expr_col_expr_lin(
                        &if_function_expr.true_result,
                        expand_value_table,
                    )?;
                    self.select_expr_col_expr_lin(
                        &if_function_expr.false_result,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::Right(right_function_expr) => {
                    self.select_expr_col_expr_lin(&right_function_expr.value, expand_value_table)?;
                    self.select_expr_col_expr_lin(&right_function_expr.length, expand_value_table)?;
                }
                FunctionExpr::CurrentTimestamp => {}
                FunctionExpr::Extract(extract_function_expr) => {
                    self.select_expr_col_expr_lin(&extract_function_expr.expr, expand_value_table)?
                }
                FunctionExpr::DateDiff(date_diff_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &date_diff_function_expr.start_date,
                        expand_value_table,
                    )?;
                    self.select_expr_col_expr_lin(
                        &date_diff_function_expr.end_date,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::DatetimeDiff(datetime_diff_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &datetime_diff_function_expr.start_datetime,
                        expand_value_table,
                    )?;
                    self.select_expr_col_expr_lin(
                        &datetime_diff_function_expr.end_datetime,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::TimestampDiff(timestamp_diff_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &timestamp_diff_function_expr.start_timestamp,
                        expand_value_table,
                    )?;
                    self.select_expr_col_expr_lin(
                        &timestamp_diff_function_expr.end_timestamp,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::TimeDiff(time_diff_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &time_diff_function_expr.start_time,
                        expand_value_table,
                    )?;
                    self.select_expr_col_expr_lin(
                        &time_diff_function_expr.end_time,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::DateTrunc(date_trunc_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &date_trunc_function_expr.date,
                        expand_value_table,
                    )?;
                }
                FunctionExpr::DatetimeTrunc(datetime_trunc_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &datetime_trunc_function_expr.datetime,
                        expand_value_table,
                    )?;
                    if let Some(timezone) = &datetime_trunc_function_expr.timezone {
                        self.select_expr_col_expr_lin(timezone, expand_value_table)?;
                    }
                }
                FunctionExpr::TimestampTrunc(timestamp_trunc_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &timestamp_trunc_function_expr.timestamp,
                        expand_value_table,
                    )?;
                    if let Some(timezone) = &timestamp_trunc_function_expr.timezone {
                        self.select_expr_col_expr_lin(timezone, expand_value_table)?;
                    }
                }
                FunctionExpr::TimeTrunc(time_trunc_function_expr) => {
                    self.select_expr_col_expr_lin(
                        &time_trunc_function_expr.time,
                        expand_value_table,
                    )?;
                }
            },
            Expr::Star => {
                // NOTE: we can enter here for a COUNT(*) – currently a no‑op placeholder
            }
            Expr::QuantifiedLike(quantified_like_expr) => match &quantified_like_expr.pattern {
                QuantifiedLikeExprPattern::ExprList { exprs } => {
                    for expr in exprs {
                        self.select_expr_col_expr_lin(expr, expand_value_table)?
                    }
                }
                QuantifiedLikeExprPattern::ArrayUnnest { expr } => {
                    self.select_expr_col_expr_lin(expr, expand_value_table)?
                }
            },
            Expr::With(with_expr) => {
                for with_var in &with_expr.vars {
                    self.select_expr_col_expr_lin(&with_var.value, expand_value_table)?
                }
            }
        }

        Ok(())
    }

    fn array_function_expr_lin(
        &mut self,
        array_function_expr: &ArrayFunctionExpr,
    ) -> anyhow::Result<ArenaIndex> {
        self.array_expr_lin(&ArrayExpr {
            r#type: None,
            exprs: vec![Expr::Query(Box::new(array_function_expr.query.clone()))],
        })
    }

    fn array_agg_function_expr_lin(
        &mut self,
        array_agg_function_expr: &ArrayAggFunctionExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<ArenaIndex> {
        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.select_expr_col_expr_lin(&array_agg_function_expr.arg.expr, expand_value_table)
        })?;
        let obj_name = self.get_anon_obj_name("anon_array");
        let obj_idx = self.context.allocate_new_ctx_object(
            &obj_name,
            ContextObjectKind::AnonymousArray,
            vec![],
        );

        let consumed_node_idx = consumed_nodes[0];
        let consumed_node = &self.context.arena_lineage_nodes[consumed_node_idx];

        let lin_node = LineageNode {
            name: NodeName::Anonymous,
            r#type: NodeType::Array(Box::new(ArrayNodeType {
                r#type: consumed_node.r#type.clone(),
                input: vec![consumed_node_idx],
            })),
            source_obj: consumed_node.source_obj,
            input: consumed_node.input.clone(),
            nested_nodes: IndexMap::new(),
        };
        let lin_node_idx = self.context.arena_lineage_nodes.allocate(lin_node);
        self.context.add_nested_nodes(lin_node_idx);

        let obj = &mut self.context.arena_objects[obj_idx];
        obj.lineage_nodes.push(consumed_node_idx);

        self.context.lineage_stack.push(lin_node_idx);
        self.context.output.push(lin_node_idx);
        Ok(lin_node_idx)
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
            .clone()
            .map_or(HashSet::default(), |cols| {
                cols.iter()
                    .map(|c| c.as_str().to_lowercase())
                    .collect::<HashSet<String>>()
            });
        for (col_name, sources) in self
            .context
            .curr_columns_stack()
            .unwrap_or(&IndexMap::new())
            .iter()
        {
            if except_columns.contains(col_name) {
                continue;
            }

            if sources.len() > 1
                && !sources.iter().any(|el| {
                    self.context
                        .curr_stack()
                        .unwrap()
                        .get(&self.context.arena_objects[*el].name)
                        .map(|el| &self.context.arena_objects[*el])
                        .is_some_and(|x| matches!(x.kind, ContextObjectKind::UsingTable))
                })
            {
                return Err(anyhow!(
                    "Column {} is ambiguous. It is contained in more than one table: {:?}.",
                    col_name,
                    sources
                        .iter()
                        .map(|source_idx| self.context.arena_objects[*source_idx].name.clone())
                        .collect::<Vec<String>>()
                ));
            }
            let col_source_idx = if sources.len() > 1 {
                // Pick the last using_table
                *sources.last().unwrap()
            } else {
                sources[0]
            };
            let table = self
                .context
                .curr_stack()
                .unwrap()
                .get(&self.context.arena_objects[col_source_idx].name)
                .map(|idx| &self.context.arena_objects[*idx])
                .unwrap();

            let col_in_table_idx = table
                .lineage_nodes
                .iter()
                .map(|&n_idx| (&self.context.arena_lineage_nodes[n_idx], n_idx))
                .filter(|(n, _)| n.name.string().eq_ignore_ascii_case(col_name))
                .collect::<Vec<_>>()[0]
                .1;

            new_lineage_nodes.push((
                NodeName::Defined(col_name.clone()),
                self.context.arena_lineage_nodes[col_in_table_idx]
                    .r#type
                    .clone(),
                anon_obj_idx,
                vec![col_in_table_idx],
            ));
        }

        for tup in new_lineage_nodes {
            let lineage_node_idx = self
                .context
                .allocate_new_lineage_node(tup.0, tup.1, tup.2, tup.3)?;
            self.context.lineage_stack.push(lineage_node_idx);
            lineage_nodes.push(lineage_node_idx);
            self.context.arena_objects[anon_obj_idx]
                .lineage_nodes
                .push(lineage_node_idx);
        }

        Ok(())
    }

    fn select_expr_col_all_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColAllExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let except_columns = col_expr.except.clone().map_or(HashSet::default(), |cols| {
            cols.iter()
                .map(|c| c.as_str().to_lowercase())
                .collect::<HashSet<String>>()
        });

        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.select_expr_col_expr_lin(&col_expr.expr, false)
        })?;

        let mut new_lineage_nodes = vec![];
        for node_idx in &consumed_nodes {
            let node = &mut self.context.arena_lineage_nodes[*node_idx];
            if except_columns.contains(node.name.string()) {
                continue;
            }

            new_lineage_nodes.push((
                node.name.clone(),
                node.r#type.clone(),
                anon_obj_idx,
                vec![*node_idx],
            ))
        }

        for tup in new_lineage_nodes {
            let lineage_node_idx = self
                .context
                .allocate_new_lineage_node(tup.0, tup.1, tup.2, tup.3)?;
            self.context.lineage_stack.push(lineage_node_idx);
            lineage_nodes.push(lineage_node_idx);
            self.context.arena_objects[anon_obj_idx]
                .lineage_nodes
                .push(lineage_node_idx);
        }

        Ok(())
    }

    fn select_expr_col_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let pending_node_idx = self.context.allocate_new_lineage_node(
            NodeName::Anonymous,
            NodeType::Unknown,
            anon_obj_idx,
            vec![],
        )?;
        self.context.arena_objects[anon_obj_idx]
            .lineage_nodes
            .push(pending_node_idx);

        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.select_expr_col_expr_lin(&col_expr.expr, false)
        })?;

        let first_node = consumed_nodes
            .first()
            .map(|idx| &self.context.arena_lineage_nodes[*idx])
            .cloned();

        let pending_node = &mut self.context.arena_lineage_nodes[pending_node_idx];

        pending_node.input.extend(consumed_nodes.clone());

        if let Some(alias) = &col_expr.alias {
            pending_node.name = NodeName::Defined(alias.as_str().to_lowercase());
        }

        if pending_node.input.len() == 1 {
            let first_node = first_node.unwrap();
            if let NodeName::Anonymous = pending_node.name {
                pending_node.name = first_node.name.clone();
            }
            pending_node.r#type = first_node.r#type.clone();
        }

        self.context
            .add_nested_nodes_from_input_nodes(pending_node_idx, &consumed_nodes);

        self.context.lineage_stack.push(pending_node_idx);
        lineage_nodes.push(pending_node_idx);
        Ok(())
    }

    fn add_new_from_table(
        &self,
        from_tables: &mut Vec<ArenaIndex>,
        new_table_idx: ArenaIndex,
    ) -> anyhow::Result<()> {
        let new_table = &self.context.arena_objects[new_table_idx];
        if from_tables
            .iter()
            .map(|idx| &self.context.arena_objects[*idx])
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

    #[allow(clippy::wrong_self_convention)]
    fn from_path_expr_lin(
        &mut self,
        from_path_expr: &FromPathExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &[ArenaIndex],
        check_unnest: bool,
    ) -> anyhow::Result<()> {
        let table_name = &from_path_expr.path.name;
        let table_like_obj_id = self.get_table_from_context(table_name);
        let path_identifiers = from_path_expr.path.identifiers();

        if table_like_obj_id.is_none() {
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
                    let mut ctx_objects = from_tables
                        .iter()
                        .map(|idx| (self.context.arena_objects[*idx].name.clone(), *idx))
                        .collect::<IndexMap<String, ArenaIndex>>();

                    ctx_objects.extend(
                        joined_tables
                            .iter()
                            .cloned()
                            .map(|idx| (self.context.arena_objects[idx].name.clone(), idx)),
                    );
                    self.context.push_new_ctx(ctx_objects, true);

                    let col_source_idx = self.get_column(Some(table), column)?;
                    let col_node = &self.context.arena_lineage_nodes[col_source_idx];
                    let nested_node_idx = col_node.access(&access_path)?;

                    let name = from_path_expr
                        .alias
                        .as_ref()
                        .map_or(self.get_anon_obj_name("unnest"), |alias| {
                            alias.as_str().to_owned()
                        });

                    let nested_node = &self.context.arena_lineage_nodes[nested_node_idx];
                    let col_name = from_path_expr
                        .alias
                        .as_ref()
                        .map_or(NodeName::Anonymous, |alias| {
                            NodeName::Defined(alias.as_str().to_owned())
                        });

                    let unnest_nodes = match &nested_node.r#type {
                        NodeType::Struct(struct_node_type) => {
                            let mut nodes = vec![];
                            for field in &struct_node_type.fields {
                                let inner_node_idx = nested_node.access(&AccessPath {
                                    path: vec![AccessOp::Field(field.name.clone())],
                                })?;
                                nodes.push((
                                    NodeName::Defined(field.name.clone()),
                                    field.r#type.clone(),
                                    vec![inner_node_idx],
                                ));
                            }
                            nodes
                        }
                        _ => vec![(col_name, nested_node.r#type.clone(), vec![nested_node_idx])],
                    };

                    let unnest_idx = self.context.allocate_new_ctx_object(
                        &name,
                        ContextObjectKind::Unnest,
                        unnest_nodes,
                    );

                    self.context.pop_curr_ctx();

                    self.context
                        .update_output_lineage_with_object_nodes(unnest_idx);
                    self.add_new_from_table(from_tables, unnest_idx)?;
                }

                return Ok(());
            }

            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                table_name
            ));
        }

        let contains_alias = from_path_expr.alias.is_some();
        let table_like_name = if contains_alias {
            from_path_expr.alias.as_ref().unwrap().as_str().to_owned()
        } else {
            table_name.clone()
        };

        let table_like_obj_id = table_like_obj_id.unwrap();

        if contains_alias {
            // If aliased, we create a new object
            let table_like_obj = &self.context.arena_objects[table_like_obj_id].clone();

            let mut new_lineage_nodes = vec![];
            for el in &table_like_obj.lineage_nodes {
                let ln = &self.context.arena_lineage_nodes[*el];
                new_lineage_nodes.push((ln.name.clone(), ln.r#type.clone(), vec![*el]))
            }

            let new_table_like_idx = self.context.allocate_new_ctx_object(
                &table_like_name,
                ContextObjectKind::TableAlias,
                new_lineage_nodes,
            );
            self.context
                .update_output_lineage_with_object_nodes(new_table_like_idx);
            self.add_new_from_table(from_tables, new_table_like_idx)?;
        } else {
            self.add_new_from_table(from_tables, table_like_obj_id)?;
        }
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_expr_lin(
        &mut self,
        from_expr: &FromExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        match from_expr {
            FromExpr::Join(join_expr)
            | FromExpr::LeftJoin(join_expr)
            | FromExpr::RightJoin(join_expr)
            | FromExpr::FullJoin(join_expr) => {
                self.join_expr_lineage(join_expr, from_tables, joined_tables)?
            }
            FromExpr::CrossJoin(cross_join_expr) => {
                self.from_expr_lin(&cross_join_expr.left, from_tables, joined_tables)?;
                match cross_join_expr.right.as_ref() {
                    FromExpr::Path(from_path_expr) => {
                        // Implicit unnest
                        self.from_path_expr_lin(from_path_expr, from_tables, joined_tables, true)?
                    }
                    _ => self.from_expr_lin(&cross_join_expr.right, from_tables, joined_tables)?,
                }

                let mut joined_table_names: Vec<&str> = vec![];
                let from_tables_len = from_tables.len();

                let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
                let left_join_table = if !joined_tables.is_empty() {
                    // We have already joined two tables
                    &self.context.arena_objects[*joined_tables.last().unwrap()]
                } else {
                    // This is the first join, which corresponds to index -2 in the original from_tables
                    &self.context.arena_objects[*from_tables_split.0.last().unwrap()]
                };
                joined_table_names.push(&left_join_table.name);

                let right_join_table =
                    &self.context.arena_objects[*from_tables_split.1.last_mut().unwrap()];
                joined_table_names.push(&right_join_table.name);
                let mut lineage_nodes = vec![];
                let mut joined_columns = HashSet::new();
                let mut add_lineage_nodes = |table: &ContextObject| {
                    for node_idx in &table.lineage_nodes {
                        let node = &self.context.arena_lineage_nodes[*node_idx];
                        let newly_inserted = joined_columns.insert(node.name.string());
                        if newly_inserted {
                            lineage_nodes.push((
                                node.name.clone(),
                                node.r#type.clone(),
                                vec![*node_idx],
                            ))
                        }
                    }
                };
                add_lineage_nodes(left_join_table);
                add_lineage_nodes(right_join_table);
                let joined_table_name = format!(
                    // Create a new name for the join_tavke.
                    // This name is not a valid bq table name (we use {})
                    "{{{}}}",
                    joined_table_names
                        .into_iter()
                        .fold(String::from("join"), |acc, name| {
                            format!("{}_{}", acc, name)
                        })
                );

                let table_like_idx = self.context.allocate_new_ctx_object(
                    &joined_table_name,
                    ContextObjectKind::JoinTable,
                    lineage_nodes,
                );
                self.context
                    .update_output_lineage_with_object_nodes(table_like_idx);
                joined_tables.push(table_like_idx);
            }
            FromExpr::Unnest(unnest_expr) => {
                // Push new context just for unnest expr
                let mut ctx_objects = from_tables
                    .iter()
                    .map(|idx| (self.context.arena_objects[*idx].name.clone(), *idx))
                    .collect::<IndexMap<String, ArenaIndex>>();

                ctx_objects.extend(
                    joined_tables
                        .iter()
                        .map(|idx| (self.context.arena_objects[*idx].name.clone(), *idx)),
                );
                self.context.push_new_ctx(ctx_objects, true);

                let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                    this.select_expr_col_expr_lin(unnest_expr.array.as_ref(), false)
                })?;

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

                debug_assert!(consumed_nodes.len() == 1);

                let consumed_node = &self.context.arena_lineage_nodes[*consumed_nodes
                    .first()
                    .ok_or(anyhow!("Could not find consumed node while unnesting"))?];
                let nested_node_idx = consumed_node.access(&AccessPath {
                    path: vec![AccessOp::Index],
                })?;
                let nested_node = &self.context.arena_lineage_nodes[nested_node_idx];

                let unnest_nodes = match &nested_node.r#type {
                    NodeType::Struct(struct_node_type) => {
                        let mut nodes = vec![];
                        for field in &struct_node_type.fields {
                            let inner_node_idx = nested_node.access(&AccessPath {
                                path: vec![AccessOp::Field(field.name.clone())],
                            })?;
                            nodes.push((
                                NodeName::Defined(field.name.clone()),
                                field.r#type.clone(),
                                vec![inner_node_idx],
                            ));
                        }
                        nodes
                    }
                    _ => vec![(col_name, nested_node.r#type.clone(), vec![nested_node_idx])],
                };

                let unnest_idx = self.context.allocate_new_ctx_object(
                    &name,
                    ContextObjectKind::Unnest,
                    unnest_nodes,
                );

                self.context.pop_curr_ctx();

                self.context
                    .update_output_lineage_with_object_nodes(unnest_idx);
                self.add_new_from_table(from_tables, unnest_idx)?;
            }
            FromExpr::Path(from_path_expr) => {
                self.from_path_expr_lin(from_path_expr, from_tables, joined_tables, false)?
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let source_name = &from_grouping_query_expr
                    .alias
                    .as_ref()
                    .map(|alias| alias.as_str().to_owned());

                let consumed_lineage_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                    this.query_expr_lin(&from_grouping_query_expr.query, true)
                })?;

                let new_source_name = if let Some(name) = source_name {
                    name
                } else {
                    &self.get_anon_obj_name("query")
                };

                let table_like_idx = self.context.allocate_new_ctx_object(
                    new_source_name,
                    ContextObjectKind::Query,
                    consumed_lineage_nodes
                        .into_iter()
                        .map(|idx| {
                            let node = &self.context.arena_lineage_nodes[idx];
                            (node.name.clone(), node.r#type.clone(), vec![idx])
                        })
                        .collect(),
                );
                self.context
                    .update_output_lineage_with_object_nodes(table_like_idx);
                self.add_new_from_table(from_tables, table_like_idx)?;
            }
            FromExpr::GroupingFrom(grouping_from_expr) => {
                self.from_expr_lin(&grouping_from_expr.query, from_tables, joined_tables)?
            }
        }
        Ok(())
    }

    fn join_expr_lineage(
        &mut self,
        join_expr: &JoinExpr,
        from_tables: &mut Vec<ArenaIndex>,
        joined_tables: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        self.from_expr_lin(&join_expr.left, from_tables, joined_tables)?;
        self.from_expr_lin(&join_expr.right, from_tables, joined_tables)?;

        let mut joined_table_names: Vec<&str> = vec![];
        let from_tables_len = from_tables.len();

        let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
        let left_join_table = if !joined_tables.is_empty() {
            // We have already joined two tables
            &self.context.arena_objects[*joined_tables.last().unwrap()]
        } else {
            // This is the first join, which corresponds to index -2 in the original from_tables
            &self.context.arena_objects[*from_tables_split.0.last().unwrap()]
        };
        joined_table_names.push(&left_join_table.name);

        let right_join_table =
            &self.context.arena_objects[*from_tables_split.1.last_mut().unwrap()];
        joined_table_names.push(&right_join_table.name);

        if let JoinCondition::Using {
            columns: using_columns,
        } = &join_expr.cond
        {
            let mut lineage_nodes = vec![];
            let mut using_columns_added = HashSet::new();
            for col in using_columns {
                let col_name = col.as_str().to_lowercase();
                let left_lineage_node_idx = left_join_table
                    .lineage_nodes
                    .iter()
                    .map(|&idx| (&self.context.arena_lineage_nodes[idx], idx))
                    .find(|(n, _)| n.name.string() == col_name)
                    .ok_or(anyhow!(
                        "Cannot find column {:?} in table {:?}.",
                        col_name,
                        left_join_table.name
                    ))?
                    .1;

                let right_lineage_node_idx = right_join_table
                    .lineage_nodes
                    .iter()
                    .map(|&idx| (&self.context.arena_lineage_nodes[idx], idx))
                    .find(|(n, _)| n.name.string() == col_name)
                    .ok_or(anyhow!(
                        "Cannot find column {:?} in table {:?}.",
                        col_name,
                        right_join_table.name
                    ))?
                    .1;

                lineage_nodes.push((
                    NodeName::Defined(col_name.clone()),
                    self.context.arena_lineage_nodes[left_lineage_node_idx]
                        .r#type
                        .clone(),
                    vec![left_lineage_node_idx, right_lineage_node_idx],
                ));
                using_columns_added.insert(col_name);
            }

            // Add remaning columns not in using clause
            lineage_nodes.extend(
                left_join_table
                    .lineage_nodes
                    .iter()
                    .map(|idx| (&self.context.arena_lineage_nodes[*idx], idx))
                    .filter(|(node, _)| !using_columns_added.contains(node.name.string()))
                    .map(|(node, idx)| (node.name.clone(), node.r#type.clone(), vec![*idx])),
            );

            lineage_nodes.extend(
                right_join_table
                    .lineage_nodes
                    .iter()
                    .map(|idx| (&self.context.arena_lineage_nodes[*idx], idx))
                    .filter(|(node, _)| !using_columns_added.contains(node.name.string()))
                    .map(|(node, idx)| (node.name.clone(), node.r#type.clone(), vec![*idx])),
            );

            // At this point lineage nodes can contain duplicate columns which would be ambiguous if selected in the query
            // (those not used in USING(...))
            // TODO: we could check if this is the case and raise an error when getting the column

            let joined_table_name = format!(
                // Create a new name for the using_table.
                // This name is not a valid bq table name (we use {})
                "{{{}}}",
                joined_table_names
                    .into_iter()
                    .fold(String::from("join"), |acc, name| {
                        format!("{}_{}", acc, name)
                    })
            );

            let table_like_idx = self.context.allocate_new_ctx_object(
                &joined_table_name,
                ContextObjectKind::UsingTable,
                lineage_nodes,
            );
            self.context
                .update_output_lineage_with_object_nodes(table_like_idx);
            joined_tables.push(table_like_idx);
        } else {
            let mut lineage_nodes = vec![];
            let mut joined_columns = HashSet::new();
            let mut add_lineage_nodes = |table: &ContextObject| {
                for node_idx in &table.lineage_nodes {
                    let node = &self.context.arena_lineage_nodes[*node_idx];
                    let newly_inserted = joined_columns.insert(node.name.string());
                    if newly_inserted {
                        lineage_nodes.push((
                            node.name.clone(),
                            node.r#type.clone(),
                            vec![*node_idx],
                        ))
                    }
                }
            };
            add_lineage_nodes(left_join_table);
            add_lineage_nodes(right_join_table);
            let joined_table_name = format!(
                // Create a new name for the join_tavke.
                // This name is not a valid bq table name (we use {})
                "{{{}}}",
                joined_table_names
                    .into_iter()
                    .fold(String::from("join"), |acc, name| {
                        format!("{}_{}", acc, name)
                    })
            );

            let table_like_idx = self.context.allocate_new_ctx_object(
                &joined_table_name,
                ContextObjectKind::JoinTable,
                lineage_nodes,
            );
            self.context
                .update_output_lineage_with_object_nodes(table_like_idx);
            joined_tables.push(table_like_idx);
        }

        Ok(())
    }

    fn select_query_expr_lin(
        &mut self,
        select_query_expr: &SelectQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<()> {
        let ctx_objects_start_size = self.context.objects_stack.len();

        let pushed_empty_cte_ctx = if let Some(with) = select_query_expr.with.as_ref() {
            // We push an empty context since a CTE on a subquery may not reference correlated columns from the outer query
            self.context.push_new_ctx(IndexMap::new(), false);
            self.with_lin(with)?;
            true
        } else {
            false
        };

        let pushed_context = if let Some(from) = select_query_expr.select.from.as_ref() {
            self.from_lin(from)?;
            true
        } else {
            false
        };

        let anon_obj_name = self.get_anon_obj_name("anon");
        let anon_obj_idx = self.context.allocate_new_ctx_object(
            &anon_obj_name,
            ContextObjectKind::Anonymous,
            vec![],
        );

        let mut lineage_nodes = vec![];
        for expr in &select_query_expr.select.exprs {
            match expr {
                SelectExpr::Col(col_expr) => {
                    self.select_expr_col_lin(anon_obj_idx, col_expr, &mut lineage_nodes)?;
                }
                SelectExpr::All(all_expr) => {
                    self.select_expr_all_lin(anon_obj_idx, all_expr, &mut lineage_nodes)?
                }
                SelectExpr::ColAll(col_all_expr) => {
                    self.select_expr_col_all_lin(anon_obj_idx, col_all_expr, &mut lineage_nodes)?
                }
            }
        }

        if let Some(table_value) = &select_query_expr.select.table_value {
            match table_value {
                SelectTableValue::Struct if !expand_value_table => {
                    let mut struct_node_tyupes = vec![];
                    let mut input = vec![];
                    for node_idx in &lineage_nodes {
                        self.context.lineage_stack.pop();
                        let node = &self.context.arena_lineage_nodes[*node_idx];
                        struct_node_tyupes.push(StructNodeFieldType {
                            name: node.name.string().to_owned(),
                            r#type: node.r#type.clone(),
                            input: vec![*node_idx],
                        });
                        input.extend(&node.input);
                    }
                    lineage_nodes.drain(..);
                    let struct_node = NodeType::Struct(StructNodeType {
                        fields: struct_node_tyupes,
                    });

                    let struct_node_idx = self.context.allocate_new_lineage_node(
                        NodeName::Anonymous,
                        struct_node,
                        anon_obj_idx,
                        input,
                    )?;

                    lineage_nodes.push(struct_node_idx);
                    self.context.lineage_stack.push(struct_node_idx);
                }
                SelectTableValue::Value if expand_value_table => {
                    let struct_node = &self.context.arena_lineage_nodes[lineage_nodes[0]].clone();
                    self.context.lineage_stack.pop();
                    lineage_nodes.pop();
                    match &struct_node.r#type {
                        NodeType::Struct(struct_node_type) => {
                            for field in &struct_node_type.fields {
                                let access_path = AccessPath {
                                    path: vec![AccessOp::Field(field.name.clone())],
                                };
                                let nested_field_idx = struct_node.access(&access_path)?;
                                let nested_field =
                                    &self.context.arena_lineage_nodes[nested_field_idx];

                                let new_node_idx = self.context.allocate_new_lineage_node(
                                    NodeName::Defined(nested_field.name.string().to_owned()),
                                    nested_field.r#type.clone(),
                                    anon_obj_idx,
                                    vec![nested_field_idx],
                                )?;

                                lineage_nodes.push(new_node_idx);
                                self.context.lineage_stack.push(new_node_idx);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => {}
            }
        }

        self.context
            .update_output_lineage_from_nodes(&lineage_nodes);

        let ctx_objects_curr_size = self.context.objects_stack.len();
        for _ in 0..ctx_objects_curr_size - ctx_objects_start_size {
            self.context.pop_object();
        }

        if pushed_context {
            self.context.pop_curr_ctx();
        }
        if pushed_empty_cte_ctx {
            self.context.pop_curr_ctx();
        }
        Ok(())
    }

    fn grouping_query_expr_lin(
        &mut self,
        grouping_query_expr: &GroupingQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<()> {
        let pushed_empty_cte_ctx = if let Some(with) = grouping_query_expr.with.as_ref() {
            // We push an empty context since a CTE on a subquery may not reference correlated columns from the outer query
            self.context.push_new_ctx(IndexMap::new(), false);
            self.with_lin(with)?;
            true
        } else {
            false
        };
        self.query_expr_lin(&grouping_query_expr.query, expand_value_table)?;
        if pushed_empty_cte_ctx {
            self.context.pop_curr_ctx();
        }
        Ok(())
    }

    fn set_select_query_expr_lin(
        &mut self,
        set_select_query_expr: &SetSelectQueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<()> {
        let ctx_objects_start_size = self.context.objects_stack.len();

        let pushed_empty_cte_ctx = if let Some(with) = set_select_query_expr.with.as_ref() {
            // We push an empty context since a CTE on a subquery may not reference correlated columns from the outer query
            self.context.push_new_ctx(IndexMap::new(), false);
            self.with_lin(with)?;
            true
        } else {
            false
        };

        let left_consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.query_expr_lin(&set_select_query_expr.left_query, expand_value_table)
        })?;

        let right_consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.query_expr_lin(&set_select_query_expr.right_query, expand_value_table)
        })?;

        let anon_obj_name = self.get_anon_obj_name("anon");
        let set_obj_idx = self.context.allocate_new_ctx_object(
            &anon_obj_name,
            ContextObjectKind::Anonymous,
            vec![],
        );

        let nodes_elems = left_consumed_nodes
            .iter()
            .zip(right_consumed_nodes.iter())
            .map(|(l_node_idx, r_node_idx)| {
                let l_node = &self.context.arena_lineage_nodes[*l_node_idx];
                (
                    l_node.name.clone(),
                    l_node.r#type.clone(),
                    set_obj_idx,
                    vec![*l_node_idx, *r_node_idx],
                )
            })
            .collect::<Vec<_>>();

        let mut set_nodes = vec![];
        for (name, r#type, source_obj, input) in nodes_elems.into_iter() {
            let node_idx = self
                .context
                .allocate_new_lineage_node(name, r#type, source_obj, input)?;
            set_nodes.push(node_idx);
        }

        let set_obj = &mut self.context.arena_objects[set_obj_idx];
        set_obj.lineage_nodes = set_nodes;

        set_obj
            .lineage_nodes
            .iter()
            .for_each(|node| self.context.lineage_stack.push(*node));
        self.context
            .update_output_lineage_with_object_nodes(set_obj_idx);

        let ctx_objects_curr_size = self.context.objects_stack.len();
        for _ in 0..ctx_objects_curr_size - ctx_objects_start_size {
            self.context.pop_object();
        }
        if pushed_empty_cte_ctx {
            self.context.pop_curr_ctx();
        }

        Ok(())
    }

    fn query_expr_lin(
        &mut self,
        query: &QueryExpr,
        expand_value_table: bool,
    ) -> anyhow::Result<()> {
        match query {
            QueryExpr::Grouping(grouping_query_expr) => {
                self.grouping_query_expr_lin(grouping_query_expr, expand_value_table)?
            }
            QueryExpr::Select(select_query_expr) => {
                self.select_query_expr_lin(select_query_expr, expand_value_table)?
            }
            QueryExpr::SetSelect(set_select_query_expr) => {
                self.set_select_query_expr_lin(set_select_query_expr, expand_value_table)?
            }
        }

        Ok(())
    }

    fn query_statement_lin(&mut self, query_statement: &QueryStatement) -> anyhow::Result<()> {
        self.query_expr_lin(&query_statement.query, false)
    }

    fn create_table_statement_lin(
        &mut self,
        create_table_statement: &CreateTableStatement,
    ) -> anyhow::Result<()> {
        let table_name = &create_table_statement.name.name;
        let table_kind = if create_table_statement.is_temporary {
            ContextObjectKind::TempTable
        } else {
            ContextObjectKind::Table
        };
        let temp_table_idx = if let Some(ref query) = create_table_statement.query {
            // Extract the schema from the query lineage
            let consumed_lineage_nodes =
                self.call_func_and_consume_lineage_nodes(|this| this.query_expr_lin(query, false))?;

            self.context.allocate_new_ctx_object(
                table_name,
                table_kind,
                consumed_lineage_nodes
                    .into_iter()
                    .map(|idx| {
                        let node = &self.context.arena_lineage_nodes[idx];
                        (node.name.clone(), node.r#type.clone(), vec![idx])
                    })
                    .collect(),
            )
        } else {
            let schema = create_table_statement
                .schema
                .as_ref()
                .ok_or(anyhow!("Schema not found for table: `{:?}`.", table_name))?;
            self.context.allocate_new_ctx_object(
                table_name,
                table_kind,
                schema
                    .iter()
                    .map(|col_schema| {
                        (
                            NodeName::Defined(col_schema.name.as_str().to_owned()),
                            node_type_from_parser_parameterized_type(&col_schema.r#type),
                            vec![],
                        )
                    })
                    .collect(),
            )
        };
        self.context.add_object(temp_table_idx);
        self.context
            .update_output_lineage_with_object_nodes(temp_table_idx);
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_lin(&mut self, from: &crate::ast::From) -> anyhow::Result<()> {
        // TODO: handle pivot, unpivot
        let mut from_tables: Vec<ArenaIndex> = Vec::new();
        let mut joined_tables: Vec<ArenaIndex> = Vec::new();

        self.from_expr_lin(&from.expr, &mut from_tables, &mut joined_tables)?;

        let mut clean_joined_tables: Vec<ArenaIndex> = vec![];
        let mut last_using_idx: Option<ArenaIndex> = None;
        for jt in &joined_tables {
            let jt_obj = &self.context.arena_objects[*jt];
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

        joined_tables = clean_joined_tables;

        let mut ctx_objects = from_tables
            .into_iter()
            .map(|idx| (self.context.arena_objects[idx].name.clone(), idx))
            .collect::<IndexMap<String, ArenaIndex>>();
        ctx_objects.extend(
            joined_tables
                .into_iter()
                .map(|idx| (self.context.arena_objects[idx].name.clone(), idx)),
        );
        self.context.push_new_ctx(ctx_objects, true);
        Ok(())
    }

    /// Get the object index for the table-like obj named `table_name`.
    /// Context objects are checked first, then source tables.
    fn get_table_from_context(&self, table_name: &str) -> Option<ArenaIndex> {
        self.context
            .get_object(table_name)
            .filter(|&obj_idx| {
                matches!(
                    self.context.arena_objects[obj_idx].kind,
                    ContextObjectKind::Cte
                        | ContextObjectKind::TempTable
                        | ContextObjectKind::Table
                )
            })
            .map_or(self.context.source_objects.get(table_name).cloned(), Some)
    }

    /// Build a mapping col_name -> node_idx for each column/node in `table_obj`
    #[inline]
    fn target_table_nodes_map(&self, table_obj: &ContextObject) -> IndexMap<String, ArenaIndex> {
        table_obj
            .lineage_nodes
            .iter()
            .map(|idx| {
                (
                    self.context.arena_lineage_nodes[*idx]
                        .name
                        .string()
                        .to_owned(),
                    *idx,
                )
            })
            .collect::<IndexMap<String, ArenaIndex>>()
    }

    fn update_statement_lin(&mut self, update_statement: &UpdateStatement) -> anyhow::Result<()> {
        let target_table = &update_statement.table.name;
        let target_table_alias = if let Some(ref alias) = update_statement.alias {
            alias.as_str()
        } else {
            target_table
        };

        let target_table_id = self.get_table_from_context(target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        let pushed_context = if let Some(ref from) = update_statement.from {
            self.from_lin(from)?;
            true
        } else {
            false
        };

        let target_table_obj = &self.context.arena_objects[target_table_id.unwrap()];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        // NOTE: we push the target table after pushing the from context
        self.context.push_new_ctx(
            IndexMap::from([(target_table_alias.to_owned(), target_table_id.unwrap())]),
            true,
        );

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

            let col_source_idx = target_table_nodes.get(&column).ok_or(anyhow!(
                "Cannot find column {} in table {}",
                column,
                target_table_alias
            ))?;

            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(&update_item.expr, false)
            })?;

            if !consumed_nodes.is_empty() {
                let col_lineage_node = &mut self.context.arena_lineage_nodes[*col_source_idx];
                col_lineage_node.input.extend(consumed_nodes);
                self.context.output.push(*col_source_idx);
            }
        }

        self.context.pop_curr_ctx();

        if pushed_context {
            self.context.pop_curr_ctx();
        }
        Ok(())
    }

    fn insert_statement_lin(&mut self, insert_statement: &InsertStatement) -> anyhow::Result<()> {
        let target_table = &insert_statement.table.name;

        let target_table_id = self.get_table_from_context(target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        let target_table_obj = &self.context.arena_objects[target_table_id.unwrap()];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        let target_columns = if let Some(columns) = &insert_statement.columns {
            let mut filtered_columns = vec![];
            for col in columns {
                let col_name = col.as_str().to_owned();
                let col_idx = target_table_nodes.get(&col_name).ok_or(anyhow!(
                    "Cannot find column {} in table {}",
                    col_name,
                    target_table
                ))?;
                filtered_columns.push(*col_idx);
            }
            filtered_columns
        } else {
            target_table_obj.lineage_nodes.clone()
        };

        if let Some(query_expr) = &insert_statement.query {
            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.query_expr_lin(query_expr, false)
            })?;

            if consumed_nodes.len() != target_columns.len() {
                return Err(anyhow!(
                    "The number of insert columns is not equal to the number of insert values."
                ));
            }

            target_columns
                .iter()
                .zip(consumed_nodes)
                .for_each(|(target_col, value)| {
                    let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_col];
                    target_lineage_node.input.push(value);
                    self.context.output.push(*target_col);
                });
        } else {
            for (target_col, value) in target_columns
                .iter()
                .zip(insert_statement.values.as_ref().unwrap())
            {
                let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                    this.select_expr_col_expr_lin(value, false)
                })?;

                let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_col];
                target_lineage_node.input.extend(consumed_nodes);
                self.context.output.push(*target_col);
            }
        }

        Ok(())
    }

    fn merge_insert(
        &mut self,
        target_table_id: ArenaIndex,
        merge_insert: &MergeInsert,
    ) -> anyhow::Result<()> {
        let target_table_obj = &self.context.arena_objects[target_table_id];
        let target_table_nodes = self.target_table_nodes_map(target_table_obj);

        let target_columns = if let Some(columns) = &merge_insert.columns {
            let mut filtered_columns = vec![];
            for col in columns {
                let col_name = col.as_str().to_owned();
                let col_idx = target_table_nodes.get(&col_name).ok_or(anyhow!(
                    "Cannot find column {} in table {}",
                    col_name,
                    target_table_obj.name
                ))?;
                filtered_columns.push(*col_idx);
            }
            filtered_columns
        } else {
            target_table_obj.lineage_nodes.clone()
        };
        for (target_col, value) in target_columns.iter().zip(&merge_insert.values) {
            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(value, false)
            })?;

            let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_col];
            target_lineage_node.input.extend(consumed_nodes);
            self.context.output.push(*target_col);
        }

        Ok(())
    }

    fn merge_update(
        &mut self,
        ctx: &IndexMap<String, ArenaIndex>,
        target_table_alias: &str,
        target_table_id: ArenaIndex,
        merge_update: &MergeUpdate,
    ) -> anyhow::Result<()> {
        self.context.push_new_ctx(ctx.clone(), true);
        let target_table_obj = &self.context.arena_objects[target_table_id];
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

            let col_source_idx = target_table_nodes.get(&column).ok_or(anyhow!(
                "Cannot find column {} in table {}",
                column,
                target_table_alias
            ))?;

            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(&update_item.expr, false)
            })?;

            if !consumed_nodes.is_empty() {
                let col_lineage_node = &mut self.context.arena_lineage_nodes[*col_source_idx];
                col_lineage_node.input.extend(consumed_nodes);
                self.context.output.push(*col_source_idx);
            }
        }
        self.context.pop_curr_ctx();

        Ok(())
    }

    fn merge_insert_row(
        &mut self,
        target_table_id: ArenaIndex,
        source_table_id: Option<ArenaIndex>,
        subquery_nodes: &Option<Vec<ArenaIndex>>,
    ) -> anyhow::Result<()> {
        let nodes = if let Some(source_idx) = source_table_id {
            let source_obj = &self.context.arena_objects[source_idx];
            source_obj.lineage_nodes.clone()
        } else {
            subquery_nodes.as_ref().unwrap().clone()
        };

        let target_table = &self.context.arena_objects[target_table_id];
        let target_nodes = &target_table.lineage_nodes;

        if target_nodes.len() != nodes.len() {
            return Err(anyhow!(
                "The number of merge insert columns is not equal to the number of columns in target table `{}`.",
                target_table.name
            ));
        }

        for (target_node, source_node) in target_nodes.iter().zip(nodes) {
            let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_node];
            target_lineage_node.input.push(source_node);
            self.context.output.push(*target_node);
        }

        Ok(())
    }

    fn merge_statement_lin(&mut self, merge_statement: &MergeStatement) -> anyhow::Result<()> {
        let target_table = &merge_statement.target_table.name;
        let target_table_alias = if let Some(ref alias) = merge_statement.target_alias {
            alias.as_str()
        } else {
            target_table
        };

        let target_table_id = self.get_table_from_context(target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }
        let target_table_id = target_table_id.unwrap();

        let source_table_id = if let MergeSource::Table(path_name) = &merge_statement.source {
            let source_table = &path_name.name;
            let source_table_id = self.get_table_from_context(source_table);
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
                let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                    this.query_expr_lin(query_expr, false)
                })?;

                if let Some(alias) = &merge_statement.source_alias {
                    let new_source_name = alias.as_str();
                    let source_idx = self.context.allocate_new_ctx_object(
                        new_source_name,
                        ContextObjectKind::Query,
                        consumed_nodes
                            .iter()
                            .map(|idx| {
                                let node = &self.context.arena_lineage_nodes[*idx];
                                (node.name.clone(), node.r#type.clone(), vec![*idx])
                            })
                            .collect(),
                    );
                    self.context
                        .update_output_lineage_with_object_nodes(source_idx);
                    (Some(source_idx), Some(consumed_nodes))
                } else {
                    (None, Some(consumed_nodes))
                }
            } else {
                (None, None)
            };

        let source_table_id = source_table_id.or(subquery_table_id);

        let mut new_ctx = IndexMap::from([(target_table_alias.to_owned(), target_table_id)]);
        if let Some(alias) = &merge_statement.source_alias {
            let source_alias = alias.as_str().to_owned();
            new_ctx.insert(source_alias, source_table_id.unwrap());
        }

        for when in &merge_statement.whens {
            match when {
                When::Matched(when_matched) => match &when_matched.merge {
                    Merge::Update(merge_update) => self.merge_update(
                        &new_ctx,
                        target_table_alias,
                        target_table_id,
                        merge_update,
                    )?,
                    Merge::Delete => {}
                    _ => unreachable!(),
                },
                When::NotMatchedByTarget(when_not_matched_by_target) => {
                    match &when_not_matched_by_target.merge {
                        Merge::Insert(merge_insert) => {
                            self.merge_insert(target_table_id, merge_insert)?
                        }
                        Merge::InsertRow => self.merge_insert_row(
                            target_table_id,
                            source_table_id,
                            &subquery_nodes,
                        )?,
                        _ => unreachable!(),
                    }
                }
                When::NotMatchedBySource(when_not_matched_by_source) => {
                    match &when_not_matched_by_source.merge {
                        Merge::Update(merge_update) => self.merge_update(
                            &new_ctx,
                            target_table_alias,
                            target_table_id,
                            merge_update,
                        )?,
                        Merge::Delete => {}
                        _ => unreachable!(),
                    }
                }
            }
        }

        Ok(())
    }

    fn add_var_to_scope(
        &mut self,
        name: &str,
        node_type: NodeType,
        input_lineage_nodes: &[ArenaIndex],
    ) -> ArenaIndex {
        // Var names are case insensitive (like column names)
        let var_ident = name.to_lowercase();
        let obj_name = format!("!var_{}", var_ident);

        let object_idx = self.context.allocate_new_ctx_object(
            &obj_name,
            ContextObjectKind::Var,
            vec![(
                NodeName::Defined(var_ident.clone()),
                node_type,
                input_lineage_nodes.into(),
            )],
        );
        let var_node_idx = self.context.arena_objects[object_idx].lineage_nodes[0];
        self.context.vars.insert(var_ident, var_node_idx);
        self.context.output.push(var_node_idx);
        object_idx
    }

    fn declare_var_statement_lin(
        &mut self,
        declare_var_statement: &DeclareVarStatement,
    ) -> anyhow::Result<Vec<ArenaIndex>> {
        let declared_vars = Vec::with_capacity(declare_var_statement.vars.len());

        let input_lineage_nodes = if let Some(default_expr) = &declare_var_statement.default {
            self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(default_expr, false)
            })?
        } else {
            vec![]
        };

        for var in &declare_var_statement.vars {
            self.add_var_to_scope(
                var.as_str(),
                declare_var_statement.r#type.as_ref().map_or_else(
                    || NodeType::Unknown,
                    node_type_from_parser_parameterized_type,
                ),
                &input_lineage_nodes,
            );
        }

        Ok(declared_vars)
    }

    fn set_var_statement_lin(&mut self, set_var_statement: &SetVarStatement) -> anyhow::Result<()> {
        if set_var_statement.vars.len() > 1 && set_var_statement.exprs.len() == 1 {
            let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                this.select_expr_col_expr_lin(&set_var_statement.exprs[0], true)
            })?;

            for (i, var) in set_var_statement.vars.iter().enumerate() {
                match var {
                    SetVariable::UserVariable(var_name) => {
                        let var_node_idx = self.get_var(var_name.as_str())?;
                        let var_node = &mut self.context.arena_lineage_nodes[var_node_idx];
                        var_node.input = vec![consumed_nodes[i]];
                    }
                    SetVariable::SystemVariable(_) => {}
                }
            }
        } else {
            debug_assert!(set_var_statement.vars.len() == set_var_statement.exprs.len());
            for (var, expr) in set_var_statement.vars.iter().zip(&set_var_statement.exprs) {
                match var {
                    SetVariable::UserVariable(var_name) => {
                        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
                            this.select_expr_col_expr_lin(expr, false)
                        })?;
                        let var_node_idx = self.get_var(var_name.as_str())?;
                        let var_node = &mut self.context.arena_lineage_nodes[var_node_idx];
                        var_node.input = consumed_nodes;
                    }
                    SetVariable::SystemVariable(_) => {}
                }
            }
        }

        Ok(())
    }

    fn drop_table_statement_lin(
        &mut self,
        drop_table_statement: &DropTableStatement,
    ) -> anyhow::Result<()> {
        let table_name = &drop_table_statement.name.name;
        let removed = self.context.source_objects.swap_remove(table_name);

        if removed.is_none() {
            // Not a source object, it is a table created in this script
            self.context.objects_stack.retain(|obj_idx| {
                let obj = &self.context.arena_objects[*obj_idx];
                !(obj.name == *table_name
                    && (obj.kind == ContextObjectKind::Table
                        || obj.kind == ContextObjectKind::TempTable))
            });
        }
        Ok(())
    }

    /// Remove vars from current scope
    fn remove_scope_vars(&mut self, vars: &[ArenaIndex]) {
        vars.iter().for_each(|obj_idx| {
            let var_obj = &self.context.arena_objects[*obj_idx];
            let var_node = &self.context.arena_lineage_nodes[var_obj.lineage_nodes[0]];
            self.context.vars.swap_remove(var_node.name.string());
        });
    }

    fn statements_block_lin(&mut self, statements_block: &StatementsBlock) -> anyhow::Result<()> {
        let mut declared_vars = vec![];

        for statement in &statements_block.statements {
            match statement {
                Statement::DeclareVar(declare_var_statement) => {
                    declared_vars.extend(self.declare_var_statement_lin(declare_var_statement)?);
                }
                _ => self.statement_lin(statement)?,
            }
        }

        if let Some(exception_statements) = &statements_block.exception_statements {
            for statement in exception_statements {
                // A declare statement can't be here as variable declarations
                // must appear at the start of a block
                self.statement_lin(statement)?;
            }
        }

        self.remove_scope_vars(&declared_vars);
        Ok(())
    }

    fn for_in_statement_lin(&mut self, for_in_statement: &ForInStatement) -> anyhow::Result<()> {
        let consumed_nodes = self.call_func_and_consume_lineage_nodes(|this| {
            this.query_expr_lin(&for_in_statement.table_expr, false)
        })?;

        let mut struct_node_tyupes = vec![];
        let mut input = vec![];
        for node_idx in &consumed_nodes {
            let node = &self.context.arena_lineage_nodes[*node_idx];
            struct_node_tyupes.push(StructNodeFieldType {
                name: node.name.string().to_owned(),
                r#type: node.r#type.clone(),
                input: vec![*node_idx],
            });
            input.extend(&node.input);
        }
        let struct_node = NodeType::Struct(StructNodeType {
            fields: struct_node_tyupes,
        });

        let obj_name = self.get_anon_obj_name("anon_struct");
        let obj_idx = self.context.allocate_new_ctx_object(
            &obj_name,
            ContextObjectKind::AnonymousStruct,
            vec![],
        );

        let node = LineageNode {
            name: NodeName::Anonymous,
            r#type: struct_node.clone(),
            source_obj: obj_idx,
            input: input.clone(),
            nested_nodes: IndexMap::new(),
        };

        let node_idx = self.context.arena_lineage_nodes.allocate(node);
        self.context.add_nested_nodes(node_idx);

        self.context.output.push(node_idx);
        let var_idx =
            self.add_var_to_scope(for_in_statement.var_name.as_str(), struct_node, &[node_idx]);

        for statement in &for_in_statement.statements {
            self.statement_lin(statement)?
        }

        self.remove_scope_vars(&[var_idx]);

        Ok(())
    }

    fn statement_lin(&mut self, statement: &Statement) -> anyhow::Result<()> {
        match statement {
            Statement::Labeled(labeled_statement) => {
                self.statement_lin(&labeled_statement.statement)?
            }
            Statement::Query(query_statement) => self.query_statement_lin(query_statement)?,
            Statement::Update(update_statement) => self.update_statement_lin(update_statement)?,
            Statement::While(while_statement) => {
                for statement in &while_statement.statements {
                    self.statement_lin(statement)?
                }
            }
            Statement::ForIn(for_in_statement) => self.for_in_statement_lin(for_in_statement)?,
            Statement::Repeat(repeat_statement) => {
                for statement in &repeat_statement.statements {
                    self.statement_lin(statement)?
                }
            }
            Statement::Insert(insert_statement) => self.insert_statement_lin(insert_statement)?,
            Statement::Merge(merge_statement) => self.merge_statement_lin(merge_statement)?,
            Statement::CreateTable(create_table_statement) => {
                self.create_table_statement_lin(create_table_statement)?
            }
            Statement::CreateView(_) => {
                // TODO
                return Err(anyhow!(
                    "Cannot extract lineage from a create view statement (still a todo)."
                ));
            }
            Statement::DeclareVar(declare_var_statement) => {
                self.declare_var_statement_lin(declare_var_statement)?;
            }
            Statement::SetVar(set_var_statement) => {
                self.set_var_statement_lin(set_var_statement)?
            }
            Statement::Block(statements_block) => self.statements_block_lin(statements_block)?,
            Statement::Loop(loop_statement) => {
                for statement in &loop_statement.statements {
                    self.statement_lin(statement)?
                }
            }
            Statement::DropTableStatement(drop_table_statement) => {
                self.drop_table_statement_lin(drop_table_statement)?
            }
            Statement::If(if_statement) => {
                for statement in &if_statement.r#if.statements {
                    self.statement_lin(statement)?;
                }
                if let Some(ref else_ifs) = if_statement.else_ifs {
                    for else_if in else_ifs {
                        for statement in &else_if.statements {
                            self.statement_lin(statement)?;
                        }
                    }
                }
                if let Some(ref else_statements) = if_statement.r#else {
                    for statement in else_statements {
                        self.statement_lin(statement)?;
                    }
                }
            }
            Statement::Case(case_statement) => {
                for when_then in &case_statement.when_thens {
                    for statement in &when_then.then {
                        self.statement_lin(statement)?;
                    }
                }
                if let Some(ref else_statements) = case_statement.r#else {
                    for statement in else_statements {
                        self.statement_lin(statement)?;
                    }
                }
            }
            Statement::Delete(_)
            | Statement::Truncate(_)
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
        }
        Ok(())
    }

    fn ast_lin(&mut self, query: &Ast) -> anyhow::Result<()> {
        let mut declared_vars = vec![];
        for statement in &query.statements {
            match statement {
                Statement::DeclareVar(declare_var_statement) => {
                    declared_vars.extend(self.declare_var_statement_lin(declare_var_statement)?);
                }
                _ => self.statement_lin(statement)?,
            }
        }
        self.remove_scope_vars(&declared_vars);
        Ok(())
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
    pub source_object: usize,
    pub input: Vec<usize>,
}

#[derive(Serialize, Debug, Clone)]
pub struct RawLineage {
    pub objects: Vec<RawLineageObject>,
    pub lineage_nodes: Vec<RawLineageNode>,
    pub output_lineage: Vec<usize>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageNodeInput {
    pub obj_name: String,
    pub node_name: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct ReadyLineageNode {
    pub name: String,
    pub input: Vec<ReadyLineageNodeInput>,
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

#[derive(Debug, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaObject {
    pub name: String, // This is the project.dataset.name uid
    pub kind: SchemaObjectKind,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Catalog {
    pub schema_objects: Vec<SchemaObject>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObjectKind {
    Table { columns: Vec<Column> },
    View { columns: Vec<Column> },
}

#[derive(Debug, Clone, Serialize)]
pub struct Lineage {
    pub raw_lineage: Option<RawLineage>,
    pub lineage: ReadyLineage,
}

fn node_type_from_parser_type(param_type: &Type, types_vec: &mut Vec<NodeType>) -> NodeType {
    let r#type = match param_type {
        Type::Array { r#type } => NodeType::Array(Box::new(ArrayNodeType {
            r#type: node_type_from_parser_type(r#type, types_vec),
            input: vec![],
        })),
        Type::BigNumeric => NodeType::Base("BIGNUMERIC".to_owned()),
        Type::Bool => NodeType::Base("BOOLEAN".to_owned()),
        Type::Bytes => NodeType::Base("BYTES".to_owned()),
        Type::Date => NodeType::Base("DATE".to_owned()),
        Type::Datetime => NodeType::Base("DATETIME".to_owned()),
        Type::Float64 => NodeType::Base("FLOAT64".to_owned()),
        Type::Geography => NodeType::Base("GEOGRAPHY".to_owned()),
        Type::Int64 => NodeType::Base("INT64".to_owned()),
        Type::Interval => NodeType::Base("INTERVAL".to_owned()),
        Type::Json => NodeType::Base("JSON".to_owned()),
        Type::Numeric => NodeType::Base("NUMERIC".to_owned()),
        Type::Range { r#type: _ } => NodeType::Base("RANGE".to_owned()),
        Type::String => NodeType::Base("STRING".to_owned()),
        Type::Struct { fields } => NodeType::Struct(StructNodeType {
            fields: fields
                .iter()
                .map(|field| StructNodeFieldType {
                    name: field
                        .name
                        .as_ref()
                        .map_or("anonymous".to_owned(), |name| name.as_str().to_owned()),
                    r#type: node_type_from_parser_type(&field.r#type, types_vec),
                    input: vec![],
                })
                .collect(),
        }),
        Type::Time => NodeType::Base("TIME".to_owned()),
        Type::Timestamp => NodeType::Base("TIMESTAMP".to_owned()),
    };
    types_vec.push(r#type.clone());
    r#type
}

fn node_type_from_parser_parameterized_type(param_type: &ParameterizedType) -> NodeType {
    match param_type {
        ParameterizedType::Array {
            r#type: parameterized_type,
        } => NodeType::Array(Box::new(ArrayNodeType {
            r#type: node_type_from_parser_parameterized_type(parameterized_type),
            input: vec![],
        })),
        ParameterizedType::BigNumeric {
            precision: _,
            scale: _,
        } => NodeType::Base("BIGNUMERIC".to_owned()),
        ParameterizedType::Bool => NodeType::Base("BOOLEAN".to_owned()),
        ParameterizedType::Bytes { max_length: _ } => NodeType::Base("BYTES".to_owned()),
        ParameterizedType::Date => NodeType::Base("DATE".to_owned()),
        ParameterizedType::Datetime => NodeType::Base("DATETIME".to_owned()),
        ParameterizedType::Float64 => NodeType::Base("FLOAT64".to_owned()),
        ParameterizedType::Geography => NodeType::Base("GEOGRAPHY".to_owned()),
        ParameterizedType::Int64 => NodeType::Base("INT64".to_owned()),
        ParameterizedType::Interval => NodeType::Base("INTERVAL".to_owned()),
        ParameterizedType::Json => NodeType::Base("JSON".to_owned()),
        ParameterizedType::Numeric {
            precision: _,
            scale: _,
        } => NodeType::Base("NUMERIC".to_owned()),
        ParameterizedType::Range { r#type: _ } => NodeType::Base("RANGE".to_owned()),
        ParameterizedType::String { max_length: _ } => NodeType::Base("STRING".to_owned()),
        ParameterizedType::Struct { fields } => NodeType::Struct(StructNodeType {
            fields: fields
                .iter()
                .map(|field| StructNodeFieldType {
                    name: field.name.as_str().to_owned(),
                    r#type: node_type_from_parser_parameterized_type(&field.r#type),
                    input: vec![],
                })
                .collect(),
        }),
        ParameterizedType::Time => NodeType::Base("TIME".to_owned()),
        ParameterizedType::Timestamp => NodeType::Base("TIMESTAMP".to_owned()),
    }
}

fn parse_column_dtype(column: &Column) -> anyhow::Result<NodeType> {
    let mut scanner = Scanner::new(&column.dtype);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let r#type = parser.parse_parameterized_bq_type()?;
    Ok(node_type_from_parser_parameterized_type(&r#type))
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
    let mut ctx = Context::default();
    let mut source_objects = IndexMap::new();
    for schema_object in &catalog.schema_objects {
        if source_objects.contains_key(&schema_object.name) {
            panic!(
                "Found duplicate definition of schema object `{}`.",
                schema_object.name
            );
        }

        match &schema_object.kind {
            SchemaObjectKind::Table { columns } | SchemaObjectKind::View { columns } => {
                let context_object_kind = ContextObjectKind::Table;

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
                        &schema_object.name, duplicate_columns
                    );
                }

                let mut nodes = vec![];

                for col in columns {
                    let node_type = match parse_column_dtype(col) {
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
                                    &col.name, &schema_object.name, &field.name
                                );
                            }
                            set.insert(&field.name);
                        }
                    }

                    nodes.push((
                        NodeName::Defined(col.name.to_lowercase()),
                        node_type,
                        vec![],
                    ));
                }

                let table_idx =
                    ctx.allocate_new_ctx_object(&schema_object.name, context_object_kind, nodes);
                source_objects.insert(schema_object.name.clone(), table_idx);
            }
        }
    }
    ctx.source_objects = source_objects;

    let mut lineages = vec![];
    let mut lineage_extractor = LineageExtractor {
        anon_id: 0,
        context: ctx,
    };

    for (i, &ast) in asts.iter().enumerate() {
        if i > 0 {
            lineage_extractor.context.reset();
        }
        if let Err(err) = lineage_extractor.ast_lin(ast) {
            lineages.push(Err(err));
            continue;
        };

        // Remove duplicates
        for obj in &lineage_extractor.context.arena_objects {
            for node_idx in &obj.lineage_nodes {
                let lineage_node = &mut lineage_extractor.context.arena_lineage_nodes[*node_idx];
                let mut set = HashSet::new();
                let mut unique_input = vec![];
                for inp_idx in &lineage_node.input {
                    if !set.contains(&inp_idx) {
                        unique_input.push(*inp_idx);
                        set.insert(inp_idx);
                    }
                }
                lineage_node.input = unique_input
            }
        }

        log::debug!("Output Lineage Nodes:");
        for pending_node in &lineage_extractor.context.output {
            LineageNode::pretty_log_lineage_node(*pending_node, &lineage_extractor.context);
        }

        let mut objects: IndexMap<
            ArenaIndex,
            IndexMap<ArenaIndex, HashSet<(ArenaIndex, ArenaIndex)>>,
        > = IndexMap::new();

        for output_node_idx in &lineage_extractor.context.output {
            let output_node = &lineage_extractor.context.arena_lineage_nodes[*output_node_idx];
            let output_source_idx = output_node.source_obj;

            let mut stack = output_node.input.clone();
            loop {
                if stack.is_empty() {
                    break;
                }
                let node_idx = stack.pop().unwrap();
                let node = &lineage_extractor.context.arena_lineage_nodes[node_idx];

                let source_obj_idx = node.source_obj;
                let source_obj = &lineage_extractor.context.arena_objects[source_obj_idx];

                if lineage_extractor
                    .context
                    .source_objects
                    .contains_key(&source_obj.name)
                {
                    objects
                        .entry(output_source_idx)
                        .or_default()
                        .entry(*output_node_idx)
                        .and_modify(|s| {
                            s.insert((source_obj_idx, node_idx));
                        })
                        .or_insert(HashSet::from([(source_obj_idx, node_idx)]));
                } else {
                    stack.extend(&node.input);
                }
            }
        }

        let just_include_source_objects = true;

        let mut ready_lineage = ReadyLineage { objects: vec![] };
        for (obj_idx, obj_map) in objects {
            let obj = &lineage_extractor.context.arena_objects[obj_idx];
            if just_include_source_objects
                && !lineage_extractor
                    .context
                    .source_objects
                    .contains_key(&obj.name)
            {
                continue;
            }

            let mut obj_nodes = vec![];

            for (node_idx, input) in obj_map {
                let node = &lineage_extractor.context.arena_lineage_nodes[node_idx];
                let mut node_input = vec![];
                for (inp_obj_idx, inp_node_idx) in input {
                    let inp_obj = &lineage_extractor.context.arena_objects[inp_obj_idx];
                    if just_include_source_objects
                        && !lineage_extractor
                            .context
                            .source_objects
                            .contains_key(&inp_obj.name)
                    {
                        continue;
                    }
                    let inp_node = &lineage_extractor.context.arena_lineage_nodes[inp_node_idx];
                    node_input.push(ReadyLineageNodeInput {
                        obj_name: inp_obj.name.clone(),
                        node_name: inp_node.name.nested_path().to_owned(),
                    });
                }

                obj_nodes.push(ReadyLineageNode {
                    name: node.name.nested_path().to_owned(),
                    input: node_input,
                });
            }
            ready_lineage.objects.push(ReadyLineageObject {
                name: obj.name.clone(),
                kind: obj.kind.into(),
                nodes: obj_nodes,
            });
        }

        let raw_lineage = if include_raw {
            let arena_objects = &lineage_extractor.context.arena_objects;
            let objects = arena_objects
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
                    source_object: node.source_obj.index,
                    input: node.input.iter().map(|aidx| aidx.index).collect(),
                })
                .collect();

            let output_lineage = lineage_extractor
                .context
                .output
                .clone()
                .iter()
                .map(|aidx| aidx.index)
                .collect();

            Some(RawLineage {
                objects,
                lineage_nodes,
                output_lineage,
            })
        } else {
            None
        };

        let lineage = Lineage {
            raw_lineage,
            lineage: ready_lineage,
        };
        lineages.push(Ok(lineage));
    }

    lineages
}
