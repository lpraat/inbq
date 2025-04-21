use anyhow::{anyhow, Ok};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
};

use crate::{
    arena::{Arena, ArenaIndex},
    parser::{
        Ast, CreateTableStatement, Cte, Expr, FromExpr, GroupingQueryExpr, InsertStatement,
        JoinCondition, JoinExpr, Merge, MergeInsert, MergeSource, MergeStatement, MergeUpdate,
        ParseToken, QueryExpr, QueryStatement, SelectAllExpr, SelectColAllExpr, SelectColExpr,
        SelectExpr, SelectQueryExpr, Statement, UpdateStatement, When, With,
    },
    scanner::TokenType,
};

#[derive(Debug, Clone)]
pub enum NodeName {
    Anonymous,
    Defined(String),
}

impl From<NodeName> for String {
    fn from(val: NodeName) -> Self {
        val.string().into()
    }
}

impl NodeName {
    fn string(&self) -> &str {
        match self {
            NodeName::Anonymous => "__anonymous__",
            NodeName::Defined(s) => s,
        }
    }
}

impl Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string())
    }
}

#[derive(Clone, Debug)]
struct LineageNode {
    name: NodeName,
    source_obj: ArenaIndex,
    input: Vec<ArenaIndex>,
}

impl LineageNode {
    fn compute_deep_lineage(&self, ctx: &Context) -> Vec<String> {
        let mut input_cols = vec![];
        let mut stack = self.input.clone();
        while let Some(popped) = stack.pop() {
            let inp = &ctx.arena_lineage_nodes[popped];
            let col_name = inp.name.clone();
            let table_name = &ctx.arena_objects[inp.source_obj].name;
            let input = format!("[{}]{}.{}", inp.source_obj.index, table_name, col_name);
            input_cols.push(input);
            stack.extend(inp.input.clone());
        }
        input_cols
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
                    in_node.name
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
            node.name,
            in_str
        )
    }
}

#[derive(Debug, Clone)]
pub struct ContextObject {
    name: String,
    lineage_nodes: Vec<ArenaIndex>,
    kind: ContextObjectKind,
}

#[derive(Debug, Clone, Copy)]
pub enum ContextObjectKind {
    TempTable,
    Table,
    Cte,
    Query,
    UsingTable,
    Anonymous, // TODO: anonymous_subquery?
}

impl From<ContextObjectKind> for String {
    fn from(val: ContextObjectKind) -> Self {
        match val {
            ContextObjectKind::TempTable => "temp_table".to_owned(),
            ContextObjectKind::Table => "table".to_owned(),
            ContextObjectKind::Cte => "cte".to_owned(),
            ContextObjectKind::Query => "query".to_owned(),
            ContextObjectKind::UsingTable => "using_table".to_owned(),
            ContextObjectKind::Anonymous => "anonymous".to_owned(),
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
    lineage_stack: Vec<ArenaIndex>,
    output: Vec<ArenaIndex>,
}

impl Context {
    fn allocate_new_ctx_object(
        &mut self,
        name: &str,
        kind: ContextObjectKind,
        nodes: Vec<(NodeName, Vec<ArenaIndex>)>,
    ) -> ArenaIndex {
        let new_obj = ContextObject {
            name: name.to_owned(),
            lineage_nodes: vec![],
            kind,
        };
        let new_id = self.arena_objects.allocate(new_obj);
        let new_obj = &mut self.arena_objects[new_id];

        new_obj.lineage_nodes = nodes
            .into_iter()
            .map(|(n, item)| {
                self.arena_lineage_nodes.allocate(LineageNode {
                    name: n,
                    source_obj: new_id,
                    input: item,
                })
            })
            .collect();
        new_id
    }

    fn allocate_new_lineage_node(
        &mut self,
        name: NodeName,
        source_obj: ArenaIndex,
        input: Vec<ArenaIndex>,
    ) -> ArenaIndex {
        let new_lineage_node = LineageNode {
            name,
            source_obj,
            input,
        };
        self.arena_lineage_nodes.allocate(new_lineage_node)
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
                let ctx_obj = &self.arena_objects[prev_ctx[key]];
                if !new_ctx.contains_key(key) {
                    new_ctx.insert(key.clone(), prev_ctx[key]);
                }
                for node_idx in &ctx_obj.lineage_nodes {
                    let node = &self.arena_lineage_nodes[*node_idx];
                    new_columns
                        .entry(node.name.string().to_lowercase())
                        .or_default()
                        .push(prev_ctx[key]);
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

struct LineageExtractor {
    anon_id: u64,
    context: Context,
}

impl LineageExtractor {
    fn get_anon_id(&mut self) -> u64 {
        let curr = self.anon_id;
        self.anon_id += 1;
        curr
    }

    // TODO: we need to refactor this
    fn consume_lineage_nodes(
        &mut self,
        initial_stack_size: usize,
        final_stack_size: usize,
    ) -> Vec<ArenaIndex> {
        let mut lineage_nodes = vec![];
        for _ in 0..final_stack_size - initial_stack_size {
            let node_idx = self.context.lineage_stack.pop().unwrap();
            lineage_nodes.push(node_idx);
        }
        lineage_nodes.reverse();
        lineage_nodes
    }

    fn cte_lin(&mut self, cte: &Cte) -> anyhow::Result<()> {
        match cte {
            Cte::NonRecursive(non_recursive_cte) => {
                let cte_name = non_recursive_cte.name.identifier();

                let start_lineage_len = self.context.lineage_stack.len();
                self.query_expr_lin(&non_recursive_cte.query)?;
                let curr_lineage_len = self.context.lineage_stack.len();

                let consumed_lineage_nodes =
                    self.consume_lineage_nodes(start_lineage_len, curr_lineage_len);
                let cte_idx = self.context.allocate_new_ctx_object(
                    &cte_name,
                    ContextObjectKind::Cte,
                    consumed_lineage_nodes
                        .into_iter()
                        .map(|idx| {
                            let node = &self.context.arena_lineage_nodes[idx];
                            (node.name.clone(), vec![idx])
                        })
                        .collect(),
                );
                self.context.add_object(cte_idx);
                self.context
                    .update_output_lineage_with_object_nodes(cte_idx);
            }
            Cte::Recursive(recursive_cte) => todo!(),
        }
        Ok(())
    }

    fn with_lin(&mut self, with: &With) -> anyhow::Result<()> {
        for cte in &with.ctes {
            self.cte_lin(cte)?;
        }
        Ok(())
    }

    fn get_column_source(
        &mut self,
        table: Option<&String>,
        column: &str,
    ) -> anyhow::Result<ArenaIndex> {
        // columns are case insensitive
        let column = column.to_lowercase();
        if let Some(table) = table {
            if let Some(ctx_table_idx) = self
                .context
                .curr_stack()
                .ok_or(anyhow!("Table `{}` not found in context.", table))?
                .get(&table.to_owned())
            {
                let ctx_table = &self.context.arena_objects[*ctx_table_idx];
                let col_in_schema = ctx_table
                    .lineage_nodes
                    .iter()
                    .map(|n_idx| (&self.context.arena_lineage_nodes[*n_idx], *n_idx))
                    .find(|(n, n_idx)| n.name.string() == column);
                if let Some((col, col_idx)) = col_in_schema {
                    return Ok(col_idx);
                }
                Err(anyhow!(
                    "Column {} not found in the schema of table {}",
                    column,
                    table
                ))
            } else {
                Err(anyhow!(
                    "Table {} not found in context for column {}",
                    table,
                    column
                ))
            }
        } else if let Some(target_tables) = self
            .context
            .curr_columns_stack()
            .unwrap_or(&IndexMap::new())
            .get(&column)
        {
            if target_tables.len() > 1
                && !target_tables.iter().any(|el| {
                    self.context
                        .curr_stack()
                        .unwrap()
                        .get(&self.context.arena_objects[*el].name)
                        .map(|el| &self.context.arena_objects[*el])
                        .is_some_and(|x| matches!(x.kind, ContextObjectKind::UsingTable))
                })
            {
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
                // Pick the last using_table
                *target_tables.last().unwrap()
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
                .filter(|(n, _)| n.name.string() == column)
                .collect::<Vec<_>>()[0]
                .1);
        } else {
            return Err(anyhow!("Column `{}` not found in context.", column));
        }
    }

    fn select_expr_col_expr(&mut self, expr: &Expr) -> anyhow::Result<()> {
        match expr {
            Expr::Binary(binary_expr) => {
                let source: String;
                let col_name: String;

                let operator = binary_expr.operator.lexeme(None);
                if operator == "." {
                    let left_expr = binary_expr.left.as_ref();
                    let right_expr = binary_expr.right.as_ref();
                    match left_expr {
                        Expr::Identifier(ident) => {
                            source = ident.clone();
                        }
                        Expr::QuotedIdentifier(qident) => {
                            source = qident.clone();
                        }
                        Expr::String(_)
                        | Expr::Number(_)
                        | Expr::Bool(_)
                        | Expr::Null
                        | Expr::Star => return Err(anyhow!("Invalid query.")),
                        _ => {
                            // binary expr (e.g., tmp.s.x[0]) where s is a struct
                            todo!()
                        }
                    }

                    match right_expr {
                        Expr::Identifier(ident) => {
                            col_name = ident.clone();
                        }
                        Expr::QuotedIdentifier(qident) => {
                            col_name = qident.clone();
                        }
                        Expr::String(_)
                        | Expr::Number(_)
                        | Expr::Bool(_)
                        | Expr::Null
                        | Expr::Star => return Err(anyhow!("Invalid query.")),
                        _ => {
                            // TODO: struct is valid, e.g. this is valid ( struct(1 as x).x )
                            return Err(anyhow!("Invalid query."));
                        }
                        _ => todo!(),
                    }

                    let col_source_idx = self.get_column_source(Some(&source), &col_name)?;
                    self.context.lineage_stack.push(col_source_idx);
                } else {
                    self.select_expr_col_expr(binary_expr.left.as_ref())?;
                    self.select_expr_col_expr(binary_expr.right.as_ref())?;
                }
            }
            Expr::Unary(unary_expr) => todo!(),
            Expr::Grouping(grouping_expr) => todo!(),
            Expr::Identifier(ident) | Expr::QuotedIdentifier(ident) => {
                let col_name = ident.clone();
                let col_source_idx = self.get_column_source(None, &col_name)?;
                self.context.lineage_stack.push(col_source_idx);
            }
            Expr::String(_) => {}
            Expr::Bytes(_) => {}
            Expr::Number(_) => {}
            Expr::Numeric(_) | Expr::BigNumeric(_) => {}
            Expr::Range(_) => {}
            Expr::Date(_) | Expr::Timestamp(_) | Expr::Datetime(_) | Expr::Time(_) => {}
            Expr::Interval(_) => {}
            Expr::Json(_) => {}
            Expr::Bool(_) => {}
            Expr::Null => {}
            Expr::Default => {}
            Expr::Star => todo!(),
            Expr::Array(array_expr) => todo!(),
            Expr::Struct(struct_expr) => todo!(),
            Expr::Query(query_expr) => self.query_expr_lin(query_expr)?,
            Expr::GenericFunction(generic_function_expr) => todo!(),
            Expr::Function(function_expr) => todo!(),
        }

        Ok(())
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
                cols.into_iter()
                    .map(|c| c.identifier().to_lowercase())
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
                .filter(|(n, _)| n.name.string() == col_name)
                .collect::<Vec<_>>()[0]
                .1;

            new_lineage_nodes.push((
                NodeName::Defined(col_name.clone()),
                anon_obj_idx,
                vec![col_in_table_idx],
            ));
        }
        new_lineage_nodes.into_iter().for_each(|tup| {
            let lineage_node_idx = self.context.allocate_new_lineage_node(tup.0, tup.1, tup.2);
            self.context.lineage_stack.push(lineage_node_idx);
            lineage_nodes.push(lineage_node_idx);
            self.context.arena_objects[anon_obj_idx]
                .lineage_nodes
                .push(lineage_node_idx);
        });

        Ok(())
    }

    fn select_expr_col_all_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColAllExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let except_columns = col_expr.except.clone().map_or(HashSet::default(), |cols| {
            cols.into_iter()
                .map(|c| c.identifier().to_lowercase())
                .collect::<HashSet<String>>()
        });
        match &col_expr.expr {
            Expr::Binary(binary_expr) => {
                let star = &binary_expr.right;
                assert!(matches!(**star, Expr::Star));
                let mut curr_left = &binary_expr.left;

                // Retrieve the last object before the star
                // if col_expr=x.y.z.* then curr_left = z
                let curr_left = loop {
                    match **curr_left {
                        Expr::Binary(ref binary_expr) => {
                            curr_left = &binary_expr.left;
                        }
                        Expr::Identifier(ref ident) => break ident.clone(),
                        Expr::QuotedIdentifier(ref qident) => break qident.clone(),
                        _ => return Err(anyhow!("Invalid query.")),
                    }
                };

                let source_obj_idx = *self
                    .context
                    .curr_stack()
                    .unwrap()
                    .get(&curr_left)
                    .ok_or(anyhow!("Cannot find table like obj {:?}", curr_left))?;
                let source_obj = &self.context.arena_objects[source_obj_idx];
                let mut new_lineage_nodes = vec![];

                for node_idx in source_obj.lineage_nodes.clone() {
                    let node = &self.context.arena_lineage_nodes[node_idx];

                    if except_columns.contains(node.name.string()) {
                        continue;
                    }

                    new_lineage_nodes.push((node.name.clone(), anon_obj_idx, node.input.clone()))
                }
                new_lineage_nodes.into_iter().for_each(|tup| {
                    let lineage_node_idx =
                        self.context.allocate_new_lineage_node(tup.0, tup.1, tup.2);
                    self.context.lineage_stack.push(lineage_node_idx);
                    lineage_nodes.push(lineage_node_idx);
                    self.context.arena_objects[anon_obj_idx]
                        .lineage_nodes
                        .push(lineage_node_idx);
                });
            }
            _ => return Err(anyhow!("Invalid query.")),
        };
        Ok(())
    }

    fn select_expr_col_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        col_expr: &SelectColExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let pending_node_idx =
            self.context
                .allocate_new_lineage_node(NodeName::Anonymous, anon_obj_idx, vec![]);
        self.context.arena_objects[anon_obj_idx]
            .lineage_nodes
            .push(pending_node_idx);

        let start_pending_len = self.context.lineage_stack.len();
        self.select_expr_col_expr(&col_expr.expr)?;
        let curr_pending_len = self.context.lineage_stack.len();

        let mut consumed_nodes = vec![];
        for _ in 0..curr_pending_len - start_pending_len {
            consumed_nodes.push(self.context.lineage_stack.pop().unwrap());
        }

        let first_node_name = consumed_nodes
            .first()
            .map(|idx| &self.context.arena_lineage_nodes[*idx].name)
            .cloned();

        let pending_node = &mut self.context.arena_lineage_nodes[pending_node_idx];

        pending_node.input.extend(consumed_nodes);

        if let Some(alias) = &col_expr.alias {
            pending_node.name = NodeName::Defined(alias.identifier().to_lowercase());
        }

        if pending_node.input.len() == 1 {
            if let NodeName::Anonymous = pending_node.name {
                pending_node.name = first_node_name.unwrap();
            }
        }

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
                self.from_expr_lin(&cross_join_expr.right, from_tables, joined_tables)?;
            }
            FromExpr::Unnest(_) => todo!(),
            FromExpr::Path(from_path_expr) => {
                let table_name = from_path_expr.path_expr.path.identifier();
                let table_like_obj_id = self.get_table_id_from_context(&table_name);

                if table_like_obj_id.is_none() {
                    return Err(anyhow!(
                        "Table like obj name `{}` not in context.",
                        table_name
                    ));
                }

                let contains_alias = from_path_expr.alias.is_some();
                let table_like_name = if contains_alias {
                    from_path_expr.alias.as_ref().unwrap().identifier()
                } else {
                    table_name.clone()
                };

                let table_like_obj_id = table_like_obj_id.unwrap();

                if contains_alias {
                    // If aliased, we create a new object
                    let table_like_obj = &self.context.arena_objects[table_like_obj_id].clone();

                    let new_lineage_nodes: Vec<ArenaIndex> = table_like_obj
                        .lineage_nodes
                        .iter()
                        .map(|el| {
                            let ln = &self.context.arena_lineage_nodes[*el];
                            self.context.allocate_new_lineage_node(
                                ln.name.clone(),
                                table_like_obj_id,
                                vec![*el],
                            )
                        })
                        .collect();

                    let new_table_like_idx = self.context.allocate_new_ctx_object(
                        &table_like_name,
                        table_like_obj.kind,
                        new_lineage_nodes
                            .iter()
                            .map(|ln| {
                                let ln = &self.context.arena_lineage_nodes[*ln];
                                (ln.name.clone(), ln.input.clone())
                            })
                            .collect(),
                    );
                    self.context
                        .update_output_lineage_with_object_nodes(new_table_like_idx);
                    self.add_new_from_table(from_tables, new_table_like_idx)?;
                } else {
                    self.add_new_from_table(from_tables, table_like_obj_id)?;
                }
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let start_lineage_len = self.context.lineage_stack.len();
                self.query_expr_lin(&from_grouping_query_expr.query_expr)?;
                let curr_lineage_len = self.context.lineage_stack.len();

                let source_name = &from_grouping_query_expr
                    .alias
                    .as_ref()
                    .map(|alias| alias.identifier());

                let consumed_lineage_nodes =
                    self.consume_lineage_nodes(start_lineage_len, curr_lineage_len);
                let lineage_nodes_source =
                    self.context.arena_lineage_nodes[consumed_lineage_nodes[0]].source_obj;

                let new_source_name = if let Some(name) = source_name {
                    name
                } else {
                    &self.context.arena_objects[lineage_nodes_source]
                        .name
                        .clone()
                };

                let table_like_idx = self.context.allocate_new_ctx_object(
                    new_source_name,
                    ContextObjectKind::Query,
                    consumed_lineage_nodes
                        .into_iter()
                        .map(|idx| {
                            let node = &self.context.arena_lineage_nodes[idx];
                            (node.name.clone(), vec![idx])
                        })
                        .collect(),
                );
                self.context
                    .update_output_lineage_with_object_nodes(table_like_idx);
                self.add_new_from_table(from_tables, table_like_idx)?;
            }
            FromExpr::GroupingFrom(grouping_from_expr) => {
                self.from_expr_lin(&grouping_from_expr.query_expr, from_tables, joined_tables)?
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
        if let JoinCondition::Using(using_columns) = &join_expr.cond {
            let mut joined_table_names: Vec<String> = vec![];
            let mut lineage_nodes = vec![];
            let from_tables_len = from_tables.len();

            let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
            let left_join_table = if !joined_tables.is_empty() {
                // We have already joined two tables
                &self.context.arena_objects[*joined_tables.last().unwrap()]
            } else {
                // This is the first join, which corresponds to index -2 in the original from_tables
                &self.context.arena_objects[*from_tables_split.0.last().unwrap()]
            };
            joined_table_names.push(left_join_table.name.clone());

            let right_join_table =
                &self.context.arena_objects[*from_tables_split.1.last_mut().unwrap()];
            joined_table_names.push(right_join_table.name.clone());

            let mut using_columns_added = HashSet::new();
            for col in using_columns {
                let col_name = col.identifier().to_lowercase();
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
                    .map(|(node, idx)| (node.name.clone(), vec![*idx])),
            );

            lineage_nodes.extend(
                right_join_table
                    .lineage_nodes
                    .iter()
                    .map(|idx| (&self.context.arena_lineage_nodes[*idx], idx))
                    .filter(|(node, _)| !using_columns_added.contains(node.name.string()))
                    .map(|(node, idx)| (node.name.clone(), vec![*idx])),
            );

            let mut added_columns = HashSet::new();
            for (node, _) in &lineage_nodes {
                let is_new_column = added_columns.insert(node.string());
                if !is_new_column {
                    return Err(anyhow!(
                        "Column `{}` is ambiguous. It is contained in more than one table.",
                        node.string()
                    ));
                }
            }

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
        }

        Ok(())
    }

    fn select_query_expr_lin(&mut self, select_query_expr: &SelectQueryExpr) -> anyhow::Result<()> {
        let ctx_objects_start_size = self.context.objects_stack.len();

        let pushed_empty_cte_ctx = if let Some(with) = select_query_expr.with.as_ref() {
            // We push and empty context since a CTE on a subquery may not reference correlated columns from the outer query
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

        let anon_id = format!("anon_{}", self.get_anon_id());
        let anon_obj_idx =
            self.context
                .allocate_new_ctx_object(&anon_id, ContextObjectKind::Anonymous, vec![]);
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
    ) -> anyhow::Result<()> {
        let pushed_empty_cte_ctx = if let Some(with) = grouping_query_expr.with.as_ref() {
            // We push and empty context since a CTE on a subquery may not reference correlated columns from the outer query
            self.context.push_new_ctx(IndexMap::new(), false);
            self.with_lin(with)?;
            true
        } else {
            false
        };
        self.query_expr_lin(&grouping_query_expr.query_expr)?;
        if pushed_empty_cte_ctx {
            self.context.pop_curr_ctx();
        }
        Ok(())
    }

    fn query_expr_lin(&mut self, query: &QueryExpr) -> anyhow::Result<()> {
        match query {
            QueryExpr::Grouping(grouping_query_expr) => {
                self.grouping_query_expr_lin(grouping_query_expr)?
            }
            QueryExpr::Select(select_query_expr) => {
                self.select_query_expr_lin(select_query_expr)?;
            }
            QueryExpr::SetSelect(set_select_query_expr) => todo!(),
        }

        Ok(())
    }

    fn query_statement_lin(&mut self, query_statement: &QueryStatement) -> anyhow::Result<()> {
        self.query_expr_lin(&query_statement.query_expr)
    }

    fn create_table_statement_lin(
        &mut self,
        create_table_statement: &CreateTableStatement,
    ) -> anyhow::Result<()> {
        let table_name = create_table_statement.name.identifier();
        let table_kind = if create_table_statement.is_temporary {
            ContextObjectKind::TempTable
        } else {
            ContextObjectKind::Table
        };
        let temp_table_idx = if let Some(ref query) = create_table_statement.query {
            // Extract the schema from the query lineage
            let start_lineage_len = self.context.lineage_stack.len();
            self.query_expr_lin(query)?;
            let curr_lineage_len = self.context.lineage_stack.len();

            let consumed_lineage_nodes =
                self.consume_lineage_nodes(start_lineage_len, curr_lineage_len);
            self.context.allocate_new_ctx_object(
                &table_name,
                table_kind,
                consumed_lineage_nodes
                    .into_iter()
                    .map(|idx| {
                        let node = &self.context.arena_lineage_nodes[idx];
                        (node.name.clone(), vec![idx])
                    })
                    .collect(),
            )
        } else {
            let schema = create_table_statement
                .schema
                .as_ref()
                .ok_or(anyhow!("Schema not found for table: `{:?}`.", table_name))?;
            self.context.allocate_new_ctx_object(
                &table_name,
                table_kind,
                schema
                    .iter()
                    .map(|col_schema| (NodeName::Defined(col_schema.name.identifier()), vec![]))
                    .collect(),
            )
        };
        self.context.add_object(temp_table_idx);
        self.context
            .update_output_lineage_with_object_nodes(temp_table_idx);
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_lin(&mut self, from: &crate::parser::From) -> anyhow::Result<()> {
        let mut from_tables: Vec<ArenaIndex> = Vec::new();
        let mut joined_tables: Vec<ArenaIndex> = Vec::new();

        self.from_expr_lin(&from.expr, &mut from_tables, &mut joined_tables)?;
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

    // TODO: reorder methods
    fn get_table_id_from_context(&self, table_name: &str) -> Option<ArenaIndex> {
        // We first check whether it is a context object, otherwise we check for source tables
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

    fn update_statement_lin(&mut self, update_statement: &UpdateStatement) -> anyhow::Result<()> {
        let target_table = update_statement.target_table.identifier();
        let target_table_alias = if let Some(ref alias) = update_statement.alias {
            alias.identifier()
        } else {
            target_table.clone()
        };

        let target_table_id = self.get_table_id_from_context(&target_table);
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
        let target_table_nodes = target_table_obj
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
            .collect::<HashMap<String, ArenaIndex>>();

        // NOTE: we push the target table after pushing the from context
        self.context.push_new_ctx(
            IndexMap::from([(target_table_alias.clone(), target_table_id.unwrap())]),
            true,
        );

        for update_item in &update_statement.update_items {
            let column = match &update_item.column_path {
                // col = ...
                ParseToken::Single(_) => update_item.column_path.identifier(),
                // table.col = ...
                ParseToken::Multiple(vec) => match &vec.last().unwrap().kind {
                    TokenType::Identifier(ident) => ident.to_owned(),
                    TokenType::QuotedIdentifier(qident) => qident.to_owned(),
                    _ => unreachable!(),
                },
            }
            .to_lowercase();

            let col_source_idx = target_table_nodes.get(&column).ok_or(anyhow!(
                "Cannot find column {} in table {}",
                column,
                target_table_alias
            ))?;

            let start_pending_len = self.context.lineage_stack.len();
            self.select_expr_col_expr(&update_item.expr)?;
            let curr_pending_len = self.context.lineage_stack.len();
            let consumed_nodes = self.consume_lineage_nodes(start_pending_len, curr_pending_len);

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
        let target_table = insert_statement.target_table.identifier();

        let target_table_id = self.get_table_id_from_context(&target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }

        let target_table_obj = &self.context.arena_objects[target_table_id.unwrap()];
        let target_table_nodes = target_table_obj
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
            .collect::<HashMap<String, ArenaIndex>>();

        let target_columns = if let Some(columns) = &insert_statement.columns {
            let mut filtered_columns = vec![];
            for col in columns {
                let col_name = col.identifier();
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

        if let Some(query_expr) = &insert_statement.query_expr {
            let start_lineage_len = self.context.lineage_stack.len();
            self.query_expr_lin(query_expr)?;
            let curr_lineage_len = self.context.lineage_stack.len();
            let consumed_lineage_nodes =
                self.consume_lineage_nodes(start_lineage_len, curr_lineage_len);
            if consumed_lineage_nodes.len() != target_columns.len() {
                return Err(anyhow!(
                    "The number of insert columns is not equal to the number of insert values."
                ));
            }

            target_columns
                .iter()
                .zip(consumed_lineage_nodes)
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
                let start_pending_len = self.context.lineage_stack.len();
                self.select_expr_col_expr(value)?;
                let curr_pending_len = self.context.lineage_stack.len();
                let consumed_nodes =
                    self.consume_lineage_nodes(start_pending_len, curr_pending_len);

                let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_col];
                target_lineage_node.input.extend(consumed_nodes.clone());
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

        // TODO: this code is repeated (see insert and update statement)
        let target_table_nodes = target_table_obj
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
            .collect::<HashMap<String, ArenaIndex>>();

        let target_columns = if let Some(columns) = &merge_insert.columns {
            let mut filtered_columns = vec![];
            for col in columns {
                let col_name = col.identifier();
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
            let start_pending_len = self.context.lineage_stack.len();
            self.select_expr_col_expr(value)?;
            let curr_pending_len = self.context.lineage_stack.len();
            let consumed_nodes = self.consume_lineage_nodes(start_pending_len, curr_pending_len);

            let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_col];
            target_lineage_node.input.extend(consumed_nodes.clone());
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

        // TODO: this code is repeated (see insert and update statement)
        let target_table_nodes = target_table_obj
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
            .collect::<HashMap<String, ArenaIndex>>();

        for update_item in &merge_update.update_items {
            let column = match &update_item.column_path {
                // col = ...
                ParseToken::Single(_) => update_item.column_path.identifier(),
                // table.col = ...
                ParseToken::Multiple(vec) => match &vec.last().unwrap().kind {
                    TokenType::Identifier(ident) => ident.to_owned(),
                    TokenType::QuotedIdentifier(qident) => qident.to_owned(),
                    _ => unreachable!(),
                },
            }
            .to_lowercase();

            let col_source_idx = target_table_nodes.get(&column).ok_or(anyhow!(
                "Cannot find column {} in table {}",
                column,
                target_table_alias
            ))?;

            let start_pending_len = self.context.lineage_stack.len();
            self.select_expr_col_expr(&update_item.expr)?;
            let curr_pending_len = self.context.lineage_stack.len();
            let consumed_nodes = self.consume_lineage_nodes(start_pending_len, curr_pending_len);

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
            return Err(anyhow!("The number of merge insert columns is not equal to the number of columns in target table `{}`.", target_table.name));
        }

        for (target_node, source_node) in target_nodes.iter().zip(nodes) {
            let target_lineage_node = &mut self.context.arena_lineage_nodes[*target_node];
            target_lineage_node.input.push(source_node);
            self.context.output.push(*target_node);
        }

        Ok(())
    }

    fn merge_statement_lin(&mut self, merge_statement: &MergeStatement) -> anyhow::Result<()> {
        let target_table = merge_statement.target_table.identifier();
        let target_table_alias = if let Some(ref alias) = merge_statement.target_alias {
            alias.identifier()
        } else {
            target_table.clone()
        };

        let target_table_id = self.get_table_id_from_context(&target_table);
        if target_table_id.is_none() {
            return Err(anyhow!(
                "Table like obj name `{}` not in context.",
                target_table
            ));
        }
        let target_table_id = target_table_id.unwrap();

        let source_table_id = if let MergeSource::Table(parse_token) = &merge_statement.source {
            let source_table = parse_token.identifier();
            let source_table_id = self.get_table_id_from_context(&source_table);
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

        let (subquery_table_id, subquery_nodes) = if let MergeSource::Subquery(query_expr) =
            &merge_statement.source
        {
            let start_pending_len = self.context.lineage_stack.len();
            self.query_expr_lin(query_expr)?;
            let curr_pending_len = self.context.lineage_stack.len();
            let consumed_nodes = self.consume_lineage_nodes(start_pending_len, curr_pending_len);

            if let Some(alias) = &merge_statement.source_alias {
                let new_source_name = alias.identifier();
                let source_idx = self.context.allocate_new_ctx_object(
                    &new_source_name,
                    ContextObjectKind::Query,
                    consumed_nodes
                        .iter()
                        .cloned()
                        .map(|idx| {
                            let node = &self.context.arena_lineage_nodes[idx];
                            (node.name.clone(), vec![idx])
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

        let mut new_ctx = IndexMap::from([(target_table_alias.clone(), target_table_id)]);
        if let Some(alias) = &merge_statement.source_alias {
            let source_alias = alias.identifier();
            new_ctx.insert(source_alias.clone(), source_table_id.unwrap());
        }

        for when in &merge_statement.whens {
            match when {
                When::Matched(when_matched) => match &when_matched.merge {
                    Merge::Update(merge_update) => self.merge_update(
                        &new_ctx,
                        &target_table_alias,
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
                            &target_table_alias,
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

    fn ast_lin(&mut self, query: &Ast) -> anyhow::Result<()> {
        for statement in &query.statements {
            match statement {
                Statement::Query(query_statement) => self.query_statement_lin(query_statement)?,
                Statement::Update(update_statement) => {
                    self.update_statement_lin(update_statement)?
                }
                Statement::Insert(insert_statement) => {
                    self.insert_statement_lin(insert_statement)?
                }
                Statement::Merge(merge_statement) => self.merge_statement_lin(merge_statement)?,
                Statement::CreateTable(create_table_statement) => {
                    self.create_table_statement_lin(create_table_statement)?
                }
                Statement::Delete(_) => {}
                Statement::Truncate(_) => {}
            }
        }
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
pub struct SchemaObject {
    pub name: String,
    pub kind: SchemaObjectKind,
    pub columns: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Catalog {
    pub schema_objects: Vec<SchemaObject>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObjectKind {
    Table,
    View,
}

#[derive(Debug, Clone)]
pub struct Lineage {
    pub raw: RawLineage,
    pub ready: ReadyLineage,
}

pub fn lineage(ast: &Ast, catalog: &Catalog) -> anyhow::Result<Lineage> {
    let mut ctx = Context::default();
    let mut source_objects = IndexMap::new();
    for schema_object in &catalog.schema_objects {
        if source_objects.contains_key(&schema_object.name) {
            return Err(anyhow!(
                "Found duplicate definition of schema object `{}`.",
                schema_object.name
            ));
        }
        let context_object_kind = match schema_object.kind {
            SchemaObjectKind::Table => ContextObjectKind::Table,
            SchemaObjectKind::View => ContextObjectKind::Table,
        };

        let mut unique_columns = HashSet::new();
        let mut duplicate_columns = vec![];
        let are_columns_unique = schema_object.columns.iter().all(|col| {
            let is_unique = unique_columns.insert(col);
            if !is_unique {
                duplicate_columns.push(col);
            }
            is_unique
        });
        if !are_columns_unique {
            return Err(anyhow!(
                "Found duplicate columns in schema object `{}`: `{:?}`.",
                schema_object.name,
                duplicate_columns
            ));
        }

        let table_idx = ctx.allocate_new_ctx_object(
            &schema_object.name,
            context_object_kind,
            schema_object
                .columns
                .iter()
                .map(|col|
                // columns are case insensitive, we lowercase them
                (NodeName::Defined(col.to_lowercase()), vec![]))
                .collect(),
        );
        source_objects.insert(schema_object.name.clone(), table_idx);
    }
    ctx.source_objects = source_objects;

    let mut lineage = LineageExtractor {
        anon_id: 0,
        context: ctx,
    };
    lineage.ast_lin(ast)?;

    // Remove duplicates
    for obj in &lineage.context.arena_objects {
        for node_idx in obj.lineage_nodes.clone() {
            let lineage_node = &mut lineage.context.arena_lineage_nodes[node_idx];
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

    let output_lineage_nodes = lineage.context.output.clone();
    log::debug!("Output Lineage Nodes:");
    for pending_node in output_lineage_nodes {
        LineageNode::pretty_log_lineage_node(pending_node, &lineage.context);
    }

    let mut objects: IndexMap<ArenaIndex, IndexMap<ArenaIndex, HashSet<(ArenaIndex, ArenaIndex)>>> =
        IndexMap::new();

    for output_node_idx in &lineage.context.output {
        let output_node = &lineage.context.arena_lineage_nodes[*output_node_idx];
        let output_source_idx = output_node.source_obj;

        let mut stack = output_node.input.clone();
        loop {
            if stack.is_empty() {
                break;
            }
            let node_idx = stack.pop().unwrap();
            let node = &lineage.context.arena_lineage_nodes[node_idx];

            let source_obj_idx = node.source_obj;
            let source_obj = &lineage.context.arena_objects[source_obj_idx];

            if lineage
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
                stack.extend(node.input.clone());
            }
        }
    }

    let just_include_source_objects = true;

    let mut ready_lineage = ReadyLineage { objects: vec![] };
    for (obj_idx, obj_map) in objects {
        let obj = &lineage.context.arena_objects[obj_idx];
        if just_include_source_objects && !lineage.context.source_objects.contains_key(&obj.name) {
            continue;
        }

        let mut obj_nodes = vec![];

        for (node_idx, input) in obj_map {
            let node = &lineage.context.arena_lineage_nodes[node_idx];
            let mut node_input = vec![];
            for (inp_obj_idx, inp_node_idx) in input {
                let inp_obj = &lineage.context.arena_objects[inp_obj_idx];
                if just_include_source_objects
                    && !lineage.context.source_objects.contains_key(&inp_obj.name)
                {
                    continue;
                }
                let inp_node = &lineage.context.arena_lineage_nodes[inp_node_idx];
                node_input.push(ReadyLineageNodeInput {
                    obj_name: inp_obj.name.clone(),
                    node_name: inp_node.name.string().to_owned(),
                });
            }

            obj_nodes.push(ReadyLineageNode {
                name: node.name.string().to_owned(),
                input: node_input,
            });
        }
        ready_lineage.objects.push(ReadyLineageObject {
            name: obj.name.clone(),
            kind: obj.kind.into(),
            nodes: obj_nodes,
        });
    }

    let raw_lineage = RawLineage {
        objects: lineage
            .context
            .arena_objects
            .into_iter()
            .enumerate()
            .map(|(idx, obj)| RawLineageObject {
                id: idx,
                name: obj.name,
                kind: obj.kind.into(),
                nodes: obj
                    .lineage_nodes
                    .into_iter()
                    .map(|aidx| aidx.index)
                    .collect(),
            })
            .collect(),
        lineage_nodes: lineage
            .context
            .arena_lineage_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, node)| RawLineageNode {
                id: idx,
                name: node.name.into(),
                source_object: node.source_obj.index,
                input: node.input.into_iter().map(|aidx| aidx.index).collect(),
            })
            .collect(),
        output_lineage: lineage
            .context
            .output
            .into_iter()
            .map(|aidx| aidx.index)
            .collect(),
    };

    let lineage = Lineage {
        raw: raw_lineage,
        ready: ready_lineage,
    };

    Ok(lineage)
}
