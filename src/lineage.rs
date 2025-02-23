use anyhow::{anyhow, Ok};
use std::{
    collections::HashMap,
    fmt::{Debug, Display}, future::pending,
};

use crate::{
    arena::{Arena, ArenaIndex}, parser::{
        ColExpr, Cte, Expr, FromExpr, GroupingQueryExpr, JoinCondition, JoinExpr, LiteralExpr,
        QueryExpr, SelectColExpr, SelectQueryExpr, With,
    }, scanner::TokenLiteral
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
            NodeName::Anonymous => "",
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

fn pretty_print_lineage_node(node_idx: ArenaIndex, ctx: &Context) {
    let node = &ctx.arena_lineage_nodes[node_idx];
    let node_source_name = &ctx.arena_objects[node.source_obj].name;
    let in_str = node
        .input
        .iter()
        .map(|idx| {
            let node = &ctx.arena_lineage_nodes[*idx];
            format!("{}->{}", ctx.arena_objects[node.source_obj].name, node.name)
        })
        .fold((0, String::from("")), |acc, el| {
            if acc.0 == 0 {
                (acc.0 + 1, el.to_string())
            } else {
                (acc.0 + 1, format!("{}, {}", acc.1, el))
            }
        })
        .1;
    println!("{}->{} <-[{}]", node_source_name, node.name, in_str)
}

#[derive(Debug, Clone)]
pub struct ContextObject {
    name: String,
    lineage_nodes: Vec<ArenaIndex>,
    kind: ContextObjectKind,
}

#[derive(Debug, Clone, Copy)]
pub enum ContextObjectKind {
    Table,
    Cte,
    Query,
    UsingTable,
    Anonymous
}

#[derive(Debug, Default)]
struct Context {
    arena_objects: Arena<ContextObject>,
    arena_lineage_nodes: Arena<LineageNode>,
    source_objects: HashMap<String, ArenaIndex>,
    objects_stack: Vec<ArenaIndex>,
    stack: Vec<HashMap<String, ArenaIndex>>,
    columns_stack: Vec<HashMap<String, Vec<ArenaIndex>>>,
    lineage_stack: Vec<ArenaIndex>,
    output: Vec<ArenaIndex>,
}

impl Context {
    fn allocate_new_ctx_object(&mut self, name: &str, kind: ContextObjectKind, nodes: Vec<(NodeName, Vec<ArenaIndex>)>) -> ArenaIndex {
        let new_obj = ContextObject {
            name: name.to_owned(),
            lineage_nodes: vec![],
            kind,
        };
        let new_id = self.arena_objects.allocate(new_obj);
        let new_obj = &mut self.arena_objects[new_id];

        new_obj.lineage_nodes = nodes.into_iter().map(|(n, item)| {
            let new_lineage_node_idx = self.arena_lineage_nodes.allocate(
                LineageNode {
                    name: n,
                    source_obj: new_id,
                    input: item,
                }
            );
            new_lineage_node_idx
        }).collect();
        new_id
    }

    fn allocate_new_lineage_node(&mut self, name: NodeName, source_obj: ArenaIndex, input: Vec<ArenaIndex>) -> ArenaIndex {
        let new_lineage_node = LineageNode {
            name,
            source_obj,
            input
        };
        self.arena_lineage_nodes.allocate(new_lineage_node)
    }

    fn curr_stack(&self) -> Option<&HashMap<String, ArenaIndex>> {
        self.stack.last()
    }
    fn curr_columns_stack(&self) -> Option<&HashMap<String, Vec<ArenaIndex>>> {
        self.columns_stack.last()
    }

    fn push_new_ctx(&mut self, ctx_objects: HashMap<String, ArenaIndex>) {
        let mut new_ctx: HashMap<String, ArenaIndex> = ctx_objects;
        let mut new_columns: HashMap<String, Vec<ArenaIndex>> = HashMap::new();

        for key in new_ctx.keys() {
            let ctx_obj = &self.arena_objects[new_ctx[key]];
            for node_idx in &ctx_obj.lineage_nodes {
                let node = &self.arena_lineage_nodes[*node_idx];
                new_columns
                    .entry(node.name.string().into())
                    .or_default()
                    .push(new_ctx[key]);
            }
        }

        if !self.stack.is_empty() {
            let prev_ctx = self.stack.last().unwrap();
            for key in prev_ctx.keys() {
                let ctx_obj = &self.arena_objects[prev_ctx[key]];
                if !new_ctx.contains_key(key) {
                    new_ctx.insert(ctx_obj.name.to_string(), prev_ctx[key]);
                }
                for node_idx in &ctx_obj.lineage_nodes {
                    let node = &self.arena_lineage_nodes[*node_idx];
                    new_columns
                        .entry(node.name.string().into())
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

    fn get_object(&self, key: &String) -> Option<ArenaIndex> {
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

struct Lineage {
    anon_id: u64,
    context: Context,
}

impl Lineage {
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
    ) -> Vec<(NodeName, ArenaIndex, Vec<ArenaIndex>)> {
        let mut lineage_nodes = vec![];
        for _ in 0..final_stack_size - initial_stack_size {
            let node_idx = self.context.lineage_stack.pop().unwrap();
            let node = &self.context.arena_lineage_nodes[node_idx];
            lineage_nodes.push((node.name.clone(), node.source_obj, node.input.clone()));
        }
        lineage_nodes.reverse();
        lineage_nodes
    }

    fn cte_lin(&mut self, cte: &Cte) -> anyhow::Result<()> {
        match cte {
            Cte::NonRecursive(non_recursive_cte) => {
                let cte_name = non_recursive_cte.name.lexeme(None);

                let start_lineage_len = self.context.lineage_stack.len();
                self.query_expr_lin(&non_recursive_cte.query)?;
                let curr_lineage_len = self.context.lineage_stack.len();

                let lineage_nodes = self.consume_lineage_nodes(
                    start_lineage_len,
                    curr_lineage_len,
                );
                let cte_idx = self.context.allocate_new_ctx_object(
                    &cte_name, ContextObjectKind::Cte,
                    lineage_nodes.into_iter().map(|(names,  _, nodes)| (names,nodes)).collect());
                self.context.add_object(cte_idx);
                self.context.update_output_lineage_with_object_nodes(cte_idx);
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
        if let Some(table) = table {
            if let Some(ctx_table_idx) = self.context.curr_stack().unwrap().get(&table.to_owned()) {
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
        } else if let Some(target_tables) = self.context.curr_columns_stack().unwrap().get(column) {
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
                    "Column {} is ambiguous. It is contained in more than one table: {:?}.",
                    column,
                    target_tables
                ));
            }
            let target_table_idx = target_tables[0];
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
                .collect::<Vec<_>>()[0].1)
        } else {
            return Err(anyhow!("Column {} not found in context.", column));
        }
    }

    fn select_col_expr_expr_lin(&mut self, expr: &Expr) -> anyhow::Result<()> {
        let mut source: Option<String> = None;
        let mut col_name: Option<String> = None;
        match expr {
            Expr::Binary(binary_expr) => {
                let operator = binary_expr.operator.lexeme(None);
                if operator == "." {
                    let left_expr = binary_expr.left.as_ref();
                    let right_expr = binary_expr.right.as_ref();
                    match left_expr {
                        Expr::Literal(literal_expr) => {
                            match literal_expr {
                                LiteralExpr::Identifier(ident) => {
                                    source = Some(ident.clone());
                                }
                                LiteralExpr::QuotedIdentifier(qident) => {
                                    source = Some(qident.clone());
                                }
                                LiteralExpr::String(_)
                                | LiteralExpr::Number(_)
                                | LiteralExpr::Bool(_)
                                | LiteralExpr::Null
                                | LiteralExpr::Star => return Err(anyhow!("Invalid query.")),
                                _ => {
                                    // TODO: struct is valid, e.g. this is valid ( struct(1 as x).x )
                                    return Err(anyhow!("Invalid query."));
                                }
                            }
                        }
                        _ => todo!(),
                    }

                    match right_expr {
                        Expr::Literal(literal_expr) => {
                            match literal_expr {
                                LiteralExpr::Identifier(ident) => {
                                    col_name = Some(ident.clone());
                                }
                                LiteralExpr::QuotedIdentifier(qident) => {
                                    col_name = Some(qident.clone());
                                }
                                LiteralExpr::String(_)
                                | LiteralExpr::Number(_)
                                | LiteralExpr::Bool(_)
                                | LiteralExpr::Null
                                | LiteralExpr::Star => return Err(anyhow!("Invalid query.")),
                                _ => {
                                    // TODO: struct is valid, e.g. this is valid ( struct(1 as x).x )
                                    unreachable!("Invalid query.")
                                }
                            }
                        }
                        _ => todo!(),
                    }

                    let col_source_idx = self.get_column_source(source.as_ref(), col_name.as_ref().unwrap())?;
                    let col_source = &self.context.arena_lineage_nodes[col_source_idx];

                    let new_lineage_node_idx = self.context.allocate_new_lineage_node(NodeName::Defined(col_name.unwrap()), col_source.source_obj, vec![col_source_idx]);

                    self.context.lineage_stack.push(new_lineage_node_idx);
                } else {
                    self.select_col_expr_expr_lin(binary_expr.left.as_ref())?;
                    self.select_col_expr_expr_lin(binary_expr.right.as_ref())?;
                }
            }
            Expr::Unary(unary_expr) => todo!(),
            Expr::Grouping(grouping_expr) => todo!(),
            Expr::Literal(literal_expr) => match literal_expr {
                LiteralExpr::Identifier(ident) | LiteralExpr::QuotedIdentifier(ident) => {
                    col_name = Some(ident.clone());
                    let col_source_idx = self.get_column_source(None, col_name.as_ref().unwrap())?;
                    let col_source = &self.context.arena_lineage_nodes[col_source_idx];

                    let new_lineage_node_idx = self.context.allocate_new_lineage_node(NodeName::Defined(col_name.unwrap()), col_source.source_obj, vec![col_source_idx]);
                    self.context.lineage_stack.push(new_lineage_node_idx);
                }

                LiteralExpr::String(_) => todo!(),
                LiteralExpr::Number(_) => {}
                LiteralExpr::Bool(_) => todo!(),
                LiteralExpr::Null => todo!(),
                LiteralExpr::Star => todo!(),
            },
            Expr::Array(array_expr) => todo!(),
            Expr::Query(query_expr) => self.query_expr_lin(query_expr)?,
        }

        Ok(())
    }

    fn grouping_query_expr_lin(
        &mut self,
        grouping_query_expr: &GroupingQueryExpr,
    ) -> anyhow::Result<()> {
        if let Some(with) = grouping_query_expr.with.as_ref() {
            self.with_lin(with)?;
        }
        self.query_expr_lin(&grouping_query_expr.query_expr)?;
        Ok(())
    }

    fn select_col_expr_col_all(
        &mut self,
        anon_obj_idx: ArenaIndex,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let mut new_lineage_nodes = vec![];
        for (col_name, sources) in self.context.columns_stack.last().unwrap().iter() {
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
                ));
            }
            let col_source_idx = sources.first().unwrap();
            let table = self
                .context
                .curr_stack()
                .unwrap()
                .get(&self.context.arena_objects[*col_source_idx].name)
                .map(|idx| &self.context.arena_objects[*idx])
                .unwrap();

            let col_in_table_idx = table
                .lineage_nodes
                .iter()
                .map(|&n_idx| (&self.context.arena_lineage_nodes[n_idx], n_idx))
                .filter(|(n, _)| n.name.string() == col_name)
                .collect::<Vec<_>>()[0].1;

            new_lineage_nodes.push((NodeName::Defined(col_name.clone()), anon_obj_idx, vec![col_in_table_idx]));
        }
        new_lineage_nodes.into_iter().for_each(|tup| {
            let lineage_node_idx = self.context.allocate_new_lineage_node(tup.0, tup.1, tup.2);
            self.context.lineage_stack.push(lineage_node_idx);
            lineage_nodes.push(lineage_node_idx);
            self.context.arena_objects[anon_obj_idx].lineage_nodes.push(lineage_node_idx);
        });

        Ok(())
    }

    fn select_col_expr_col_lin(
        &mut self,
        anon_obj_idx: ArenaIndex,
        col_expr: &ColExpr,
        lineage_nodes: &mut Vec<ArenaIndex>,
    ) -> anyhow::Result<()> {
        let pending_node_idx = self.context.allocate_new_lineage_node(NodeName::Anonymous, anon_obj_idx, vec![]);
        self.context.arena_objects[anon_obj_idx].lineage_nodes.push(pending_node_idx);

        let start_pending_len = self.context.lineage_stack.len();
        self.select_col_expr_expr_lin(&col_expr.expr)?;
        let curr_pending_len = self.context.lineage_stack.len();

        let mut consumed_nodes = vec![];
        for _ in 0..curr_pending_len - start_pending_len {
            consumed_nodes.push(self.context.lineage_stack.pop().unwrap());
        }
        let first_node_name = consumed_nodes.first().map(|idx|&self.context.arena_lineage_nodes[*idx].name).cloned();

        let pending_node = &mut self.context.arena_lineage_nodes[pending_node_idx];

        pending_node.input.extend(consumed_nodes);

        if let Some(alias) = &col_expr.alias {
            pending_node.name = NodeName::Defined(alias.lexeme(None));
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
        new_table_idx: ArenaIndex
    ) -> anyhow::Result<()> {
        let new_table = &self.context.arena_objects[new_table_idx];
        if from_tables.iter().map(|idx| &self.context.arena_objects[*idx]).any(|obj| obj.name == new_table.name) {
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
            FromExpr::Join(join_expr) => {
                self.join_expr_lineage(join_expr, from_tables, joined_tables)?
            }
            FromExpr::LeftJoin(join_expr) => {
                self.join_expr_lineage(join_expr, from_tables, joined_tables)?
            }
            FromExpr::RightJoin(join_expr) => {
                self.join_expr_lineage(join_expr, from_tables, joined_tables)?
            }
            FromExpr::CrossJoin(cross_join_expr) => {
                self.from_expr_lin(&cross_join_expr.left, from_tables, joined_tables)?;
                self.from_expr_lin(&cross_join_expr.right, from_tables, joined_tables)?;
            }
            FromExpr::Path(from_path_expr) => {
                let table_name = from_path_expr.path_expr.path.lexeme(Some(""));

                // We first check whether it is a context object (cte), otherwise we check for source tables
                let table_like_obj_id = self
                    .context
                    .get_object(&table_name)
                    .filter(|&obj_idx| matches!(self.context.arena_objects[obj_idx].kind, ContextObjectKind::Cte))
                    .map_or(self.context.source_objects.get(&table_name).cloned(), Some);

                if table_like_obj_id.is_none() {
                    return Err(anyhow!(
                        "Table like obj name {} not in context.",
                        table_name
                    ));
                }

                let contains_alias = from_path_expr.alias.is_some();
                let table_like_name = if contains_alias {
                    from_path_expr.alias.as_ref().unwrap().lexeme(None)
                } else {
                    table_name.clone()
                };

                let table_like_obj_id = table_like_obj_id.unwrap();
                let table_like_obj = &self.context.arena_objects[table_like_obj_id];

                let new_table_like_idx = self.context.allocate_new_ctx_object(
                    &table_like_name,
                    table_like_obj.kind,
                    table_like_obj.lineage_nodes.iter().map(|ln| {
                        let ln = &self.context.arena_lineage_nodes[*ln];
                        (ln.name.clone(), ln.input.clone())
                    }).collect()
                );
                self.add_new_from_table(from_tables, new_table_like_idx)?;
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let start_lineage_len = self.context.lineage_stack.len();
                self.query_expr_lin(&from_grouping_query_expr.query_expr)?;
                let curr_lineage_len = self.context.lineage_stack.len();

                let source_name = &from_grouping_query_expr
                    .alias
                    .as_ref()
                    .map(|alias| alias.lexeme(None));

                let lineage_nodes = self.consume_lineage_nodes(start_lineage_len, curr_lineage_len);
                let lineage_nodes_source = lineage_nodes[0].1;

                let new_source_name = if let Some(name) = source_name {
                    name
                } else {
                    &self.context.arena_objects[lineage_nodes_source].name.clone()
                };

                let table_like_idx = self.context.allocate_new_ctx_object(new_source_name, ContextObjectKind::Query, lineage_nodes.into_iter().map(|(names,  _, nodes)| (names,nodes)).collect());
                self.context.update_output_lineage_with_object_nodes(table_like_idx);
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

            let right_join_table = &self.context.arena_objects[*from_tables_split.1.last_mut().unwrap()];
            joined_table_names.push(right_join_table.name.clone());

            for col in using_columns {
                let col_name = col.lexeme(None);
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

                lineage_nodes.push((NodeName::Defined(col.lexeme(None)),vec![left_lineage_node_idx, right_lineage_node_idx] ));


                // lineage_nodes.push(LineageNode {
                //     name: NodeName::Defined(col.lexeme(None)),
                //     source: String::from(""),
                //     input: vec![left_lineage_node, right_lineage_node],
                // });
            }
            let joined_table_name = joined_table_names
                .into_iter()
                .fold(String::from("join"), |acc, name| {
                    format!("{}_{}", acc, name)
                });

            let table_like_idx = self.context.allocate_new_ctx_object(&joined_table_name, ContextObjectKind::UsingTable, lineage_nodes);
            self.context.update_output_lineage_with_object_nodes(table_like_idx);
            joined_tables.push(table_like_idx);
        }

        Ok(())
    }
    fn select_query_expr_lin(&mut self, select_query_expr: &SelectQueryExpr) -> anyhow::Result<()> {
        let ctx_objects_start_size = self.context.objects_stack.len();

        if let Some(with) = select_query_expr.with.as_ref() {
            self.with_lin(with)?;
        }

        let mut pushed_context: bool = false;
        if let Some(from) = select_query_expr.select.from.as_ref() {
            let mut from_tables: Vec<ArenaIndex> = Vec::new();
            let mut joined_tables: Vec<ArenaIndex> = Vec::new();

            for expr in &from.exprs {
                self.from_expr_lin(expr, &mut from_tables, &mut joined_tables)?;
            }
            let mut ctx_objects = from_tables
                .into_iter()
                .map(|idx| (self.context.arena_objects[idx].name.clone(), idx))
                .collect::<HashMap<String, ArenaIndex>>();
            ctx_objects.extend(joined_tables.into_iter().map(|idx| (self.context.arena_objects[idx].name.clone(), idx)));
            self.context.push_new_ctx(ctx_objects);
            pushed_context = true;
        }

        let anon_id = format!("anon_{}", self.get_anon_id());
        let anon_obj_idx = self.context.allocate_new_ctx_object(&anon_id, ContextObjectKind::Anonymous, vec![]);
        let mut lineage_nodes = vec![];
        for expr in &select_query_expr.select.exprs {
            match expr {
                SelectColExpr::Col(col_expr) => {
                    self.select_col_expr_col_lin(anon_obj_idx, col_expr, &mut lineage_nodes)?
                }
                SelectColExpr::All => self.select_col_expr_col_all(anon_obj_idx, &mut lineage_nodes)?,
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
}

// TODO: add line in errors

pub fn compute_lineage(query: &QueryExpr) -> anyhow::Result<()> {
    // TODO: we should read this from a config file, the user must provide it in some way
    let mut ctx = Context::default();

    let tmp_idx = ctx.allocate_new_ctx_object("tmp", ContextObjectKind::Cte, vec![(NodeName::Defined("z".to_owned()), vec![])]);
    let d_idx = ctx.allocate_new_ctx_object("D", ContextObjectKind::Cte, vec![(NodeName::Defined("z".to_owned()), vec![])]);
    ctx.source_objects = HashMap::from(
        [
            (String::from("tmp"), tmp_idx),
            (String::from("D"), d_idx)
        ]
    );
    let mut lineage = Lineage {
        anon_id: 0,
        context: ctx,
    };
    lineage.query_expr_lin(query)?;

    println!("Final lineage:");
    // TODO: we should place this code in the Lineage class, save all the lineage in one place
    let output_lineage_nodes = lineage.context.output.clone();
    for pending_node in output_lineage_nodes{
        pretty_print_lineage_node(pending_node, &lineage.context);
        // println!("{:?}", pending_node);
    }
    // TODO: pop all the remaining contexts and return the final lineage
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage() {
        // TODO:
    }
}
