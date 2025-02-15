use anyhow::{anyhow, Ok};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use crate::{
    parser::{
        ColExpr, Cte, Expr, FromExpr, GroupingQueryExpr, JoinCondition, JoinExpr, LiteralExpr, QueryExpr, SelectColExpr, SelectQueryExpr, With
    },
    scanner::TokenLiteral,
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

#[derive(Clone)]
struct LineageNode {
    name: NodeName,
    source: String,
    input: Vec<LineageNode>,
}

impl Debug for LineageNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LineageNode({}->{}, input={:?}))",
            self.source,
            self.name.string(),
            self.input
        )
    }
}

impl Display for LineageNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let in_str = self
            .input
            .iter()
            .map(|node| format!("{}->{}", node.source, node.name))
            .fold((0, String::from("")), |acc, el| {
                if acc.0 == 0 {
                    (acc.0 + 1, el.to_string())
                } else {
                    (acc.0 + 1, format!("{}, {}", acc.1, el))
                }
            })
            .1;
        write!(f, "{}->{} <-[{}]", self.source, self.name, in_str)
    }
}

#[derive(Debug, Clone)]
pub struct ContextObject {
    name: String,
    lineage_nodes: Vec<LineageNode>,
    kind: ContextObjectKind,
}

#[derive(Debug, Clone, Copy)]
pub enum ContextObjectKind {
    Table,
    Cte,
    Query,
    UsingTable,
}

#[derive(Debug, Clone)]
struct Context {
    source_objects: HashMap<String, ContextObject>,
    objects_stack: Vec<ContextObject>,
    stack: Vec<HashMap<String, ContextObject>>,
    columns_stack: Vec<HashMap<String, Vec<String>>>,
    lineage_stack: Vec<LineageNode>,
    output: Vec<LineageNode>,
}

impl Context {
    fn curr_stack(&self) -> Option<&HashMap<String, ContextObject>> {
        self.stack.last()
    }
    fn curr_columns_stack(&self) -> Option<&HashMap<String, Vec<String>>> {
        self.columns_stack.last()
    }

    fn push_new_ctx(&mut self, ctx_objects: HashMap<String, ContextObject>) {
        let mut new_ctx: HashMap<String, ContextObject> = ctx_objects;
        let mut new_columns: HashMap<String, Vec<String>> = HashMap::new();

        for key in new_ctx.keys() {
            let ctx_obj = &new_ctx[key];
            for node in &ctx_obj.lineage_nodes {
                new_columns
                    .entry(node.name.string().into())
                    .or_default()
                    .push(ctx_obj.name.to_owned());
            }
        }

        if !self.stack.is_empty() {
            let prev_ctx = self.stack.last().unwrap();
            for key in prev_ctx.keys() {
                let ctx_obj = &prev_ctx[key];
                if !new_ctx.contains_key(key) {
                    new_ctx.insert(ctx_obj.name.to_string(), ctx_obj.clone());
                }
                for node in &ctx_obj.lineage_nodes {
                    new_columns
                        .entry(node.name.string().into())
                        .or_default()
                        .push(ctx_obj.name.to_owned());
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

    fn get_object(&self, key: &String) -> Option<&ContextObject> {
        for i in (0..self.objects_stack.len()).rev() {
            if self.objects_stack[i].name == *key {
                return Some(&self.objects_stack[i]);
            }
        }
        None
    }

    fn add_object(&mut self, object: ContextObject) {
        self.objects_stack.push(object);
    }

    fn pop_object(&mut self) {
        self.objects_stack.pop();
    }

    fn update_output_lineage_from_nodes(&mut self, new_lineage_nodes: &[LineageNode]) {
        self.output.extend_from_slice(new_lineage_nodes);
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

    fn consume_lineage_nodes(
        &mut self,
        initial_stack_size: usize,
        final_stack_size: usize,
        new_source: &Option<String>,
    ) -> Vec<LineageNode> {
        let mut lineage_nodes = vec![];
        for _ in 0..final_stack_size - initial_stack_size {
            let mut node = self.context.lineage_stack.pop().unwrap();
            if let Some(new_source) = new_source {
                node.source = new_source.clone();
            }
            lineage_nodes.push(node);
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
                    &Some(cte_name.clone()),
                );
                let cte_ctx = ContextObject {
                    name: cte_name.clone(),
                    lineage_nodes,
                    kind: ContextObjectKind::Cte,
                };
                self.context
                    .update_output_lineage_from_nodes(&cte_ctx.lineage_nodes);
                self.context.add_object(cte_ctx);
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
    ) -> anyhow::Result<LineageNode> {
        if let Some(table) = table {
            if let Some(ctx_table) = self.context.curr_stack().unwrap().get(&table.to_owned()) {
                let col_in_schema = ctx_table
                    .lineage_nodes
                    .iter()
                    .find(|n| n.name.string() == column);
                if let Some(col) = col_in_schema {
                    return Ok(col.clone());
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
                        .get(el)
                        .is_some_and(|x| matches!(x.kind, ContextObjectKind::UsingTable))
                })
            {
                return Err(anyhow!(
                    "Column {} is ambiguous. It is contained in more than one table: {:?}.",
                    column,
                    target_tables
                ));
            }
            let target_table = target_tables[0].clone();
            let ctx_table = self
                .context
                .curr_stack()
                .unwrap()
                .get(&target_table)
                .unwrap();
            return Ok(ctx_table
                .lineage_nodes
                .iter()
                .filter(|n| n.name.string() == column)
                .collect::<Vec<_>>()[0]
                .clone());
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

                    let col_source =
                        self.get_column_source(source.as_ref(), col_name.as_ref().unwrap())?;
                    self.context.lineage_stack.push(LineageNode {
                        name: NodeName::Defined(col_name.unwrap()),
                        source: col_source.source.clone(),
                        input: vec![col_source.clone()],
                    });
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
                    let col_source = self.get_column_source(None, col_name.as_ref().unwrap())?;
                    self.context.lineage_stack.push(LineageNode {
                        name: NodeName::Defined(col_name.unwrap()),
                        source: col_source.source.clone(),
                        input: vec![col_source.clone()],
                    });
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
        anon_id: &str,
        lineage_nodes: &mut Vec<LineageNode>,
    ) -> anyhow::Result<()> {
        for (col_name, sources) in self.context.columns_stack.last().unwrap().iter() {
            if sources.len() > 1
                && !sources.iter().any(|el| {
                    self.context
                        .curr_stack()
                        .unwrap()
                        .get(el)
                        .is_some_and(|x| matches!(x.kind, ContextObjectKind::UsingTable))
                })
            {
                return Err(anyhow!(
                    "Column {} is ambiguous. It is contained in more than one table: {:?}.",
                    col_name,
                    sources
                ));
            }
            let col_source = sources.first().unwrap();
            let table = self
                .context
                .curr_stack()
                .unwrap()
                .get(&col_source.to_owned())
                .unwrap();
            let col_in_table = table
                .lineage_nodes
                .iter()
                .filter(|n| n.name.string() == col_name)
                .collect::<Vec<_>>()[0];
            let lineage_node = LineageNode {
                name: NodeName::Defined(col_name.clone()),
                source: anon_id.to_owned(),
                input: vec![col_in_table.clone()],
            };
            self.context.lineage_stack.push(lineage_node.clone());
            lineage_nodes.push(lineage_node);
        }
        Ok(())
    }

    fn select_col_expr_col_lin(
        &mut self,
        anon_id: &str,
        col_expr: &ColExpr,
        lineage_nodes: &mut Vec<LineageNode>,
    ) -> anyhow::Result<()> {
        let mut pending_node = LineageNode {
            name: NodeName::Anonymous,
            source: anon_id.to_owned(),
            input: vec![],
        };
        let start_pending_len = self.context.lineage_stack.len();

        self.select_col_expr_expr_lin(&col_expr.expr)?;
        if let Some(alias) = &col_expr.alias {
            pending_node.name = NodeName::Defined(alias.lexeme(None));
        }

        let curr_pending_len = self.context.lineage_stack.len();
        for _ in 0..curr_pending_len - start_pending_len {
            pending_node
                .input
                .push(self.context.lineage_stack.pop().unwrap());
        }

        if pending_node.input.len() == 1 {
            if let NodeName::Anonymous = pending_node.name {
                pending_node.name = pending_node.input[0].name.clone()
            }
        }

        self.context.lineage_stack.push(pending_node.clone());
        lineage_nodes.push(pending_node);
        Ok(())
    }

    fn add_new_from_table(
        &self,
        from_tables: &mut Vec<ContextObject>,
        new_table: ContextObject,
    ) -> anyhow::Result<()> {
        if from_tables.iter().any(|obj| obj.name == new_table.name) {
            return Err(anyhow!(
                "Found duplicate table object in from with name {}",
                new_table.name
            ));
        }
        from_tables.push(new_table.clone());
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_expr_lin(
        &mut self,
        from_expr: &FromExpr,
        from_tables: &mut Vec<ContextObject>,
        joined_tables: &mut Vec<ContextObject>,
    ) -> anyhow::Result<()> {
        match from_expr {
            FromExpr::Join(join_expr) => self.join_expr_lineage(join_expr, from_tables, joined_tables)?,
            FromExpr::LeftJoin(join_expr) => self.join_expr_lineage(join_expr, from_tables, joined_tables)?,
            FromExpr::RightJoin(join_expr) => self.join_expr_lineage(join_expr, from_tables, joined_tables)?,
            FromExpr::CrossJoin(cross_join_expr) => {
                self.from_expr_lin(&cross_join_expr.left, from_tables, joined_tables)?;
                self.from_expr_lin(&cross_join_expr.right, from_tables, joined_tables)?;
            },
            FromExpr::Path(from_path_expr) => {
                let table_name = from_path_expr.path_expr.path.lexeme(Some(""));

                // We first check whether it is a context object (cte), otherwise we check for source tables
                let table_like_obj = self
                    .context
                    .get_object(&table_name)
                    .filter(|obj| matches!(obj.kind, ContextObjectKind::Cte))
                    .map_or(self.context.source_objects.get(&table_name), Some);
                if table_like_obj.is_none() {
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

                let table_like_obj = table_like_obj.unwrap();
                let table_like = ContextObject {
                    name: table_like_name,
                    lineage_nodes: table_like_obj.lineage_nodes.clone(),
                    kind: table_like_obj.kind,
                };
                self.add_new_from_table(from_tables, table_like)?;
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let start_lineage_len = self.context.lineage_stack.len();
                self.query_expr_lin(&from_grouping_query_expr.query_expr)?;
                let curr_lineage_len = self.context.lineage_stack.len();
                let source_name = &from_grouping_query_expr
                    .alias
                    .as_ref()
                    .map(|alias| alias.lexeme(None));

                let lineage_nodes =
                    self.consume_lineage_nodes(start_lineage_len, curr_lineage_len, source_name);
                let table_like = ContextObject {
                    name: lineage_nodes[0].source.clone(),
                    lineage_nodes,
                    kind: ContextObjectKind::Query,
                };

                self.context
                    .update_output_lineage_from_nodes(&table_like.lineage_nodes);

                self.add_new_from_table(from_tables, table_like)?;
            }
            FromExpr::GroupingFrom(grouping_from_expr) => self.from_expr_lin(&grouping_from_expr.query_expr, from_tables, joined_tables)?,
        }
        Ok(())
    }

    fn join_expr_lineage(&mut self, join_expr: &JoinExpr, from_tables: &mut Vec<ContextObject>, joined_tables: &mut Vec<ContextObject>) -> anyhow::Result<()> {
        self.from_expr_lin(&join_expr.left, from_tables, joined_tables)?;
        self.from_expr_lin(&join_expr.right, from_tables, joined_tables)?;
        if let JoinCondition::Using(using_columns) = &join_expr.cond {
            let mut joined_table_names: Vec<String> = vec![];
            let mut lineage_nodes = vec![];
            let from_tables_len = from_tables.len();

            let from_tables_split = from_tables.split_at_mut(from_tables_len - 1);
            let left_join_table: &mut ContextObject = if !joined_tables.is_empty() {
                // We have already joined two tables
                joined_tables.last_mut().unwrap()
            } else {
                // This is the first join, which corresponds to index -2 in the original from_tables
                from_tables_split.0.last_mut().unwrap()
            };
            joined_table_names.push(left_join_table.name.clone());

            let right_join_table = &mut from_tables_split.1.last_mut().unwrap();
            joined_table_names.push(right_join_table.name.clone());

            for col in using_columns {
                let col_name = col.lexeme(None);
                let left_lineage_node = left_join_table
                    .lineage_nodes
                    .iter()
                    .find(|n| n.name.string() == col_name)
                    .ok_or(anyhow!(
                        "Cannot find column {:?} in table {:?}.",
                        col_name,
                        left_join_table.name
                    ))?
                    .clone();
                let right_lineage_node = right_join_table
                    .lineage_nodes
                    .iter()
                    .find(|n| n.name.string() == col_name)
                    .ok_or(anyhow!(
                        "Cannot find column {:?} in table {:?}.",
                        col_name,
                        right_join_table.name
                    ))?
                    .clone();
                lineage_nodes.push(LineageNode {
                    name: NodeName::Defined(col.lexeme(None)),
                    source: String::from(""),
                    input: vec![left_lineage_node, right_lineage_node],
                });
            }
            let joined_table_name = joined_table_names
                .into_iter()
                .fold(String::from("join"), |acc, name| {
                    format!("{}_{}", acc, name)
                });
            let table_like = ContextObject {
                name: joined_table_name.clone(),
                lineage_nodes: lineage_nodes
                    .into_iter()
                    .map(|n| LineageNode {
                        name: n.name,
                        source: joined_table_name.clone(),
                        input: n.input,
                    })
                    .collect(),
                kind: ContextObjectKind::UsingTable,
            };
            self.context
                .update_output_lineage_from_nodes(&table_like.lineage_nodes);
            joined_tables.push(table_like);
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
            let mut from_tables: Vec<ContextObject> = Vec::new();
            let mut joined_tables: Vec<ContextObject> = Vec::new();

            for expr in &from.exprs {
                self.from_expr_lin(expr, &mut from_tables, &mut joined_tables)?;
            }
            let mut ctx_objects = from_tables
                .into_iter()
                .map(|obj| (obj.name.clone(), obj))
                .collect::<HashMap<String, ContextObject>>();
            ctx_objects.extend(joined_tables.into_iter().map(|v| (v.name.clone(), v)));
            self.context.push_new_ctx(ctx_objects);
            pushed_context = true;
        }

        let anon_id = format!("anon_{}", self.get_anon_id());

        let mut lineage_nodes = vec![];
        for expr in &select_query_expr.select.exprs {
            match expr {
                SelectColExpr::Col(col_expr) => {
                    self.select_col_expr_col_lin(&anon_id, col_expr, &mut lineage_nodes)?
                }
                SelectColExpr::All => self.select_col_expr_col_all(&anon_id, &mut lineage_nodes)?,
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
    let ctx = Context {
        columns_stack: vec![],
        stack: vec![],
        objects_stack: vec![],
        output: vec![],
        source_objects: HashMap::from([
            (
                String::from("tmp"),
                ContextObject {
                    kind: ContextObjectKind::Table,
                    name: String::from("tmp"),
                    lineage_nodes: vec![LineageNode {
                        name: NodeName::Defined(String::from("z")),
                        source: String::from("tmp"),
                        input: vec![],
                    }],
                },
            ),
            (
                String::from("D"),
                ContextObject {
                    kind: ContextObjectKind::Table,
                    name: String::from("D"),
                    lineage_nodes: vec![LineageNode {
                        name: NodeName::Defined(String::from("z")),
                        source: String::from("D"),
                        input: vec![],
                    }],
                },
            ),
        ]),
        lineage_stack: vec![],
    };
    let mut lineage = Lineage {
        anon_id: 0,
        context: ctx,
    };
    lineage.query_expr_lin(query)?;

    println!("Final lineage:");
    // TODO: we should place this code in the Lineage class, save all the lineage in one place
    for pending_node in &lineage.context.output {
        println!("{}", pending_node);
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
