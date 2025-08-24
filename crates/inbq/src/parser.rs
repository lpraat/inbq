use anyhow::anyhow;
use strum::IntoDiscriminant;

use crate::ast::{
    ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, CaseExpr,
    CastFunctionExpr, ColumnSchema, ConcatFunctionExpr, CreateTableStatement, CrossJoinExpr, Cte,
    CurrentDateFunctionExpr, DeclareVarStatement, DeleteStatement, DropTableStatement, Expr,
    FrameBound, From, FromExpr, FromGroupingQueryExpr, FromPathExpr, FunctionAggregate,
    FunctionAggregateHaving, FunctionAggregateHavingKind, FunctionAggregateNulls,
    FunctionAggregateOrderBy, FunctionExpr, GenericFunctionExpr, GenericFunctionExprArg, GroupBy,
    GroupByExpr, GroupingExpr, GroupingFromExpr, GroupingQueryExpr, Having, InsertStatement,
    IntervalExpr, IntervalPart, JoinCondition, JoinExpr, JoinKind, Limit, Merge, MergeInsert,
    MergeSource, MergeStatement, MergeUpdate, NamedWindow, NamedWindowExpr, NonRecursiveCte,
    OrderBy, OrderByExpr, OrderByNulls, OrderBySortDirection, ParameterizedType, ParseToken,
    PathExpr, Qualify, QueryExpr, QueryStatement, RangeExpr, RecursiveCte, SafeCastFunctionExpr,
    Select, SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr, SelectQueryExpr,
    SelectTableValue, SetQueryOperator, SetSelectQueryExpr, SetVarStatement, Statement,
    StatementsBlock, StructExpr, StructField, StructFieldType, StructParameterizedFieldType, Token,
    TokenType, TokenTypeVariant, TruncateStatement, Type, UnaryExpr, UnnestExpr, UpdateItem,
    UpdateStatement, When, WhenMatched, WhenNotMatchedBySource, WhenNotMatchedByTarget, Where,
    Window, WindowFrame, WindowFrameKind, WindowOrderByExpr, WindowSpec, With,
};
use crate::scanner::Scanner;
pub struct Parser<'a> {
    source_tokens: &'a Vec<Token>,
    curr: usize,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: &'a Vec<Token>) -> Parser<'a> {
        Self {
            source_tokens: tokens,
            curr: 0,
        }
    }

    pub fn parse(&mut self) -> anyhow::Result<Ast> {
        self.parse_query()
    }

    fn peek_prev(&self) -> &Token {
        &self.source_tokens[self.curr - 1]
    }

    fn peek(&self) -> &Token {
        &self.source_tokens[self.curr]
    }

    fn peek_next_i(&self, i: usize) -> &Token {
        if self.curr + i >= self.source_tokens.len() {
            self.source_tokens.last().unwrap() // Eof
        } else {
            &self.source_tokens[self.curr + i]
        }
    }

    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            // Do not advance if we peek Eof
            self.curr += 1;
        }
        self.peek_prev()
    }

    fn is_at_end(&self) -> bool {
        self.peek().kind == TokenType::Eof
    }

    fn check_token_type(&self, token_type: TokenTypeVariant) -> bool {
        self.peek().kind.discriminant() == token_type
    }

    fn match_token_type(&mut self, token_type: TokenTypeVariant) -> bool {
        if self.check_token_type(token_type) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn match_token_types(&mut self, token_types: &[TokenTypeVariant]) -> bool {
        for tok in token_types {
            if self.check_token_type(*tok) {
                self.advance();
                return true;
            }
        }
        false
    }

    fn check_non_reserved_keyword(&self, value: &str) -> bool {
        let peek = self.peek();
        match &peek.kind {
            TokenType::Identifier(ident) => ident.to_lowercase() == value,
            _ => false,
        }
    }

    fn check_identifier(&mut self) -> bool {
        self.check_token_type(TokenTypeVariant::Identifier)
            || self.check_token_type(TokenTypeVariant::QuotedIdentifier)
    }

    fn match_non_reserved_keyword(&mut self, value: &str) -> bool {
        if self.check_non_reserved_keyword(value) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn consume_non_reserved_keyword(&mut self, value: &str) -> anyhow::Result<&Token> {
        if self.check_non_reserved_keyword(value) {
            Ok(self.advance())
        } else {
            let err_msg = format!("Expected `{}`.", value.to_uppercase());
            Err(anyhow!(self.error(self.peek(), &err_msg)))
        }
    }

    fn consume_one_of_non_reserved_keywords(&mut self, values: &[&str]) -> anyhow::Result<&Token> {
        for value in values {
            if self.check_non_reserved_keyword(value) {
                return Ok(self.advance());
            }
        }
        let err_msg = values
            .iter()
            .map(|el| format!("`{}`", el.to_uppercase()))
            .collect::<Vec<String>>()
            .join(" or ");
        Err(anyhow!(self.error(
            self.peek(),
            &format!("Expected one of: {}.", err_msg)
        )))
    }

    fn consume(&mut self, token_type: TokenTypeVariant) -> anyhow::Result<&Token> {
        if self.check_token_type(token_type) {
            Ok(self.advance())
        } else {
            let err_msg = format!("Expected `{}`.", token_type.variant_str());
            Err(anyhow!(self.error(self.peek(), &err_msg)))
        }
    }

    fn match_identifier(&mut self) -> bool {
        self.match_token_types(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])
    }

    fn consume_identifier(&mut self) -> anyhow::Result<&Token> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])
    }

    fn consume_one_of(&mut self, token_types: &[TokenTypeVariant]) -> anyhow::Result<&Token> {
        for token_type in token_types {
            if self.check_token_type(*token_type) {
                return Ok(self.advance());
            }
        }
        let err_msg = token_types
            .iter()
            .map(|el| format!("`{}`", el.variant_str()))
            .collect::<Vec<String>>()
            .join(" or ");
        Err(anyhow!(self.error(
            self.peek(),
            &format!("Expected one of: {}.", err_msg)
        )))
    }

    fn error(&self, token: &Token, message: &str) -> String {
        format!(
            "[line {}, col {}] Error {}: {}",
            token.line,
            token.col,
            &format!("at '{}'", token.lexeme),
            message
        )
    }

    // query -> statement (; statement [";"])*
    fn parse_query(&mut self) -> anyhow::Result<Ast> {
        let mut statements = vec![];

        if self.check_token_type(TokenTypeVariant::Eof) {
            // Empty SQL
            return Ok(Ast { statements });
        }

        loop {
            if self.check_token_type(TokenTypeVariant::Eof) {
                break;
            }

            statements.push(self.parse_statement()?);

            if !self.match_token_type(TokenTypeVariant::Semicolon) {
                break;
            }
        }

        self.consume(TokenTypeVariant::Eof)?;
        Ok(Ast { statements })
    }

    fn parse_statement(&mut self) -> anyhow::Result<Statement> {
        let peek = self.peek();

        let statement = match &peek.kind {
            TokenType::Create => self.parse_create_table_statement()?,
            TokenType::Merge => self.parse_merge_statement()?,
            TokenType::Set => self.parse_set_var_statement()?,
            TokenType::Identifier(non_reserved_keyword) => {
                match non_reserved_keyword.to_lowercase().as_str() {
                    "insert" => self.parse_insert_statement()?,
                    "delete" => self.parse_delete_statement()?,
                    "update" => self.parse_update_statement()?,
                    "truncate" => self.parse_truncate_statement()?,
                    "declare" => self.parse_declare_var_statement()?,
                    "begin" => self.parse_statements_block()?,
                    "drop" => self.parse_drop_statement()?,
                    _ => {
                        return Err(anyhow!(self.error(
                            peek,
                            &format!(
                                "Unexpected non reserved keyword: `{}`.",
                                non_reserved_keyword
                            ),
                        )));
                    }
                }
            }
            _ => self.parse_query_statement()?,
        };
        Ok(statement)
    }

    fn parse_drop_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("drop")?;

        let peek = self.peek();
        let statement = match &peek.kind {
            TokenType::Identifier(non_reserved_keyword) => {
                match non_reserved_keyword.to_lowercase().as_str() {
                    "table" => self.parse_drop_table_statement()?,
                    _ => {
                        return Err(anyhow!(self.error(
                            peek,
                            &format!(
                                "Unexpected non reserved keyword while parsing drop statement: `{}`.",
                                non_reserved_keyword
                            ),
                        )));
                    }
                }
            }
            _ => return Err(anyhow!("Unexpected token.")),
        };
        Ok(statement)
    }

    // drop_table_statement -> "DROP" "TABLE" ["IF" "EXISTS"] table_name
    fn parse_drop_table_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("table")?;

        let if_exists = if self.match_token_type(TokenTypeVariant::If) {
            self.consume(TokenTypeVariant::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_path()?.expr;

        Ok(Statement::DropTableStatement(DropTableStatement {
            name,
            if_exists,
        }))
    }

    // statements_block -> "BEGIN" [statement (";" statement)*] ["EXCEPTION" "WHEN" "ERROR" "THEN"] [statement (";" statement)*] "END"
    fn parse_statements_block(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("begin")?;

        let mut statements = vec![];
        let mut exception_statements: Option<Vec<Statement>> = None;
        let mut curr_vec = &mut statements;
        loop {
            if self.match_token_type(TokenTypeVariant::End) {
                break;
            }
            if self.match_non_reserved_keyword("exception") {
                self.consume(TokenTypeVariant::When)?;
                self.consume_non_reserved_keyword("error")?;
                self.consume(TokenTypeVariant::Then)?;
                exception_statements = Some(vec![]);
                curr_vec = exception_statements.as_mut().unwrap();
                continue;
            }

            curr_vec.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
        }

        Ok(Statement::Block(StatementsBlock {
            statements,
            exception_statements,
        }))
    }

    // TODO: add collate, partition, etc...
    // CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] TABLE [ IF NOT EXISTS ] table_name
    // ["(" column_name parameterized_bq_type ("," column_name parameterized_bq_Type)* ")"]
    // ["AS" query_statement]
    fn parse_create_table_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Create)?;
        let replace = self.match_token_type(TokenTypeVariant::Or);
        if replace {
            self.consume_non_reserved_keyword("replace")?;
        }

        let is_temporary =
            self.match_non_reserved_keyword("temp") || self.match_non_reserved_keyword("temporary");
        self.consume_non_reserved_keyword("table")?;

        let if_not_exists = self.match_token_type(TokenTypeVariant::If);
        if if_not_exists {
            self.consume(TokenTypeVariant::Not)?;
            self.consume(TokenTypeVariant::Exists)?;
        }

        let name = self.parse_path()?.expr;

        let schema = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut column_schema = vec![];
            loop {
                let col_name = self.consume_identifier()?.clone();
                let col_type = self.parse_parameterized_bq_type()?;
                column_schema.push(ColumnSchema {
                    name: ParseToken::Single(col_name),
                    r#type: col_type,
                });

                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Some(column_schema)
        } else {
            None
        };

        let query = if self.match_token_type(TokenTypeVariant::As) {
            Some(self.parse_query_expr()?)
        } else {
            None
        };

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            schema,
            replace,
            is_temporary,
            if_not_exists,
            query,
        }))
    }

    // query_statement -> query_expr
    fn parse_query_statement(&mut self) -> anyhow::Result<Statement> {
        let query_expr = self.parse_query_expr()?;
        Ok(Statement::Query(QueryStatement { query: query_expr }))
    }

    // declare_var_statement -> "DECLARE" var_name ("," var_name)* [bq_parameterized_type] ["DEFAULT" expr]
    fn parse_declare_var_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("declare")?;

        let mut var_names = vec![];
        loop {
            let var_name = self.consume_identifier()?;
            var_names.push(ParseToken::Single(var_name.clone()));

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        let (r#type, default) = if self.match_token_type(TokenTypeVariant::Default) {
            // type is inferred from default expr
            (None, Some(self.parse_expr()?))
        } else {
            let r#type = self.parse_parameterized_bq_type()?;
            let default = if self.match_token_type(TokenTypeVariant::Default) {
                Some(self.parse_expr()?)
            } else {
                None
            };
            (Some(r#type), default)
        };

        Ok(Statement::DeclareVar(DeclareVarStatement {
            var_names,
            r#type,
            default,
        }))
    }

    // set_var_statement -> "SET" (var_name = expr | "(" var_name ("," var_name)* ")" = "(" expr ("," expr)* ")"
    fn parse_set_var_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Set)?;

        let mut var_names: Vec<ParseToken>;
        let mut exprs: Vec<Expr>;
        if self.match_token_type(TokenTypeVariant::LeftParen) {
            // multiple vars
            var_names = vec![];
            loop {
                let var_name = self.consume_identifier()?;
                var_names.push(ParseToken::Single(var_name.clone()));

                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            self.consume(TokenTypeVariant::Equal)?;

            exprs = vec![];
            if self.check_token_type(TokenTypeVariant::LeftParen)
                && self.peek_next_i(1).kind == TokenType::Select
            {
                // Single subquery
                exprs.push(self.parse_expr()?);
            } else {
                self.consume(TokenTypeVariant::LeftParen)?;
                loop {
                    let expr = self.parse_expr()?;
                    exprs.push(expr.clone());
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                self.consume(TokenTypeVariant::RightParen)?;
            }
        } else {
            var_names = vec![ParseToken::Single(self.consume_identifier()?.clone())];
            self.consume(TokenTypeVariant::Equal)?;
            exprs = vec![self.parse_expr()?];
        }

        Ok(Statement::SetVar(SetVarStatement { var_names, exprs }))
    }

    // insert_statement -> "INSERT" ["INTO"] path ["(" column_name ("," column_name)* ")"] input
    // where:
    // input -> query_expr | "VALUES" "(" expr ")" ("(" expr ")")*
    // column_name -> "Identifier" | "QuotedIdentifier"
    fn parse_insert_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("insert")?;
        self.match_token_type(TokenTypeVariant::Into);
        let table = self.parse_path()?.expr;
        let columns = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut columns = vec![];
            loop {
                let column_name = self.consume_identifier()?;
                columns.push(ParseToken::Single(column_name.clone()));
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Some(columns)
        } else {
            None
        };

        let values = if self.match_non_reserved_keyword("values") {
            let mut values = vec![];
            loop {
                self.consume(TokenTypeVariant::LeftParen)?;

                loop {
                    let expr = self.parse_expr()?;
                    values.push(expr);
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                self.consume(TokenTypeVariant::RightParen)?;

                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(values)
        } else {
            None
        };

        let query_expr = if values.is_none() {
            Some(self.parse_query_expr()?)
        } else {
            None
        };

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
            query: query_expr,
        }))
    }

    // delete_statement -> "DELETE" ["FROM"] path ["AS"] [alias] "WHERE" expr
    fn parse_delete_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("delete")?;
        self.match_token_type(TokenTypeVariant::From);
        let table = self.parse_path()?.expr;
        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Where)?;
        let cond = self.parse_expr()?;
        Ok(Statement::Delete(DeleteStatement { table, alias, cond }))
    }

    // update statement -> "UPDATE" path ["AS"] [alias] SET set_clause ["FROM" from_expr] "WHERE" expr
    // where:
    // set_clause = ("Identifier" | "QuotedIdentifier") = expr ("," ("Identifier" | "QuotedIdentifier") = expr)*
    fn parse_update_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("update")?;
        let table = self.parse_path()?.expr;
        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_path()?.expr;
            self.consume(TokenTypeVariant::Equal)?;
            let expr = self.parse_expr()?;
            update_items.push(UpdateItem {
                column: column_path,
                expr,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        let from = if self.match_token_type(TokenTypeVariant::From) {
            Some(From {
                expr: Box::new(self.parse_from_expr()?),
            })
        } else {
            None
        };

        self.consume(TokenTypeVariant::Where)?;
        let where_expr = self.parse_where_expr()?;

        Ok(Statement::Update(UpdateStatement {
            table,
            alias,
            update_items,
            from,
            r#where: Where {
                expr: Box::new(where_expr),
            },
        }))
    }

    // truncate_statement -> "TRUNCATE" "TABLE" path
    fn parse_truncate_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("truncate")?;
        self.consume_non_reserved_keyword("table")?;
        let table = self.parse_path()?.expr;
        Ok(Statement::Truncate(TruncateStatement { table }))
    }

    // merge_statement -> "MERGE" ["INTO"] path ["AS"] [alias] "USING" path "ON" merge_condition (when_clause)+
    // where:
    // when_clause -> matched_clause | not_matched_by_target_clause | not_matched_by_source_clause
    // matched_clause -> "WHEN" "MATCHED" ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    // not_matched_by_target_clause -> "WHEN" "NOT" "MATCHED" ["BY" "TARGET"] ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    fn parse_merge_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Merge)?;
        self.match_token_type(TokenTypeVariant::Into);
        let target_table = self.parse_path()?.expr;
        let target_alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Using)?;
        let source = if self.check_token_type(TokenTypeVariant::LeftParen) {
            MergeSource::Subquery(self.parse_query_expr()?)
        } else {
            MergeSource::Table(self.parse_path()?.expr)
        };
        let source_alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::On)?;
        let condition = self.parse_expr()?;

        self.consume(TokenTypeVariant::When)?;

        let mut whens = vec![];
        loop {
            let when = if self.match_token_type(TokenTypeVariant::Not) {
                self.consume_non_reserved_keyword("matched")?;

                let matched_by = self.match_token_type(TokenTypeVariant::By);
                if !matched_by || self.match_non_reserved_keyword("target") {
                    // not_matched_by_target_clause
                    let search_condition = self.parse_merge_search_condition()?;
                    self.consume(TokenTypeVariant::Then)?;
                    let merge_insert = self.parse_merge_insert()?;
                    When::NotMatchedByTarget(WhenNotMatchedByTarget {
                        search_condition,
                        merge: merge_insert,
                    })
                } else {
                    // not_matched_by_source_clause
                    self.consume_non_reserved_keyword("source")?;
                    let search_condition = self.parse_merge_search_condition()?;
                    self.consume(TokenTypeVariant::Then)?;
                    if self.match_non_reserved_keyword("delete") {
                        When::NotMatchedBySource(WhenNotMatchedBySource {
                            search_condition,
                            merge: Merge::Delete,
                        })
                    } else {
                        When::NotMatchedBySource(WhenNotMatchedBySource {
                            search_condition,
                            merge: self.parse_merge_update()?,
                        })
                    }
                }
            } else {
                // matched_clause
                self.consume_non_reserved_keyword("matched")?;
                let search_condition = self.parse_merge_search_condition()?;
                self.consume(TokenTypeVariant::Then)?;
                let merge = if self.match_non_reserved_keyword("delete") {
                    Merge::Delete
                } else {
                    self.parse_merge_update()?
                };
                When::Matched(WhenMatched {
                    search_condition,
                    merge,
                })
            };

            whens.push(when);

            if !self.match_token_type(TokenTypeVariant::When) {
                break;
            }
        }

        Ok(Statement::Merge(Box::new(MergeStatement {
            target_table,
            target_alias,
            source,
            source_alias,
            condition,
            whens,
        })))
    }

    // merge_update -> "UPDATE" "SET" update_item ("," update_item)*
    // where:
    // update_item -> ("Identifier" | "QuotedIdentifier") "=" expr
    fn parse_merge_update(&mut self) -> anyhow::Result<Merge> {
        self.consume_non_reserved_keyword("update")?;
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_path()?.expr;
            self.consume(TokenTypeVariant::Equal)?;
            let expr = self.parse_expr()?;
            update_items.push(UpdateItem {
                column: column_path,
                expr,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        Ok(Merge::Update(MergeUpdate { update_items }))
    }

    // merge_insert -> "INSERT" "ROW" | "INSERT" [(" column ("," column)*] ")"] "VALUES" "(" expr ("," expr)* ")"
    // where:
    // columns -> "Identifier" | "QuotedIdentifier"
    fn parse_merge_insert(&mut self) -> anyhow::Result<Merge> {
        self.consume_non_reserved_keyword("insert")?;
        if self.match_non_reserved_keyword("row") {
            return Ok(Merge::InsertRow);
        }

        let columns = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut columns = vec![];
            loop {
                let column_name = self.consume_identifier()?;
                columns.push(ParseToken::Single(column_name.clone()));
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Some(columns)
        } else {
            None
        };

        self.consume_non_reserved_keyword("values")?;
        let mut values = vec![];
        loop {
            self.consume(TokenTypeVariant::LeftParen)?;

            loop {
                let expr = self.parse_expr()?;
                values.push(expr);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        Ok(Merge::Insert(MergeInsert { columns, values }))
    }

    // merge_search_condition -> ["AND" expr]
    fn parse_merge_search_condition(&mut self) -> anyhow::Result<Option<Expr>> {
        let expr = if self.match_token_type(TokenTypeVariant::And) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(expr)
    }

    // query_expr ->
    // ["WITH" with_expr] select | "(" query_expr ")"
    // select_query_expr (("UNION" [("ALL" | "Distinct")] | "Intersect" "Distinct" | "Except" "Distinct") select_query_expr)*
    // ["ORDER BY" order_by_expr]
    // ["LIMIT" limit_expr]
    fn parse_query_expr(&mut self) -> anyhow::Result<QueryExpr> {
        let with = if self.match_token_type(TokenTypeVariant::With) {
            Some(self.parse_with_expr()?)
        } else {
            None
        };
        let mut output: QueryExpr = self.parse_select_query_expr()?;

        loop {
            let peek_token = self.peek();
            match peek_token.kind {
                TokenType::Union => {
                    self.advance();
                    let token =
                        self.consume_one_of(&[TokenTypeVariant::All, TokenTypeVariant::Distinct])?;
                    let set_operator = match &token.kind {
                        TokenType::All => SetQueryOperator::Union,
                        TokenType::Distinct => SetQueryOperator::UnionDistinct,
                        _ => unreachable!(),
                    };
                    let right_query_expr = self.parse_select_query_expr()?;
                    output = QueryExpr::SetSelect(SetSelectQueryExpr {
                        with: None,
                        left_query: Box::new(output),
                        set_operator,
                        right_query: Box::new(right_query_expr),
                        order_by: None,
                        limit: None,
                    })
                }
                TokenType::Intersect | TokenType::Except => {
                    let set_operator = match &peek_token.kind {
                        TokenType::Intersect => SetQueryOperator::IntersectDistinct,
                        TokenType::Except => SetQueryOperator::ExceptDistinct,
                        _ => unreachable!(),
                    };
                    self.advance();
                    self.consume(TokenTypeVariant::Distinct)?;
                    let right_query_expr = self.parse_select_query_expr()?;
                    output = QueryExpr::SetSelect(SetSelectQueryExpr {
                        with: None,
                        left_query: Box::new(output),
                        set_operator,
                        right_query: Box::new(right_query_expr),
                        order_by: None,
                        limit: None,
                    })
                }
                _ => {
                    break;
                }
            };
        }

        let order_by = if self.match_token_type(TokenTypeVariant::Order) {
            self.consume(TokenTypeVariant::By)?;
            Some(OrderBy {
                exprs: self.parse_order_by_expr()?,
            })
        } else {
            None
        };

        let limit = if self.match_token_type(TokenTypeVariant::Limit) {
            let tok = self.consume(TokenTypeVariant::Number)?;
            let count = match &tok.kind {
                TokenType::Number(num) => Expr::Number(num.clone()),
                _ => unreachable!(),
            };

            let offset = if self.match_non_reserved_keyword("offset") {
                let tok = self.consume(TokenTypeVariant::Number)?;
                match &tok.kind {
                    TokenType::Number(num) => Some(Box::new(Expr::Number(num.clone()))),
                    _ => unreachable!(),
                }
            } else {
                None
            };

            Some(Limit {
                count: Box::new(count),
                offset,
            })
        } else {
            None
        };

        match output {
            QueryExpr::Grouping(ref mut grouping_query_expr) => {
                grouping_query_expr.with = with;
                grouping_query_expr.order_by = order_by;
                grouping_query_expr.limit = limit;
            }
            QueryExpr::Select(ref mut select_query_expr) => {
                select_query_expr.with = with;
                select_query_expr.order_by = order_by;
                select_query_expr.limit = limit;
            }
            QueryExpr::SetSelect(ref mut set_select_query_expr) => {
                set_select_query_expr.with = with;
                set_select_query_expr.order_by = order_by;
            }
        }

        Ok(output)
    }

    // select_query_expr -> select | "(" query_expr ")"
    fn parse_select_query_expr(&mut self) -> anyhow::Result<QueryExpr> {
        if self.match_token_type(TokenTypeVariant::LeftParen) {
            let query_expr = self.parse_query_expr()?;
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(QueryExpr::Grouping(GroupingQueryExpr {
                with: None,
                order_by: None,
                query: Box::new(query_expr),
                limit: None,
            }))
        } else {
            let select = self.parse_select()?;
            Ok(QueryExpr::Select(SelectQueryExpr {
                with: None,
                order_by: None,
                select,
                limit: None,
            }))
        }
    }

    // with_expr -> ["RECURSIVE"] (recursive_cte | non_recursive_cte) ("," (recursive_cte | non_recursive_cte))*
    // where:
    // non_recursive_cte -> ("Identifier" | "QuotedIdentifier") AS "(" query_expr ")"
    // recursive_cte -> ("Identifier" | "QuotedIdentifier") AS "(" query_expr "UNION" "ALL" query_expr ")"
    fn parse_with_expr(&mut self) -> anyhow::Result<With> {
        self.match_token_type(TokenTypeVariant::Recursive);
        let mut ctes = vec![];
        loop {
            let cte_name = self.consume_identifier()?.clone();
            self.consume(TokenTypeVariant::As)?;
            self.consume(TokenTypeVariant::LeftParen)?;
            ctes.push(self.parse_cte(&cte_name)?);

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        Ok(With { ctes })
    }

    fn parse_cte(&mut self, name: &Token) -> anyhow::Result<Cte> {
        let cte_query = self.parse_query_expr()?;
        if self.match_token_type(TokenTypeVariant::Union) {
            self.consume(TokenTypeVariant::All)?;
            let recursive_query = self.parse_query_expr()?;
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(Cte::Recursive(RecursiveCte {
                name: ParseToken::Single(name.clone()),
                base_query: cte_query,
                recursive_query,
            }))
        } else {
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(Cte::NonRecursive(NonRecursiveCte {
                name: ParseToken::Single(name.clone()),
                query: cte_query,
            }))
        }
    }

    // order_by_expr -> order_by_expr_item [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")] ("," order_by_expr_item [("NULLS" "FIRST" | "NULLS" "LAST")])*
    fn parse_order_by_expr(&mut self) -> anyhow::Result<Vec<OrderByExpr>> {
        let mut order_by_exprs = vec![];

        loop {
            let expr = self.parse_expr()?;

            let asc_desc = if self.match_token_type(TokenTypeVariant::Asc) {
                Some(OrderBySortDirection::Asc)
            } else if self.match_token_type(TokenTypeVariant::Desc) {
                Some(OrderBySortDirection::Desc)
            } else {
                None
            };

            let nulls = if self.match_token_type(TokenTypeVariant::Nulls) {
                let tok = self.consume_one_of_non_reserved_keywords(&["first", "last"])?;
                match &tok.kind {
                    TokenType::Identifier(s) if s.to_lowercase() == "first" => {
                        Some(OrderByNulls::First)
                    }
                    TokenType::Identifier(s) if s.to_lowercase() == "last" => {
                        Some(OrderByNulls::Last)
                    }
                    _ => unreachable!(),
                }
            } else {
                None
            };

            order_by_exprs.push(OrderByExpr {
                expr,
                sort_direction: asc_desc,
                nulls,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        Ok(order_by_exprs)
    }

    // select ->
    // "SELECT"
    // [("ALL" | "DISTINCT")]
    // ["AS" ("STRUCT" | "VALUE")]
    // select_col_expr [","] (select_col_expr [","])*
    // ["FROM" from_expr]
    // ["WHERE" where_expr]
    // ["GROUP BY" group_by_expr]
    // ["HAVING" having_expr]
    // ["QUALIFY" qualify_expr]
    // ["WINDOW" window]
    fn parse_select(&mut self) -> anyhow::Result<Select> {
        self.consume(TokenTypeVariant::Select)?;

        let distinct = self.match_token_type(TokenTypeVariant::Distinct);
        self.match_token_type(TokenTypeVariant::All);
        let table_value = if self.match_token_type(TokenTypeVariant::As) {
            if self.match_token_type(TokenTypeVariant::Struct) {
                Some(SelectTableValue::Struct)
            } else if self.match_non_reserved_keyword("value") {
                Some(SelectTableValue::Value)
            } else {
                return Err(anyhow!(
                    self.error(self.peek(), "Expected one of: `VALUE` or `STRUCT`.")
                ));
            }
        } else {
            None
        };

        let mut select_exprs = vec![];
        let col_expr = self.parse_select_expr()?;
        select_exprs.push(col_expr);

        let mut comma_matched = self.match_token_type(TokenTypeVariant::Comma);
        let mut last_position = self.curr - (comma_matched as usize);

        loop {
            // NOTE: this is needed to handle the trailing comma, we need to look ahead
            if self.check_token_type(TokenTypeVariant::Eof)
                || self.check_token_type(TokenTypeVariant::Semicolon)
                || self.check_token_type(TokenTypeVariant::From)
                || self.check_token_type(TokenTypeVariant::RightParen)
                || self.check_token_type(TokenTypeVariant::Union)
                || self.check_token_type(TokenTypeVariant::Intersect)
                || self.check_token_type(TokenTypeVariant::Except)
            {
                break;
            }

            if self.check_token_type(TokenTypeVariant::Select) {
                return Err(anyhow!(self.error(self.peek(), "Expected `;`.")));
            }

            match self.parse_select_expr() {
                Ok(col_expr) => {
                    if self.source_tokens[last_position].kind != TokenType::Comma {
                        return Err(anyhow!(self.error(self.peek_prev(), "Expected `,`.")));
                    }
                    select_exprs.push(col_expr);
                    comma_matched = self.match_token_type(TokenTypeVariant::Comma);
                    last_position = self.curr - (comma_matched as usize);
                }
                Err(_) => {
                    return Err(anyhow!(self.error(self.peek(), "Expected expression.")));
                }
            }
        }
        let from = if self.match_token_type(TokenTypeVariant::From) {
            Some(crate::parser::From {
                expr: Box::new(self.parse_from_expr()?),
            })
        } else {
            None
        };

        let r#where = if self.match_token_type(TokenTypeVariant::Where) {
            Some(crate::parser::Where {
                expr: Box::new(self.parse_where_expr()?),
            })
        } else {
            None
        };

        let group_by = if self.match_token_type(TokenTypeVariant::Group) {
            self.consume(TokenTypeVariant::By)?;
            Some(GroupBy {
                expr: self.parse_group_by_expr()?,
            })
        } else {
            None
        };

        let having = if self.match_token_type(TokenTypeVariant::Having) {
            Some(Having {
                expr: Box::new(self.parse_having_expr()?),
            })
        } else {
            None
        };

        let qualify = if self.match_token_type(TokenTypeVariant::Qualify) {
            Some(Qualify {
                expr: Box::new(self.parse_qualify_expr()?),
            })
        } else {
            None
        };

        let window = if self.check_token_type(TokenTypeVariant::Window) {
            Some(self.parse_window()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            table_value,
            exprs: select_exprs,
            from,
            r#where,
            group_by,
            having,
            qualify,
            window,
        })
    }

    // TODO: add replace
    // select_expr -> [expr.]"*" [except] | expr [["AS"] "Identifier"]
    fn parse_select_expr(&mut self) -> anyhow::Result<SelectExpr> {
        if self.match_token_type(TokenTypeVariant::Star) {
            let except = self.parse_except()?;
            return Ok(SelectExpr::All(SelectAllExpr { except }));
        }

        let expr = match self.parse_expr() {
            Err(_) => {
                return Err(anyhow!(self.error(self.peek(), "Expected Expression.")));
            }
            Ok(expr) => expr,
        };

        if self.peek_prev().kind == TokenType::Star {
            let except = self.parse_except()?;
            return Ok(SelectExpr::ColAll(SelectColAllExpr { expr, except }));
        }

        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        Ok(SelectExpr::Col(SelectColExpr { expr, alias }))
    }

    // except -> "EXCEPT" "(" ("Identifier" | "QuotedIdentifier") ["," ("Identifier" | "QuotedIdentifier")]* ")"
    fn parse_except(&mut self) -> anyhow::Result<Option<Vec<ParseToken>>> {
        if !self.match_token_type(TokenTypeVariant::Except) {
            return Ok(None);
        }

        self.consume(TokenTypeVariant::LeftParen)?;

        let mut except_columns = vec![];
        let column = self.consume_identifier()?;
        except_columns.push(ParseToken::Single(column.clone()));
        while self.match_token_type(TokenTypeVariant::Comma) {
            except_columns.push(ParseToken::Single(self.consume_identifier()?.clone()));
        }
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Some(except_columns))
    }

    // from_expr -> from_item_expr (cross_join_op from_item_expr | cond_join_op from_item_expr cond)*
    // where:
    // cross_join_op -> "CROSS" "JOIN" | ","
    // cond_join_op -> (["INNER"] "JOIN" | "FULL" ["OUTER"] "JOIN" | "LEFT" ["OUTER"] "JOIN" | "RIGHT" ["OUTER"] "JOIN")
    // cond -> ("ON" expr | "USING" "(" ("Identifier" | "QuotedIdentifier") ("," ("Identifier" | "QuotedIdentifier"))*) ")")
    fn parse_from_expr(&mut self) -> anyhow::Result<FromExpr> {
        let expr = self.parse_from_item_expr()?;
        let mut output: FromExpr = expr;

        loop {
            let curr_peek = self.peek();
            match curr_peek.kind {
                TokenType::Inner | TokenType::Join => {
                    self.match_token_type(TokenTypeVariant::Inner);
                    self.consume(TokenTypeVariant::Join)?;
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::Join(JoinExpr {
                        kind: JoinKind::Inner,
                        left: Box::new(output),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Left => {
                    self.advance();
                    self.match_token_type(TokenTypeVariant::Outer);
                    self.consume(TokenTypeVariant::Join)?;
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::LeftJoin(JoinExpr {
                        kind: JoinKind::Left,
                        left: Box::new(output),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Right => {
                    self.advance();
                    self.match_token_type(TokenTypeVariant::Outer);
                    self.consume(TokenTypeVariant::Join)?;
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::FullJoin(JoinExpr {
                        kind: JoinKind::Full,
                        left: Box::new(output),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Full => {
                    self.advance();
                    self.match_token_type(TokenTypeVariant::Outer);
                    self.consume(TokenTypeVariant::Join)?;
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::RightJoin(JoinExpr {
                        kind: JoinKind::Right,
                        left: Box::new(output),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Cross => {
                    self.advance();
                    self.consume(TokenTypeVariant::Join)?;
                    let right = self.parse_from_item_expr()?;
                    output = FromExpr::CrossJoin(CrossJoinExpr {
                        left: Box::new(output),
                        right: Box::new(right),
                    })
                }
                TokenType::Comma => {
                    self.advance();
                    let right = self.parse_from_item_expr()?;
                    output = FromExpr::CrossJoin(CrossJoinExpr {
                        left: Box::new(output),
                        right: Box::new(right),
                    })
                }
                _ => {
                    break;
                }
            }
        }
        Ok(output)
    }

    fn parse_cond(&mut self) -> anyhow::Result<JoinCondition> {
        if self.match_token_type(TokenTypeVariant::On) {
            let bool_expr = self.parse_expr()?;
            Ok(JoinCondition::On(bool_expr))
        } else if self.match_token_type(TokenTypeVariant::Using) {
            let mut using_tokens = vec![];
            self.consume(TokenTypeVariant::LeftParen)?;
            let ident = self.consume_identifier()?.clone();
            using_tokens.push(ident);
            while self.match_token_type(TokenTypeVariant::Comma) {
                let ident = self.consume_identifier()?.clone();
                using_tokens.push(ident);
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(JoinCondition::Using(
                using_tokens.into_iter().map(ParseToken::Single).collect(),
            ))
        } else {
            return Err(anyhow!(
                self.error(self.peek(), "Expected `ON` or `USING`.")
            ));
        }
    }

    // from_item_expr -> path [as_alias] | "(" query_expr ")" [as_alias] | "(" from_expr ")" | unnest_operator
    fn parse_from_item_expr(&mut self) -> anyhow::Result<FromExpr> {
        if self.match_token_type(TokenTypeVariant::LeftParen) {
            let curr = self.curr;
            // lookahead to check whether we can parse a query expr
            while self.peek().kind == TokenType::LeftParen {
                self.curr += 1;
            }
            let lookahead = self.peek();
            if lookahead.kind == TokenType::Select || lookahead.kind == TokenType::With {
                self.curr = curr;
                let query_expr = self.parse_query_expr()?;
                self.consume(TokenTypeVariant::RightParen)?;
                let alias = self.parse_as_alias()?;
                Ok(FromExpr::GroupingQuery(FromGroupingQueryExpr {
                    query: Box::new(query_expr),
                    alias: alias.map(|tok| ParseToken::Single(tok.clone())),
                }))
            } else {
                self.curr = curr;
                let parse_from_expr = self.parse_from_expr()?;
                match parse_from_expr {
                    FromExpr::Join(_)
                    | FromExpr::LeftJoin(_)
                    | FromExpr::RightJoin(_)
                    | FromExpr::FullJoin(_) => {
                        // Only these from expressions can be parenthesized
                        self.consume(TokenTypeVariant::RightParen)?;
                        Ok(FromExpr::GroupingFrom(GroupingFromExpr {
                            query: Box::new(parse_from_expr),
                        }))
                    }
                    _ => Err(anyhow!(self.error(self.peek(), "Expected `JOIN`."))),
                }
            }
        } else if self.check_token_type(TokenTypeVariant::Unnest) {
            let unnest_expr = self.parse_unnest()?;
            Ok(FromExpr::Unnest(unnest_expr))
        } else {
            let path = self.parse_path()?;
            let alias = self.parse_as_alias()?;
            Ok(FromExpr::Path(FromPathExpr {
                path,
                alias: alias.map(|tok| ParseToken::Single(tok.clone())),
            }))
        }
    }

    // unnest -> ("UNNEST" "(" expr ")" [as_alias] | array_path [as alias]) ["WITH" "OFFSET" [as_alias]]
    // where:
    // array_path -> expr
    fn parse_unnest(&mut self) -> anyhow::Result<UnnestExpr> {
        let array = if self.match_token_type(TokenTypeVariant::Unnest) {
            self.consume(TokenTypeVariant::LeftParen)?;
            let array = self.parse_expr()?;
            self.consume(TokenTypeVariant::RightParen)?;
            array
        } else {
            self.parse_expr()?
        };
        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        let has_offset = self.match_token_type(TokenTypeVariant::With);
        let offset_alias = if has_offset {
            self.consume_non_reserved_keyword("offset")?;
            self.parse_as_alias()?
                .map(|tok| ParseToken::Single(tok.clone()))
        } else {
            None
        };

        Ok(UnnestExpr {
            array: Box::new(array),
            alias,
            with_offset: has_offset,
            offset_alias,
        })
    }

    // as_alias -> ["AS"] ("Identifier" | "QuotedIdentifier")
    fn parse_as_alias(&mut self) -> anyhow::Result<Option<&Token>> {
        if self.match_token_type(TokenTypeVariant::As) {
            return Ok(Some(self.consume_identifier()?));
        }
        if self.match_identifier() {
            return Ok(Some(self.peek_prev()));
        }
        Ok(None)
    }

    // path -> path_expr ("." path_expr)*
    fn parse_path(&mut self) -> anyhow::Result<PathExpr> {
        let mut path_identifiers = vec![];
        let identifier = self.parse_path_expression()?;
        path_identifiers.extend(identifier);
        while self.match_token_type(TokenTypeVariant::Dot) {
            path_identifiers.push(self.peek_prev().clone());
            let identifier = self.parse_path_expression()?;
            path_identifiers.extend(identifier);
        }
        Ok(PathExpr {
            expr: ParseToken::Multiple(path_identifiers),
        })
    }

    // path_expr -> first_part (("/" | ":" | "-") subsequent_part)*
    // where:
    // first_part -> ("QuotedIdentifier" | "Identifier")
    // subsequent_part -> ("QuotedIdentifier" | "Identifier" | "Number")
    fn parse_path_expression(&mut self) -> anyhow::Result<Vec<Token>> {
        let mut path_expression_parts = vec![];
        path_expression_parts.push(self.consume_identifier()?.clone());

        while self.match_token_types(&[
            TokenTypeVariant::Slash,
            TokenTypeVariant::Colon,
            TokenTypeVariant::Minus,
        ]) {
            path_expression_parts.push(self.peek_prev().clone());
            path_expression_parts.push(
                self.consume_one_of(&[
                    TokenTypeVariant::QuotedIdentifier,
                    TokenTypeVariant::Identifier,
                    TokenTypeVariant::Number,
                ])?
                .clone(),
            );
        }

        Ok(path_expression_parts)
    }

    // where_expr -> expr
    fn parse_where_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    // group_by_expr -> "ALL" | group_by_items
    // where:
    // group_by_items -> expr ("," expr)*
    // TODO: other group by expressions
    fn parse_group_by_expr(&mut self) -> anyhow::Result<GroupByExpr> {
        if self.match_token_type(TokenTypeVariant::All) {
            Ok(GroupByExpr::All)
        } else {
            let mut items = vec![self.parse_expr()?];
            while self.match_token_type(TokenTypeVariant::Comma) {
                items.push(self.parse_expr()?);
            }
            Ok(GroupByExpr::Items(items))
        }
    }

    // having_expr -> expr
    fn parse_having_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    // qualify_expr -> expr
    fn parse_qualify_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    // frame_bound -> ("UNBOUNDED" "PRECEDING" | "UNBOUNDED" "FOLLOWING" | "Number" "PRECEDING" | "Number" "FOLLOWING" | "CURRENT" "ROW")
    fn parse_frame_bound(&mut self) -> anyhow::Result<Option<FrameBound>> {
        let frame_bound = if self.match_non_reserved_keyword("unbounded") {
            let tok =
                self.consume_one_of(&[TokenTypeVariant::Preceding, TokenTypeVariant::Following])?;
            match &tok.kind {
                TokenType::Preceding => Some(FrameBound::UnboundedPreceding),
                TokenType::Following => Some(FrameBound::UnboundedFollowing),
                _ => unreachable!(),
            }
        } else if self.match_token_type(TokenTypeVariant::Current) {
            self.consume_non_reserved_keyword("row")?;
            Some(FrameBound::CurrentRow)
        } else if self.match_token_type(TokenTypeVariant::Number) {
            match self.peek_prev().clone().kind {
                TokenType::Number(num) => {
                    let tok = self.consume_one_of(&[
                        TokenTypeVariant::Preceding,
                        TokenTypeVariant::Following,
                    ])?;
                    match &tok.kind {
                        TokenType::Preceding => Some(FrameBound::Preceding(num)),
                        TokenType::Following => Some(FrameBound::Following(num)),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        } else {
            None
        };
        Ok(frame_bound)
    }

    // window_frame -> ("ROWS" | "RANGE") (frame_start | frame_between)
    // where:
    // frame_start -> frame_bound
    // frame_between -> "BETWEEN" frame_bound "AND" frame_bound
    fn parse_window_frame(&mut self) -> anyhow::Result<WindowFrame> {
        let tok = self.consume_one_of(&[TokenTypeVariant::Rows, TokenTypeVariant::Range])?;
        let kind = match &tok.kind {
            TokenType::Rows => WindowFrameKind::Rows,
            TokenType::Range => WindowFrameKind::Range,
            _ => unreachable!(),
        };

        let (start, end) = if self.match_token_type(TokenTypeVariant::Between) {
            // We first try to parse a frame bound, then we return an error if it's not a valid frame bound given the context
            let start = self
                .parse_frame_bound()?
                .ok_or(anyhow!("Expected one of: `UNBOUNDED`, `Number`, `CURRENT`"))?;
            if let FrameBound::UnboundedFollowing = start {
                return Err(anyhow!("Expected `PRECEDING`."));
            };
            self.consume(TokenTypeVariant::And)?;

            let end = self.parse_frame_bound()?.ok_or_else(|| match start {
                FrameBound::UnboundedPreceding
                | FrameBound::Preceding(_)
                | FrameBound::CurrentRow => {
                    anyhow!("Expected one of: `UNBOUNDED`, `Number`, `CURRENT`.")
                }
                FrameBound::Following(_) => anyhow!("Expected one of: `UNBOUNDED`, `Number`."),
                _ => unreachable!(),
            })?;
            (Some(start), Some(end))
        } else {
            let start = self.parse_frame_bound()?;
            (start, None)
        };

        Ok(WindowFrame { kind, start, end })
    }

    // named_window_expr -> ((Identifier" | "QuotedIdentifier") |  [("Identifier" | "QuotedIdentifier")] [partition_by] [order_by] [frame])
    // where:
    // partition_by -> "PARTITION" "BY" expr ("," expr)*
    // order_by -> "ORDER" "BY" expr [("ASC" | "DESC")] ("," expr [("ASC" | "DESC")?])*
    // frame -> window_frame
    fn parse_named_window_expr(&mut self) -> anyhow::Result<NamedWindowExpr> {
        if !self.match_token_type(TokenTypeVariant::LeftParen) {
            let name = ParseToken::Single(self.consume_identifier()?.clone());
            return Ok(NamedWindowExpr::Reference(name));
        }
        let ref_window = if self.check_identifier() {
            Some(ParseToken::Single(self.consume_identifier()?.clone()))
        } else {
            None
        };
        let partition_by = if self.match_token_type(TokenTypeVariant::Partition) {
            self.consume(TokenTypeVariant::By)?;
            let mut partition_by_exprs = vec![];
            loop {
                let expr = self.parse_expr()?;
                partition_by_exprs.push(expr);

                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(partition_by_exprs)
        } else {
            None
        };

        let order_by = if self.match_token_type(TokenTypeVariant::Order) {
            self.consume(TokenTypeVariant::By)?;
            let mut order_by_exprs = vec![];
            loop {
                let expr = self.parse_expr()?;

                let asc_desc = if self.match_token_type(TokenTypeVariant::Asc) {
                    Some(OrderBySortDirection::Asc)
                } else if self.match_token_type(TokenTypeVariant::Desc) {
                    Some(OrderBySortDirection::Desc)
                } else {
                    None
                };

                order_by_exprs.push(WindowOrderByExpr { expr, asc_desc });

                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(order_by_exprs)
        } else {
            None
        };

        let frame = if self.check_token_type(TokenTypeVariant::Range)
            || self.check_token_type(TokenTypeVariant::Rows)
        {
            Some(self.parse_window_frame()?)
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;

        Ok(NamedWindowExpr::WindowSpec(WindowSpec {
            window_name: ref_window,
            partition_by,
            order_by,
            frame,
        }))
    }

    // window -> "WINDOW" ("Identifier" | "QuotedIdentifier") "AS" named_window_expr ("," ("Identifier" | "QuotedIdentifier") "AS" named_window_expr)*
    fn parse_window(&mut self) -> anyhow::Result<Window> {
        self.consume(TokenTypeVariant::Window)?;
        let mut named_windows = vec![];
        loop {
            let name = ParseToken::Single(self.consume_identifier()?.clone());
            self.consume(TokenTypeVariant::As)?;
            let named_window_expr = self.parse_named_window_expr()?;
            named_windows.push(NamedWindow {
                name,
                window: named_window_expr,
            });
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        Ok(Window { named_windows })
    }

    // expr -> or_expr
    fn parse_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_or_expr()
    }

    /// Util function to parse a standard binary rule expression of kind
    ///
    /// `parse_rule -> parse_rule | next_parsing_rule ("T1" | "T2" | ... next_parsing_rule)*`
    fn parse_standard_binary_expr(
        &mut self,
        token_types_to_match: &[TokenTypeVariant],
        next_parsing_rule_fn: impl Fn(&mut Self) -> anyhow::Result<Expr>,
    ) -> anyhow::Result<Expr> {
        let mut output = next_parsing_rule_fn(self)?;

        while self.match_token_types(token_types_to_match) {
            let operator = self.peek_prev().clone();
            let right = next_parsing_rule_fn(self)?;
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: ParseToken::Single(operator),
                right: Box::new(right),
            });
        }

        Ok(output)
    }

    // or_expr -> and_expr ("OR" and_expr)*
    fn parse_or_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenTypeVariant::Or], Self::parse_and_expr)
    }

    // and_expr -> not_expr ("AND" not_expr)*
    fn parse_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenTypeVariant::And], Self::parse_not_expr)
    }

    // not_expr -> "NOT" not_expr | comparison_expr
    fn parse_not_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_type(TokenTypeVariant::Not) {
            let operator = self.peek_prev().clone();
            return Ok(Expr::Unary(UnaryExpr {
                operator: ParseToken::Single(operator),
                right: Box::new(self.parse_not_expr()?),
            }));
        }
        self.parse_comparison_expr()
    }

    // comparison_expr ->
    // bitwise_or_expr
    // | bitwise_or_expr (("=" | ">" | "<" | ">=" | "<=", | "!=", | "<>") bitwise_or_expr)*
    // | bitwise_or_expr "Is" ["Not"] ("True" | "False" | "Null")
    // | bitwise_or_expr (["Not"] ("In" | "Between") bitwise_or_expr)*
    // | bitwise_or_expr (["Not"] "Like") biwtise_or_expr)*
    fn parse_comparison_expr(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_bitwise_or_expr()?;

        loop {
            let curr_token = self.peek().clone();
            match curr_token.kind {
                TokenType::Equal
                | TokenType::Greater
                | TokenType::Less
                | TokenType::GreaterEqual
                | TokenType::LessEqual
                | TokenType::BangEqual
                | TokenType::NotEqual
                | TokenType::Like
                | TokenType::In
                | TokenType::Between => {
                    let operator = curr_token;
                    self.advance();
                    let right = self.parse_bitwise_or_expr()?;
                    output = Expr::Binary(BinaryExpr {
                        left: Box::new(output),
                        operator: ParseToken::Single(operator),
                        right: Box::new(right),
                    })
                }
                TokenType::Is => {
                    let mut parse_tokens = vec![curr_token];
                    self.advance();
                    if self.match_token_type(TokenTypeVariant::Not) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    let right_literal = self
                        .consume_one_of(&[
                            TokenTypeVariant::Null,
                            TokenTypeVariant::True,
                            TokenTypeVariant::False,
                        ])?
                        .clone();
                    output = Expr::Binary(BinaryExpr {
                        left: Box::new(output),
                        operator: ParseToken::Multiple(parse_tokens),
                        right: match right_literal.kind {
                            TokenType::True => Box::new(Expr::Bool(true)),
                            TokenType::False => {
                                self.advance();
                                Box::new(Expr::Bool(false))
                            }
                            TokenType::Null => {
                                self.advance();
                                Box::new(Expr::Null)
                            }
                            _ => {
                                unreachable!()
                            }
                        },
                    })
                }
                TokenType::Not => {
                    let mut parse_tokens = vec![curr_token];
                    self.advance();
                    parse_tokens.push(
                        self.consume_one_of(&[
                            TokenTypeVariant::In,
                            TokenTypeVariant::Between,
                            TokenTypeVariant::Like,
                        ])?
                        .clone(),
                    );
                    let right = self.parse_bitwise_or_expr()?;
                    output = Expr::Binary(BinaryExpr {
                        left: Box::new(output),
                        operator: ParseToken::Multiple(parse_tokens),
                        right: Box::new(right),
                    })
                }
                _ => {
                    break;
                }
            }
        }
        Ok(output)
    }

    // bitwise_or_expr -> primary_expr | primary_expr ("|" primary_expr)*
    fn parse_bitwise_or_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::BitwiseOr],
            Self::parse_bitwise_and_expr,
        )
    }

    // bitwise_and_expr -> bitwise_shift_expr | bitwise_shift_expr ("&" bitwise_shift_expr)*
    fn parse_bitwise_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::BitwiseAnd],
            Self::parse_bitwise_shift_expr,
        )
    }

    // bitwise_shift_expr -> add_expr | add_expr (("<<" | ">>") add_expr)*
    fn parse_bitwise_shift_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[
                TokenTypeVariant::BitwiseRightShift,
                TokenTypeVariant::BitwiseLeftShift,
            ],
            Self::parse_add_expr,
        )
    }

    // add_expr -> mul_concat_expr | mul_concat_expr (("+" | "-") mul_concat_expr)*
    fn parse_add_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::Plus, TokenTypeVariant::Minus],
            Self::parse_mul_concat_expr,
        )
    }

    // mul_concat_expr -> unary_expr | unary_expr (("*" | "/" | "||") unary_expr)*
    fn parse_mul_concat_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[
                TokenTypeVariant::Star,
                TokenTypeVariant::Slash,
                TokenTypeVariant::ConcatOperator,
            ],
            Self::parse_unary_expr,
        )
    }

    // unary_expr -> ("+" | "-" | "~") unary_expr | field_access_expr
    fn parse_unary_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_types(&[
            TokenTypeVariant::Plus,
            TokenTypeVariant::Minus,
            TokenTypeVariant::BitwiseNot,
        ]) {
            let operator = self.peek_prev().clone();
            return Ok(Expr::Unary(UnaryExpr {
                operator: ParseToken::Single(operator),
                right: Box::new(self.parse_unary_expr()?),
            }));
        }
        self.parse_field_access_expr()
    }

    // field_access_expr -> array_subscript_operator | array_subscript_operator ("." array_subscript_operator )* ["." "*"]
    fn parse_field_access_expr(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_array_subscript_operator()?;

        while self.match_token_type(TokenTypeVariant::Dot) {
            let operator = self.peek_prev().clone();
            if self.match_token_type(TokenTypeVariant::Star) {
                return Ok(Expr::Binary(BinaryExpr {
                    left: Box::new(output),
                    operator: ParseToken::Single(operator),
                    right: Box::new(Expr::Star),
                }));
            }
            let right = self.parse_array_subscript_operator()?;
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: ParseToken::Single(operator),
                right: Box::new(right),
            });
        }
        Ok(output)
    }

    // array_subscript_operator -> primary_expr | primary_expr ("[" expr "]")*
    fn parse_array_subscript_operator(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_primary_expr()?;

        while self.match_token_type(TokenTypeVariant::LeftSquare) {
            let left_paren = self.peek_prev().clone();
            let index = self.parse_expr()?;
            let right_paren = self.consume(TokenTypeVariant::RightSquare)?.clone();
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: ParseToken::Multiple(vec![left_paren, right_paren]),
                right: Box::new(index),
            });
        }
        Ok(output)
    }

    // array_expr -> "ARRAY" ([array_type] "[" expr ("," expr)* "]" | "(" query_expr ")")
    fn parse_array_expr(&mut self) -> anyhow::Result<Expr> {
        let mut array_type: Option<Type> = None;
        if self.match_token_type(TokenTypeVariant::Array)
            && self.check_token_type(TokenTypeVariant::Less)
        {
            array_type = Some(self.parse_array_type()?);
        }
        self.consume(TokenTypeVariant::LeftSquare)?;
        let mut array_elements = vec![];
        array_elements.push(self.parse_expr()?);
        while self.match_token_type(TokenTypeVariant::Comma) {
            array_elements.push(self.parse_expr()?);
        }
        self.consume(TokenTypeVariant::RightSquare)?;
        Ok(Expr::Array(ArrayExpr {
            exprs: array_elements,
            r#type: array_type,
        }))
    }

    // struct_expr -> "STRUCT" [struct_type] "(" expr ["AS" field_name]] ("," expr ["AS" field_name])* ")"
    // where:
    // field_name -> "Identifier" | "QuotedIdentifier"
    fn parse_struct_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Struct)?;

        let struct_type = if self.check_token_type(TokenTypeVariant::Less) {
            Some(self.parse_struct_type()?)
        } else {
            None
        };

        self.consume(TokenTypeVariant::LeftParen)?;

        let mut struct_fields = vec![];
        loop {
            let field_expr = self.parse_expr()?;
            let field_alias = if self.match_token_type(TokenTypeVariant::As) {
                let alias = self.consume_identifier()?.clone();
                Some(ParseToken::Single(alias))
            } else {
                None
            };

            struct_fields.push(StructField {
                expr: field_expr,
                alias: field_alias,
            });
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;

        Ok(Expr::Struct(StructExpr {
            r#type: struct_type,
            fields: struct_fields,
        }))
    }

    // struct_tuple_expr -> "(" expr ("," expr)* ")"
    fn parse_struct_tuple_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::LeftParen)?;

        let mut struct_exprs = vec![];
        loop {
            let field_expr = self.parse_expr()?;
            struct_exprs.push(field_expr);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;

        Ok(Expr::Struct(StructExpr {
            r#type: None,
            fields: struct_exprs
                .into_iter()
                .map(|expr| StructField { expr, alias: None })
                .collect(),
        }))
    }

    // interval_part -> "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND"
    fn parse_interval_part(&mut self) -> anyhow::Result<IntervalPart> {
        let literal = match &self.advance().kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        }
        .to_lowercase();
        match literal.as_str() {
            "year" => Ok(IntervalPart::Year),
            "month" => Ok(IntervalPart::Month),
            "week" => Ok(IntervalPart::Week),
            "day" => Ok(IntervalPart::Day),
            "hour" => Ok(IntervalPart::Hour),
            "minute" => Ok(IntervalPart::Minute),
            "second" => Ok(IntervalPart::Second),
            "millisecond" => Ok(IntervalPart::Millisecond),
            "microsecond" => Ok(IntervalPart::Microsecond),
            _ => unreachable!(),
        }
    }

    // interval_expr -> interval | interval_range
    // where:
    // interval -> "INTERVAL" expr interval_part
    // interval_range -> "String" interval_part "TO" interval_part
    fn parse_interval_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Interval)?;

        if self.match_token_type(TokenTypeVariant::String) {
            let value = match &self.peek_prev().kind {
                TokenType::String(value) => value.clone(),
                _ => unreachable!(),
            };
            let start_part = self.parse_interval_part()?;
            self.consume(TokenTypeVariant::To)?;
            let end_part = self.parse_interval_part()?;
            return Ok(Expr::Interval(IntervalExpr::IntervalRange {
                value,
                start_part,
                end_part,
            }));
        }

        let value = self.parse_expr()?;
        let part = self.parse_interval_part()?;
        Ok(Expr::Interval(IntervalExpr::Interval {
            value: Box::new(value),
            part,
        }))
    }

    // range_expr -> "RANGE" "<" bq_parameterized_type ">" "String"
    fn parse_range_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Range)?;
        self.consume(TokenTypeVariant::Less)?;
        let bq_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        let curr = self.consume(TokenTypeVariant::String)?;
        Ok(match &curr.kind {
            TokenType::String(ts_str) => Expr::Range(RangeExpr {
                r#type: bq_type,
                value: ts_str.clone(),
            }),
            _ => unreachable!(),
        })
    }

    // array_type -> "<" bq_type ">"
    fn parse_array_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(Type::Array(Box::new(array_type)))
    }

    // range_type -> "<" bq_type ">"
    fn parse_range_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let range_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(Type::Range(Box::new(range_type)))
    }

    // struct_type -> "<" ["field_name"] bq_type ("," ["field_name"] bq_type)* ">"
    fn parse_struct_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let mut struct_field_types = vec![];
        loop {
            let lookahead = self.peek_next_i(1);
            let field_type_name = if (self.check_token_type(TokenTypeVariant::Identifier)
                || self.check_token_type(TokenTypeVariant::QuotedIdentifier))
                && (matches!(
                    lookahead.kind.discriminant(),
                    TokenTypeVariant::Identifier
                        | TokenTypeVariant::QuotedIdentifier
                        | TokenTypeVariant::Struct
                        | TokenTypeVariant::Array
                        | TokenTypeVariant::Interval
                        | TokenTypeVariant::Range
                )) {
                self.advance();
                Some(ParseToken::Single(self.peek_prev().clone()))
            } else {
                None
            };

            let field_type = self.parse_bq_type()?;
            struct_field_types.push(StructFieldType {
                name: field_type_name,
                r#type: field_type,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::Greater)?;

        Ok(Type::Struct(struct_field_types))
    }

    // bq_type ->
    // "ARRAY" array_type | "STRUCT" struct_type
    // | "BIGNUMERIC" | "NUMERIC" | "BOOL" | "BYTES" | "DATE"" | "DATETIME" "FLOAT64" | "GEOGRAPHY"
    // | "INT64" | "INTERVAL"" | "JSON" | "NUMERIC" | "RANGE" | "STRING" | "TIME"" | "TIMESTAMP"
    fn parse_bq_type(&mut self) -> anyhow::Result<Type> {
        let peek_token = self.advance().clone();

        // reserved keywords
        match &peek_token.kind {
            TokenType::Array => return self.parse_array_type(),
            TokenType::Struct => return self.parse_struct_type(),
            TokenType::Range => return self.parse_range_type(),
            TokenType::Interval => return Ok(Type::Interval),
            _ => {}
        }

        // identifier or quotedidentifier
        let literal = match &peek_token.kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        }
        .to_lowercase();

        match literal.as_str() {
            "bignumeric" | "bigdecimal" => Ok(Type::BigNumeric),
            "bool" | "boolean" => Ok(Type::Bool),
            "bytes" => Ok(Type::Bytes),
            "date" => Ok(Type::Date),
            "datetime" => Ok(Type::Datetime),
            "float64" => Ok(Type::Float64),
            "geography" => Ok(Type::Geography),
            "int64" | "int" | "smallint" | "integer" | "bigint" | "tinyint" | "byteint" => {
                Ok(Type::Int64)
            }
            "interval" => Ok(Type::Interval),
            "json" => Ok(Type::Json),
            "numeric" | "decimal" => Ok(Type::Numeric),
            "range" => {
                // we cannot use range as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `RANGE`, found `QuotedIdentifier` `RANGE`."
                    ));
                }
                Ok(self.parse_range_type()?)
            }
            "string" => Ok(Type::String),
            "time" => Ok(Type::Time),
            "timestamp" => Ok(Type::Timestamp),
            "struct" => {
                // we cannot use struct as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `STRUCT`, found `QuotedIdentifier` `STRUCT`."
                    ));
                }
                Ok(self.parse_struct_type()?)
            }
            "array" => {
                // we cannot use array as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `ARRAY`, found `QuotedIdentifier` `ARRAY`."
                    ));
                }
                Ok(self.parse_array_type()?)
            }
            _ => {
                Err(anyhow!( self.error(
                    &peek_token,
                    "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                     `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
                )))
            }
        }
    }

    // range_type -> "<" bq_paramterized_type ">"
    fn parse_parameterized_range_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let range_type = self.parse_parameterized_range_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(ParameterizedType::Range(Box::new(range_type)))
    }

    // array_type -> "<" bq_paramterized_type ">"
    fn parse_parameterized_array_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_parameterized_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(ParameterizedType::Array(Box::new(array_type)))
    }

    // struct_type -> "<" field_name bq_paramterized_type ("," field_name bq_paramterized_type)* ">"
    // where:
    // field_name -> "Identifier" | "QuotedIdentifier"
    fn parse_parameterized_struct_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let mut struct_field_types = vec![];
        loop {
            let field_name = self
                .consume_one_of(&[
                    TokenTypeVariant::Identifier,
                    TokenTypeVariant::QuotedIdentifier,
                ])?
                .clone();
            let field_type = self.parse_parameterized_bq_type()?;
            struct_field_types.push(StructParameterizedFieldType {
                name: ParseToken::Single(field_name),
                r#type: field_type,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::Greater)?;

        Ok(ParameterizedType::Struct(struct_field_types))
    }

    // parameterized_bq_type ->
    // "ARRAY" array_type | "STRUCT" struct_type
    // | "BIGNUMERIC" ["(" number ["," number] ")" ]
    // | "NUMERIC" ["(" number ["," number] ")" ]
    // | "BOOL" | "BYTES" ["(" number ")"] | "STRING" ["(" number ")"]
    // | "DATE"" | "DATETIME" "FLOAT64" | "GEOGRAPHY"
    // | "INT64" | "INTERVAL"" | "JSON" | "NUMERIC" | "RANGE" |  | "TIME"" | "TIMESTAMP"
    pub(crate) fn parse_parameterized_bq_type(&mut self) -> anyhow::Result<ParameterizedType> {
        let peek_token = self.advance().clone();

        // reserved keywords
        match &peek_token.kind {
            TokenType::Array => return self.parse_parameterized_array_type(),
            TokenType::Struct => return self.parse_parameterized_struct_type(),
            TokenType::Range => return self.parse_parameterized_range_type(),
            TokenType::Interval => return Ok(ParameterizedType::Interval),
            _ => {}
        }

        // identifier or quotedidentifier
        let literal = match &peek_token.kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        }
        .to_lowercase();

        match literal.as_str() {
            "bignumeric" | "bigdecimal" => {
                let (precision, scale) = if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let precision = match &self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => number.clone(),
                        _ => unreachable!(),
                    };

                    let scale = if self.match_token_type(TokenTypeVariant::Comma) {
                        match &self.consume(TokenTypeVariant::Number)?.kind {
                            TokenType::Number(number) => Some(number.clone()),
                            _ => unreachable!(),
                        }
                    } else {
                        None
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    (Some(precision), scale)
                } else {
                    (None, None)
                };
                Ok(ParameterizedType::BigNumeric(precision, scale))
            }
            "bool" | "boolean" => Ok(ParameterizedType::Bool),
            "bytes" => {
                let max_len = if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let max_len = match &self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => Some(number.clone()),
                        _ => None,
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    max_len
                } else {
                    None
                };

                Ok(ParameterizedType::Bytes(max_len))
            }
            "date" => Ok(ParameterizedType::Date),
            "datetime" => Ok(ParameterizedType::Datetime),
            "float64" => Ok(ParameterizedType::Float64),
            "geography" => Ok(ParameterizedType::Geography),
            "int64" | "int" | "smallint" | "integer" | "bigint" | "tinyint" | "byteint" => {
                Ok(ParameterizedType::Int64)
            }
            "interval" => Ok(ParameterizedType::Interval),
            "json" => Ok(ParameterizedType::Json),
            "numeric" | "decimal" => {
                let (precision, scale) = if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let precision = match &self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => number.clone(),
                        _ => unreachable!(),
                    };

                    let scale = if self.match_token_type(TokenTypeVariant::Comma) {
                        match &self.consume(TokenTypeVariant::Number)?.kind {
                            TokenType::Number(number) => Some(number.clone()),
                            _ => unreachable!(),
                        }
                    } else {
                        None
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    (Some(precision), scale)
                } else {
                    (None, None)
                };
                Ok(ParameterizedType::Numeric(precision, scale))
            }
            "range" => {
                // we cannot use range as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `RANGE`, found `QuotedIdentifier` `RANGE`."
                    ));
                }
                Ok(self.parse_parameterized_range_type()?)
            }
            "string" => {
                let max_len = if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let max_len = match &self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => Some(number.clone()),
                        _ => None,
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    max_len
                } else {
                    None
                };
                Ok(ParameterizedType::String(max_len))
            }
            "time" => Ok(ParameterizedType::Time),
            "timestamp" => Ok(ParameterizedType::Timestamp),
            "struct" => {
                // we cannot use struct as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `STRUCT`, found `QuotedIdentifier` `STRUCT`."
                    ));
                }
                Ok(self.parse_parameterized_struct_type()?)
            }
            "array" => {
                // we cannot use array as a quotedidentifier
                if peek_token.kind.discriminant() == TokenTypeVariant::QuotedIdentifier {
                    return Err(anyhow!(
                        "Expected `Identifier` `ARRAY`, found `QuotedIdentifier` `ARRAY`."
                    ));
                }
                Ok(self.parse_parameterized_array_type()?)
            }
            _ => {
                Err(anyhow!( self.error(
                    &peek_token,
                    "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                     `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
                )))
            }
        }
    }

    // array_fn -> "ARRAY" "(" query_expr ")"
    fn parse_array_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Array)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let query = self.parse_query_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(FunctionExpr::Array(ArrayFunctionExpr {
            query,
        })))
    }

    // concat_fn -> "CONCAT" "(" expr  ( "," expr )* ")"
    fn parse_concat_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;
        self.consume(TokenTypeVariant::LeftParen)?;

        let mut values = vec![];
        loop {
            let value = self.parse_expr()?;
            values.push(value);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(FunctionExpr::Concat(ConcatFunctionExpr {
            values,
        })))
    }

    fn parse_cast_fn_arguments(
        &mut self,
    ) -> anyhow::Result<(Box<Expr>, ParameterizedType, Option<Box<Expr>>)> {
        self.consume(TokenTypeVariant::LeftParen)?;
        let expr = self.parse_expr()?;
        self.consume(TokenTypeVariant::As)?;
        let r#type = self.parse_parameterized_bq_type()?;
        let format = if self.match_non_reserved_keyword("format") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;
        Ok((Box::new(expr), r#type, format))
    }

    // cast -> "CAST" "(" expr "AS" bq_parameterized_type ["FORMAT" expr] ")"
    fn parse_cast_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Cast)?;
        let (expr, r#type, format) = self.parse_cast_fn_arguments()?;
        Ok(Expr::Function(FunctionExpr::Cast(CastFunctionExpr {
            expr,
            r#type,
            format,
        })))
    }

    // safe_cast -> "SAFE_CAST" "(" expr "AS" bq_parameterized_type ["FORMAT" expr] ")"
    fn parse_safe_cast_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;
        let (expr, r#type, format) = self.parse_cast_fn_arguments()?;
        Ok(Expr::Function(FunctionExpr::SafeCast(
            SafeCastFunctionExpr {
                expr,
                r#type,
                format,
            },
        )))
    }

    // array_agg -> ("Identifier" | "QuotedIdentifier") "(" arg ")" ["OVER" named_window_expr]
    // where:
    // arg ->
    //  ["DISTINCT"]
    //  expr
    //  [("IGNORE" | "RESPECT") "NULLS"]
    //  ["HAVING ("MAX" | "MIN") expr]
    //  ["ORDER" "BY" expr ("ASC" | "DESC") ("," expr ("ASC" | "DESC"))*]
    //  ["LIMIT" "Number"]
    fn parse_array_agg_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;
        self.consume(TokenTypeVariant::LeftParen)?;

        let distinct = self.match_token_type(TokenTypeVariant::Distinct);

        let arg_expr = self.parse_expr()?;

        let nulls =
            if self.match_token_types(&[TokenTypeVariant::Ignore, TokenTypeVariant::Respect]) {
                Some(match &self.peek_prev().kind {
                    TokenType::Ignore => FunctionAggregateNulls::Ignore,
                    TokenType::Respect => FunctionAggregateNulls::Respect,
                    _ => unreachable!(),
                })
            } else {
                None
            };

        let having = if self.match_token_type(TokenTypeVariant::Having) {
            let tok = self.consume_one_of_non_reserved_keywords(&["max", "min"])?;
            let kind = match &tok.kind {
                TokenType::Identifier(s) => match s.to_lowercase().as_str() {
                    "max" => FunctionAggregateHavingKind::Max,
                    "min" => FunctionAggregateHavingKind::Min,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };
            let expr = self.parse_expr()?;
            Some(FunctionAggregateHaving {
                kind,
                expr: Box::new(expr),
            })
        } else {
            None
        };

        let order_by = if self.match_token_type(TokenTypeVariant::Order) {
            self.consume(TokenTypeVariant::By)?;
            let mut exprs = vec![];
            loop {
                let expr = self.parse_expr()?;
                let sort_direction =
                    if self.match_token_types(&[TokenTypeVariant::Asc, TokenTypeVariant::Desc]) {
                        Some(match self.peek_prev().kind {
                            TokenType::Asc => OrderBySortDirection::Asc,
                            TokenType::Desc => OrderBySortDirection::Desc,
                            _ => unreachable!(),
                        })
                    } else {
                        None
                    };
                exprs.push(FunctionAggregateOrderBy {
                    expr: Box::new(expr),
                    sort_direction,
                });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(exprs)
        } else {
            None
        };

        let limit = if self.match_token_type(TokenTypeVariant::Limit) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let aggregate = if distinct
            || nulls.is_some()
            || having.is_some()
            || order_by.is_some()
            || limit.is_some()
        {
            Some(FunctionAggregate {
                distinct,
                nulls,
                having,
                order_by,
                limit,
            })
        } else {
            None
        };

        let arg = GenericFunctionExprArg {
            expr: arg_expr,
            aggregate,
        };
        self.consume(TokenTypeVariant::RightParen)?;

        let over = if self.match_token_type(TokenTypeVariant::Over) {
            Some(self.parse_named_window_expr()?)
        } else {
            None
        };

        Ok(Expr::Function(FunctionExpr::ArrayAgg(
            ArrayAggFunctionExpr {
                arg: Box::new(arg),
                over,
            },
        )))
    }

    // current_date -> "CURRENT_DATE" | "CURRENT_DATE" "(" [expr] ")"
    fn parse_current_date_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        let timezone = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let timezone = if self.check_token_type(TokenTypeVariant::RightParen) {
                None
            } else {
                Some(Box::new(self.parse_expr()?))
            };
            self.consume(TokenTypeVariant::RightParen)?;
            timezone
        } else {
            None
        };

        Ok(Expr::Function(FunctionExpr::CurrentDate(
            CurrentDateFunctionExpr { timezone },
        )))
    }

    // current_timestamp -> "CURRENT_TIMESTAMP" | "CURRENT_TIMESTAMP" "(" ")"
    fn parse_current_timestamp_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        if self.match_token_type(TokenTypeVariant::LeftParen) {
            self.consume(TokenTypeVariant::RightParen)?;
        }

        Ok(Expr::Function(FunctionExpr::CurrentTimestamp))
    }

    fn parse_function_expr(&mut self) -> anyhow::Result<Expr> {
        let peek_function_name = match &self.peek().kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        }
        .to_lowercase();
        match peek_function_name.as_str() {
            "concat" => self.parse_concat_fn_expr(),
            "safe_cast" => self.parse_safe_cast_fn_expr(),
            "array_agg" => self.parse_array_agg_fn_expr(),
            "current_date" => self.parse_current_date_fn_expr(),
            "current_timestamp" => self.parse_current_timestamp_fn_expr(),
            _ => self.parse_generic_function(),
        }
    }

    // generic_function -> ("Identifier" | "QuotedIdentifier") "(" arg ("," arg)* ")" ["OVER" named_window_expr]
    // where:
    // arg ->
    //  ["DISTINCT"]
    //  expr
    //  [("IGNORE" | "RESPECT") "NULLS"]
    //  ["HAVING ("MAX" | "MIN") expr]
    //  ["ORDER" "BY" expr ("ASC" | "DESC") ("," expr ("ASC" | "DESC"))*]
    //  ["LIMIT" "Number"]
    fn parse_generic_function(&mut self) -> anyhow::Result<Expr> {
        let function_name = self
            .consume_one_of(&[
                TokenTypeVariant::Identifier,
                TokenTypeVariant::QuotedIdentifier,
            ])?
            .clone();
        self.consume(TokenTypeVariant::LeftParen)?;

        let mut arguments = vec![];
        loop {
            if self.is_at_end() {
                return Err(anyhow!("Expected `)`."));
            }
            if self.match_token_type(TokenTypeVariant::RightParen) {
                break;
            }

            let distinct = self.match_token_type(TokenTypeVariant::Distinct);

            let arg_expr = self.parse_expr()?;

            let nulls =
                if self.match_token_types(&[TokenTypeVariant::Ignore, TokenTypeVariant::Respect]) {
                    Some(match &self.peek_prev().kind {
                        TokenType::Ignore => FunctionAggregateNulls::Ignore,
                        TokenType::Respect => FunctionAggregateNulls::Respect,
                        _ => unreachable!(),
                    })
                } else {
                    None
                };

            let having = if self.match_token_type(TokenTypeVariant::Having) {
                // TODO: this is error prone (we need to remember to lowercase, write a method)
                let tok = self.consume_one_of_non_reserved_keywords(&["max", "min"])?;
                let kind = match &tok.kind {
                    TokenType::Identifier(s) => match s.to_lowercase().as_str() {
                        "max" => FunctionAggregateHavingKind::Max,
                        "min" => FunctionAggregateHavingKind::Min,
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                let expr = self.parse_expr()?;
                Some(FunctionAggregateHaving {
                    kind,
                    expr: Box::new(expr),
                })
            } else {
                None
            };

            let order_by = if self.match_token_type(TokenTypeVariant::Order) {
                self.consume(TokenTypeVariant::By)?;
                let mut exprs = vec![];
                loop {
                    let expr = self.parse_expr()?;
                    let sort_direction = if self
                        .match_token_types(&[TokenTypeVariant::Asc, TokenTypeVariant::Desc])
                    {
                        Some(match self.peek_prev().kind {
                            TokenType::Asc => OrderBySortDirection::Asc,
                            TokenType::Desc => OrderBySortDirection::Desc,
                            _ => unreachable!(),
                        })
                    } else {
                        None
                    };
                    exprs.push(FunctionAggregateOrderBy {
                        expr: Box::new(expr),
                        sort_direction,
                    });
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                Some(exprs)
            } else {
                None
            };

            let limit = if self.match_token_type(TokenTypeVariant::Limit) {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };

            let aggregate = if distinct
                || nulls.is_some()
                || having.is_some()
                || order_by.is_some()
                || limit.is_some()
            {
                Some(FunctionAggregate {
                    distinct,
                    nulls,
                    having,
                    order_by,
                    limit,
                })
            } else {
                None
            };

            arguments.push(GenericFunctionExprArg {
                expr: arg_expr,
                aggregate,
            });
            if !self.match_token_type(TokenTypeVariant::Comma) {
                self.consume(TokenTypeVariant::RightParen)?;
                break;
            }
        }

        let over = if self.match_token_type(TokenTypeVariant::Over) {
            Some(self.parse_named_window_expr()?)
        } else {
            None
        };

        Ok(Expr::GenericFunction(Box::new(GenericFunctionExpr {
            name: ParseToken::Single(function_name),
            arguments,
            over,
        })))
    }

    // case_expr -> "CASE" ("WHEN" expr "THEN" expr)+ "ELSE" expr "END"
    fn parse_case_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Case)?;

        let case = if self.check_token_type(TokenTypeVariant::When) {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };

        let mut when_thens = vec![];

        loop {
            self.consume(TokenTypeVariant::When)?;
            let when_expr = self.parse_expr()?;
            self.consume(TokenTypeVariant::Then)?;
            let then_expr = self.parse_expr()?;

            when_thens.push((when_expr, then_expr));

            if self.match_token_type(TokenTypeVariant::Else) {
                break;
            }
        }

        let r#else = self.parse_expr()?;

        self.consume(TokenTypeVariant::End)?;

        Ok(Expr::Case(CaseExpr {
            case,
            when_thens,
            r#else: Box::new(r#else),
        }))
    }

    // primary_expr ->
    // "True" | "False" | "Null" | "Identifier" | "QuotedIdentifier" | "String" | "Number"
    // | NUMERIC "Number" | BIGNUMERIC "Number"
    // | DATE "String" | TIMESTAMP "String" | DATETIME "String" | TIME "String"
    // | "RANGE" "<" bq_parameterized_type ">" "String"
    // | interval_expr | json_expr
    // | array_expr | struct_expr | struct_tuple_expr
    // | case_expr
    // | function_expr
    // | "(" expression ")" | "(" query_expr ")"
    fn parse_primary_expr(&mut self) -> anyhow::Result<Expr> {
        let peek_token = self.peek().clone();
        let primary_expr = match peek_token.kind {
            TokenType::True => {
                self.advance();
                Expr::Bool(true)
            }
            TokenType::False => {
                self.advance();
                Expr::Bool(false)
            }
            TokenType::Null => {
                self.advance();
                Expr::Null
            }
            TokenType::Struct => {
                return self.parse_struct_expr();
            }
            TokenType::LeftSquare => {
                return self.parse_array_expr();
            }
            TokenType::Array => {
                if self.peek_next_i(1).kind == TokenType::LeftParen {
                    return self.parse_array_fn_expr();
                } else {
                    return self.parse_array_expr();
                }
            }
            TokenType::Case => self.parse_case_expr()?,
            TokenType::Range => self.parse_range_expr()?,
            TokenType::Interval => self.parse_interval_expr()?,
            TokenType::Identifier(ident) => {
                let lower_ident = ident.to_lowercase();
                if self.peek_next_i(1).kind == TokenType::LeftParen
                    || lower_ident == "current_date"
                    || lower_ident == "current_timestamp"
                {
                    return self.parse_function_expr();
                } else if self.peek_prev().kind != TokenType::Dot {
                    match lower_ident.as_str() {
                        "date" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(date_str) => Expr::Date(date_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "timestamp" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(date_str) => Expr::Timestamp(date_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "datetime" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(date_str) => Expr::Datetime(date_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "time" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(date_str) => Expr::Time(date_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "numeric" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(num_str) => Expr::Numeric(num_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "bignumeric" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(num_str) => Expr::BigNumeric(num_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        "json" => {
                            self.advance();
                            let curr = self.consume(TokenTypeVariant::String)?;
                            match &curr.kind {
                                TokenType::String(json_str) => Expr::Json(json_str.clone()),
                                _ => unreachable!(),
                            }
                        }
                        _ => {
                            self.advance();
                            Expr::Identifier(ident)
                        }
                    }
                } else {
                    self.advance();
                    Expr::Identifier(ident)
                }
            }
            TokenType::QuotedIdentifier(qident) => {
                if self.peek_next_i(1).kind == TokenType::LeftParen {
                    return self.parse_function_expr();
                } else {
                    self.advance();
                    Expr::QuotedIdentifier(qident)
                }
            }
            TokenType::Number(num) => {
                self.advance();
                Expr::Number(num)
            }
            TokenType::String(str) | TokenType::RawString(str) => {
                self.advance();
                Expr::String(str)
            }
            TokenType::Bytes(str) | TokenType::RawBytes(str) => {
                self.advance();
                Expr::Bytes(str)
            }
            TokenType::Default => {
                self.advance();
                Expr::Default
            }
            TokenType::LeftParen => {
                self.advance();
                let curr_position = self.curr;
                // Look ahead to check whether we need to parse a query_expr or an expr
                if self.check_token_type(TokenTypeVariant::With)
                    || self.check_token_type(TokenTypeVariant::Select)
                {
                    self.curr = curr_position;
                    let query_expr = self.parse_query_expr()?;
                    self.consume(TokenTypeVariant::RightParen)?;
                    return Ok(Expr::Query(QueryExpr::Grouping(GroupingQueryExpr {
                        with: None,
                        query: Box::new(query_expr),
                        order_by: None,
                        limit: None,
                    })));
                } else {
                    let expr = self.parse_expr()?;
                    if self.match_token_type(TokenTypeVariant::Comma) {
                        self.curr = curr_position - 1; // -1 parse again the LeftParen
                        return self.parse_struct_tuple_expr();
                    }
                    self.consume(TokenTypeVariant::RightParen)?;
                    return Ok(Expr::Grouping(GroupingExpr {
                        expr: Box::new(expr),
                    }));
                }
            }
            // Functions whose name is a reserved keyword
            TokenType::Cast => self.parse_cast_fn_expr()?,
            _ => {
                return Err(anyhow!(self.error(&peek_token, "Expected Expression.")));
            }
        };

        Ok(primary_expr)
    }
}

pub fn parse_sql(sql: &str) -> anyhow::Result<Ast> {
    log::debug!("Parsing {}", &sql[..std::cmp::min(50, sql.len())]);

    let mut scanner = Scanner::new(sql);

    scanner.scan()?;

    log::debug!("Tokens:");
    scanner
        .tokens()
        .iter()
        .for_each(|tok| log::debug!("{:?}", tok));

    let mut parser = Parser::new(scanner.tokens());
    let ast = parser.parse()?;
    log::debug!("AST: {:?}", ast);
    Ok(ast)
}
