use core::panic;
use std::fmt::Display;

use anyhow::anyhow;
use strum::IntoDiscriminant;

use crate::scanner::{Scanner, Token, TokenType, TokenTypeVariant};

// TODO: make this a real error
#[derive(Debug)]
struct ParseError;

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseError")
    }
}

#[derive(Debug, Clone)]
pub enum ParseToken {
    Single(Token),
    Multiple(Vec<Token>),
}

impl ParseToken {
    pub fn lexeme(&self, join_char: Option<&str>) -> String {
        match self {
            ParseToken::Single(token) => token.lexeme.clone(),
            ParseToken::Multiple(vec) => vec
                .iter()
                .map(|tok| tok.lexeme.clone())
                .collect::<Vec<String>>()
                .join(join_char.map_or(" ", |c| c)),
        }
    }
    pub fn identifier(&self) -> String {
        match self {
            ParseToken::Single(token) => match &token.kind {
                TokenType::Identifier(ident) => ident.to_owned(),
                TokenType::QuotedIdentifier(qident) => qident.to_owned(),
                _ => panic!("Can't call identifier on {:?}", self),
            },

            ParseToken::Multiple(vec) => vec
                .iter()
                .map(|tok| match &tok.kind {
                    TokenType::Identifier(ident) => ident.to_owned(),
                    TokenType::QuotedIdentifier(qident) => qident.to_owned(),
                    _ => tok.lexeme.to_owned(),
                })
                .collect::<Vec<String>>()
                .join(""),
        }
    }
}

// TODO: create another file to place all these AST objects
#[derive(Debug, Clone)]
pub struct Ast {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone)]
pub enum Statement {
    Query(QueryStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    Truncate(TruncateStatement),
    Merge(MergeStatement),
    CreateTable(CreateTableStatement),
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub name: ParseToken,
    pub schema: Option<Vec<ColumnSchema>>,
    pub replace: bool,
    pub is_temporary: bool,
    pub if_not_exists: bool,
    pub query: Option<QueryExpr>,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: ParseToken,
    pub r#type: ParameterizedType,
}

#[derive(Debug, Clone)]
pub enum ParameterizedType {
    Array(Box<ParameterizedType>),
    BigNumeric(Option<u32>, Option<u32>),
    Bool,
    Bytes(Option<u32>),
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Json,
    Numeric(Option<u32>, Option<u32>),
    Range,
    String(Option<u32>),
    Struct(Vec<StructParameterizedFieldType>),
    Time,
    Timestamp,
}

#[derive(Debug, Clone)]
pub struct StructParameterizedFieldType {
    name: ParseToken,
    r#type: ParameterizedType,
}

#[derive(Debug, Clone)]
pub struct QueryStatement {
    pub query_expr: QueryExpr,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub target_table: ParseToken,
    pub columns: Option<Vec<ParseToken>>,
    pub values: Option<Vec<Expr>>,
    pub query_expr: Option<QueryExpr>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    target_table: ParseToken,
    alias: Option<ParseToken>,
    cond: Expr,
}

#[derive(Debug, Clone)]
pub struct UpdateItem {
    pub column_path: ParseToken,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub target_table: ParseToken,
    pub alias: Option<ParseToken>,
    pub update_items: Vec<UpdateItem>,
    pub from: Option<From>,
    pub r#where: Where,
}

#[derive(Debug, Clone)]
pub struct TruncateStatement {
    target_table: ParseToken,
}

#[derive(Debug, Clone)]
pub enum Merge {
    Update(MergeUpdate),
    Insert(MergeInsert),
    InsertRow,
    Delete,
}

#[derive(Debug, Clone)]
pub struct MergeUpdate {
    pub update_items: Vec<UpdateItem>,
}

#[derive(Debug, Clone)]
pub struct MergeInsert {
    pub columns: Option<Vec<ParseToken>>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub enum When {
    Matched(WhenMatched),
    NotMatchedByTarget(WhenNotMatchedByTarget),
    NotMatchedBySource(WhenNotMatchedBySource),
}

#[derive(Debug, Clone)]
pub struct WhenMatched {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone)]
pub struct WhenNotMatchedByTarget {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone)]
pub struct WhenNotMatchedBySource {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone)]
pub struct MergeStatement {
    pub target_table: ParseToken,
    pub target_alias: Option<ParseToken>,
    pub source: MergeSource,
    pub source_alias: Option<ParseToken>,
    pub condition: Expr,
    pub whens: Vec<When>,
}

#[derive(Debug, Clone)]
pub enum MergeSource {
    Table(ParseToken),
    Subquery(QueryExpr),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Binary(BinaryExpr),
    Unary(UnaryExpr),
    Grouping(GroupingExpr),
    Array(ArrayExpr),
    Struct(StructExpr),
    Identifier(String),
    QuotedIdentifier(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Star,
    Query(QueryExpr),
    GenericFunction(GenericFunctionExpr), // a generic function call, whose signature is not yet implemented in the parser
    Function(FunctionExpr),
}

#[derive(Debug, Clone)]
pub enum FunctionExpr {
    // list of known functions here
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all
    ConcatFn(ConcatFnExpr),
}

#[derive(Debug, Clone)]
pub struct ConcatFnExpr {
    values: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct GenericFunctionExpr {
    name: ParseToken,
    arguments: ParseToken,
}

#[derive(Debug, Clone)]
pub struct UnaryExpr {
    pub operator: ParseToken,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub operator: ParseToken,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct GroupingExpr {
    expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum Type {
    Array(Box<Type>),
    BigNumeric,
    Bool,
    Bytes,
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Json,
    Numeric,
    Range,
    String,
    Struct(Vec<StructFieldType>),
    Time,
    Timestamp,
}

#[derive(Debug, Clone)]
pub struct StructFieldType {
    name: Option<ParseToken>,
    r#type: Type,
}

#[derive(Debug, Clone)]
pub struct ArrayExpr {
    r#type: Option<Type>,
    exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct StructExpr {
    r#type: Option<Type>,
    fields: Vec<StructField>,
}

#[derive(Debug, Clone)]
pub struct StructField {
    expr: Expr,
    alias: Option<ParseToken>,
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Grouping(GroupingQueryExpr),
    Select(SelectQueryExpr),
    SetSelect(SetSelectQueryExpr),
}

#[derive(Debug, Clone)]
pub struct GroupingQueryExpr {
    pub with: Option<With>,
    pub query_expr: Box<QueryExpr>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone)]
pub struct SelectQueryExpr {
    pub with: Option<With>,
    pub select: Select,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    exprs: Vec<OrderByExpr>,
}

#[derive(Debug, Clone)]
pub enum OrderAscDesc {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum OrderNulls {
    First,
    Last,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    expr: Expr,
    asc_desc: Option<OrderAscDesc>,
    nulls: Option<OrderNulls>,
}

#[derive(Debug, Clone)]
pub struct Limit {
    count: Box<Expr>,
    offset: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct With {
    pub ctes: Vec<Cte>,
    pub is_recursive: bool, // TODO: we can eliminate this field
}

#[derive(Debug, Clone)]
pub enum Cte {
    NonRecursive(NonRecursiveCte),
    Recursive(RecursiveCte),
}

#[derive(Debug, Clone)]
pub struct NonRecursiveCte {
    pub name: ParseToken,
    pub query: QueryExpr,
}

#[derive(Debug, Clone)]
pub struct RecursiveCte {
    name: ParseToken,
    base_query: QueryExpr,
    recursive_query: QueryExpr,
}

#[derive(Debug, Clone)]
pub struct SetSelectQueryExpr {
    with: Option<With>,
    left_query_expr: Box<QueryExpr>,
    set_operator: ParseToken,
    right_query_expr: Box<QueryExpr>,
    order_by: Option<OrderBy>,
    limit: Option<Limit>,
}

#[derive(Debug, Clone)]
pub struct Select {
    pub exprs: Vec<SelectExpr>,
    pub from: Option<From>,
    pub r#where: Option<Where>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,
}

#[derive(Debug, Clone)]
pub enum SelectExpr {
    Col(SelectColExpr),
    ColAll(SelectColAllExpr),
    All(SelectAllExpr),
}

#[derive(Debug, Clone)]
pub struct SelectColExpr {
    pub expr: Expr,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone)]
pub struct SelectColAllExpr {
    pub expr: Expr,
    pub except: Option<Vec<ParseToken>>,
}

#[derive(Debug, Clone)]
pub struct SelectAllExpr {
    pub except: Option<Vec<ParseToken>>,
}

#[derive(Debug, Clone)]
pub struct From {
    pub exprs: Vec<FromExpr>,
}

#[derive(Debug, Clone)]
pub enum FromExpr {
    Join(JoinExpr),
    LeftJoin(JoinExpr),
    RightJoin(JoinExpr),
    CrossJoin(CrossJoinExpr),
    Path(FromPathExpr),
    GroupingQuery(FromGroupingQueryExpr),
    GroupingFrom(GroupingFromExpr),
}

#[derive(Debug, Clone)]
pub struct CrossJoinExpr {
    pub left: Box<FromExpr>,
    pub join: ParseToken,
    pub right: Box<FromExpr>,
}

#[derive(Debug, Clone)]
pub struct JoinExpr {
    pub left: Box<FromExpr>,
    pub join: ParseToken,
    pub right: Box<FromExpr>,
    pub cond: JoinCondition,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<ParseToken>),
}

#[derive(Debug, Clone)]
pub struct PathExpr {
    pub path: ParseToken,
}

#[derive(Debug, Clone)]
pub struct FromPathExpr {
    pub path_expr: PathExpr,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone)]
pub struct GroupingFromExpr {
    pub query_expr: Box<FromExpr>,
}

#[derive(Debug, Clone)]
pub struct FromGroupingQueryExpr {
    pub query_expr: Box<QueryExpr>,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone)]
pub struct Where {
    expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum GroupByExpr {
    Items(Vec<Expr>),
    All,
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    expr: GroupByExpr,
}

#[derive(Debug, Clone)]
pub struct Having {
    expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct Qualify {
    expr: Box<Expr>,
}

// TODO: this struct should own the scanner and use it
pub struct Parser<'a> {
    source_tokens: &'a Vec<Token>,
    curr: i32,
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
        &self.source_tokens[(self.curr as usize) - 1]
    }

    fn peek(&self) -> &Token {
        &self.source_tokens[self.curr as usize]
    }

    fn peek_next_i(&self, i: i32) -> &Token {
        if self.curr + i >= self.source_tokens.len() as i32 {
            self.source_tokens.last().unwrap() // Eof
        } else {
            &self.source_tokens[(self.curr as usize) + (i as usize)]
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
            self.error(self.peek(), &err_msg);
            Err(anyhow!(ParseError))
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
        self.error(self.peek(), &format!("Expected one of: {}.", err_msg));
        Err(anyhow!(ParseError))
    }

    fn consume(&mut self, token_type: TokenTypeVariant) -> anyhow::Result<&Token> {
        if self.check_token_type(token_type) {
            Ok(self.advance())
        } else {
            let err_msg = format!("Expected `{}`.", token_type.variant_str());
            self.error(self.peek(), &err_msg);
            Err(anyhow!(ParseError))
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
        self.error(self.peek(), &format!("Expected one of: {}.", err_msg));
        Err(anyhow!(ParseError))
    }

    fn error(&self, token: &Token, message: &str) {
        if token.kind == TokenType::Eof {
            self.report(token.line, token.col, "at end", message);
        } else {
            self.report(
                token.line,
                token.col,
                &format!("at '{}'", token.lexeme),
                message,
            );
        }
    }

    fn report(&self, line: i32, col: i32, location: &str, message: &str) {
        log::debug!(
            "[line {}, col {}] Error {}: {}",
            line,
            col,
            location,
            message
        );
    }

    // query -> query_statement (; query_statement [";"])*
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

            let peek = self.peek().clone();

            let statement = match &peek.kind {
                TokenType::Create => self.parse_create_table_statement()?,
                TokenType::Identifier(non_reserved_keyword) => {
                    match non_reserved_keyword.to_lowercase().as_str() {
                        "insert" => self.parse_insert_statement()?,
                        "delete" => self.parse_delete_statement()?,
                        "update" => self.parse_update_statement()?,
                        "truncate" => self.parse_truncate_statement()?,
                        "merge" => self.parse_merge_statement()?,
                        _ => {
                            self.error(
                                &peek,
                                &format!(
                                    "Unexpected non reserved keyword: `{}`.",
                                    non_reserved_keyword
                                ),
                            );
                            return Err(anyhow!(ParseError));
                        }
                    }
                }
                _ => self.parse_query_statement()?,
            };
            statements.push(statement);

            if !self.match_token_type(TokenTypeVariant::Semicolon) {
                break;
            }
        }

        self.consume(TokenTypeVariant::Eof)?;
        Ok(Ast { statements })
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

        let name = self.parse_path()?.path;

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
        Ok(Statement::Query(QueryStatement { query_expr }))
    }

    // insert_statement -> "INSERT" ["INTO"] path ["(" column_name ("," column_name)* ")"] input
    // where:
    // input -> query_expr | "VALUES" "(" expr ")" ("(" expr ")")*
    // column_name -> "Identifier" | "QuotedIdentifier"
    fn parse_insert_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("insert")?;
        self.match_token_type(TokenTypeVariant::Into);
        let target_table = self.parse_path()?.path;
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
            target_table,
            columns,
            values,
            query_expr,
        }))
    }

    // delete_statement -> "DELETE" ["FROM"] path ["AS"] [alias] "WHERE" expr
    fn parse_delete_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("delete")?;
        self.match_token_type(TokenTypeVariant::From);
        let target_table = self.parse_path()?.path;
        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Where)?;
        let cond = self.parse_expr()?;
        Ok(Statement::Delete(DeleteStatement {
            target_table,
            alias,
            cond,
        }))
    }

    // update statement -> "UPDATE" path ["AS"] [alias] SET set_clause ["FROM" from_expr] "WHERE" expr
    // where:
    // set_clause = ("Identifier" | "QuotedIdentifier") = expr ("," ("Identifier" | "QuotedIdentifier") = expr)*
    fn parse_update_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("update")?;
        let target_table = self.parse_path()?.path;
        let alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_path()?.path;
            self.consume(TokenTypeVariant::Equal)?;
            let expr = self.parse_expr()?;
            update_items.push(UpdateItem { column_path, expr });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        let from = if self.match_token_type(TokenTypeVariant::From) {
            Some(From {
                exprs: vec![self.parse_from_expr()?],
            })
        } else {
            None
        };

        self.consume(TokenTypeVariant::Where)?;
        let where_expr = self.parse_where_expr()?;

        Ok(Statement::Update(UpdateStatement {
            target_table,
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
        let target_table = self.parse_path()?.path;
        Ok(Statement::Truncate(TruncateStatement { target_table }))
    }

    // merge_statement -> "MERGE" ["INTO"] path ["AS"] [alias] "USING" path "ON" merge_condition (when_clause)+
    // where:
    // when_clause -> matched_clause | not_matched_by_target_clause | not_matched_by_source_clause
    // matched_clause -> "WHEN" "MATCHED" ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    // not_matched_by_target_clause -> "WHEN" "NOT" "MATCHED" ["BY" "TARGET"] ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    fn parse_merge_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("merge")?;
        self.match_token_type(TokenTypeVariant::Into);
        let target_table = self.parse_path()?.path;
        let target_alias = self
            .parse_as_alias()?
            .map(|tok| ParseToken::Single(tok.clone()));
        self.consume(TokenTypeVariant::Using)?;
        let source = if self.check_token_type(TokenTypeVariant::LeftParen) {
            MergeSource::Subquery(self.parse_query_expr()?)
        } else {
            MergeSource::Table(self.parse_path()?.path)
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

        Ok(Statement::Merge(MergeStatement {
            target_table,
            target_alias,
            source,
            source_alias,
            condition,
            whens,
        }))
    }

    // merge_update -> "UPDATE" "SET" update_item ("," update_item)*
    // where:
    // update_item -> ("Identifier" | "QuotedIdentifier") "=" expr
    fn parse_merge_update(&mut self) -> anyhow::Result<Merge> {
        self.consume_non_reserved_keyword("update")?;
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_path()?.path;
            self.consume(TokenTypeVariant::Equal)?;
            let expr = self.parse_expr()?;
            update_items.push(UpdateItem { column_path, expr });

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
            let curr_token = self.peek().clone();
            match curr_token.kind {
                TokenType::Union => {
                    let mut set_operators = vec![curr_token];
                    self.advance();
                    let token =
                        self.consume_one_of(&[TokenTypeVariant::All, TokenTypeVariant::Distinct])?;
                    set_operators.push(token.clone());
                    let right_query_expr = self.parse_select_query_expr()?;
                    output = QueryExpr::SetSelect(SetSelectQueryExpr {
                        with: None,
                        left_query_expr: Box::new(output),
                        set_operator: ParseToken::Multiple(set_operators),
                        right_query_expr: Box::new(right_query_expr),
                        order_by: None,
                        limit: None,
                    })
                }
                TokenType::Intersect | TokenType::Except => {
                    let mut set_operators = vec![curr_token];
                    self.advance();
                    set_operators.push(self.consume(TokenTypeVariant::Distinct)?.clone());
                    let right_query_expr = self.parse_select_query_expr()?;
                    output = QueryExpr::SetSelect(SetSelectQueryExpr {
                        with: None,
                        left_query_expr: Box::new(output),
                        set_operator: ParseToken::Multiple(set_operators),
                        right_query_expr: Box::new(right_query_expr),
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
            let count = match tok.kind {
                TokenType::Number(num) => Expr::Number(num),
                _ => unreachable!(),
            };

            let offset = if self.match_non_reserved_keyword("offset") {
                let tok = self.consume(TokenTypeVariant::Number)?;
                match tok.kind {
                    TokenType::Number(num) => Some(Box::new(Expr::Number(num))),
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
                query_expr: Box::new(query_expr),
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
        let is_recursive = self.match_token_type(TokenTypeVariant::Recursive);
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
        Ok(With { ctes, is_recursive })
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
                Some(OrderAscDesc::Asc)
            } else if self.match_token_type(TokenTypeVariant::Desc) {
                Some(OrderAscDesc::Desc)
            } else {
                None
            };

            let nulls = if self.match_token_type(TokenTypeVariant::Nulls) {
                let tok = self.consume_one_of_non_reserved_keywords(&["first", "last"])?;
                match &tok.kind {
                    TokenType::Identifier(s) if s.to_lowercase() == "first" => {
                        Some(OrderNulls::First)
                    }
                    TokenType::Identifier(s) if s.to_lowercase() == "last" => {
                        Some(OrderNulls::Last)
                    }
                    _ => unreachable!(),
                }
            } else {
                None
            };

            order_by_exprs.push(OrderByExpr {
                expr,
                asc_desc,
                nulls,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        Ok(order_by_exprs)
    }

    // TODO: add all distinct
    // select ->
    // "SELECT" select_col_expr [","] (select_col_expr [","])*
    // ["FROM" from_expr]
    // ["WHERE" where_expr]
    // ["GROUP BY" group_by_expr]
    // ["HAVING" having_expr]
    // ["QUALIFY" qualify_expr]
    fn parse_select(&mut self) -> anyhow::Result<Select> {
        self.consume(TokenTypeVariant::Select)?;
        let mut select_exprs = vec![];
        let col_expr = self.parse_select_expr()?;
        select_exprs.push(col_expr);

        let mut comma_matched = self.match_token_type(TokenTypeVariant::Comma);
        let mut last_position = self.curr - (comma_matched as i32);

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
                self.error(self.peek(), "Expected `;`.");
                return Err(anyhow!(ParseError));
            }

            match self.parse_select_expr() {
                Ok(col_expr) => {
                    if self.source_tokens[last_position as usize].kind != TokenType::Comma {
                        self.error(self.peek_prev(), "Expected `,`.");
                        return Err(anyhow!(ParseError));
                    }
                    select_exprs.push(col_expr);
                    comma_matched = self.match_token_type(TokenTypeVariant::Comma);
                    last_position = self.curr - (comma_matched as i32);
                }
                Err(_) => {
                    self.error(self.peek(), "Expected expression.");
                    return Err(anyhow!(ParseError));
                }
            }
        }
        let from = if self.match_token_type(TokenTypeVariant::From) {
            Some(crate::parser::From {
                exprs: vec![self.parse_from_expr()?],
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
            Some(crate::parser::Having {
                expr: Box::new(self.parse_having_expr()?),
            })
        } else {
            None
        };

        let qualify = if self.match_token_type(TokenTypeVariant::Qualify) {
            Some(crate::parser::Qualify {
                expr: Box::new(self.parse_qualify_expr()?),
            })
        } else {
            None
        };

        Ok(Select {
            exprs: select_exprs,
            from,
            r#where,
            group_by,
            having,
            qualify,
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
                self.error(self.peek(), "Expected Expression.");
                return Err(anyhow!(ParseError));
            }
            Ok(expr) => expr,
        };

        if self.peek_prev().kind == TokenType::Star {
            let except = self.parse_except()?;
            return Ok(SelectExpr::ColAll(SelectColAllExpr { expr, except }));
        }

        if self.match_token_type(TokenTypeVariant::As) {
            let identifier = ParseToken::Single(self.consume_identifier()?.clone());
            return Ok(SelectExpr::Col(SelectColExpr {
                expr,
                alias: Some(identifier),
            }));
        }
        if self.match_identifier() {
            let identifier = ParseToken::Single(self.peek_prev().clone());
            return Ok(SelectExpr::Col(SelectColExpr {
                expr,
                alias: Some(identifier),
            }));
        }
        Ok(SelectExpr::Col(SelectColExpr { expr, alias: None }))
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
                    let mut parse_tokens = vec![];
                    if self.check_token_type(TokenTypeVariant::Inner) {
                        parse_tokens.push(curr_peek.clone());
                        self.advance();
                    }
                    parse_tokens.push(self.consume(TokenTypeVariant::Join)?.clone());
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::Join(JoinExpr {
                        left: Box::new(output),
                        join: ParseToken::Multiple(parse_tokens),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Left => {
                    let mut parse_tokens = vec![curr_peek.clone()];
                    self.advance();
                    if self.match_token_type(TokenTypeVariant::Outer) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    parse_tokens.push(self.consume(TokenTypeVariant::Join)?.clone());
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::LeftJoin(JoinExpr {
                        left: Box::new(output),
                        join: ParseToken::Multiple(parse_tokens),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Right => {
                    let mut parse_tokens = vec![curr_peek.clone()];
                    self.advance();
                    if self.match_token_type(TokenTypeVariant::Outer) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    parse_tokens.push(self.consume(TokenTypeVariant::Join)?.clone());
                    let right = self.parse_from_item_expr()?;
                    let join_cond = self.parse_cond()?;
                    output = FromExpr::RightJoin(JoinExpr {
                        left: Box::new(output),
                        join: ParseToken::Multiple(parse_tokens),
                        right: Box::new(right),
                        cond: join_cond,
                    })
                }
                TokenType::Cross => {
                    let mut parse_tokens = vec![curr_peek.clone()];
                    self.advance();
                    parse_tokens.push(self.consume(TokenTypeVariant::Join)?.clone());
                    let right = self.parse_from_item_expr()?;
                    output = FromExpr::CrossJoin(CrossJoinExpr {
                        left: Box::new(output),
                        join: ParseToken::Multiple(parse_tokens),
                        right: Box::new(right),
                    })
                }
                TokenType::Comma => {
                    let parse_token = self.advance().clone();
                    let right = self.parse_from_item_expr()?;
                    output = FromExpr::CrossJoin(CrossJoinExpr {
                        left: Box::new(output),
                        join: ParseToken::Single(parse_token),
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
            self.error(self.peek(), "Expected `ON` or `USING`.");
            return Err(anyhow!(ParseError));
        }
    }

    // TODO: add FROM UNNEST
    // from_item_expr -> path [as_alias] | "(" query_expr ")" [as_alias] | "(" from_expr ")"
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
                    query_expr: Box::new(query_expr),
                    alias: alias.map(|tok| ParseToken::Single(tok.clone())),
                }))
            } else {
                self.curr = curr;
                let parse_from_expr = self.parse_from_expr()?;
                match parse_from_expr {
                    FromExpr::Join(_) | FromExpr::LeftJoin(_) | FromExpr::RightJoin(_) => {
                        // Only these from expressions can be parenthesized
                        self.consume(TokenTypeVariant::RightParen)?;
                        Ok(FromExpr::GroupingFrom(GroupingFromExpr {
                            query_expr: Box::new(parse_from_expr),
                        }))
                    }
                    _ => {
                        self.error(self.peek(), "Expected `JOIN`.");
                        Err(anyhow!(ParseError))
                    }
                }
            }
        } else {
            let path = self.parse_path()?;
            let alias = self.parse_as_alias()?;
            Ok(FromExpr::Path(FromPathExpr {
                path_expr: path,
                alias: alias.map(|tok| ParseToken::Single(tok.clone())),
            }))
        }
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
            path: ParseToken::Multiple(path_identifiers),
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

    // array_expr -> ["ARRAY" [array_type] "[" expr ("," expr)* "]"
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

    // array_type -> "<" bq_type ("," bq_type)* ">"
    fn parse_array_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(Type::Array(Box::new(array_type)))
    }

    // struct_type -> "<" ["field_name"] bq_type ("," ["field_name"] bq_type)* ">"
    fn parse_struct_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let mut struct_field_types = vec![];
        loop {
            let lookahead = self.peek_next_i(1);
            let field_type_name = if (self.check_token_type(TokenTypeVariant::Identifier)
                || self.check_token_type(TokenTypeVariant::QuotedIdentifier))
                && (lookahead.kind.discriminant() == TokenTypeVariant::Identifier
                    || lookahead.kind.discriminant() == TokenTypeVariant::QuotedIdentifier)
            {
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
            "range" => Ok(Type::Range),
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
                self.error(
                    &peek_token,
                    "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                     `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
                );
                Err(anyhow!(ParseError))
            }
        }
    }

    // array_type -> "<" bq_type ("," bq_type)* ">"
    fn parse_parameterized_array_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_parameterized_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(ParameterizedType::Array(Box::new(array_type)))
    }

    // struct_type -> "<" field_name bq_type ("," field_name bq_type)* ">"
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
    fn parse_parameterized_bq_type(&mut self) -> anyhow::Result<ParameterizedType> {
        let peek_token = self.advance().clone();

        // reserved keywords
        match &peek_token.kind {
            TokenType::Array => return self.parse_parameterized_array_type(),
            TokenType::Struct => return self.parse_parameterized_struct_type(),
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
                    let precision: u32 = match self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => number as u32,
                        _ => unreachable!(),
                    };

                    let scale: Option<u32> = if self.match_token_type(TokenTypeVariant::Comma) {
                        match self.consume(TokenTypeVariant::Number)?.kind {
                            TokenType::Number(number) => Some(number as u32),
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
                    let max_len = match self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => Some(number as u32),
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
                    let precision: u32 = match self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => number as u32,
                        _ => unreachable!(),
                    };

                    let scale: Option<u32> = if self.match_token_type(TokenTypeVariant::Comma) {
                        match self.consume(TokenTypeVariant::Number)?.kind {
                            TokenType::Number(number) => Some(number as u32),
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
            "range" => Ok(ParameterizedType::Range),
            "string" => {
                let max_len = if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let max_len = match self.consume(TokenTypeVariant::Number)?.kind {
                        TokenType::Number(number) => Some(number as u32),
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
                self.error(
                    &peek_token,
                    "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                     `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
                );
                Err(anyhow!(ParseError))
            }
        }
    }

    // generic_function -> ("Identifier" | "QuotedIdentifier") "(" ... ")"
    fn parse_generic_function(&mut self) -> anyhow::Result<Expr> {
        let function_name = self
            .consume_one_of(&[
                TokenTypeVariant::Identifier,
                TokenTypeVariant::QuotedIdentifier,
            ])?
            .clone();
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut n_parens = 1;

        let mut argument_tokens = vec![];
        loop {
            if self.is_at_end() {
                return Err(anyhow!("Expected `)`."));
            }
            let tok = self.advance();
            argument_tokens.push(tok.clone());
            match tok.kind {
                TokenType::LeftParen => {
                    n_parens += 1;
                }
                TokenType::RightParen => {
                    n_parens -= 1;
                    if n_parens == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        Ok(Expr::GenericFunction(GenericFunctionExpr {
            name: ParseToken::Single(function_name),
            arguments: ParseToken::Multiple(argument_tokens),
        }))
    }

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
        Ok(Expr::Function(FunctionExpr::ConcatFn(ConcatFnExpr {
            values,
        })))
    }

    fn parse_function_expr(&mut self) -> anyhow::Result<Expr> {
        let peek_function_name = match &self.peek().kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        };
        match peek_function_name.as_str() {
            "concat" => self.parse_concat_fn_expr(),
            _ => self.parse_generic_function(),
        }
    }

    // primary_expr ->
    // "True" | "False" | "Null" | "Identifier" | "QuotedIdentifier" | "String" | "Number"
    // | NUMERIC "Number"
    // | array_expr | struct_expr | struct_tuple_expr
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
            TokenType::Identifier(ident) => {
                if self.peek_next_i(1).kind == TokenType::LeftParen {
                    return self.parse_function_expr();
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
            TokenType::String(str) => {
                self.advance();
                Expr::String(str)
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
                        query_expr: Box::new(query_expr),
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
            TokenType::Struct => {
                return self.parse_struct_expr();
            }
            TokenType::LeftSquare | TokenType::Array => {
                return self.parse_array_expr();
            }
            _ => {
                self.error(&peek_token, "Expected Expression.");
                return Err(anyhow!(ParseError));
            }
        };

        Ok(primary_expr)
    }
}

pub fn parse_sql(sql: &str) -> anyhow::Result<Ast> {
    log::debug!("Parsing {}", &sql[..std::cmp::min(50, sql.len())]);

    let mut scanner = Scanner::new(sql);

    let tokens = scanner.scan();

    // TODO: just use a result also in the Scanner
    if scanner.had_error {
        log::debug!("Exiting. Found error while scanning.");
        return Err(anyhow!("scanner error"));
    }

    log::debug!("Tokens:");
    tokens.iter().for_each(|tok| log::debug!("{:?}", tok));

    let mut parser = Parser::new(&tokens);
    let ast = parser.parse()?;
    log::debug!("AST: {:?}", ast);
    Ok(ast)
}
