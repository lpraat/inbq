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
    Range(Box<ParameterizedType>),
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
    Bytes(String),
    Numeric(String),
    BigNumeric(String),
    Number(f64),
    Bool(bool),
    Date(String),
    Time(String),
    Datetime(String),
    Timestamp(String),
    Range(RangeExpr),
    Interval(IntervalExpr),
    Json(String),
    Default,
    Null,
    Star,
    Query(QueryExpr),
    GenericFunction(Box<GenericFunctionExpr>),
    Function(FunctionExpr),
}

#[derive(Debug, Clone)]
pub struct RangeExpr {
    r#type: Type,
    value: String,
}

#[derive(Debug, Clone)]
pub enum IntervalExpr {
    Interval {
        value: Box<Expr>,
        part: IntervalPart,
    },
    IntervalRange {
        value: String,
        start_part: IntervalPart,
        end_part: IntervalPart,
    },
}

#[derive(Debug, Clone)]
pub enum IntervalPart {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

#[derive(Debug, Clone)]
pub enum FunctionExpr {
    // list of known functions here
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all
    ConcatFn(ConcatFnExpr),
    Cast(CastFnExpr),
    SafeCast(SafeCastFnExpr),
}

#[derive(Debug, Clone)]
pub struct CastFnExpr {
    expr: Box<Expr>,
    r#type: ParameterizedType,
    format: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct SafeCastFnExpr {
    expr: Box<Expr>,
    r#type: ParameterizedType,
    format: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct ConcatFnExpr {
    values: Vec<Expr>,
}

/// Generic function call, whose signature is not yet implemented in the parser
#[derive(Debug, Clone)]
pub struct GenericFunctionExpr {
    name: ParseToken,
    arguments: Vec<GenericFunctionExprArg>,
    over: Option<NamedWindowExpr>,
}

#[derive(Debug, Clone)]
pub struct GenericFunctionExprArg {
    arg: Expr,
    aggregate: Option<Aggregate>,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    distinct: bool,
    nulls: Option<AggregateFnNulls>,
    having: Option<AggregateHaving>,
    order_by: Option<Vec<AggregateOrderBy>>,
    limit: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct AggregateOrderBy {
    expr: Box<Expr>,
    sort_direction: Option<OrderAscDesc>,
}

#[derive(Debug, Clone)]
pub enum AggregateFnNulls {
    Ignore,
    Respect,
}

#[derive(Debug, Clone)]
pub struct AggregateHaving {
    expr: Box<Expr>,
    kind: AggregateHavingKind,
}

#[derive(Debug, Clone)]
pub enum AggregateHavingKind {
    Max,
    Min,
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
    Range(Box<Type>),
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
    pub window: Option<Window>,
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
    pub expr: Box<FromExpr>,
}

#[derive(Debug, Clone)]
pub enum FromExpr {
    Join(JoinExpr),
    FullJoin(JoinExpr),
    LeftJoin(JoinExpr),
    RightJoin(JoinExpr),
    CrossJoin(CrossJoinExpr),
    Path(FromPathExpr),
    Unnest(UnnestExpr),
    GroupingQuery(FromGroupingQueryExpr),
    GroupingFrom(GroupingFromExpr),
}

#[derive(Debug, Clone)]
pub struct CrossJoinExpr {
    pub left: Box<FromExpr>,
    pub right: Box<FromExpr>,
}

#[derive(Debug, Clone)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct JoinExpr {
    pub kind: JoinKind,
    pub left: Box<FromExpr>,
    pub right: Box<FromExpr>,
    pub cond: JoinCondition,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<ParseToken>),
}

#[derive(Debug, Clone)]
pub struct UnnestExpr {
    pub array: Box<Expr>,
    pub alias: Option<ParseToken>,
    pub with_offset: bool,
    pub offset_alias: Option<ParseToken>,
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

#[derive(Debug, Clone)]
pub struct Window {
    named_windows: Vec<NamedWindow>,
}

#[derive(Debug, Clone)]
pub struct WindowOrderByExpr {
    expr: Expr,
    asc_desc: Option<OrderAscDesc>,
}

#[derive(Debug, Clone)]
pub struct NamedWindow {
    name: ParseToken,
    expr: NamedWindowExpr,
}

#[derive(Debug, Clone)]
pub enum NamedWindowExpr {
    Named(ParseToken),
    Window(WindowExpr),
}

#[derive(Debug, Clone)]
pub struct WindowExpr {
    ref_window: Option<ParseToken>,
    partition_by: Option<Vec<Expr>>,
    order_by: Option<Vec<WindowOrderByExpr>>,
    frame: Option<WindowFrame>,
}

#[derive(Debug, Clone)]
pub struct WindowFrame {
    kind: WindowFrameKind,
    start: Option<FrameBound>,
    end: Option<FrameBound>,
}

#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    UnboundedFollowing,
    Following(i64),
    CurrentRow,
}

#[derive(Debug, Clone)]
pub enum WindowFrameKind {
    Range,
    Rows,
}

// TODO: this struct should own the scanner and use it
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

    fn report(&self, line: u32, col: u32, location: &str, message: &str) {
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
                TokenType::Merge => self.parse_merge_statement()?,
                TokenType::Identifier(non_reserved_keyword) => {
                    match non_reserved_keyword.to_lowercase().as_str() {
                        "insert" => self.parse_insert_statement()?,
                        "delete" => self.parse_delete_statement()?,
                        "update" => self.parse_update_statement()?,
                        "truncate" => self.parse_truncate_statement()?,
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
                expr: Box::new(self.parse_from_expr()?),
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
        self.consume(TokenTypeVariant::Merge)?;
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
    // ["WINDOW" window]
    fn parse_select(&mut self) -> anyhow::Result<Select> {
        self.consume(TokenTypeVariant::Select)?;
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
                self.error(self.peek(), "Expected `;`.");
                return Err(anyhow!(ParseError));
            }

            match self.parse_select_expr() {
                Ok(col_expr) => {
                    if self.source_tokens[last_position].kind != TokenType::Comma {
                        self.error(self.peek_prev(), "Expected `,`.");
                        return Err(anyhow!(ParseError));
                    }
                    select_exprs.push(col_expr);
                    comma_matched = self.match_token_type(TokenTypeVariant::Comma);
                    last_position = self.curr - (comma_matched as usize);
                }
                Err(_) => {
                    self.error(self.peek(), "Expected expression.");
                    return Err(anyhow!(ParseError));
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

        let window = if self.check_token_type(TokenTypeVariant::Window) {
            Some(self.parse_window()?)
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
            self.error(self.peek(), "Expected `ON` or `USING`.");
            return Err(anyhow!(ParseError));
        }
    }

    // TODO: add FROM UNNEST
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
                    query_expr: Box::new(query_expr),
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
                            query_expr: Box::new(parse_from_expr),
                        }))
                    }
                    _ => {
                        self.error(self.peek(), "Expected `JOIN`.");
                        Err(anyhow!(ParseError))
                    }
                }
            }
        } else if self.check_token_type(TokenTypeVariant::Unnest) {
            let unnest_expr = self.parse_unnest()?;
            Ok(FromExpr::Unnest(unnest_expr))
        } else {
            let path = self.parse_path()?;
            let alias = self.parse_as_alias()?;
            Ok(FromExpr::Path(FromPathExpr {
                path_expr: path,
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
            match self.peek_prev().kind {
                TokenType::Number(num) => {
                    let tok = self.consume_one_of(&[
                        TokenTypeVariant::Preceding,
                        TokenTypeVariant::Following,
                    ])?;
                    match tok.kind {
                        TokenType::Preceding => Some(FrameBound::Preceding(num as i64)),
                        TokenType::Following => Some(FrameBound::Following(num as i64)),
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
            return Ok(NamedWindowExpr::Named(name));
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
                    Some(OrderAscDesc::Asc)
                } else if self.match_token_type(TokenTypeVariant::Desc) {
                    Some(OrderAscDesc::Desc)
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

        Ok(NamedWindowExpr::Window(WindowExpr {
            ref_window,
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
                expr: named_window_expr,
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
                self.error(
                    &peek_token,
                    "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                     `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
                );
                Err(anyhow!(ParseError))
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
    fn parse_parameterized_bq_type(&mut self) -> anyhow::Result<ParameterizedType> {
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
        Ok(Expr::Function(FunctionExpr::ConcatFn(ConcatFnExpr {
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
        Ok(Expr::Function(FunctionExpr::Cast(CastFnExpr {
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
        Ok(Expr::Function(FunctionExpr::SafeCast(SafeCastFnExpr {
            expr,
            r#type,
            format,
        })))
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
                        TokenType::Ignore => AggregateFnNulls::Ignore,
                        TokenType::Respect => AggregateFnNulls::Respect,
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
                        "max" => AggregateHavingKind::Max,
                        "min" => AggregateHavingKind::Min,
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                let expr = self.parse_expr()?;
                Some(AggregateHaving {
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
                            TokenType::Asc => OrderAscDesc::Asc,
                            TokenType::Desc => OrderAscDesc::Desc,
                            _ => unreachable!(),
                        })
                    } else {
                        None
                    };
                    exprs.push(AggregateOrderBy {
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
                Some(Aggregate {
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
                arg: arg_expr,
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

    // primary_expr ->
    // "True" | "False" | "Null" | "Identifier" | "QuotedIdentifier" | "String" | "Number"
    // | NUMERIC "Number" | BIGNUMERIC "Number"
    // | DATE "String" | TIMESTAMP "String" | DATETIME "String" | TIME "String"
    // | "RANGE" "<" bq_parameterized_type ">" "String"
    // | interval_expr | json_expr
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
            TokenType::Struct => {
                return self.parse_struct_expr();
            }
            TokenType::LeftSquare | TokenType::Array => {
                return self.parse_array_expr();
            }
            TokenType::Range => self.parse_range_expr()?,
            TokenType::Interval => self.parse_interval_expr()?,
            TokenType::Identifier(ident) => {
                if self.peek_next_i(1).kind == TokenType::LeftParen {
                    return self.parse_function_expr();
                } else if ident.to_lowercase() == "date" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(date_str) => Expr::Date(date_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "timestamp" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(date_str) => Expr::Timestamp(date_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "datetime" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(date_str) => Expr::Datetime(date_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "time" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(date_str) => Expr::Time(date_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "numeric" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(num_str) => Expr::Numeric(num_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "bignumeric" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(num_str) => Expr::BigNumeric(num_str.clone()),
                        _ => unreachable!(),
                    }
                } else if ident.to_lowercase() == "json" {
                    self.advance();
                    let curr = self.consume(TokenTypeVariant::String)?;
                    match &curr.kind {
                        TokenType::String(json_str) => Expr::Json(json_str.clone()),
                        _ => unreachable!(),
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
            // Functions whose name is a reserved keyword
            TokenType::Cast => self.parse_cast_fn_expr()?,
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
