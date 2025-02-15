use core::panic;
use std::fmt::Display;

use anyhow::anyhow;

use crate::scanner::{Token, TokenLiteral, TokenType};

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
    pub fn literal(&self) -> Vec<&Option<TokenLiteral>> {
        match self {
            ParseToken::Single(token) => vec![&token.literal],
            ParseToken::Multiple(vec) => vec.iter().map(|el| &el.literal).collect(),
        }
    }
}

// TODO: create another file to place all these AST objects
#[derive(Debug, Clone)]
pub struct Query {
    pub statements: Vec<QueryStatement>,
}

#[derive(Debug, Clone)]
pub struct QueryStatement {
    pub query_expr: QueryExpr,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Binary(BinaryExpr),
    Unary(UnaryExpr),
    Grouping(GroupingExpr),
    Literal(LiteralExpr),
    Array(ArrayExpr),
    Query(QueryExpr),
}

#[derive(Debug, Clone)]
pub enum LiteralExpr {
    Identifier(String),
    QuotedIdentifier(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Star,
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
pub struct ArrayExpr {
    exprs: Vec<Expr>,
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
    // TODO: SelectQueryExpr
    pub with: Option<With>,
    pub select: Select, // TODO: SelectExpr
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
    count: LiteralExpr,
    offset: Option<LiteralExpr>,
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
    pub exprs: Vec<SelectColExpr>,
    pub from: Option<From>,
    pub r#where: Option<Where>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,
}

#[derive(Debug, Clone)]
pub enum SelectColExpr {
    Col(ColExpr),
    All,
}

#[derive(Debug, Clone)]
pub struct ColExpr {
    pub expr: Expr,
    pub alias: Option<ParseToken>,
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
    left: Box<FromExpr>,
    join: ParseToken,
    right: Box<FromExpr>,
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
    query_expr: Box<FromExpr>,
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

impl Expr {
    fn accept<T>(&self, visitor: &mut impl Visitor<T>) -> T {
        visitor.visit_expr(self)
    }
}

trait Visitor<T> {
    // TODO: mut or not? currently not
    fn visit_expr(&mut self, expr: &Expr) -> T;
}

pub struct AstPrinter;
impl Visitor<String> for AstPrinter {
    fn visit_expr(&mut self, expr: &Expr) -> String {
        match expr {
            Expr::Unary(unary_expr) => {
                self.parenthesize(&unary_expr.operator.lexeme(None), &[&unary_expr.right])
            }
            Expr::Binary(binary_expr) => self.parenthesize(
                &binary_expr.operator.lexeme(None),
                &[&binary_expr.left, &binary_expr.right],
            ),
            Expr::Grouping(grouping_expr) => self.parenthesize("group", &[&grouping_expr.expr]),
            Expr::Literal(literal_expr) => match literal_expr {
                LiteralExpr::Identifier(s) | LiteralExpr::QuotedIdentifier(s) => s.to_string(),
                LiteralExpr::String(s) => format!("\"{}\"", s),
                LiteralExpr::Number(num) => num.to_string(),
                LiteralExpr::Bool(bool) => bool.to_string(),
                LiteralExpr::Null => String::from("NULL"),
                LiteralExpr::Star => String::from("*"),
            },
            _ => {
                todo!()
            }
        }
    }
}
impl AstPrinter {
    pub fn print(&mut self, expr: &Expr) {
        println!("{}", self.visit_expr(expr));
    }
    fn parenthesize(&mut self, name: &str, exprs: &[&Expr]) -> String {
        let mut out = format!("({}", name);
        for expr in exprs {
            out += format!(" {}", &expr.accept(self)).as_str();
        }
        out += ")";
        out
    }
}

pub struct Parser<'a> {
    source_tokens: &'a Vec<Token>,
    curr: i32,
}

// TODO:
// - modify error handling (right now we are interested only in the last error printed, since errors my be printed when trying to match grammar rules e.g. see parse_select method)

// - check that identifiers are not reserved keywords

// - we are currently considering equality operators as left associative but they are not. In bigquery this is a valid expression only if you add parentheses (x < y) is False
//   we will fix this once we implement pratt parsing
impl<'a> Parser<'a> {
    pub fn new(tokens: &'a Vec<Token>) -> Parser<'a> {
        Self {
            source_tokens: tokens,
            curr: 0,
        }
    }

    pub fn parse(&mut self) -> anyhow::Result<Query> {
        self.parse_query()
    }

    fn peek_prev(&self) -> &Token {
        &self.source_tokens[(self.curr as usize) - 1]
    }

    fn peek(&self) -> &Token {
        &self.source_tokens[self.curr as usize]
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

    fn check_token_type(&self, target: TokenType) -> bool {
        self.peek().kind == target
    }

    fn match_token_type(&mut self, token_type: TokenType) -> bool {
        if self.check_token_type(token_type) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn match_token_types(&mut self, target_tokens: &[TokenType]) -> bool {
        for tok in target_tokens {
            if self.check_token_type(*tok) {
                self.advance();
                return true;
            }
        }
        false
    }

    fn consume(&mut self, token_type: TokenType, message: &str) -> anyhow::Result<&Token> {
        if self.check_token_type(token_type) {
            Ok(self.advance())
        } else {
            self.error(self.peek(), message);
            Err(anyhow!(ParseError))
        }
    }

    fn consume_one_of(
        &mut self,
        token_types: &[TokenType],
        message: &str,
    ) -> anyhow::Result<&Token> {
        for token_type in token_types {
            if self.check_token_type(*token_type) {
                return Ok(self.advance());
            }
        }
        self.error(self.peek(), message);
        Err(anyhow!(ParseError))
    }

    fn error(&self, token: &Token, message: &str) {
        if token.kind == TokenType::Eof {
            self.report(token.line, "at end", message);
        } else {
            self.report(token.line, &format!("at '{}'", token.lexeme), message);
        }
    }

    fn report(&self, line: i32, location: &str, message: &str) {
        println!("[line {}] Error {}: {}", line, location, message);
    }

    // TODO: add the other procedural language statements
    // query -> query_statement (; query_statement [";"])*
    fn parse_query(&mut self) -> anyhow::Result<Query> {
        let mut statements = vec![];
        statements.push(self.parse_query_statement()?);

        while self.match_token_type(TokenType::Semicolon) {
            if self.check_token_type(TokenType::Eof) {
                break;
            }
            statements.push(self.parse_query_statement()?);
        }

        self.consume(TokenType::Eof, "Expected EOF")?;
        Ok(Query { statements })
    }

    // query_statement -> query_expr
    fn parse_query_statement(&mut self) -> anyhow::Result<QueryStatement> {
        let query_expr = self.parse_query_expr()?;
        Ok(QueryStatement { query_expr })
    }

    // query_expr ->
    // ["WITH" with_expr] select | "(" query_expr ")"
    // select_query_expr (("UNION" [("ALL" | "Distinct")] | "Intersect" "Distinct" | "Except" "Distinct") select_query_expr)*
    // ["ORDER BY" order_by_expr]
    // ["LIMIT" limit_expr]
    fn parse_query_expr(&mut self) -> anyhow::Result<QueryExpr> {
        let with = if self.match_token_type(TokenType::With) {
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
                    let token = self.consume_one_of(
                        &[TokenType::All, TokenType::Distinct],
                        "Expected `ALL` or `DISTINCT`.",
                    )?;
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
                    set_operators.push(
                        self.consume(TokenType::Distinct, "Expected `DISTINCT`.")?
                            .clone(),
                    );
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

        let order_by = if self.match_token_types(&[TokenType::Order]) {
            self.consume(TokenType::By, "Expected `BY`.")?;
            Some(OrderBy {
                exprs: self.parse_order_by_expr()?,
            })
        } else {
            None
        };

        let limit = if self.match_token_type(TokenType::Limit) {
            let count = if let TokenLiteral::Number(num) = self
                .consume(TokenType::Number, "Expected Number.")?
                .literal
                .as_ref()
                .unwrap()
            {
                LiteralExpr::Number(*num)
            } else {
                unreachable!()
            };
            let offset = if self.match_token_type(TokenType::Offset) {
                if let TokenLiteral::Number(num) = self
                    .consume(TokenType::Number, "Expected Number")?
                    .literal
                    .as_ref()
                    .unwrap()
                {
                    Some(LiteralExpr::Number(*num))
                } else {
                    unreachable!()
                }
            } else {
                None
            };

            Some(Limit { count, offset })
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
        if self.match_token_type(TokenType::LeftParen) {
            let query_expr = self.parse_query_expr()?;
            self.consume(TokenType::RightParen, "Expected `)`.")?;
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
        let is_recursive = self.match_token_type(TokenType::Recursive);
        let mut ctes = vec![];
        loop {
            let cte_name = self
                .consume_one_of(
                    &[TokenType::Identifier, TokenType::QuotedIdentifier],
                    "Expected Identifier or QuotedIdentifier.",
                )?
                .clone();
            self.consume(TokenType::As, "Expected `AS`.")?;
            self.consume(TokenType::LeftParen, "Expected `(`.")?;
            ctes.push(self.parse_cte(&cte_name)?);

            if !self.match_token_type(TokenType::Comma) {
                break;
            }
        }
        Ok(With { ctes, is_recursive })
    }

    fn parse_cte(&mut self, name: &Token) -> anyhow::Result<Cte> {
        let cte_query = self.parse_query_expr()?;
        if self.match_token_type(TokenType::Union) {
            self.consume(TokenType::All, "Expected `ALL`.")?;
            let recursive_query = self.parse_query_expr()?;
            self.consume(TokenType::RightParen, "Expected `)`.")?;
            Ok(Cte::Recursive(RecursiveCte {
                name: ParseToken::Single(name.clone()),
                base_query: cte_query,
                recursive_query,
            }))
        } else {
            self.consume(TokenType::RightParen, "Expected `)`.")?;
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

            let asc_desc = if self.match_token_type(TokenType::Asc) {
                Some(OrderAscDesc::Asc)
            } else if self.match_token_type(TokenType::Desc) {
                Some(OrderAscDesc::Desc)
            } else {
                None
            };

            let nulls = if self.match_token_type(TokenType::Null) {
                match self
                    .consume_one_of(
                        &[TokenType::First, TokenType::Last],
                        "Expected `FIRST` or `LAST`.",
                    )?
                    .kind
                {
                    TokenType::First => Some(OrderNulls::First),
                    TokenType::Last => Some(OrderNulls::Last),
                    _ => {
                        unreachable!();
                    }
                }
            } else {
                None
            };

            order_by_exprs.push(OrderByExpr {
                expr,
                asc_desc,
                nulls,
            });

            if !self.match_token_type(TokenType::Comma) {
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
        self.consume(TokenType::Select, "Expected `SELECT`.")?;
        let mut select_exprs = vec![];
        let col_expr = self.parse_select_col_expr()?;
        select_exprs.push(col_expr);

        let mut comma_matched = self.match_token_type(TokenType::Comma);
        let mut last_position = self.curr - (comma_matched as i32);

        loop {
            // NOTE: this is needed to handle the trailing comma, we need to look ahead
            if self.check_token_type(TokenType::Eof)
                || self.check_token_type(TokenType::Semicolon)
                || self.check_token_type(TokenType::From)
                || self.check_token_type(TokenType::RightParen)
                || self.check_token_type(TokenType::Union)
                || self.check_token_type(TokenType::Intersect)
                || self.check_token_type(TokenType::Except)
            {
                break;
            }

            if self.check_token_type(TokenType::Select) {
                self.error(self.peek(), "Expected `;`.");
                return Err(anyhow!(ParseError));
            }

            match self.parse_select_col_expr() {
                Ok(col_expr) => {
                    if self.source_tokens[last_position as usize].kind != TokenType::Comma {
                        self.error(self.peek_prev(), "Expected `,`.");
                        return Err(anyhow!(ParseError));
                    }
                    select_exprs.push(col_expr);
                    comma_matched = self.match_token_type(TokenType::Comma);
                    last_position = self.curr - (comma_matched as i32);
                }
                Err(_) => {
                    self.error(self.peek(), "Expected expression.");
                    return Err(anyhow!(ParseError));
                }
            }
        }
        let from = if self.match_token_type(TokenType::From) {
            Some(crate::parser::From {
                exprs: vec![self.parse_from_expr()?],
            })
        } else {
            None
        };

        let r#where = if self.match_token_type(TokenType::Where) {
            Some(crate::parser::Where {
                expr: Box::new(self.parse_where_expr()?),
            })
        } else {
            None
        };

        let group_by = if self.match_token_type(TokenType::Group) {
            self.consume(TokenType::By, "Expected `BY`.")?;
            Some(GroupBy {
                expr: self.parse_group_by_expr()?,
            })
        } else {
            None
        };

        let having = if self.match_token_type(TokenType::Having) {
            Some(crate::parser::Having {
                expr: Box::new(self.parse_having_expr()?),
            })
        } else {
            None
        };

        let qualify = if self.match_token_type(TokenType::Qualify) {
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

    // TODO: add except .. replace
    // select_col_expr -> [expr.]"*" | expr [["AS"] "Identifier"]
    fn parse_select_col_expr(&mut self) -> anyhow::Result<SelectColExpr> {
        if self.match_token_type(TokenType::Star) {
            return Ok(SelectColExpr::All);
        }

        let expr = match self.parse_expr() {
            Err(_) => {
                self.error(self.peek(), "Expected Expression.");
                return Err(anyhow!(ParseError));
            }
            Ok(expr) => expr,
        };

        if self.match_token_type(TokenType::Dot) {
            self.consume(TokenType::Star, "Expected `*`.")?;
            Ok(SelectColExpr::All)
        } else {
            if self.match_token_type(TokenType::As) {
                let identifier = ParseToken::Single(
                    self.consume(TokenType::Identifier, "Expected Identifier.")?
                        .clone(),
                );
                return Ok(SelectColExpr::Col(ColExpr {
                    expr,
                    alias: Some(identifier),
                }));
            }
            if self.match_token_type(TokenType::Identifier) {
                let identifier = ParseToken::Single(self.peek_prev().clone());
                return Ok(SelectColExpr::Col(ColExpr {
                    expr,
                    alias: Some(identifier),
                }));
            }
            Ok(SelectColExpr::Col(ColExpr { expr, alias: None }))
        }
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
                    if self.check_token_type(TokenType::Inner) {
                        parse_tokens.push(curr_peek.clone());
                        self.advance();
                    }
                    parse_tokens.push(self.consume(TokenType::Join, "Expected `JOIN`.")?.clone());
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
                    if self.match_token_type(TokenType::Outer) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    parse_tokens.push(self.consume(TokenType::Join, "Expected `JOIN`.")?.clone());
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
                    if self.match_token_type(TokenType::Outer) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    parse_tokens.push(self.consume(TokenType::Join, "Expected `JOIN`.")?.clone());
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
                    parse_tokens.push(self.consume(TokenType::Join, "Expected `JOIN`.")?.clone());
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
        if self.match_token_type(TokenType::On) {
            let bool_expr = self.parse_expr()?;
            Ok(JoinCondition::On(bool_expr))
        } else if self.match_token_type(TokenType::Using) {
            let mut using_tokens = vec![];
            self.consume(TokenType::LeftParen, "Expected `(`.")?;
            let ident = self
                .consume_one_of(
                    &[TokenType::Identifier, TokenType::QuotedIdentifier],
                    "Expected Identifier or QuotedIdentifier.",
                )?
                .clone();
            using_tokens.push(ident);
            while self.match_token_type(TokenType::Comma) {
                let ident = self
                    .consume_one_of(
                        &[TokenType::Identifier, TokenType::QuotedIdentifier],
                        "Expected Identifier or QuotedIdentifier.",
                    )?
                    .clone();
                using_tokens.push(ident);
            }
            self.consume(TokenType::RightParen, "Expected `)`.")?;
            Ok(JoinCondition::Using(
                using_tokens.into_iter().map(ParseToken::Single).collect(),
            ))
        } else {
            self.error(self.peek(), "Expected `ON` or `USING`.");
            return Err(anyhow!(ParseError));
        }
    }

    // from_item_expr -> path [as_alias] | "(" query_expr ")" [as_alias] | "(" from_expr ")"
    fn parse_from_item_expr(&mut self) -> anyhow::Result<FromExpr> {
        if self.match_token_type(TokenType::LeftParen) {
            let curr = self.curr;
            // lookahead to check whether we can parse a query expr
            while self.peek().kind == TokenType::LeftParen {
                self.curr += 1;
            }
            if self.peek().kind == TokenType::Select {
                self.curr = curr;
                let query_expr = self.parse_query_expr()?;
                self.consume(TokenType::RightParen, "Expected `)`.")?;
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
                        self.consume(TokenType::RightParen, "Expected `)`.")?;
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

    fn parse_as_alias(&mut self) -> anyhow::Result<Option<&Token>> {
        if self.match_token_type(TokenType::As) {
            return Ok(Some(self.consume_one_of(
                &[TokenType::Identifier, TokenType::QuotedIdentifier],
                "Expected Identifier or QuotedIdentifier.",
            )?));
        }
        if self.match_token_types(&[TokenType::Identifier, TokenType::QuotedIdentifier]) {
            return Ok(Some(self.peek_prev()));
        }
        Ok(None)
    }

    // path -> path_expr ("." path_expr)*
    fn parse_path(&mut self) -> anyhow::Result<PathExpr> {
        let mut path_identifiers = vec![];
        let identifier = self.parse_path_expression()?;
        path_identifiers.extend(identifier);
        while self.match_token_type(TokenType::Dot) {
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
        path_expression_parts.push(
            self.consume_one_of(
                &[TokenType::Identifier, TokenType::QuotedIdentifier],
                "Expected Identifier or QuotedIdentifier.",
            )?
            .clone(),
        );

        while self.match_token_types(&[TokenType::Slash, TokenType::Colon, TokenType::Minus]) {
            path_expression_parts.push(self.peek_prev().clone());
            path_expression_parts.push(
                self.consume_one_of(
                    &[
                        TokenType::QuotedIdentifier,
                        TokenType::Identifier,
                        TokenType::Number,
                    ],
                    "Expected QuotedIdentifier, Identifier, or Number.",
                )?
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
        if self.match_token_type(TokenType::All) {
            Ok(GroupByExpr::All)
        } else {
            let mut items = vec![self.parse_expr()?];
            while self.match_token_type(TokenType::Comma) {
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
        token_types_to_match: &[TokenType],
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
        self.parse_standard_binary_expr(&[TokenType::Or], Self::parse_and_expr)
    }

    // and_expr -> not_expr ("AND" not_expr)*
    fn parse_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenType::And], Self::parse_not_expr)
    }

    // not_expr -> "NOT" not_expr | comparison_expr
    fn parse_not_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_type(TokenType::Not) {
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
                | TokenType::Like => {
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
                    if self.match_token_type(TokenType::Not) {
                        parse_tokens.push(self.peek_prev().clone());
                    }
                    let right_literal = self
                        .consume_one_of(
                            &[TokenType::Null, TokenType::True, TokenType::False],
                            "Expected `NULL`, `TRUE`, or `FALSE`.",
                        )?
                        .clone();
                    output = Expr::Binary(BinaryExpr {
                        left: Box::new(output),
                        operator: ParseToken::Multiple(parse_tokens),
                        right: match right_literal.kind {
                            TokenType::True => Box::new(Expr::Literal(LiteralExpr::Bool(true))),
                            TokenType::False => {
                                self.advance();
                                Box::new(Expr::Literal(LiteralExpr::Bool(false)))
                            }
                            TokenType::Null => {
                                self.advance();
                                Box::new(Expr::Literal(LiteralExpr::Null))
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
                        self.consume_one_of(
                            &[TokenType::In, TokenType::Between, TokenType::Like],
                            "Expected `BETWEEN`, `IN`, or `LIKE`.",
                        )?
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
        self.parse_standard_binary_expr(&[TokenType::BitwiseOr], Self::parse_bitwise_and_expr)
    }

    // bitwise_and_expr -> bitwise_shift_expr | bitwise_shift_expr ("&" bitwise_shift_expr)*
    fn parse_bitwise_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenType::BitwiseAnd], Self::parse_bitwise_shift_expr)
    }

    // bitwise_shift_expr -> add_expr | add_expr (("<<" | ">>") add_expr)*
    fn parse_bitwise_shift_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenType::BitwiseRightShift, TokenType::BitwiseLeftShift],
            Self::parse_add_expr,
        )
    }

    // add_expr -> mul_concat_expr | mul_concat_expr (("+" | "-") mul_concat_expr)*
    fn parse_add_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenType::Plus, TokenType::Minus],
            Self::parse_mul_concat_expr,
        )
    }

    // mul_concat_expr -> unary_expr | unary_expr (("*" | "/" | "||") unary_expr)*
    fn parse_mul_concat_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenType::Star, TokenType::Slash, TokenType::Concat],
            Self::parse_unary_expr,
        )
    }

    // unary_expr -> ("+" | "-" | "~") unary_expr | field_access_expr
    fn parse_unary_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_types(&[TokenType::Plus, TokenType::Minus, TokenType::BitwiseNot]) {
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

        while self.match_token_type(TokenType::Dot) {
            let operator = self.peek_prev().clone();
            if self.match_token_type(TokenType::Star) {
                return Ok(Expr::Binary(BinaryExpr {
                    left: Box::new(output),
                    operator: ParseToken::Single(operator),
                    right: Box::new(Expr::Literal(LiteralExpr::Star)),
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

        while self.match_token_type(TokenType::LeftSquare) {
            let left_paren = self.peek_prev().clone();
            let index = self.parse_expr()?;
            let right_paren = self
                .consume(TokenType::RightSquare, "Expected `]`.")?
                .clone();
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: ParseToken::Multiple(vec![left_paren, right_paren]),
                right: Box::new(index),
            });
        }
        Ok(output)
    }

    // array_expr -> "[" expr ("," expr)* "]"
    fn parse_array_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenType::LeftSquare, "Expected `[`.")?;
        let mut array_elements = vec![];
        array_elements.push(self.parse_expr()?);
        while self.match_token_type(TokenType::Comma) {
            array_elements.push(self.parse_expr()?);
        }
        self.consume(TokenType::RightSquare, "Expected `]`.")?;
        Ok(Expr::Array(ArrayExpr {
            exprs: array_elements,
        }))
    }

    // primary_expr -> "True" | "False" | "Null" | "Identifier" | "String" | "Number" | array_expr | "(" expression ")" | "(" query_expr ")"
    // where:
    // array_expr -> "[" array_elements "]"
    fn parse_primary_expr(&mut self) -> anyhow::Result<Expr> {
        let peek_token = self.peek().clone();
        let primary_expr = match peek_token.kind {
            TokenType::True => {
                self.advance();
                Expr::Literal(LiteralExpr::Bool(true))
            }
            TokenType::False => {
                self.advance();
                Expr::Literal(LiteralExpr::Bool(false))
            }
            TokenType::Null => {
                self.advance();
                Expr::Literal(LiteralExpr::Null)
            }
            TokenType::Identifier => {
                if let TokenLiteral::String(ident) = peek_token.literal.as_ref().unwrap() {
                    // TODO: refactor this?
                    self.advance();
                    Expr::Literal(LiteralExpr::Identifier(ident.to_string()))
                } else {
                    panic!("Found unexpected TokenLiteral for TokenType::Identifier.");
                }
            }
            TokenType::Number => {
                if let TokenLiteral::Number(num) = peek_token.literal.as_ref().unwrap() {
                    self.advance();
                    Expr::Literal(LiteralExpr::Number(*num))
                } else {
                    panic!("Found unexpected TokenLiteral for TokenType::Number.");
                }
            }
            TokenType::String => {
                if let TokenLiteral::String(str) = peek_token.literal.as_ref().unwrap() {
                    self.advance();
                    Expr::Literal(LiteralExpr::String(str.to_string()))
                } else {
                    panic!("Found unexpected TokenLiteral for TokenType::String.");
                }
            }
            TokenType::LeftParen => {
                self.advance();
                let curr_position = self.curr;
                // Look ahead to check whether we need to parse a query_expr or an expr
                if self.check_token_type(TokenType::With)
                    || self.check_token_type(TokenType::Select)
                {
                    self.curr = curr_position;
                    let query_expr = self.parse_query_expr()?;
                    self.consume(TokenType::RightParen, "Expected `)`.")?;
                    return Ok(Expr::Query(QueryExpr::Grouping(GroupingQueryExpr {
                        with: None,
                        query_expr: Box::new(query_expr),
                        order_by: None,
                        limit: None,
                    })));
                } else {
                    let expr = self.parse_expr()?;
                    self.consume(TokenType::RightParen, "Expected `)`.")?;
                    return Ok(Expr::Grouping(GroupingExpr {
                        expr: Box::new(expr),
                    }));
                }
            }
            TokenType::LeftSquare => {
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
