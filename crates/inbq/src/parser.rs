use anyhow::anyhow;
use rayon::prelude::*;
use strum::IntoDiscriminant;

use crate::ast::{
    ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, BinaryOperator,
    BytesConcatExpr, CallStatement, CaseExpr, CaseStatement, CaseWhenThenStatements,
    CastFunctionExpr, CastFunctionFormat, CoalesceFunctionExpr, ColumnSchema, ColumnSetToUnpivot,
    ColumnToUnpivot, ConcatFunctionExpr, CreateJsFunctionStatement, CreateSchemaStatement,
    CreateSqlFunctionStatement, CreateTableStatement, CreateViewStatement, CrossJoinExpr, Cte,
    CurrentDateFunctionExpr, CurrentDatetimeFunctionExpr, CurrentTimeFunctionExpr,
    DateDiffFunctionExpr, DateTruncFunctionExpr, DatetimeDiffFunctionExpr,
    DatetimeTruncFunctionExpr, DdlOption, DeclareVarStatement, DeleteStatement,
    DropFunctionStatement, DropTableStatement, ExecuteImmediateStatement,
    ExecuteImmediateUsingIdentifier, Expr, ExtractFunctionExpr, ExtractFunctionPart,
    ForInStatement, ForeignKeyConstraintNotEnforced, ForeignKeyReference, FrameBound, From,
    FromExpr, FromGroupingQueryExpr, FromPathExpr, FromUnnestExpr, FunctionAggregate,
    FunctionAggregateHaving, FunctionAggregateHavingKind, FunctionAggregateNulls,
    FunctionAggregateOrderBy, FunctionArgument, FunctionArgumentType, FunctionExpr,
    GenericFunctionExpr, GenericFunctionExprArg, Granularity, GroupBy, GroupByExpr, GroupingExpr,
    GroupingFromExpr, GroupingQueryExpr, Having, Identifier, IfBranch, IfFunctionExpr, IfStatement,
    InsertStatement, IntervalExpr, IntervalPart, JoinCondition, JoinExpr, JoinKind,
    LabeledStatement, LastDayFunctionExpr, LeftFunctionExpr, LikeQuantifier, Limit, LoopStatement,
    Merge, MergeInsert, MergeSource, MergeStatement, MergeUpdate, MultiColumnUnpivot, Name,
    NamedWindow, NamedWindowExpr, NonRecursiveCte, Number, OrderBy, OrderByExpr, OrderByNulls,
    OrderBySortDirection, ParameterizedType, PathName, PathPart, Pivot, PivotAggregate,
    PivotColumn, PrimaryKeyConstraintNotEnforced, Qualify, QuantifiedLikeExpr,
    QuantifiedLikeExprPattern, QueryExpr, QueryStatement, QuotedIdentifier, RaiseStatement,
    RangeExpr, RecursiveCte, RepeatStatement, RightFunctionExpr, SafeCastFunctionExpr, Select,
    SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr, SelectQueryExpr, SelectTableValue,
    SetQueryOperator, SetSelectQueryExpr, SetVarStatement, SetVariable, SingleColumnUnpivot,
    Statement, StatementsBlock, StringConcatExpr, StructExpr, StructField, StructFieldType,
    StructParameterizedFieldType, SystemVariable, TableConstraint, TableFunctionArgument,
    TableFunctionExpr, TableSample, TimeDiffFunctionExpr, TimeTruncFunctionExpr,
    TimestampDiffFunctionExpr, TimestampTruncFunctionExpr, Token, TokenType, TokenTypeVariant,
    TruncateStatement, Type, UnaryExpr, UnaryOperator, UnnestExpr, Unpivot, UnpivotKind,
    UnpivotNulls, UpdateItem, UpdateStatement, ViewColumn, WeekBegin, When, WhenMatched,
    WhenNotMatchedBySource, WhenNotMatchedByTarget, WhenThen, Where, WhileStatement, Window,
    WindowFrame, WindowFrameKind, WindowOrderByExpr, WindowSpec, With, WithExpr, WithExprVar,
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
        debug_assert!(self.curr > 0);
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

    fn check_token_types(&self, token_types: &[TokenTypeVariant]) -> bool {
        let peek_discriminant = self.peek().kind.discriminant();
        for tok in token_types {
            if peek_discriminant == *tok {
                return true;
            }
        }
        false
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
            TokenType::Identifier(ident) => ident.eq_ignore_ascii_case(value),
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

    #[allow(dead_code)]
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

    fn consume_identifier_into_name(&mut self) -> anyhow::Result<Name> {
        Ok(
            match &self
                .consume_one_of(&[
                    TokenTypeVariant::Identifier,
                    TokenTypeVariant::QuotedIdentifier,
                ])?
                .kind
            {
                TokenType::Identifier(ident) => Name::Identifier(Identifier {
                    name: ident.clone(),
                }),
                TokenType::QuotedIdentifier(qident) => Name::QuotedIdentifier(QuotedIdentifier {
                    name: qident.clone(),
                }),
                _ => unreachable!(),
            },
        )
    }

    fn consume_and_get_identifier(&mut self) -> anyhow::Result<&str> {
        Ok(match &self.consume_identifier()?.kind {
            TokenType::Identifier(ident) => ident,
            TokenType::QuotedIdentifier(qident) => qident,
            _ => unreachable!(),
        })
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

    /// Rule:
    /// ```text
    /// query -> statement (";" statement [";"])*
    /// ```
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

    /// Rule:
    /// ```text
    /// statement ->
    ///  create_table_statement | merge_statement | set_var_statement | if_statement
    ///  | case_statement | insert_statement | delete_statement | update_statement
    ///  | truncate_statement | declare_var_statement | begin_transaction | commit_transaction
    ///  | rollback_transaction  | raise_statement | drop_statement | call_statement | return_statement
    ///  | query_statement
    /// ```
    fn parse_statement(&mut self) -> anyhow::Result<Statement> {
        let is_labeled_statement = self.check_token_types(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ]) && self.peek_next_i(1).kind == TokenType::Colon;
        if is_labeled_statement {
            let peek = self.peek_next_i(2);
            let statement = match &peek.kind {
                TokenType::For => self.parse_for_in_statement()?,
                TokenType::Identifier(non_reserved_keyword) => {
                    match non_reserved_keyword.to_lowercase().as_str() {
                        "begin" => self.parse_statements_block()?,
                        "loop" => self.parse_loop_statement()?,
                        "repeat" => self.parse_repeat_statement()?,
                        "while" => self.parse_while_statement()?,
                        "break" => self.parse_break_statement()?,
                        "leave" => self.parse_leave_statement()?,
                        "continue" => self.parse_continue_statement()?,
                        "iterate" => self.parse_iterate_statement()?,
                        _ => {
                            return Err(anyhow!(self.error(
                                peek,
                                &format!(
                                    "Found unexpected keyword `{}`. No valid labeled statement begins with `{}`.",
                                    non_reserved_keyword,non_reserved_keyword
                                ),
                            )));
                        }
                    }
                }
                _ => self.parse_query_statement()?,
            };
            Ok(statement)
        } else {
            let peek = self.peek();
            let statement = match &peek.kind {
                TokenType::Create => {
                    let peek_one = self.peek_next_i(1);
                    let peek_two = self.peek_next_i(2);
                    let peek_three = self.peek_next_i(3);
                    let peek_four = self.peek_next_i(4);

                    match (
                        &peek_one.kind,
                        &peek_two.kind,
                        &peek_three.kind,
                        &peek_four.kind,
                    ) {
                        (TokenType::Identifier(ident), _, _, _)
                        | (_, _, TokenType::Identifier(ident), _)
                            if ident.eq_ignore_ascii_case("view") =>
                        {
                            self.parse_create_view_statement()?
                        }
                        (TokenType::Identifier(ident), _, _, _)
                        | (_, TokenType::Identifier(ident), _, _)
                        | (_, _, TokenType::Identifier(ident), _)
                        | (_, _, _, TokenType::Identifier(ident))
                            if ident.eq_ignore_ascii_case("function") =>
                        {
                            self.parse_create_function_statement()?
                        }
                        _ => match &peek_one.kind {
                            TokenType::Identifier(non_reserved_keyword) => {
                                match non_reserved_keyword.to_lowercase().as_str() {
                                    "schema" => self.parse_create_schema_statement()?,
                                    _ => self.parse_create_table_statement()?,
                                }
                            }
                            _ => self.parse_create_table_statement()?,
                        },
                    }
                }
                TokenType::Merge => self.parse_merge_statement()?,
                TokenType::Set => self.parse_set_var_statement()?,
                TokenType::If => self.parse_if_statement()?,
                TokenType::Case => self.parse_case_statement()?,
                TokenType::For => self.parse_for_in_statement()?,
                TokenType::Identifier(non_reserved_keyword) => {
                    match non_reserved_keyword.to_lowercase().as_str() {
                        "insert" => self.parse_insert_statement()?,
                        "delete" => self.parse_delete_statement()?,
                        "update" => self.parse_update_statement()?,
                        "truncate" => self.parse_truncate_statement()?,
                        "declare" => self.parse_declare_var_statement()?,
                        "begin" => {
                            self.advance();
                            if self.match_non_reserved_keyword("transaction") {
                                return Ok(Statement::BeginTransaction);
                            }
                            self.curr -= 1;
                            self.parse_statements_block()?
                        }
                        "commit" => {
                            self.advance();
                            self.consume_non_reserved_keyword("transaction")?;
                            return Ok(Statement::CommitTransaction);
                        }
                        "rollback" => {
                            self.advance();
                            self.consume_non_reserved_keyword("transaction")?;
                            return Ok(Statement::RollbackTransaction);
                        }
                        "raise" => self.parse_raise_statement()?,
                        "drop" => self.parse_drop_statement()?,
                        "call" => self.parse_call_statement()?,
                        "execute" => self.parse_execute_immediate_statement()?,
                        "loop" => self.parse_loop_statement()?,
                        "repeat" => self.parse_repeat_statement()?,
                        "while" => self.parse_while_statement()?,
                        "break" => self.parse_break_statement()?,
                        "leave" => self.parse_leave_statement()?,
                        "continue" => self.parse_continue_statement()?,
                        "iterate" => self.parse_iterate_statement()?,
                        "return" => {
                            self.advance();
                            return Ok(Statement::Return);
                        }
                        _ => {
                            return Err(anyhow!(self.error(
                                peek,
                                &format!(
                                    "Found unexpected keyword `{}`. No valid statement begins with `{}`.",
                                    non_reserved_keyword,non_reserved_keyword
                                ),
                            )));
                        }
                    }
                }
                _ => self.parse_query_statement()?,
            };
            Ok(statement)
        }
    }

    /// Rule:
    /// ```text
    /// case_statement -> "CASE" expr "WHEN" expr "THEN" statements ("," "WHEN" expr "THEN" statements)* ["ELSE" statements] "END" "CASE"
    /// where:
    /// statements -> statement (";" statement)*
    /// ```
    fn parse_case_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Case)?;

        let case = if self.check_token_type(TokenTypeVariant::When) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        let mut when_thens = vec![];

        while self.match_token_type(TokenTypeVariant::When) {
            let when = self.parse_expr()?;
            self.consume(TokenTypeVariant::Then)?;
            let mut statements = vec![];
            loop {
                statements.push(self.parse_statement()?);
                self.consume(TokenTypeVariant::Semicolon)?;
                if self.check_token_types(&[
                    TokenTypeVariant::When,
                    TokenTypeVariant::Else,
                    TokenTypeVariant::End,
                ]) {
                    break;
                }
            }
            when_thens.push(CaseWhenThenStatements {
                when,
                then: statements,
            });
        }

        let r#else = if self.match_token_type(TokenTypeVariant::Else) {
            let mut statements = vec![];
            loop {
                statements.push(self.parse_statement()?);
                self.consume(TokenTypeVariant::Semicolon)?;
                if self.match_token_type(TokenTypeVariant::End) {
                    break;
                }
            }
            Some(statements)
        } else {
            None
        };

        self.consume(TokenTypeVariant::Case)?;

        Ok(Statement::Case(CaseStatement {
            case,
            when_thens,
            r#else,
        }))
    }

    /// Rule:
    /// ```text
    /// loop_statement -> [label] "LOOP" statement (";" statement)* "END" "LOOP" [label]
    /// ```
    fn parse_loop_statement(&mut self) -> anyhow::Result<Statement> {
        let start_label = self.parse_start_label()?;
        self.consume_non_reserved_keyword("loop")?;
        let mut statements = vec![];
        loop {
            statements.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
            if self.match_token_type(TokenTypeVariant::End) {
                break;
            }
        }

        self.consume_non_reserved_keyword("loop")?;

        let end_label = self.parse_end_label(&start_label)?;
        let loop_statement = Statement::Loop(LoopStatement { statements });
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(loop_statement),
                start_label,
                end_label,
            }))
        } else {
            Ok(loop_statement)
        }
    }

    /// Rule:
    /// ```text
    /// repeat_statement -> [label] "REPEAT" statement (";" statement)* "UNTIL" expr "END" "REPEAT" [label]
    /// ```
    fn parse_repeat_statement(&mut self) -> anyhow::Result<Statement> {
        let start_label = self.parse_start_label()?;
        self.consume_non_reserved_keyword("repeat")?;
        let mut statements = vec![];
        loop {
            statements.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
            if self.match_non_reserved_keyword("until") {
                break;
            }
        }
        let until = self.parse_expr()?;
        self.consume(TokenTypeVariant::End)?;
        self.consume_non_reserved_keyword("repeat")?;
        let end_label = self.parse_end_label(&start_label)?;

        let repeat_statement = Statement::Repeat(RepeatStatement { statements, until });
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(repeat_statement),
                start_label,
                end_label,
            }))
        } else {
            Ok(repeat_statement)
        }
    }

    /// Rule:
    /// ```text
    /// while_statement -> [label] "WHILE" expr "DO" statement (";" statement)* "END" "WHILE" [label]
    /// ```
    fn parse_while_statement(&mut self) -> anyhow::Result<Statement> {
        let start_label = self.parse_start_label()?;
        self.consume_non_reserved_keyword("while")?;
        let condition = self.parse_expr()?;
        self.consume_non_reserved_keyword("do")?;

        let mut statements = vec![];
        loop {
            statements.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
            if self.match_token_type(TokenTypeVariant::End) {
                break;
            }
        }
        self.consume_non_reserved_keyword("while")?;
        let end_label = self.parse_end_label(&start_label)?;

        let while_statement = Statement::While(WhileStatement {
            condition,
            statements,
        });
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(while_statement),
                start_label,
                end_label,
            }))
        } else {
            Ok(while_statement)
        }
    }

    /// Rule:
    /// ```text
    /// for_in_statement -> [label] "FOR" var_name "IN" "(" query_expr ")" "DO" statement (";" statement)* "END" "FOR" [label]
    /// ```
    fn parse_for_in_statement(&mut self) -> anyhow::Result<Statement> {
        let start_label = self.parse_start_label()?;
        self.consume(TokenTypeVariant::For)?;
        let var_name = self.consume_identifier_into_name()?;
        self.consume(TokenTypeVariant::In)?;

        self.consume(TokenTypeVariant::LeftParen)?;
        let table_expr = self.parse_query_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;

        self.consume_non_reserved_keyword("do")?;

        let mut statements = vec![];
        loop {
            statements.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
            if self.match_token_type(TokenTypeVariant::End) {
                break;
            }
        }
        self.consume(TokenTypeVariant::For)?;
        let end_label = self.parse_end_label(&start_label)?;

        let for_in_statement = Statement::ForIn(ForInStatement {
            var_name,
            table_expr,
            statements,
        });
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(for_in_statement),
                start_label,
                end_label,
            }))
        } else {
            Ok(for_in_statement)
        }
    }

    /// Rule:
    /// ```text
    /// break_statement -> "BREAK" [label]
    /// ```
    fn parse_break_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("break")?;
        let start_label = self.parse_label()?;
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(Statement::Break),
                start_label,
                end_label: None,
            }))
        } else {
            Ok(Statement::Break)
        }
    }

    /// Rule:
    /// ```text
    /// leave_statement -> "LEAVE" [label]
    /// ```
    fn parse_leave_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("leave")?;
        let start_label = self.parse_label()?;
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(Statement::Leave),
                start_label,
                end_label: None,
            }))
        } else {
            Ok(Statement::Leave)
        }
    }

    /// Rule:
    /// ```text
    /// continue_statement -> "CONTINUE" [label]
    /// ```
    fn parse_continue_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("continue")?;
        let start_label = self.parse_label()?;
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(Statement::Continue),
                start_label,
                end_label: None,
            }))
        } else {
            Ok(Statement::Continue)
        }
    }

    /// Rule:
    /// ```text
    /// iterate_statement -> "ITERATE" [label]
    /// ```
    fn parse_iterate_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("iterate")?;
        let start_label = self.parse_label()?;
        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(Statement::Iterate),
                start_label,
                end_label: None,
            }))
        } else {
            Ok(Statement::Iterate)
        }
    }

    /// Rule:
    /// ```text
    /// call_statement -> "CALL" path "(" expr ("," expr)* ")"
    /// ```
    fn parse_call_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("call")?;
        let procedure_name = self.parse_path()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut arguments = vec![];
        loop {
            arguments.push(self.parse_expr()?);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Statement::Call(CallStatement {
            procedure_name,
            arguments,
        }))
    }

    /// Rule:
    /// ```text
    /// execute_immediate_statement ->
    /// "EXECUTE" "IMMEDIATE" expr ["INTO" var_name ("," var_name)*] ["USING" (var_name | expr) ["AS" alias] ("," (var_name | expr) ["AS" alias])*]
    /// ```
    fn parse_execute_immediate_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("execute")?;
        self.consume_non_reserved_keyword("immediate")?;
        let sql = self.parse_expr()?;
        let into_vars = if self.match_token_type(TokenTypeVariant::Into) {
            let mut vars = vec![];
            loop {
                vars.push(self.consume_identifier_into_name()?);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(vars)
        } else {
            None
        };
        let using_identifiers = if self.match_token_type(TokenTypeVariant::Using) {
            let mut identifiers = vec![];
            loop {
                let identifier = self.parse_expr()?;
                let alias = self.parse_as_alias()?;
                identifiers.push(ExecuteImmediateUsingIdentifier { identifier, alias });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(identifiers)
        } else {
            None
        };
        Ok(Statement::ExecuteImmediate(ExecuteImmediateStatement {
            sql,
            into_vars,
            using_identifiers,
        }))
    }

    /// Rule:
    /// ```text
    /// raise_statement -> "RAISE" ["USING" "MESSAGE" "=" expr]
    /// ```
    fn parse_raise_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("raise")?;
        let message = if self.match_token_type(TokenTypeVariant::Using) {
            self.consume_non_reserved_keyword("message")?;
            self.consume(TokenTypeVariant::Equal)?;
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Raise(RaiseStatement { message }))
    }

    /// Rule:
    /// ```text
    /// if_statement -> "IF" expr "THEN" statements ["ELSEIF" expr "THEN" statements]* ["ELSE" statements] "END" "IF"
    /// where:
    /// statements -> statement (";" statement)*
    /// ```
    fn parse_if_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::If)?;
        let condition = self.parse_expr()?;
        self.consume(TokenTypeVariant::Then)?;
        let mut statements = vec![];
        loop {
            statements.push(self.parse_statement()?);
            self.consume(TokenTypeVariant::Semicolon)?;
            if self.check_token_types(&[TokenTypeVariant::Else, TokenTypeVariant::End])
                || self.check_non_reserved_keyword("elseif")
            {
                break;
            }
        }

        let r#if = IfBranch {
            condition,
            statements,
        };

        let mut else_ifs = vec![];
        while self.match_non_reserved_keyword("elseif") {
            let condition = self.parse_expr()?;
            self.consume(TokenTypeVariant::Then)?;
            let mut statements = vec![];
            loop {
                statements.push(self.parse_statement()?);
                self.consume(TokenTypeVariant::Semicolon)?;
                if self.check_token_types(&[TokenTypeVariant::Else, TokenTypeVariant::End]) {
                    break;
                }
            }
            else_ifs.push(IfBranch {
                condition,
                statements,
            });
        }

        let else_ifs = if else_ifs.is_empty() {
            None
        } else {
            Some(else_ifs)
        };

        let r#else = if self.match_token_type(TokenTypeVariant::Else) {
            let mut statements = vec![];
            loop {
                statements.push(self.parse_statement()?);
                self.consume(TokenTypeVariant::Semicolon)?;
                if self.check_token_type(TokenTypeVariant::End) {
                    break;
                }
            }
            Some(statements)
        } else {
            None
        };

        self.consume(TokenTypeVariant::End)?;
        self.consume(TokenTypeVariant::If)?;
        Ok(Statement::If(IfStatement {
            r#if,
            else_ifs,
            r#else,
        }))
    }

    /// Rule:
    /// ```text
    /// drop_statement -> drop_table_statement
    /// ```
    fn parse_drop_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("drop")?;

        let peek = self.peek();
        let statement = match &peek.kind {
            TokenType::Identifier(non_reserved_keyword) => {
                match non_reserved_keyword.to_lowercase().as_str() {
                    "table" => self.parse_drop_table_statement()?,
                    "function" => self.parse_drop_function_statement()?,
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

    /// Rule:
    /// ```text
    /// drop_table_statement -> "DROP" "TABLE" ["IF" "EXISTS"] table_name
    /// ```
    fn parse_drop_table_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("table")?;

        let if_exists = if self.match_token_type(TokenTypeVariant::If) {
            self.consume(TokenTypeVariant::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_path()?;

        Ok(Statement::DropTableStatement(DropTableStatement {
            name,
            if_exists,
        }))
    }

    /// Rule:
    /// ```text
    /// drop_function_statement -> "DROP" "FUNCTION" ["IF" "EXISTS"] function_name
    /// ```
    fn parse_drop_function_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("function")?;

        let if_exists = if self.match_token_type(TokenTypeVariant::If) {
            self.consume(TokenTypeVariant::Exists)?;
            true
        } else {
            false
        };

        let name = self.parse_path()?;

        Ok(Statement::DropFunctionStatement(DropFunctionStatement {
            name,
            if_exists,
        }))
    }

    fn parse_label(&mut self) -> anyhow::Result<Option<Name>> {
        if self.check_token_types(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ]) {
            return Ok(Some(self.consume_identifier_into_name()?));
        }
        Ok(None)
    }

    /// Rule:
    /// ```text
    /// ("QuotedIdentifier" | "Identifier") ":"
    /// ```
    fn parse_start_label(&mut self) -> anyhow::Result<Option<Name>> {
        if self.check_token_types(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ]) && self.peek_next_i(1).kind == TokenType::Colon
        {
            let name = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::Colon)?;
            return Ok(Some(name));
        }
        Ok(None)
    }

    /// Rule:
    /// ```text
    /// ("QuotedIdentifier" | "Identifier")
    /// ```
    fn parse_end_label(&mut self, start_label: &Option<Name>) -> anyhow::Result<Option<Name>> {
        if start_label.is_some()
            && self.check_token_types(&[
                TokenTypeVariant::Identifier,
                TokenTypeVariant::QuotedIdentifier,
            ])
        {
            return Ok(Some(self.consume_identifier_into_name()?));
        }
        Ok(None)
    }

    /// Rule:
    /// ```text
    /// statements_block -> [label] "BEGIN" [statement (";" statement)*] ["EXCEPTION" "WHEN" "ERROR" "THEN"] [statement (";" statement)*] "END" [label]
    /// ```
    fn parse_statements_block(&mut self) -> anyhow::Result<Statement> {
        let start_label = self.parse_start_label()?;
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

        let end_label = self.parse_end_label(&start_label)?;
        let block_statement = Statement::Block(StatementsBlock {
            statements,
            exception_statements,
        });

        if let Some(start_label) = start_label {
            Ok(Statement::Labeled(LabeledStatement {
                statement: Box::new(block_statement),
                start_label,
                end_label,
            }))
        } else {
            Ok(block_statement)
        }
    }

    /// Rule:
    /// ```text
    /// foreign_key_constraint -> "FOREIGN" "KEY" "(" name ("," name)* ")" "REFERENCES" path_name "(" "(" name ("," name)* ")" ")" "NOT" "ENFORCED"
    /// ```
    fn parse_foreign_key_constraint(
        &mut self,
        name: Option<Name>,
    ) -> anyhow::Result<TableConstraint> {
        self.consume_non_reserved_keyword("foreign")?;
        self.consume_non_reserved_keyword("key")?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut columns = vec![];
        loop {
            columns.push(self.consume_identifier_into_name()?);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;
        self.consume_non_reserved_keyword("references")?;
        let reference_table = self.parse_path()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut reference_columns = vec![];
        loop {
            reference_columns.push(self.consume_identifier_into_name()?);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;
        self.consume(TokenTypeVariant::Not)?;
        self.consume_non_reserved_keyword("enforced")?;
        let reference = ForeignKeyReference {
            table: reference_table,
            columns: reference_columns,
        };

        Ok(TableConstraint::ForeignKeyNotEnforced(
            ForeignKeyConstraintNotEnforced {
                name,
                columns,
                reference,
            },
        ))
    }

    /// Rule:
    /// ```text
    /// table_constraint -> "PRIMARY" "KEY" "(" name ("," name)* ")" "NOT" "ENFORCED" | foreign_key_constraint
    /// ```
    fn parse_table_constraint(&mut self) -> anyhow::Result<TableConstraint> {
        Ok(
            match &self
                .consume_one_of_non_reserved_keywords(&["primary", "foreign", "constraint"])?
                .kind
            {
                TokenType::Identifier(ident) | TokenType::QuotedIdentifier(ident) => {
                    match ident.to_lowercase().as_str() {
                        "primary" => {
                            self.consume_non_reserved_keyword("key")?;
                            self.consume(TokenTypeVariant::LeftParen)?;
                            let mut columns = vec![];
                            loop {
                                columns.push(self.consume_identifier_into_name()?);
                                if !self.match_token_type(TokenTypeVariant::Comma) {
                                    break;
                                }
                            }
                            self.consume(TokenTypeVariant::RightParen)?;
                            self.consume(TokenTypeVariant::Not)?;
                            self.consume_non_reserved_keyword("enforced")?;
                            TableConstraint::PrimaryKeyNotEnforced(
                                PrimaryKeyConstraintNotEnforced { columns },
                            )
                        }
                        "foreign" => {
                            self.curr -= 1;
                            self.parse_foreign_key_constraint(None)?
                        }
                        "constraint" => {
                            let name = self.consume_identifier_into_name()?;
                            self.parse_foreign_key_constraint(Some(name))?
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            },
        )
    }

    /// Rule:
    /// ```text
    /// CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] TABLE [ IF NOT EXISTS ] table_name
    /// ["(" (column | table_constraint) ("," (column | table_constraint)*) ")"]
    /// ["DEFAULT" "COLLATE" expr]
    /// ["PARTITION" "BY" expr]
    /// ["CLUSTER" "BY" name ("," name)*]
    /// ["WITH CONNECTION" path_name]
    /// ["AS" query_statement]
    /// where:
    /// column -> column_name parameterized_bq_type ("," column_name parameterized_bq_type)*
    /// ```
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

        let name = self.parse_path()?;

        let (schema, constraints) = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut column_schema = vec![];
            let mut constraints = vec![];
            loop {
                let is_constraint = matches!(&self.peek_next_i(1).kind, TokenType::Identifier(ident) if ident.eq_ignore_ascii_case("key"));
                let is_constraint = is_constraint
                    || matches!(&self.peek_next_i(3).kind, TokenType::Identifier(ident) if ident.eq_ignore_ascii_case("key"));

                if is_constraint {
                    constraints.push(self.parse_table_constraint()?);
                } else {
                    let col_name = self.consume_identifier_into_name()?;
                    let col_type = self.parse_parameterized_bq_type()?;
                    column_schema.push(ColumnSchema {
                        name: col_name,
                        r#type: col_type,
                    });
                }

                let match_comma = self.match_token_type(TokenTypeVariant::Comma);

                if !match_comma {
                    break;
                }

                if self.check_token_type(TokenTypeVariant::RightParen) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            let column_schema = if column_schema.is_empty() {
                None
            } else {
                Some(column_schema)
            };
            let constraints = if constraints.is_empty() {
                None
            } else {
                Some(constraints)
            };
            (column_schema, constraints)
        } else {
            (None, None)
        };

        let default_collate = if self.match_token_type(TokenTypeVariant::Default) {
            self.consume(TokenTypeVariant::Collate)?;
            Some(self.parse_expr()?)
        } else {
            None
        };

        let partition = if self.match_token_type(TokenTypeVariant::Partition) {
            self.consume(TokenTypeVariant::By)?;
            Some(self.parse_expr()?)
        } else {
            None
        };

        let clustering_columns = if self.match_non_reserved_keyword("cluster") {
            self.consume(TokenTypeVariant::By)?;
            let mut columns = vec![];
            loop {
                columns.push(self.consume_identifier_into_name()?);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            Some(columns)
        } else {
            None
        };

        let connection = if self.match_token_type(TokenTypeVariant::With) {
            self.consume_non_reserved_keyword("connection")?;
            Some(self.parse_path()?)
        } else {
            None
        };

        let query = if self.match_token_type(TokenTypeVariant::As) {
            Some(self.parse_query_expr()?)
        } else {
            None
        };

        let options = self.parse_ddl_options()?;

        Ok(Statement::CreateTable(Box::new(CreateTableStatement {
            name,
            schema,
            constraints,
            default_collate,
            partition,
            clustering_columns,
            connection,
            options,
            replace,
            is_temporary,
            if_not_exists,
            query,
        })))
    }

    /// Rule:
    /// ```text
    /// ddl_options -> "OPTIONS" "(" name "=" expr ("," name "=" expr)* ")"
    /// ```
    fn parse_ddl_options(&mut self) -> anyhow::Result<Option<Vec<DdlOption>>> {
        Ok(if self.match_non_reserved_keyword("options") {
            self.consume(TokenTypeVariant::LeftParen)?;
            let mut options = vec![];
            loop {
                let name = self.consume_identifier_into_name()?;
                self.consume(TokenTypeVariant::Equal)?;
                let value = self.parse_expr()?;
                options.push(DdlOption { name, value });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Some(options)
        } else {
            None
        })
    }

    /// Rule:
    /// ```text
    /// "CREATE" ["OR" "REPLACE"] "VIEW" ["IF" "NOT" "EXISTS"] path_name [name [ddl_options] ("," [name [ddl_options])*
    /// [ddl_options] "AS" query_expr
    /// ```
    fn parse_create_view_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Create)?;
        let replace = self.match_token_type(TokenTypeVariant::Or);
        if replace {
            self.consume_non_reserved_keyword("replace")?;
        }

        self.consume_non_reserved_keyword("view")?;
        let if_not_exists = self.match_token_type(TokenTypeVariant::If);
        if if_not_exists {
            self.consume(TokenTypeVariant::Not)?;
            self.consume(TokenTypeVariant::Exists)?;
        }

        let name = self.parse_path()?;

        let columns = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut columns = vec![];
            loop {
                let name = self.consume_identifier_into_name()?;
                let options = self.parse_ddl_options()?;
                columns.push(ViewColumn { name, options });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Some(columns)
        } else {
            None
        };

        let options = self.parse_ddl_options()?;
        self.consume(TokenTypeVariant::As)?;
        let query = self.parse_query_expr()?;
        Ok(Statement::CreateView(CreateViewStatement {
            replace,
            if_not_exists,
            name,
            columns,
            options,
            query,
        }))
    }

    /// Rule:
    /// ```text
    /// CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] FUNCTION [ IF NOT EXISTS ] function_name
    /// args
    /// ["RETURNS" type]
    /// [("DETERMINISTIC" | "NOT" "DETERMINISTIC")]
    /// ["LANGUAGE" "js"]
    /// [ddl_options]
    /// "AS" expr
    /// where:
    /// args -> arg_name (type | ANY TYPE) ("," arg_name (type | ANY TYPE))*
    /// ```
    fn parse_create_function_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Create)?;
        let replace = self.match_token_type(TokenTypeVariant::Or);
        if replace {
            self.consume_non_reserved_keyword("replace")?;
        }

        let is_temporary =
            self.match_non_reserved_keyword("temp") || self.match_non_reserved_keyword("temporary");
        self.consume_non_reserved_keyword("function")?;

        let if_not_exists = self.match_token_type(TokenTypeVariant::If);
        if if_not_exists {
            self.consume(TokenTypeVariant::Not)?;
            self.consume(TokenTypeVariant::Exists)?;
        }

        let name = self.parse_path()?;

        let arguments = {
            self.consume(TokenTypeVariant::LeftParen)?;
            if self.match_token_type(TokenTypeVariant::RightParen) {
                vec![]
            } else {
                let mut arguments = vec![];
                loop {
                    let arg_name = self.consume_identifier_into_name()?;
                    let arg_ty = if self.match_token_type(TokenTypeVariant::Any) {
                        self.consume_non_reserved_keyword("type")?;
                        FunctionArgumentType::AnyType
                    } else {
                        FunctionArgumentType::Standard(self.parse_bq_type()?)
                    };
                    arguments.push(FunctionArgument {
                        name: arg_name,
                        r#type: arg_ty,
                    });
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                self.consume(TokenTypeVariant::RightParen)?;
                arguments
            }
        };

        let returns = if self.match_non_reserved_keyword("returns") {
            Some(self.parse_bq_type()?)
        } else {
            None
        };

        let is_deterministic = if self.match_token_type(TokenTypeVariant::Not) {
            self.consume_non_reserved_keyword("deterministic")?;
            Some(false)
        } else if self.match_non_reserved_keyword("deterministic") {
            Some(true)
        } else {
            None
        };

        let language = if self.match_non_reserved_keyword("language") {
            self.consume_non_reserved_keyword("js")?;
            Some(())
        } else {
            None
        };

        let options = self.parse_ddl_options()?;

        self.consume(TokenTypeVariant::As)?;

        let curr = self.peek().clone();
        let body = self.parse_expr()?;
        if language.is_some() {
            match body {
                Expr::String(_) | Expr::RawString(_) => {}
                _ => {
                    return Err(anyhow!(
                        self.error(&curr, "Javascript UDF must be a string literal.")
                    ));
                }
            }
        }

        Ok(if language.is_some() {
            if returns.is_none() {
                return Err(anyhow!(self.error(
                    &curr,
                    "Return type is required when creating a new Javascript UDF."
                )));
            }
            Statement::CreateJsFunction(CreateJsFunctionStatement {
                replace,
                is_temporary,
                if_not_exists,
                name,
                arguments,
                returns: returns.unwrap(),
                is_deterministic,
                options,
                body,
            })
        } else {
            Statement::CreateSqlFunction(CreateSqlFunctionStatement {
                replace,
                is_temporary,
                if_not_exists,
                name,
                arguments,
                returns,
                options,
                body,
            })
        })
    }

    /// Rule:
    /// ```text
    /// create_schema_statement ->
    /// "CREATE" "SCHEMA" ["IF" "NOT" "EXISTS"] path_name ["DEFAULT" "COLLATE" expr] [ddl_options]
    /// ```
    fn parse_create_schema_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Create)?;
        self.consume_non_reserved_keyword("schema")?;
        let if_not_exists = if self.match_token_type(TokenTypeVariant::If) {
            self.consume(TokenTypeVariant::Not)?;
            self.consume(TokenTypeVariant::Exists)?;
            true
        } else {
            false
        };
        let name = self.parse_path()?;
        let default_collate = if self.match_token_type(TokenTypeVariant::Default) {
            self.consume(TokenTypeVariant::Collate)?;
            Some(self.parse_expr()?)
        } else {
            None
        };
        let options = self.parse_ddl_options()?;
        Ok(Statement::CreateSchema(CreateSchemaStatement {
            name,
            if_not_exists,
            default_collate,
            options,
        }))
    }

    /// Rule:
    /// ```text
    /// query_statement -> query_expr
    /// ```
    fn parse_query_statement(&mut self) -> anyhow::Result<Statement> {
        let query_expr = self.parse_query_expr()?;
        Ok(Statement::Query(QueryStatement { query: query_expr }))
    }

    /// Rule:
    /// ```text
    /// declare_var_statement -> "DECLARE" var_name ("," var_name)* [bq_parameterized_type] ["DEFAULT" expr]
    /// ```
    fn parse_declare_var_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("declare")?;

        let mut var_names = vec![];
        loop {
            let var_name = self.consume_identifier_into_name()?;
            var_names.push(var_name);

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
            vars: var_names,
            r#type,
            default,
        }))
    }

    /// Rule:
    /// ```text
    /// set_var_statement -> "SET" (var_name = expr | "(" var_name ("," var_name)* ")" = "(" expr ("," expr)* ")"
    /// ```
    fn parse_set_var_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Set)?;

        let mut vars: Vec<SetVariable>;
        let mut exprs: Vec<Expr>;
        if self.match_token_type(TokenTypeVariant::LeftParen) {
            // multiple vars
            vars = vec![];
            loop {
                let var = match &self
                    .consume_one_of(&[
                        TokenTypeVariant::Identifier,
                        TokenTypeVariant::QuotedIdentifier,
                        TokenTypeVariant::SystemVariable,
                    ])?
                    .kind
                {
                    TokenType::Identifier(ident) => {
                        SetVariable::UserVariable(Name::Identifier(Identifier {
                            name: ident.clone(),
                        }))
                    }
                    TokenType::QuotedIdentifier(ident) => {
                        SetVariable::UserVariable(Name::QuotedIdentifier(QuotedIdentifier {
                            name: ident.clone(),
                        }))
                    }
                    TokenType::SystemVariable(sysvar) => {
                        SetVariable::SystemVariable(SystemVariable {
                            name: sysvar.clone(),
                        })
                    }
                    _ => unreachable!(),
                };
                vars.push(var);

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
            let var = match &self
                .consume_one_of(&[
                    TokenTypeVariant::Identifier,
                    TokenTypeVariant::QuotedIdentifier,
                    TokenTypeVariant::SystemVariable,
                ])?
                .kind
            {
                TokenType::Identifier(ident) => {
                    SetVariable::UserVariable(Name::Identifier(Identifier {
                        name: ident.clone(),
                    }))
                }
                TokenType::QuotedIdentifier(ident) => {
                    SetVariable::UserVariable(Name::QuotedIdentifier(QuotedIdentifier {
                        name: ident.clone(),
                    }))
                }
                TokenType::SystemVariable(sysvar) => SetVariable::SystemVariable(SystemVariable {
                    name: sysvar.clone(),
                }),
                _ => unreachable!(),
            };
            vars = vec![var];
            self.consume(TokenTypeVariant::Equal)?;
            exprs = vec![self.parse_expr()?];
        }

        Ok(Statement::SetVar(SetVarStatement { vars, exprs }))
    }

    /// Rule:
    /// ```text
    /// insert_statement -> "INSERT" ["INTO"] path ["(" column_name ("," column_name)* ")"] input
    /// where:
    /// input -> query_expr | "VALUES" "(" expr ")" ("(" expr ")")*
    /// column_name -> "Identifier" | "QuotedIdentifier"
    /// ```
    fn parse_insert_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("insert")?;
        self.match_token_type(TokenTypeVariant::Into);
        let table = self.parse_path()?;
        let columns = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut columns = vec![];
            loop {
                let column_name = self.consume_identifier_into_name()?;
                columns.push(column_name);
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

    /// Rule:
    /// ```text
    /// delete_statement -> "DELETE" ["FROM"] path ["AS"] [alias] "WHERE" expr
    /// ```
    fn parse_delete_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("delete")?;
        self.match_token_type(TokenTypeVariant::From);
        let table = self.parse_path()?;
        let alias = self.parse_as_alias()?;
        self.consume(TokenTypeVariant::Where)?;
        let cond = self.parse_expr()?;
        Ok(Statement::Delete(DeleteStatement { table, alias, cond }))
    }

    /// Rule:
    /// ```text
    /// update statement -> "UPDATE" path ["AS"] [alias] SET set_clause ["FROM" from_expr] "WHERE" expr
    /// where:
    /// set_clause = ("Identifier" | "QuotedIdentifier") "=" expr ("," ("Identifier" | "QuotedIdentifier") "=" expr)*
    /// ```
    fn parse_update_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("update")?;
        let table = self.parse_path()?;
        let alias = self.parse_as_alias()?;
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_field_access_expr()?;
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

        let from = self.parse_from()?;

        self.consume(TokenTypeVariant::Where)?;
        let where_expr = self.parse_where_expr()?;

        Ok(Statement::Update(Box::new(UpdateStatement {
            table,
            alias,
            update_items,
            from,
            r#where: Where {
                expr: Box::new(where_expr),
            },
        })))
    }

    /// Rule:
    /// ```text
    /// truncate_statement -> "TRUNCATE" "TABLE" path
    /// ```
    fn parse_truncate_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume_non_reserved_keyword("truncate")?;
        self.consume_non_reserved_keyword("table")?;
        let table = self.parse_path()?;
        Ok(Statement::Truncate(TruncateStatement { table }))
    }

    /// Rule:
    /// ```text
    /// merge_statement -> "MERGE" ["INTO"] path ["AS"] [alias] "USING" path "ON" merge_condition (when_clause)+
    /// where:
    /// when_clause -> matched_clause | not_matched_by_target_clause | not_matched_by_source_clause
    /// matched_clause -> "WHEN" "MATCHED" ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    /// not_matched_by_target_clause -> "WHEN" "NOT" "MATCHED" ["BY" "TARGET"] ["AND" merge_search_condition] "THEN" (merge_update | merge_delete)
    /// ```
    fn parse_merge_statement(&mut self) -> anyhow::Result<Statement> {
        self.consume(TokenTypeVariant::Merge)?;
        self.match_token_type(TokenTypeVariant::Into);
        let target_table = self.parse_path()?;
        let target_alias = self.parse_as_alias()?;
        self.consume(TokenTypeVariant::Using)?;
        let source = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let subquery = MergeSource::Subquery(self.parse_query_expr()?);
            self.consume(TokenTypeVariant::RightParen)?;
            subquery
        } else {
            MergeSource::Table(self.parse_path()?)
        };
        let source_alias = self.parse_as_alias()?;
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

    /// Rule:
    /// ```text
    /// merge_update -> "UPDATE" "SET" update_item ("," update_item)*
    /// where:
    /// update_item -> ("Identifier" | "QuotedIdentifier") "=" expr
    /// ```
    fn parse_merge_update(&mut self) -> anyhow::Result<Merge> {
        self.consume_non_reserved_keyword("update")?;
        self.consume(TokenTypeVariant::Set)?;
        let mut update_items = vec![];
        loop {
            let column_path = self.parse_field_access_expr()?;
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

    /// Rule:
    /// ```text
    /// merge_insert -> "INSERT" "ROW" | "INSERT" [(" column ("," column)*] ")"] "VALUES" "(" expr ("," expr)* ")"
    /// where:
    /// columns -> "Identifier" | "QuotedIdentifier"
    /// ```
    fn parse_merge_insert(&mut self) -> anyhow::Result<Merge> {
        self.consume_non_reserved_keyword("insert")?;
        if self.match_non_reserved_keyword("row") {
            return Ok(Merge::InsertRow);
        }

        let columns = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut columns = vec![];
            loop {
                let column_name = self.consume_identifier_into_name()?;
                columns.push(column_name);
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

    /// Rule:
    /// ```text
    /// merge_search_condition -> ["AND" expr]
    /// ```
    fn parse_merge_search_condition(&mut self) -> anyhow::Result<Option<Expr>> {
        let expr = if self.match_token_type(TokenTypeVariant::And) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(expr)
    }

    /// Rule:
    /// ```text
    /// query_expr ->
    /// ["WITH" with_expr] select | "(" query_expr ")"
    /// select_query_expr (("UNION" [("ALL" | "Distinct")] | "Intersect" "Distinct" | "Except" "Distinct") select_query_expr)*
    /// ["ORDER BY" order_by_expr]
    /// ["LIMIT" limit_expr]
    /// ```
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
                TokenType::Number(num) => Expr::Number(Number { value: num.clone() }),
                _ => unreachable!(),
            };

            let offset = if self.match_non_reserved_keyword("offset") {
                let tok = self.consume(TokenTypeVariant::Number)?;
                match &tok.kind {
                    TokenType::Number(num) => {
                        Some(Box::new(Expr::Number(Number { value: num.clone() })))
                    }
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

    /// Rule:
    /// ```text
    /// select_query_expr -> select | "(" query_expr ")"
    /// ```
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
            Ok(QueryExpr::Select(Box::new(SelectQueryExpr {
                with: None,
                order_by: None,
                select,
                limit: None,
            })))
        }
    }

    /// Rule:
    /// ```text
    /// with_expr -> ["RECURSIVE"] (recursive_cte | non_recursive_cte) ("," (recursive_cte | non_recursive_cte))*
    /// where:
    /// non_recursive_cte -> ("Identifier" | "QuotedIdentifier") AS "(" query_expr ")"
    /// recursive_cte -> ("Identifier" | "QuotedIdentifier") AS "(" query_expr "UNION" "ALL" query_expr ")"
    /// ```
    fn parse_with_expr(&mut self) -> anyhow::Result<With> {
        self.match_token_type(TokenTypeVariant::Recursive);
        let mut ctes = vec![];
        loop {
            let cte_name = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::As)?;
            self.consume(TokenTypeVariant::LeftParen)?;
            ctes.push(self.parse_cte(cte_name)?);

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        Ok(With { ctes })
    }

    fn parse_cte(&mut self, name: Name) -> anyhow::Result<Cte> {
        let cte_query = self.parse_query_expr()?;
        if self.match_token_type(TokenTypeVariant::Union) {
            self.consume(TokenTypeVariant::All)?;
            let recursive_query = self.parse_query_expr()?;
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(Cte::Recursive(RecursiveCte {
                name,
                base_query: cte_query,
                recursive_query,
            }))
        } else {
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(Cte::NonRecursive(NonRecursiveCte {
                name,
                query: cte_query,
            }))
        }
    }

    /// Rule:
    /// ```text
    /// order_by_expr -> order_by_expr_item [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")] ("," order_by_expr_item [("NULLS" "FIRST" | "NULLS" "LAST")])*
    /// ```
    fn parse_order_by_expr(&mut self) -> anyhow::Result<Vec<OrderByExpr>> {
        let mut order_by_exprs = vec![];

        loop {
            let expr = self.parse_expr()?;

            let sort_direction = if self.match_token_type(TokenTypeVariant::Asc) {
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
                sort_direction,
                nulls,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        Ok(order_by_exprs)
    }

    /// Rule:
    /// ```text
    /// select ->
    /// "SELECT"
    /// [("ALL" | "DISTINCT")]
    /// ["AS" ("STRUCT" | "VALUE")]
    /// select_col_expr [","] (select_col_expr [","])*
    /// [from]
    /// ["WHERE" where_expr]
    /// ["GROUP BY" group_by_expr]
    /// ["HAVING" having_expr]
    /// ["QUALIFY" qualify_expr]
    /// ["WINDOW" window]
    /// ```
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

            if self.match_token_type(TokenTypeVariant::Select) {
                return Err(anyhow!(self.error(self.peek(), "Expected `;`.")));
            }

            let curr = self.curr;

            match self.parse_select_expr() {
                Ok(col_expr) => {
                    if self.source_tokens[last_position].kind != TokenType::Comma {
                        self.curr = curr + 1;
                        return Err(anyhow!(self.error(self.peek_prev(), "Expected `,`.")));
                    }
                    select_exprs.push(col_expr);
                    comma_matched = self.match_token_type(TokenTypeVariant::Comma);
                    last_position = self.curr - (comma_matched as usize);
                }
                Err(_) => {
                    return Err(anyhow!(self.error(self.peek(), "Expected Expression.")));
                }
            }
        }
        let from = self.parse_from()?;

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

    /// Rule:
    /// ```text
    /// from -> "FROM" from_expr [pivot | unpivot] [tablesample]
    /// ```
    fn parse_from(&mut self) -> anyhow::Result<Option<From>> {
        Ok(if self.match_token_type(TokenTypeVariant::From) {
            let from_expr = self.parse_from_expr()?;

            let (pivot, unpivot) = if self.check_non_reserved_keyword("pivot") {
                (Some(self.parse_pivot()?), None)
            } else if self.check_non_reserved_keyword("unpivot") {
                (None, Some(self.parse_unpivot()?))
            } else {
                (None, None)
            };

            let table_sample = self.parse_tablesample()?;

            Some(crate::parser::From {
                expr: Box::new(from_expr),
                pivot,
                unpivot,
                table_sample,
            })
        } else {
            None
        })
    }

    /// Rule:
    /// ```text
    /// pivot -> "PIVOT" "(" aggregates "FOR" ("Identifier" | "QuotedIdentifier") "IN" "(" pivot_columns ")" ")" [as alias]
    /// where
    /// aggregates -> expr [as_alias] ("," expr [as_alias])*
    /// pivot_columns -> expr [as_alias] ("," expr [as_alias])*
    /// ```
    fn parse_pivot(&mut self) -> anyhow::Result<Pivot> {
        self.consume_non_reserved_keyword("pivot")?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut aggregates = vec![];
        loop {
            let aggregate_expr = self.parse_expr()?;
            let aggregate_alias = self.parse_as_alias()?;

            aggregates.push(PivotAggregate {
                expr: aggregate_expr,
                alias: aggregate_alias,
            });
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }

        self.consume(TokenTypeVariant::For)?;
        let input_column = self.consume_identifier_into_name()?;
        self.consume(TokenTypeVariant::In)?;

        self.consume(TokenTypeVariant::LeftParen)?;
        let mut pivot_columns = vec![];
        loop {
            let col_expr = self.parse_expr()?;
            let col_alias = self.parse_as_alias()?;
            pivot_columns.push(PivotColumn {
                expr: col_expr,
                alias: col_alias,
            });
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;

        self.consume(TokenTypeVariant::RightParen)?;

        let alias = self.parse_as_alias()?;

        Ok(Pivot {
            aggregates,
            input_column,
            pivot_columns,
            alias,
        })
    }

    /// Rule:
    /// ```text
    /// unpivot -> "UNPIVOT" [("INCLUDE" | "EXCLUDE") "NULLS"] "(" (single_col_unpivot | multi_col_unpivot) ")" [as_alias]
    /// where
    /// single_col_unpivot -> ("Identifier" | "QuotedIdentifier") "FOR" ("Identifier" | "QuotedIdentifier") "IN" "(" ("Identifier" | "QuotedIdentifier") ["AS"] expr ")"
    /// multi_col_unpivot -> ("Identifier" | "QuotedIdentifier") ("," ("Identifier" | "QuotedIdentifier"))* "FOR" ("Identifier" | "QuotedIdentifier") "IN"  "(" column_set_to_unpivot ("," column_set_to_unpivot)* ")"
    /// where
    /// column_set_to_unpivot -> "(" ("Identifier" | "QuotedIdentifier") ("," ("Identifier" | "QuotedIdentifier"))* ["AS"] expr ")"
    /// ```
    fn parse_unpivot(&mut self) -> anyhow::Result<Unpivot> {
        self.consume_non_reserved_keyword("unpivot")?;
        let nulls = if self.check_non_reserved_keyword("include") {
            self.consume(TokenTypeVariant::Nulls)?;
            UnpivotNulls::Include
        } else if self.check_non_reserved_keyword("exclude") {
            self.consume(TokenTypeVariant::Nulls)?;
            UnpivotNulls::Exclude
        } else {
            UnpivotNulls::Exclude
        };

        self.consume(TokenTypeVariant::LeftParen)?;
        let kind = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut values_columns = vec![];
            loop {
                values_columns.push(self.consume_identifier_into_name()?);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            self.consume(TokenTypeVariant::For)?;
            let name_column = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::In)?;
            self.consume(TokenTypeVariant::LeftParen)?;
            let mut column_sets_to_unpivot = vec![];
            loop {
                self.consume(TokenTypeVariant::LeftParen)?;
                let mut names = vec![];
                loop {
                    names.push(self.consume_identifier_into_name()?);
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                self.consume(TokenTypeVariant::RightParen)?;

                let alias = if self.match_token_type(TokenTypeVariant::As) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                column_sets_to_unpivot.push(ColumnSetToUnpivot { names, alias });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            UnpivotKind::MultiColumn(MultiColumnUnpivot {
                values_columns,
                name_column,
                column_sets_to_unpivot,
            })
        } else {
            let values_column = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::For)?;
            let name_column = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::In)?;
            self.consume(TokenTypeVariant::LeftParen)?;
            let mut columns_to_unpivot = vec![];
            loop {
                let name = self.consume_identifier_into_name()?;
                let alias = if self.match_token_type(TokenTypeVariant::As) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                columns_to_unpivot.push(ColumnToUnpivot { name, alias });
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;

            UnpivotKind::SingleColumn(SingleColumnUnpivot {
                values_column,
                name_column,
                columns_to_unpivot,
            })
        };
        self.consume(TokenTypeVariant::RightParen)?;
        let alias = self.parse_as_alias()?;

        Ok(Unpivot { nulls, kind, alias })
    }

    /// Rule:
    /// ```text
    /// tablesample -> "TABLESAMPLE" "SYSTEM" "(" expr "PERCENT" ")"
    /// ```
    fn parse_tablesample(&mut self) -> anyhow::Result<Option<TableSample>> {
        if !self.match_token_type(TokenTypeVariant::Tablesample) {
            return Ok(None);
        }
        self.consume_non_reserved_keyword("system")?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let percent = self.parse_expr()?;
        self.consume_non_reserved_keyword("percent")?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Some(TableSample { percent }))
    }

    /// Rule:
    /// ```text
    /// select_expr -> [expr.]"*" [except] | expr [["AS"] "Identifier"]
    /// ```
    fn parse_select_expr(&mut self) -> anyhow::Result<SelectExpr> {
        // TODO: add replace
        if self.match_token_type(TokenTypeVariant::Star) {
            let except = self.parse_except()?;
            return Ok(SelectExpr::All(SelectAllExpr { except }));
        }

        let expr = self.parse_expr()?;

        if self.peek_prev().kind == TokenType::Star {
            let except = self.parse_except()?;
            return Ok(SelectExpr::ColAll(SelectColAllExpr { expr, except }));
        }

        let alias = self.parse_as_alias()?;
        Ok(SelectExpr::Col(SelectColExpr { expr, alias }))
    }

    /// Rule:
    /// ```text
    /// except -> "EXCEPT" "(" ("Identifier" | "QuotedIdentifier") ["," ("Identifier" | "QuotedIdentifier")]* ")"
    /// ```
    fn parse_except(&mut self) -> anyhow::Result<Option<Vec<Name>>> {
        if !self.match_token_type(TokenTypeVariant::Except) {
            return Ok(None);
        }

        self.consume(TokenTypeVariant::LeftParen)?;

        let mut except_columns = vec![];
        let column = self.consume_identifier_into_name()?;
        except_columns.push(column);
        while self.match_token_type(TokenTypeVariant::Comma) {
            except_columns.push(self.consume_identifier_into_name()?);
        }
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Some(except_columns))
    }

    /// Rule:
    /// ```text
    /// from_expr -> from_item_expr (cross_join_op from_item_expr | cond_join_op from_item_expr cond)*
    /// where:
    /// cross_join_op -> "CROSS" "JOIN" | ","
    /// cond_join_op -> (["INNER"] "JOIN" | "FULL" ["OUTER"] "JOIN" | "LEFT" ["OUTER"] "JOIN" | "RIGHT" ["OUTER"] "JOIN")
    /// cond -> ("ON" expr | "USING" "(" ("Identifier" | "QuotedIdentifier") ("," ("Identifier" | "QuotedIdentifier"))*) ")")
    /// ```
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

    /// Rule:
    /// ```text
    /// cond -> ("ON" expr | "USING" "(" ("Identifier" | "QuotedIdentifier") ("," ("Identifier" | "QuotedIdentifier"))*) ")")
    /// ```
    fn parse_cond(&mut self) -> anyhow::Result<JoinCondition> {
        if self.match_token_type(TokenTypeVariant::On) {
            let bool_expr = self.parse_expr()?;
            Ok(JoinCondition::On(bool_expr))
        } else if self.match_token_type(TokenTypeVariant::Using) {
            let mut using_columns = vec![];
            self.consume(TokenTypeVariant::LeftParen)?;
            let ident = self.consume_identifier_into_name()?;
            using_columns.push(ident);
            while self.match_token_type(TokenTypeVariant::Comma) {
                let ident = self.consume_identifier_into_name()?;
                using_columns.push(ident);
            }
            self.consume(TokenTypeVariant::RightParen)?;
            Ok(JoinCondition::Using {
                columns: using_columns,
            })
        } else {
            return Err(anyhow!(
                self.error(self.peek(), "Expected `ON` or `USING`.")
            ));
        }
    }

    fn parse_from_item_alias(&mut self) -> anyhow::Result<Option<Name>> {
        Ok(
            if self.check_non_reserved_keyword("pivot")
                || self.check_non_reserved_keyword("unpivot")
            {
                None
            } else {
                self.parse_as_alias()?
            },
        )
    }

    /// Rule:
    /// ```text
    /// from_item_expr ->
    /// path [as_alias] ["FOR" "SYSTEM_TIME" "AS" "OF" expr] | "(" query_expr ")" [as_alias] | "(" from_expr ")" | unnest_operator
    /// | path "(" ("TABLE" path | expr) ("," ("TABLE" path | expr))* ")"
    /// ```
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
                let alias = self.parse_from_item_alias()?;
                Ok(FromExpr::GroupingQuery(FromGroupingQueryExpr {
                    query: Box::new(query_expr),
                    alias,
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
            let unnest_expr = self.parse_from_unnest()?;
            Ok(FromExpr::Unnest(unnest_expr))
        } else {
            let path = self.parse_path()?;

            if self.match_token_type(TokenTypeVariant::LeftParen) {
                let mut arguments = vec![];
                loop {
                    let argument = if self.match_non_reserved_keyword("table") {
                        TableFunctionArgument::Table(self.parse_path()?)
                    } else {
                        TableFunctionArgument::Expr(self.parse_expr()?)
                    };
                    arguments.push(argument);
                    if !self.match_token_type(TokenTypeVariant::Comma) {
                        break;
                    }
                }
                self.consume(TokenTypeVariant::RightParen)?;
                let alias = self.parse_from_item_alias()?;
                Ok(FromExpr::TableFunction(TableFunctionExpr {
                    name: path,
                    arguments,
                    alias,
                }))
            } else {
                let alias = self.parse_from_item_alias()?;
                let system_time = if self.match_token_type(TokenTypeVariant::For) {
                    self.consume_non_reserved_keyword("system_time")?;
                    self.consume(TokenTypeVariant::As)?;
                    self.consume(TokenTypeVariant::Of)?;
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Ok(FromExpr::Path(FromPathExpr {
                    path,
                    alias,
                    system_time,
                }))
            }
        }
    }

    /// Rule:
    /// ```text
    /// from_unnest -> "UNNEST" "(" expr ")"
    /// ```
    fn parse_unnest(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Unnest)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let array = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Unnest(UnnestExpr {
            array: Box::new(array),
        }))
    }

    /// Rule:
    /// ```text
    /// from_unnest -> ("UNNEST" "(" expr ")" [as_alias] | array_path [as alias]) ["WITH" "OFFSET" [as_alias]]
    /// where:
    /// array_path -> expr
    /// ```
    fn parse_from_unnest(&mut self) -> anyhow::Result<FromUnnestExpr> {
        let array = if self.match_token_type(TokenTypeVariant::Unnest) {
            self.consume(TokenTypeVariant::LeftParen)?;
            let array = self.parse_expr()?;
            self.consume(TokenTypeVariant::RightParen)?;
            array
        } else {
            self.parse_expr()?
        };
        let alias = self.parse_as_alias()?;
        let has_offset = self.match_token_type(TokenTypeVariant::With);
        let offset_alias = if has_offset {
            self.consume_non_reserved_keyword("offset")?;
            self.parse_as_alias()?
        } else {
            None
        };

        Ok(FromUnnestExpr {
            array: Box::new(array),
            alias,
            with_offset: has_offset,
            offset_alias,
        })
    }

    /// Rule:
    /// ```text
    /// as_alias -> ["AS"] ("Identifier" | "QuotedIdentifier")
    /// ```
    fn parse_as_alias(&mut self) -> anyhow::Result<Option<Name>> {
        if self.match_token_type(TokenTypeVariant::As) || self.check_identifier() {
            return Ok(Some(self.consume_identifier_into_name()?));
        }
        Ok(None)
    }

    /// Rule:
    /// ```text
    /// path -> path_part ("." path_part)*
    /// ```
    fn parse_path(&mut self) -> anyhow::Result<PathName> {
        let mut parts = vec![];
        let path_parts = self.parse_path_part()?;
        parts.extend(path_parts);
        while self.match_token_type(TokenTypeVariant::Dot) {
            parts.push(PathPart::DotSeparator);
            parts.extend(self.parse_path_part()?);
        }
        Ok(PathName::from(parts))
    }

    /// Rule:
    /// ```text
    /// path_part -> first_part (("/" | ":" | "-") subsequent_part)*
    /// where:
    /// first_part -> ("QuotedIdentifier" | "Identifier")
    /// subsequent_part -> ("QuotedIdentifier" | "Identifier" | "Number")
    /// ```
    fn parse_path_part(&mut self) -> anyhow::Result<Vec<PathPart>> {
        let mut path_parts = vec![];

        match &self.consume_identifier()?.kind {
            TokenType::Identifier(ident) => path_parts.push(PathPart::Identifier(Identifier {
                name: ident.clone(),
            })),
            TokenType::QuotedIdentifier(qident) => {
                path_parts.push(PathPart::QuotedIdentifier(QuotedIdentifier {
                    name: qident.clone(),
                }))
            }
            _ => unreachable!(),
        }

        while self.match_token_types(&[
            TokenTypeVariant::Slash,
            TokenTypeVariant::Colon,
            TokenTypeVariant::Minus,
        ]) {
            match &self.peek_prev().kind {
                TokenType::Slash => path_parts.push(PathPart::SlashSeparator),
                TokenType::Colon => path_parts.push(PathPart::ColonSeparator),
                TokenType::Minus => path_parts.push(PathPart::DashSeparator),
                _ => unreachable!(),
            }

            match &self
                .consume_one_of(&[
                    TokenTypeVariant::QuotedIdentifier,
                    TokenTypeVariant::Identifier,
                    TokenTypeVariant::Number,
                ])?
                .kind
            {
                TokenType::Identifier(ident) => path_parts.push(PathPart::Identifier(Identifier {
                    name: ident.clone(),
                })),
                TokenType::QuotedIdentifier(qident) => {
                    path_parts.push(PathPart::QuotedIdentifier(QuotedIdentifier {
                        name: qident.clone(),
                    }))
                }
                TokenType::Number(num) => {
                    path_parts.push(PathPart::Number(Number { value: num.clone() }))
                }
                _ => unreachable!(),
            }
        }

        Ok(path_parts)
    }

    /// Rule:
    /// ```text
    /// where_expr -> expr
    /// ```
    fn parse_where_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    /// Rule:
    /// ```text
    /// group_by_expr -> "ALL" | group_by_items
    /// where:
    /// group_by_items -> expr ("," expr)*
    /// ```
    fn parse_group_by_expr(&mut self) -> anyhow::Result<GroupByExpr> {
        // TODO: other group by expressions
        if self.match_token_type(TokenTypeVariant::All) {
            Ok(GroupByExpr::All)
        } else {
            let mut exprs = vec![self.parse_expr()?];
            while self.match_token_type(TokenTypeVariant::Comma) {
                exprs.push(self.parse_expr()?);
            }
            Ok(GroupByExpr::Items { exprs })
        }
    }

    /// Rule:
    /// ```text
    /// having_expr -> expr
    /// ```
    fn parse_having_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    /// Rule:
    /// ```text
    /// qualify_expr -> expr
    /// ```
    fn parse_qualify_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_expr()
    }

    /// Rule:
    /// ```text
    /// frame_bound -> ("UNBOUNDED" "PRECEDING" | "UNBOUNDED" "FOLLOWING" | "Number" "PRECEDING" | "Number" "FOLLOWING" | "CURRENT" "ROW")
    /// ```
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

    /// Rule:
    /// ```text
    /// window_frame -> ("ROWS" | "RANGE") (frame_start | frame_between)
    /// where:
    /// frame_start -> frame_bound
    /// frame_between -> "BETWEEN" frame_bound "AND" frame_bound
    /// ```
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
                .ok_or_else(|| anyhow!("Expected one of: `UNBOUNDED`, `Number`, `CURRENT`"))?;
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

    /// Rule:
    /// ```text
    /// named_window_expr -> ((Identifier" | "QuotedIdentifier") |  [("Identifier" | "QuotedIdentifier")] [partition_by] [order_by] [frame])
    /// where:
    /// partition_by -> "PARTITION" "BY" expr ("," expr)*
    /// order_by -> "ORDER" "BY" expr [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")] ("," expr [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")])*
    /// frame -> window_frame
    /// ```
    fn parse_named_window_expr(&mut self) -> anyhow::Result<NamedWindowExpr> {
        if !self.match_token_type(TokenTypeVariant::LeftParen) {
            let name = self.consume_identifier_into_name()?;
            return Ok(NamedWindowExpr::Reference(name));
        }
        let ref_window = if self.check_identifier() {
            Some(self.consume_identifier_into_name()?)
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

                let sort_direction = if self.match_token_type(TokenTypeVariant::Asc) {
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

                order_by_exprs.push(WindowOrderByExpr {
                    expr,
                    sort_direction,
                    nulls,
                });

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

    /// Rule:
    /// ```text
    /// window -> "WINDOW" ("Identifier" | "QuotedIdentifier") "AS" named_window_expr ("," ("Identifier" | "QuotedIdentifier") "AS" named_window_expr)*
    /// ```
    fn parse_window(&mut self) -> anyhow::Result<Window> {
        self.consume(TokenTypeVariant::Window)?;
        let mut named_windows = vec![];
        loop {
            let name = self.consume_identifier_into_name()?;
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

    /// Rule:
    /// ```text
    /// expr -> or_expr
    /// ```
    pub(crate) fn parse_expr(&mut self) -> anyhow::Result<Expr> {
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
            let operator = match &self.peek_prev().kind {
                TokenType::BitwiseNot => BinaryOperator::BitwiseNot,
                TokenType::Star => BinaryOperator::Star,
                TokenType::Slash => BinaryOperator::Slash,
                TokenType::ConcatOperator => BinaryOperator::Concat,
                TokenType::Plus => BinaryOperator::Plus,
                TokenType::Minus => BinaryOperator::Minus,
                TokenType::BitwiseLeftShift => BinaryOperator::BitwiseLeftShift,
                TokenType::BitwiseRightShift => BinaryOperator::BitwiseRightShift,
                TokenType::BitwiseAnd => BinaryOperator::BitwiseAnd,
                TokenType::BitwiseXor => BinaryOperator::BitwiseXor,
                TokenType::BitwiseOr => BinaryOperator::BitwiseOr,
                TokenType::And => BinaryOperator::And,
                TokenType::Or => BinaryOperator::Or,
                _ => unreachable!(),
            };
            let right = next_parsing_rule_fn(self)?;
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator,
                right: Box::new(right),
            });
        }

        Ok(output)
    }

    /// Rule:
    /// ```text
    /// or_expr -> and_expr ("OR" and_expr)*
    /// ```
    fn parse_or_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenTypeVariant::Or], Self::parse_and_expr)
    }

    /// Rule:
    /// ```text
    /// and_expr -> not_expr ("AND" not_expr)*
    /// ```
    fn parse_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(&[TokenTypeVariant::And], Self::parse_not_expr)
    }

    /// Rule:
    /// ```text
    /// not_expr -> "NOT" not_expr | comparison_expr
    /// ```
    fn parse_not_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_type(TokenTypeVariant::Not) {
            return Ok(Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Not,
                right: Box::new(self.parse_not_expr()?),
            }));
        }
        self.parse_comparison_expr()
    }

    #[inline]
    fn create_standard_binary_expr(
        &mut self,
        left: Expr,
        operator: BinaryOperator,
    ) -> anyhow::Result<Expr> {
        let right = self.parse_bitwise_or_expr()?;
        Ok(Expr::Binary(BinaryExpr {
            left: Box::new(left),
            operator,
            right: Box::new(right),
        }))
    }

    /// Rule:
    /// ```text
    /// comparison_expr ->
    /// bitwise_or_expr
    /// | bitwise_or_expr (("=" | ">" | "<" | ">=" | "<=", | "!=", | "<>") bitwise_or_expr)*
    /// | bitwise_or_expr (["NOT"] ("IN" | "BETWEEN") bitwise_or_expr)*
    /// | bitwise_or_expr (["NOT"] "LIKE" ("ANY" | "SOME" | "ALL") bitwise_or_expr)*
    /// | bitwise_or_expr (["NOT"] "LIKE") biwtise_or_expr)*
    /// | bitwise_or_expr ("IS" ["NOT"] "DISTINCT" "FROM") bitwise_or_expr)*
    /// ```
    fn parse_comparison_expr(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_bitwise_or_expr()?;

        loop {
            let curr = self.peek();

            match &curr.kind {
                TokenType::Equal => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::Equal)?;
                }
                TokenType::Greater => {
                    self.advance();
                    output =
                        self.create_standard_binary_expr(output, BinaryOperator::GreaterThan)?;
                }
                TokenType::Less => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::LessThan)?;
                }
                TokenType::GreaterEqual => {
                    self.advance();
                    output = self.create_standard_binary_expr(
                        output,
                        BinaryOperator::GreaterThanOrEqualTo,
                    )?;
                }
                TokenType::LessEqual => {
                    self.advance();
                    output = self
                        .create_standard_binary_expr(output, BinaryOperator::LessThanOrEqualTo)?;
                }
                TokenType::BangEqual | TokenType::NotEqual => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::NotEqual)?;
                }
                TokenType::Like => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::Like)?;
                }
                TokenType::In => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::In)?;
                }
                TokenType::Between => {
                    self.advance();
                    output = self.create_standard_binary_expr(output, BinaryOperator::Between)?;
                }
                TokenType::Is => {
                    self.advance();
                    let is_not = self.match_token_type(TokenTypeVariant::Not);
                    self.consume(TokenTypeVariant::Distinct)?;
                    self.consume(TokenTypeVariant::From)?;
                    output = if is_not {
                        self.create_standard_binary_expr(output, BinaryOperator::IsNotDistinctFrom)?
                    } else {
                        self.create_standard_binary_expr(output, BinaryOperator::IsDistinctFrom)?
                    };
                }
                TokenType::Not => {
                    self.advance();
                    let tok = self.consume_one_of(&[
                        TokenTypeVariant::In,
                        TokenTypeVariant::Between,
                        TokenTypeVariant::Like,
                    ])?;
                    output = match &tok.kind {
                        TokenType::In => {
                            self.create_standard_binary_expr(output, BinaryOperator::NotIn)?
                        }
                        TokenType::Between => {
                            self.create_standard_binary_expr(output, BinaryOperator::NotBetween)?
                        }
                        TokenType::Like => {
                            self.create_standard_binary_expr(output, BinaryOperator::NotLike)?
                        }
                        _ => unreachable!(),
                    };
                }
                _ => {
                    break;
                }
            }
        }
        Ok(output)
    }

    /// Rule:
    /// ```text
    /// bitwise_or_expr -> primary_expr | primary_expr ("|" primary_expr)*
    /// ```
    fn parse_bitwise_or_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::BitwiseOr],
            Self::parse_bitwise_and_expr,
        )
    }

    /// Rule:
    /// ```text
    /// bitwise_and_expr -> bitwise_shift_expr | bitwise_shift_expr ("&" bitwise_shift_expr)*
    /// ```
    fn parse_bitwise_and_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::BitwiseAnd],
            Self::parse_bitwise_shift_expr,
        )
    }

    /// Rule:
    /// ```text
    /// bitwise_shift_expr -> add_expr | add_expr (("<<" | ">>") add_expr)*
    /// ```
    fn parse_bitwise_shift_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[
                TokenTypeVariant::BitwiseRightShift,
                TokenTypeVariant::BitwiseLeftShift,
            ],
            Self::parse_add_expr,
        )
    }

    /// Rule:
    /// ```text
    /// add_expr -> mul_concat_expr | mul_concat_expr (("+" | "-") mul_concat_expr)*
    /// ```
    fn parse_add_expr(&mut self) -> anyhow::Result<Expr> {
        self.parse_standard_binary_expr(
            &[TokenTypeVariant::Plus, TokenTypeVariant::Minus],
            Self::parse_mul_concat_expr,
        )
    }

    /// Rule:
    /// ```text
    /// mul_concat_expr -> unary_expr | unary_expr (("*" | "/" | "||") unary_expr)*
    /// ```
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

    /// Rule:
    /// ```text
    /// unary_expr -> ("+" | "-" | "~") field_access_expr | field_access_expr ("Is" ["Not"] ("True" | "False" | "Null"))
    /// ```
    fn parse_unary_expr(&mut self) -> anyhow::Result<Expr> {
        if self.match_token_types(&[
            TokenTypeVariant::Plus,
            TokenTypeVariant::Minus,
            TokenTypeVariant::BitwiseNot,
        ]) {
            let operator = match &self.peek_prev().kind {
                TokenType::Plus => UnaryOperator::Plus,
                TokenType::Minus => UnaryOperator::Minus,
                TokenType::BitwiseNot => UnaryOperator::BitwiseNot,
                _ => unreachable!(),
            };
            return Ok(Expr::Unary(UnaryExpr {
                operator,
                right: Box::new(self.parse_field_access_expr()?),
            }));
        }

        let expr = self.parse_field_access_expr()?;

        // If this is not a "is [not] distinct from"
        if self.peek().kind == TokenType::Is
            && !(self.peek_next_i(1).kind == TokenType::Distinct
                || (self.peek_next_i(1).kind == TokenType::Not
                    && self.peek_next_i(2).kind == TokenType::Distinct))
        {
            self.advance();
            let not = self.match_token_type(TokenTypeVariant::Not);
            let literal = self.consume_one_of(&[
                TokenTypeVariant::Null,
                TokenTypeVariant::True,
                TokenTypeVariant::False,
            ])?;
            let operator = match (not, &literal.kind) {
                (true, TokenType::True) => UnaryOperator::IsNotTrue,
                (false, TokenType::True) => UnaryOperator::IsTrue,
                (true, TokenType::Null) => UnaryOperator::IsNotNull,
                (false, TokenType::Null) => UnaryOperator::IsNull,
                (true, TokenType::False) => UnaryOperator::IsNotFalse,
                (false, TokenType::False) => UnaryOperator::IsFalse,
                _ => unreachable!(),
            };

            Ok(Expr::Unary(UnaryExpr {
                operator,
                right: Box::new(expr),
            }))
        } else {
            Ok(expr)
        }
    }

    /// Rule:
    /// ```text
    /// field_access_expr -> array_subscript_operator | array_subscript_operator ("." array_subscript_operator )* ["." "*"]
    /// ```
    fn parse_field_access_expr(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_array_subscript_operator()?;

        while self.match_token_type(TokenTypeVariant::Dot) {
            if self.match_token_type(TokenTypeVariant::Star) {
                return Ok(Expr::Binary(BinaryExpr {
                    left: Box::new(output),
                    operator: BinaryOperator::FieldAccess,
                    right: Box::new(Expr::Star),
                }));
            }
            let right = self.parse_array_subscript_operator()?;
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: BinaryOperator::FieldAccess,
                right: Box::new(right),
            });
        }
        Ok(output)
    }

    /// Rule:
    /// ```text
    /// array_subscript_operator -> primary_expr | primary_expr ("[" expr "]")*
    /// ```
    fn parse_array_subscript_operator(&mut self) -> anyhow::Result<Expr> {
        let mut output = self.parse_primary_expr()?;

        while self.match_token_type(TokenTypeVariant::LeftSquare) {
            let index = self.parse_expr()?;
            self.consume(TokenTypeVariant::RightSquare)?;
            output = Expr::Binary(BinaryExpr {
                left: Box::new(output),
                operator: BinaryOperator::ArrayIndex,
                right: Box::new(index),
            });
        }
        Ok(output)
    }

    /// Rule:
    /// ```text
    /// array_expr -> "ARRAY" ([array_type] "[" [expr ("," expr)*] "]" | "(" query_expr ")")
    /// ```
    fn parse_array_expr(&mut self) -> anyhow::Result<Expr> {
        let mut array_type: Option<Type> = None;
        if self.match_token_type(TokenTypeVariant::Array)
            && self.check_token_type(TokenTypeVariant::Less)
        {
            array_type = Some(self.parse_array_type()?);
        }
        self.consume(TokenTypeVariant::LeftSquare)?;
        let array_elements = if self.match_token_type(TokenTypeVariant::RightSquare) {
            vec![]
        } else {
            let mut array_elements = vec![];
            loop {
                array_elements.push(self.parse_expr()?);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightSquare)?;
            array_elements
        };
        Ok(Expr::Array(ArrayExpr {
            exprs: array_elements,
            r#type: array_type,
        }))
    }

    /// Rule:
    /// ```text
    /// struct_expr -> "STRUCT" [struct_type] "(" expr ["AS" field_name]] ("," expr ["AS" field_name])* ")"
    /// where:
    /// field_name -> "Identifier" | "QuotedIdentifier"
    /// ```
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
                Some(self.consume_identifier_into_name()?)
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

    /// Rule:
    /// ```text
    /// struct_tuple_expr -> "(" expr ("," expr)* ")"
    /// ```
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

    /// Rule:
    /// ```text
    /// interval_part -> "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND"
    /// ```
    fn parse_interval_part(&mut self) -> anyhow::Result<IntervalPart> {
        match self.consume_and_get_identifier()?.to_lowercase().as_str() {
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

    /// Rule:
    /// ```text
    /// interval_expr -> interval | interval_range
    /// where:
    /// interval -> "INTERVAL" expr interval_part
    /// interval_range -> "String" interval_part "TO" interval_part
    /// ```
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

    /// Rule:
    /// ```text
    /// range_expr -> "RANGE" "<" bq_parameterized_type ">" "String"
    /// ```
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

    /// Rule:
    /// ```text
    /// array_type -> "<" bq_type ">"
    /// ```
    fn parse_array_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(Type::Array {
            r#type: Box::new(array_type),
        })
    }

    /// Rule:
    /// ```text
    /// range_type -> "<" bq_type ">"
    /// ```
    fn parse_range_type(&mut self) -> anyhow::Result<Type> {
        self.consume(TokenTypeVariant::Less)?;
        let range_type = self.parse_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(Type::Range {
            r#type: Box::new(range_type),
        })
    }

    /// Rule:
    /// ```text
    /// struct_type -> "<" ["field_name"] bq_type ("," ["field_name"] bq_type)* ">"
    /// ```
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
                Some(match &self.peek_prev().kind {
                    TokenType::Identifier(ident) => Name::Identifier(Identifier {
                        name: ident.clone(),
                    }),
                    TokenType::QuotedIdentifier(qident) => {
                        Name::QuotedIdentifier(QuotedIdentifier {
                            name: qident.clone(),
                        })
                    }
                    _ => unreachable!(),
                })
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

        Ok(Type::Struct {
            fields: struct_field_types,
        })
    }

    /// Rule:
    /// ```text
    /// bq_type ->
    /// "ARRAY" array_type | "STRUCT" struct_type
    /// | "BIGNUMERIC" | "NUMERIC" | "BOOL" | "BYTES" | "DATE"" | "DATETIME" "FLOAT64" | "GEOGRAPHY"
    /// | "INT64" | "INTERVAL"" | "JSON" | "NUMERIC" | "RANGE" | "STRING" | "TIME"" | "TIMESTAMP"
    /// ```
    pub(crate) fn parse_bq_type(&mut self) -> anyhow::Result<Type> {
        let peek_token = self.advance().clone();

        match &peek_token.kind {
            // reserved keywords
            TokenType::Array => self.parse_array_type(),
            TokenType::Struct => self.parse_struct_type(),
            TokenType::Range => self.parse_range_type(),
            TokenType::Interval => Ok(Type::Interval),

            // identifier or quoted idenfitier
            TokenType::Identifier(ident) | TokenType::QuotedIdentifier(ident) => match ident.to_lowercase().as_str() {
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
            _ => unreachable!()
        }
    }

    /// Rule:
    /// ```text
    /// parameterized_range_type -> "<" bq_paramterized_type ">"
    /// ```
    fn parse_parameterized_range_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let range_type = self.parse_parameterized_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(ParameterizedType::Range {
            r#type: Box::new(range_type),
        })
    }

    /// Rule:
    /// ```text
    /// parameterized_array_type -> "<" bq_paramterized_type ">"
    /// ```
    fn parse_parameterized_array_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let array_type = self.parse_parameterized_bq_type()?;
        self.consume(TokenTypeVariant::Greater)?;
        Ok(ParameterizedType::Array {
            r#type: Box::new(array_type),
        })
    }

    /// Rule:
    /// ```text
    /// parameterized_struct_type -> "<" field_name bq_paramterized_type ("," field_name bq_paramterized_type)* ">"
    /// where:
    /// field_name -> "Identifier" | "QuotedIdentifier"
    /// ```
    fn parse_parameterized_struct_type(&mut self) -> anyhow::Result<ParameterizedType> {
        self.consume(TokenTypeVariant::Less)?;
        let mut struct_field_types = vec![];
        loop {
            let field_name = self.consume_identifier_into_name()?;
            let field_type = self.parse_parameterized_bq_type()?;
            struct_field_types.push(StructParameterizedFieldType {
                name: field_name,
                r#type: field_type,
            });

            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::Greater)?;

        Ok(ParameterizedType::Struct {
            fields: struct_field_types,
        })
    }

    /// Rule:
    /// ```text
    /// parameterized_bq_type ->
    /// "ARRAY" array_type | "STRUCT" struct_type
    /// | "BIGNUMERIC" ["(" number ["," number] ")" ]
    /// | "NUMERIC" ["(" number ["," number] ")" ]
    /// | "BOOL" | "BYTES" ["(" number ")"] | "STRING" ["(" number ")"]
    /// | "DATE"" | "DATETIME" "FLOAT64" | "GEOGRAPHY"
    /// | "INT64" | "INTERVAL"" | "JSON" | "NUMERIC" | "RANGE" |  | "TIME"" | "TIMESTAMP"
    /// ```
    pub(crate) fn parse_parameterized_bq_type(&mut self) -> anyhow::Result<ParameterizedType> {
        let peek_token = self.advance().clone();

        match &peek_token.kind {
            // reserved keywords
            TokenType::Array => self.parse_parameterized_array_type(),
            TokenType::Struct => self.parse_parameterized_struct_type(),
            TokenType::Range => self.parse_parameterized_range_type(),
            TokenType::Interval => Ok(ParameterizedType::Interval),

            // identifier or quotedidentifier
            TokenType::Identifier(ident) | TokenType::QuotedIdentifier(ident) => match ident.to_lowercase().as_str() {
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
                        Ok(ParameterizedType::BigNumeric {precision, scale})
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

                        Ok(ParameterizedType::Bytes{max_length: max_len})
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
                        Ok(ParameterizedType::Numeric{precision, scale})
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
                        Ok(ParameterizedType::String{max_length: max_len})
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
            _ => Err(anyhow!( self.error(
                &peek_token,
                "Expected BigQuery type. One of: `ARRAY`, `BIGNUMERIC`, `NUMERIC`, `BOOL`, `BYTES`, `DATE`, `DATETIME`, \
                 `FLOAT64`, `GEOGRAPHY`, `INT64`, `INTERVAL`, `JSON`, `NUMERIC`, `RANGE`, `STRING`, `STRUCT`, `TIME`, `TIMESTAMP`."
            )))
        }
    }

    /// Rule:
    /// ```text
    /// array_fn -> "ARRAY" "(" query_expr ")"
    /// ```
    fn parse_array_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Array)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let query = self.parse_query_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::Array(
            ArrayFunctionExpr { query },
        ))))
    }

    /// Rule:
    /// ```text
    /// concat_fn -> "CONCAT" "(" expr  ( "," expr )* ")"
    /// ```
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
        Ok(Expr::Function(Box::new(FunctionExpr::Concat(
            ConcatFunctionExpr { values },
        ))))
    }

    fn parse_cast_fn_arguments(
        &mut self,
    ) -> anyhow::Result<(Box<Expr>, ParameterizedType, Option<CastFunctionFormat>)> {
        self.consume(TokenTypeVariant::LeftParen)?;
        let expr = self.parse_expr()?;
        self.consume(TokenTypeVariant::As)?;
        let r#type = self.parse_parameterized_bq_type()?;
        let format = if self.match_non_reserved_keyword("format") {
            let format = self.parse_expr()?;
            let time_zone = if self.match_token_type(TokenTypeVariant::At) {
                self.consume_non_reserved_keyword("time")?;
                self.consume_non_reserved_keyword("zone")?;
                Some(self.parse_expr()?)
            } else {
                None
            };
            Some(CastFunctionFormat { format, time_zone })
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;
        Ok((Box::new(expr), r#type, format))
    }

    /// Rule:
    /// ```text
    /// cast -> "CAST" "(" expr "AS" bq_parameterized_type ["FORMAT" expr [["AT" "TIME" "ZONE" expr]] ")"
    /// ```
    fn parse_cast_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Cast)?;
        let (expr, r#type, format) = self.parse_cast_fn_arguments()?;
        Ok(Expr::Function(Box::new(FunctionExpr::Cast(
            CastFunctionExpr {
                expr,
                r#type,
                format,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// safe_cast -> "SAFE_CAST" "(" expr "AS" bq_parameterized_type ["FORMAT" expr] ")"
    /// ```
    fn parse_safe_cast_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;
        let (expr, r#type, format) = self.parse_cast_fn_arguments()?;
        Ok(Expr::Function(Box::new(FunctionExpr::SafeCast(
            SafeCastFunctionExpr {
                expr,
                r#type,
                format,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// array_agg -> ("Identifier" | "QuotedIdentifier") "(" arg ")" ["OVER" named_window_expr]
    /// where:
    /// arg ->
    ///  ["DISTINCT"]
    ///  expr
    ///  [("IGNORE" | "RESPECT") "NULLS"]
    ///  ["HAVING ("MAX" | "MIN") expr]
    ///  ["ORDER" "BY" expr ("ASC" | "DESC") ("," expr ("ASC" | "DESC"))*]
    ///  ["LIMIT" "Number"]
    /// ```
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
                let nulls = Some(match &self.peek_prev().kind {
                    TokenType::Ignore => FunctionAggregateNulls::Ignore,
                    TokenType::Respect => FunctionAggregateNulls::Respect,
                    _ => unreachable!(),
                });
                self.consume(TokenTypeVariant::Nulls)?;
                nulls
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
                exprs.push(FunctionAggregateOrderBy {
                    expr: Box::new(expr),
                    sort_direction,
                    nulls,
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
            name: None,
            expr: arg_expr,
            aggregate,
        };
        self.consume(TokenTypeVariant::RightParen)?;

        let over = if self.match_token_type(TokenTypeVariant::Over) {
            Some(self.parse_named_window_expr()?)
        } else {
            None
        };

        Ok(Expr::Function(Box::new(FunctionExpr::ArrayAgg(
            ArrayAggFunctionExpr {
                arg: Box::new(arg),
                over,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// current_time -> "CURRENT_TIME" | "CURRENT_TIME" "(" [expr] ")"
    /// ```
    fn parse_current_time_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        let timezone = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let timezone = if self.check_token_type(TokenTypeVariant::RightParen) {
                None
            } else {
                Some(self.parse_expr()?)
            };
            self.consume(TokenTypeVariant::RightParen)?;
            timezone
        } else {
            None
        };

        Ok(Expr::Function(Box::new(FunctionExpr::CurrentTime(
            CurrentTimeFunctionExpr { timezone },
        ))))
    }

    /// Rule:
    /// ```text
    /// current_datetime -> "CURRENT_DATETIME" | "CURRENT_DATETIME" "(" [expr] ")"
    /// ```
    fn parse_current_datetime_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        let timezone = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let timezone = if self.check_token_type(TokenTypeVariant::RightParen) {
                None
            } else {
                Some(self.parse_expr()?)
            };
            self.consume(TokenTypeVariant::RightParen)?;
            timezone
        } else {
            None
        };

        Ok(Expr::Function(Box::new(FunctionExpr::CurrentDatetime(
            CurrentDatetimeFunctionExpr { timezone },
        ))))
    }

    /// Rule:
    /// ```text
    /// current_date -> "CURRENT_DATE" | "CURRENT_DATE" "(" [expr] ")"
    /// ```
    fn parse_current_date_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        let timezone = if self.match_token_type(TokenTypeVariant::LeftParen) {
            let timezone = if self.check_token_type(TokenTypeVariant::RightParen) {
                None
            } else {
                Some(self.parse_expr()?)
            };
            self.consume(TokenTypeVariant::RightParen)?;
            timezone
        } else {
            None
        };

        Ok(Expr::Function(Box::new(FunctionExpr::CurrentDate(
            CurrentDateFunctionExpr { timezone },
        ))))
    }

    /// Rule:
    /// ```text
    /// current_timestamp -> "CURRENT_TIMESTAMP" | "CURRENT_TIMESTAMP" "(" ")"
    /// ```
    fn parse_current_timestamp_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_one_of(&[
            TokenTypeVariant::Identifier,
            TokenTypeVariant::QuotedIdentifier,
        ])?;

        if self.match_token_type(TokenTypeVariant::LeftParen) {
            self.consume(TokenTypeVariant::RightParen)?;
        }

        Ok(Expr::Function(Box::new(FunctionExpr::CurrentTimestamp)))
    }

    /// Rule:
    /// ```text
    /// if -> "IF" "(" expr "," expr "," expr ")"
    /// ```
    fn parse_if_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::If)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let condition = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let true_result = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let false_result = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::If(IfFunctionExpr {
            condition: Box::new(condition),
            true_result: Box::new(true_result),
            false_result: Box::new(false_result),
        }))))
    }

    /// Rule:
    /// ```text
    /// left -> "LEFT" "(" expr "," expr ")"
    /// ```
    fn parse_left_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Left)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let value = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let length = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::Left(
            LeftFunctionExpr { value, length },
        ))))
    }

    /// Rule:
    /// ```text
    /// right -> "RIGHT" "(" expr "," expr ")"
    /// ```
    fn parse_right_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Right)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let value = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let length = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::Right(
            RightFunctionExpr { value, length },
        ))))
    }

    /// Rule:
    /// ```text
    /// granularity -> "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR"
    ///  | "DAY" | "WEEK" | "WEEK" "(" ("SUNDAY" | "MONDAY" | "TUESDAY" | "WEDNESDAY"
    ///  | "THURSDAY" | "FRIDAY" | "SATURDAY") ")" | "ISOWEEK" | "MONTH" | "QUARTER" | "YEAR" | "ISOYEAR"
    /// ```
    fn parse_granularity(&mut self) -> anyhow::Result<Granularity> {
        let diff_granularity = self.consume_and_get_identifier()?.to_lowercase();
        Ok(match diff_granularity.as_str() {
            "microsecond" => Granularity::MicroSecond,
            "millisecond" => Granularity::MilliSecond,
            "second" => Granularity::Second,
            "minute" => Granularity::Minute,
            "hour" => Granularity::Hour,
            "day" => Granularity::Day,
            "week" => {
                if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let week_begin_keyword = self.consume_and_get_identifier()?.to_lowercase();
                    let week_begin = match week_begin_keyword.as_str() {
                        "sunday" => WeekBegin::Sunday,
                        "monday" => WeekBegin::Monday,
                        "tuesday" => WeekBegin::Tuesday,
                        "wednesday" => WeekBegin::Wednesday,
                        "thursday" => WeekBegin::Thursday,
                        "friday" => WeekBegin::Friday,
                        "saturday" => WeekBegin::Saturday,
                        _ => Err(anyhow!(
                            "Found unexpected day of week: `{}`",
                            week_begin_keyword
                        ))?,
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    Granularity::WeekWithBegin(week_begin)
                } else {
                    Granularity::Week
                }
            }
            "isoweek" => Granularity::IsoWeek,
            "month" => Granularity::Month,
            "quarter" => Granularity::Quarter,
            "year" => Granularity::Year,
            "isoyear" => Granularity::IsoYear,
            "date" => Granularity::Date,
            "time" => Granularity::Time,
            _ => Err(anyhow!(
                "Found unexpected difference granularity: `{}`.",
                diff_granularity
            ))?,
        })
    }

    /// Rule:
    /// ```text
    /// date_diff -> "DATE_DIFF" "(" expr "," expr "," granularity ")"
    /// ```
    fn parse_date_diff_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let start_date = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let end_date = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::DateDiff(
            DateDiffFunctionExpr {
                start_date,
                end_date,
                granularity,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// datetime_diff -> "DATETIME_DIFF" "(" expr "," expr "," granularity ")"
    /// ```
    fn parse_datetime_diff_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let start_datetime = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let end_datetime = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::DatetimeDiff(
            DatetimeDiffFunctionExpr {
                start_datetime,
                end_datetime,
                granularity,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// timestamp_diff -> "TIMESTAMP_DIFF" "(" expr "," expr "," granularity ")"
    /// ```
    fn parse_timestamp_diff_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let start_timestamp = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let end_timestamp = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::TimestampDiff(
            TimestampDiffFunctionExpr {
                start_timestamp,
                end_timestamp,
                granularity,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// time_diff -> "TIME_DIFF" "(" expr "," expr "," granularity ")"
    /// ```
    fn parse_time_diff_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let start_time = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let end_time = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::TimeDiff(
            TimeDiffFunctionExpr {
                start_time,
                end_time,
                granularity,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// date_trunc -> "DATE_TRUNC" "(" expr "," granularity ")"
    /// ```
    fn parse_date_trunc_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let date = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::DateTrunc(
            DateTruncFunctionExpr { date, granularity },
        ))))
    }

    /// Rule:
    /// ```text
    /// datetime_trunc -> "DATETIME_TRUNC" "(" expr "," granularity ("," expr) ")"
    /// ```
    fn parse_datetime_trunc_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let datetime = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        let timezone = if self.match_token_type(TokenTypeVariant::Comma) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::DatetimeTrunc(
            DatetimeTruncFunctionExpr {
                datetime,
                granularity,
                timezone,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// timestamp_trunc -> "TIMESTAMP_TRUNC" "(" expr "," granularity ("," expr) ")"
    /// ```
    fn parse_timestamp_trunc_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let timestamp = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        let timezone = if self.match_token_type(TokenTypeVariant::Comma) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::TimestampTrunc(
            TimestampTruncFunctionExpr {
                timestamp,
                granularity,
                timezone,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// time_trunc -> "TIME_TRUNC" "(" expr "," granularity ")"
    /// ```
    fn parse_time_trunc_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let time = self.parse_expr()?;
        self.consume(TokenTypeVariant::Comma)?;
        let granularity = self.parse_granularity()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::TimeTrunc(
            TimeTruncFunctionExpr { time, granularity },
        ))))
    }

    /// Rule:
    /// ```text
    /// last_day -> "LAST_DAY" "(" expr ["," granularity] ")"
    /// ```
    fn parse_last_day_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let expr = self.parse_expr()?;
        let part = if self.match_token_type(TokenTypeVariant::Comma) {
            Some(self.parse_granularity()?)
        } else {
            None
        };
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::LastDay(
            LastDayFunctionExpr {
                expr,
                granularity: part,
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// coalesce -> "COALESCE" "(" expr ("," expr)* ")"
    /// ```
    fn parse_coalesce_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume_identifier()?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut exprs = vec![];
        loop {
            exprs.push(self.parse_expr()?);
            if !self.match_token_type(TokenTypeVariant::Comma) {
                break;
            }
        }
        self.consume(TokenTypeVariant::RightParen)?;

        Ok(Expr::Function(Box::new(FunctionExpr::Coalesce(
            CoalesceFunctionExpr { exprs },
        ))))
    }

    fn parse_function_expr(&mut self) -> anyhow::Result<Expr> {
        match &self.peek().kind {
            TokenType::Identifier(ident) | TokenType::QuotedIdentifier(ident) => {
                match ident.to_lowercase().as_str() {
                    "concat" => self.parse_concat_fn_expr(),
                    "coalesce" => self.parse_coalesce_fn_expr(),
                    "safe_cast" => self.parse_safe_cast_fn_expr(),
                    "array_agg" => self.parse_array_agg_fn_expr(),
                    "current_date" => self.parse_current_date_fn_expr(),
                    "current_datetime" => self.parse_current_datetime_fn_expr(),
                    "date_diff" => self.parse_date_diff_fn_expr(),
                    "date_trunc" => self.parse_date_trunc_fn_expr(),
                    "last_day" => self.parse_last_day_fn_expr(),
                    "datetime_diff" => self.parse_datetime_diff_fn_expr(),
                    "datetime_trunc" => self.parse_datetime_trunc_fn_expr(),
                    "current_time" => self.parse_current_time_fn_expr(),
                    "current_timestamp" => self.parse_current_timestamp_fn_expr(),
                    "timestamp_trunc" => self.parse_timestamp_trunc_fn_expr(),
                    "timestamp_diff" => self.parse_timestamp_diff_fn_expr(),
                    "time_diff" => self.parse_time_diff_fn_expr(),
                    "time_trunc" => self.parse_time_trunc_fn_expr(),
                    _ => self.parse_generic_function(),
                }
            }
            _ => unreachable!(),
        }
    }

    /// Rule:
    /// ```text
    /// generic_function -> ("Identifier" | "QuotedIdentifier") "(" arg ("," arg)* ")" ["OVER" named_window_expr]
    /// where:
    /// arg ->
    ///  ["DISTINCT"]
    ///  (expr | name => expr)
    ///  [("IGNORE" | "RESPECT") "NULLS"]
    ///  ["HAVING ("MAX" | "MIN") expr]
    ///  ["ORDER" "BY" expr [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")] ("," expr [("ASC" | "DESC")] [("NULLS" "FIRST" | "NULLS" "LAST")])*]
    ///  ["LIMIT" "Number"]
    /// ```
    fn parse_generic_function(&mut self) -> anyhow::Result<Expr> {
        let function_name = self.consume_identifier_into_name()?;
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

            let (arg_name, arg_expr) = if self.peek_next_i(1).kind == TokenType::RightArrow {
                // named arg
                let name = self.consume_identifier_into_name()?;
                self.consume(TokenTypeVariant::RightArrow)?;
                (Some(name), self.parse_expr()?)
            } else {
                (None, self.parse_expr()?)
            };

            let nulls =
                if self.match_token_types(&[TokenTypeVariant::Ignore, TokenTypeVariant::Respect]) {
                    let nulls = Some(match &self.peek_prev().kind {
                        TokenType::Ignore => FunctionAggregateNulls::Ignore,
                        TokenType::Respect => FunctionAggregateNulls::Respect,
                        _ => unreachable!(),
                    });
                    self.consume(TokenTypeVariant::Nulls)?;
                    nulls
                } else {
                    None
                };

            let having = if self.match_token_type(TokenTypeVariant::Having) {
                let kind = match &self
                    .consume_one_of_non_reserved_keywords(&["max", "min"])?
                    .kind
                {
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
                    exprs.push(FunctionAggregateOrderBy {
                        expr: Box::new(expr),
                        sort_direction,
                        nulls,
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
                name: arg_name,
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
            name: function_name,
            arguments,
            over,
        })))
    }

    /// Rule:
    /// ```text
    /// exists_subquery_expr -> "EXISTS" "(" query_expr ")"
    /// ```
    fn parse_exists_subquery_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Exists)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let query_expr = self.parse_query_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Exists(Box::new(query_expr)))
    }

    /// Rule:
    /// ```text
    /// case_expr -> "CASE" ("WHEN" expr "THEN" expr)+ "ELSE" expr "END"
    /// ```
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
            let when = self.parse_expr()?;
            self.consume(TokenTypeVariant::Then)?;
            let then = self.parse_expr()?;

            when_thens.push(WhenThen { when, then });

            if !self.check_token_type(TokenTypeVariant::When) {
                break;
            }
        }

        let else_or_end = self.consume_one_of(&[TokenTypeVariant::Else, TokenTypeVariant::End])?;
        match &else_or_end.kind {
            TokenType::Else => {
                let r#else = self.parse_expr()?;
                self.consume(TokenTypeVariant::End)?;
                Ok(Expr::Case(CaseExpr {
                    case,
                    when_thens,
                    r#else: Some(Box::new(r#else)),
                }))
            }
            TokenType::End => Ok(Expr::Case(CaseExpr {
                case,
                when_thens,
                r#else: None,
            })),
            _ => unreachable!(),
        }
    }

    /// Rule:
    /// ```text
    /// extract_expr -> "EXTRACT" "(" part "FROM" expr ")"
    /// where part -> "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR" | "DAYOFWEEK"
    ///  | "DAY" | "DAYOFYEAR" | "WEEK" | "WEEK" "(" ("SUNDAY" | "MONDAY" | "TUESDAY" | "WEDNESDAY"
    ///  | "THURSDAY" | "FRIDAY" | "SATURDAY") ")" | "ISOWEEK" | "MONTH" | "QUARTER" | "YEAR" | "ISOYEAR"
    /// ```
    fn parse_extract_fn_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::Extract)?;
        self.consume(TokenTypeVariant::LeftParen)?;

        let part_keyword = self.consume_and_get_identifier()?.to_lowercase();
        let part = match part_keyword.as_str() {
            "microsecond" => ExtractFunctionPart::MicroSecond,
            "millisecond" => ExtractFunctionPart::MilliSecond,
            "second" => ExtractFunctionPart::Second,
            "minute" => ExtractFunctionPart::Minute,
            "hour" => ExtractFunctionPart::Hour,
            "dayofweek" => ExtractFunctionPart::DayOfWeek,
            "day" => ExtractFunctionPart::Day,
            "dayofyear" => ExtractFunctionPart::DayOfYear,
            "week" => {
                if self.match_token_type(TokenTypeVariant::LeftParen) {
                    let week_begin_keyword = self.consume_and_get_identifier()?.to_lowercase();
                    let week_begin = match week_begin_keyword.as_str() {
                        "sunday" => WeekBegin::Sunday,
                        "monday" => WeekBegin::Monday,
                        "tuesday" => WeekBegin::Tuesday,
                        "wednesday" => WeekBegin::Wednesday,
                        "thursday" => WeekBegin::Thursday,
                        "friday" => WeekBegin::Friday,
                        "saturday" => WeekBegin::Saturday,
                        _ => Err(anyhow!(
                            "Found unexpected day of week: `{}`",
                            week_begin_keyword
                        ))?,
                    };
                    self.consume(TokenTypeVariant::RightParen)?;
                    ExtractFunctionPart::WeekWithBegin(week_begin)
                } else {
                    ExtractFunctionPart::Week
                }
            }
            "isoweek" => ExtractFunctionPart::IsoWeek,
            "month" => ExtractFunctionPart::Month,
            "quarter" => ExtractFunctionPart::Quarter,
            "year" => ExtractFunctionPart::Year,
            "isoyear" => ExtractFunctionPart::IsoYear,
            "date" => ExtractFunctionPart::Date,
            "time" => ExtractFunctionPart::Time,
            _ => Err(anyhow!(
                "Found unexpected extract part: `{}`.",
                part_keyword
            ))?,
        };

        self.consume(TokenTypeVariant::From)?;
        let expr = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::Function(Box::new(FunctionExpr::Extract(
            ExtractFunctionExpr {
                part,
                expr: Box::new(expr),
            },
        ))))
    }

    /// Rule:
    /// ```text
    /// quantified_like_expr -> ("ANY" | "SOME" | "ALL") (pattern_expression_list | pattern_array)
    /// where:
    /// pattern_expression_list -> "(" expr ("," expr)* ")"
    /// pattern_array -> "UNNEST" "(" expr ")"
    /// ```
    fn parse_quantified_like_expr(&mut self) -> anyhow::Result<Expr> {
        let quantifier = match &self
            .consume_one_of(&[
                TokenTypeVariant::Any,
                TokenTypeVariant::Some,
                TokenTypeVariant::All,
            ])?
            .kind
        {
            TokenType::Any => LikeQuantifier::Any,
            TokenType::Some => LikeQuantifier::Some,
            TokenType::All => LikeQuantifier::All,
            _ => unreachable!(),
        };
        if self.match_token_type(TokenTypeVariant::LeftParen) {
            let mut exprs = vec![];
            loop {
                exprs.push(self.parse_expr()?);
                if !self.match_token_type(TokenTypeVariant::Comma) {
                    break;
                }
            }
            self.consume(TokenTypeVariant::RightParen)?;
            return Ok(Expr::QuantifiedLike(QuantifiedLikeExpr {
                quantifier,
                pattern: QuantifiedLikeExprPattern::ExprList { exprs },
            }));
        }

        self.consume(TokenTypeVariant::Unnest)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let expr = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::QuantifiedLike(QuantifiedLikeExpr {
            quantifier,
            pattern: QuantifiedLikeExprPattern::ArrayUnnest {
                expr: Box::new(expr),
            },
        }))
    }

    /// Rule:
    /// ```text
    /// with_primary_expr -> "WITH" "(" name "AS" expr ("," name "AS" expr)* "," result_expr ")"
    /// ```
    fn parse_with_primary_expr(&mut self) -> anyhow::Result<Expr> {
        self.consume(TokenTypeVariant::With)?;
        self.consume(TokenTypeVariant::LeftParen)?;
        let mut vars = vec![];

        loop {
            let name = self.consume_identifier_into_name()?;
            self.consume(TokenTypeVariant::As)?;
            let value = self.parse_expr()?;

            vars.push(WithExprVar { name, value });

            if self.match_token_type(TokenTypeVariant::Comma)
                && self.peek_next_i(1).kind != TokenType::As
            {
                break;
            }
        }
        let result = self.parse_expr()?;
        self.consume(TokenTypeVariant::RightParen)?;
        Ok(Expr::With(WithExpr {
            vars,
            result: Box::new(result),
        }))
    }

    /// Rule:
    /// ```text
    /// primary_expr ->
    /// "*" | "True" | "False" | "Null" | "Identifier" | "QuotedIdentifier"
    /// | "QueryNamedParameter" | "QueryPositionalParameter" | "SystemVariable"
    /// | "String" | "Number"
    /// | NUMERIC "Number" | BIGNUMERIC "Number"
    /// | DATE "String" | TIMESTAMP "String" | DATETIME "String" | TIME "String"
    /// | "RANGE" "<" bq_parameterized_type ">" "String"
    /// | interval_expr | json_expr
    /// | array_expr | struct_expr | struct_tuple_expr
    /// | case_expr
    /// | function_expr
    /// | quantified_like_expr
    /// | with_expr
    /// | "(" expression ")" | "(" query_expr ")"
    /// | "EXISTS" "(" query_expr ")"
    /// ```
    fn parse_primary_expr(&mut self) -> anyhow::Result<Expr> {
        let peek_token = self.peek().clone();
        let primary_expr = match peek_token.kind {
            TokenType::Any | TokenType::Some | TokenType::All => {
                self.parse_quantified_like_expr()?
            }
            TokenType::Star => {
                self.advance();
                Expr::Star
            }
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
            TokenType::If => self.parse_if_fn_expr()?,
            TokenType::Left => self.parse_left_fn_expr()?,
            TokenType::Right => self.parse_right_fn_expr()?,
            TokenType::Extract => self.parse_extract_fn_expr()?,
            TokenType::With => self.parse_with_primary_expr()?,
            TokenType::Identifier(ident) => {
                let lower_ident = ident.to_lowercase();
                if self.peek_next_i(1).kind == TokenType::LeftParen
                    || lower_ident == "current_date"
                    || lower_ident == "current_datetime"
                    || lower_ident == "current_timestamp"
                    || lower_ident == "current_time"
                {
                    return self.parse_function_expr();
                } else if self.peek_prev().kind != TokenType::Dot {
                    match lower_ident.as_str() {
                        "date" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(date_str) => Expr::Date(date_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "timestamp" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(date_str) => {
                                        Expr::Timestamp(date_str.clone())
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "datetime" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(date_str) => Expr::Datetime(date_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "time" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(date_str) => Expr::Time(date_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "numeric" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(num_str) => Expr::Numeric(num_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "bignumeric" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(num_str) => Expr::BigNumeric(num_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        "json" => {
                            self.advance();
                            if self.match_token_type(TokenTypeVariant::String) {
                                match &self.peek_prev().kind {
                                    TokenType::String(json_str) => Expr::Json(json_str.clone()),
                                    _ => unreachable!(),
                                }
                            } else {
                                Expr::Identifier(Identifier { name: ident })
                            }
                        }
                        _ => {
                            self.advance();
                            Expr::Identifier(Identifier { name: ident })
                        }
                    }
                } else {
                    self.advance();
                    Expr::Identifier(Identifier { name: ident })
                }
            }
            TokenType::QuotedIdentifier(qident) => {
                if self.peek_next_i(1).kind == TokenType::LeftParen {
                    return self.parse_function_expr();
                } else {
                    self.advance();
                    Expr::QuotedIdentifier(QuotedIdentifier { name: qident })
                }
            }
            TokenType::QueryNamedParameter(param) => {
                self.advance();
                Expr::QueryNamedParameter(param)
            }
            TokenType::QueryPositionalParameter => {
                self.advance();
                Expr::QueryPositionalParameter
            }
            TokenType::SystemVariable(sysvar) => {
                self.advance();
                Expr::SystemVariable(SystemVariable { name: sysvar })
            }
            TokenType::Number(num) => {
                self.advance();
                Expr::Number(Number { value: num })
            }
            TokenType::String(ref str) | TokenType::RawString(ref str) => {
                let curr_tok = self.peek().clone();
                self.advance();
                if self.check_token_types(&[
                    TokenTypeVariant::String,
                    TokenTypeVariant::RawString,
                    TokenTypeVariant::Bytes,
                    TokenTypeVariant::RawBytes,
                ]) {
                    let mut strings = match &curr_tok.kind {
                        TokenType::String(_) => vec![Expr::String(str.clone())],
                        TokenType::RawString(_) => vec![Expr::RawString(str.clone())],
                        _ => unreachable!(),
                    };
                    loop {
                        let peek = self.peek();
                        match &peek.kind {
                            TokenType::String(s) => strings.push(Expr::String(s.clone())),
                            TokenType::RawString(s) => strings.push(Expr::RawString(s.clone())),
                            TokenType::Bytes(_) | TokenType::RawBytes(_) => {
                                return Err(anyhow!(self.error(
                                    &peek_token,
                                    "String and bytes literals cannot be concatenated."
                                )));
                            }
                            _ => {
                                break;
                            }
                        }
                        self.advance();
                    }
                    Expr::StringConcat(StringConcatExpr { strings })
                } else {
                    match &curr_tok.kind {
                        TokenType::String(_) => Expr::String(str.clone()),
                        TokenType::RawString(_) => Expr::RawString(str.clone()),
                        _ => unreachable!(),
                    }
                }
            }
            TokenType::Bytes(ref byt) | TokenType::RawBytes(ref byt) => {
                let curr_tok = self.peek().clone();
                self.advance();
                if self.check_token_types(&[
                    TokenTypeVariant::String,
                    TokenTypeVariant::RawString,
                    TokenTypeVariant::Bytes,
                    TokenTypeVariant::RawBytes,
                ]) {
                    let mut bytes = match &curr_tok.kind {
                        TokenType::Bytes(_) => vec![Expr::Bytes(byt.clone())],
                        TokenType::RawBytes(_) => vec![Expr::RawBytes(byt.clone())],
                        _ => unreachable!(),
                    };
                    loop {
                        let peek = self.peek();
                        match &peek.kind {
                            TokenType::Bytes(s) => bytes.push(Expr::Bytes(s.clone())),
                            TokenType::RawBytes(s) => bytes.push(Expr::RawBytes(s.clone())),
                            TokenType::String(_) | TokenType::RawString(_) => {
                                return Err(anyhow!(self.error(
                                    &peek_token,
                                    "Bytes and string literals cannot be concatenated."
                                )));
                            }
                            _ => {
                                break;
                            }
                        }
                        self.advance();
                    }
                    Expr::BytesConcat(BytesConcatExpr { bytes })
                } else {
                    match &curr_tok.kind {
                        TokenType::Bytes(_) => Expr::Bytes(byt.clone()),
                        TokenType::RawBytes(_) => Expr::RawBytes(byt.clone()),
                        _ => unreachable!(),
                    }
                }
            }
            TokenType::Default => {
                self.advance();
                Expr::Default
            }
            TokenType::Exists => self.parse_exists_subquery_expr()?,
            TokenType::Unnest => self.parse_unnest()?,
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
                    return Ok(Expr::Query(Box::new(QueryExpr::Grouping(
                        GroupingQueryExpr {
                            with: None,
                            query: Box::new(query_expr),
                            order_by: None,
                            limit: None,
                        },
                    ))));
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

pub fn parse_sqls(sqls: &[&str], parallel: bool) -> Vec<anyhow::Result<Ast>> {
    if parallel {
        sqls.par_iter().map(|sql| parse_sql(sql)).collect()
    } else {
        sqls.iter().map(|sql| parse_sql(sql)).collect()
    }
}
