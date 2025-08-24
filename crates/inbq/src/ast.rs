use serde::{Deserialize, Serialize};
use strum_macros::EnumDiscriminants;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ast {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    Query(QueryStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    Truncate(TruncateStatement),
    Merge(Box<MergeStatement>),
    DeclareVar(DeclareVarStatement),
    SetVar(SetVarStatement),
    Block(StatementsBlock),
    CreateTable(CreateTableStatement),
    DropTableStatement(DropTableStatement),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementsBlock {
    pub statements: Vec<Statement>,
    pub exception_statements: Option<Vec<Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareVarStatement {
    pub var_names: Vec<ParseToken>,
    pub r#type: Option<ParameterizedType>,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetVarStatement {
    pub var_names: Vec<ParseToken>,
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub name: ParseToken,
    pub schema: Option<Vec<ColumnSchema>>,
    pub replace: bool,
    pub is_temporary: bool,
    pub if_not_exists: bool,
    pub query: Option<QueryExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub name: ParseToken,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: ParseToken,
    pub r#type: ParameterizedType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterizedType {
    Array(Box<ParameterizedType>),
    BigNumeric(Option<String>, Option<String>),
    Bool,
    Bytes(Option<String>),
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Json,
    Numeric(Option<String>, Option<String>),
    Range(Box<ParameterizedType>),
    String(Option<String>),
    Struct(Vec<StructParameterizedFieldType>),
    Time,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructParameterizedFieldType {
    pub name: ParseToken,
    pub r#type: ParameterizedType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructFieldType {
    pub name: Option<ParseToken>,
    pub r#type: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatement {
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: ParseToken,
    pub columns: Option<Vec<ParseToken>>,
    pub values: Option<Vec<Expr>>,
    pub query: Option<QueryExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: ParseToken,
    pub alias: Option<ParseToken>,
    pub cond: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateItem {
    pub column: ParseToken,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: ParseToken,
    pub alias: Option<ParseToken>,
    pub update_items: Vec<UpdateItem>,
    pub from: Option<From>,
    pub r#where: Where,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateStatement {
    pub table: ParseToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeStatement {
    pub target_table: ParseToken,
    pub target_alias: Option<ParseToken>,
    pub source: MergeSource,
    pub source_alias: Option<ParseToken>,
    pub condition: Expr,
    pub whens: Vec<When>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeSource {
    Table(ParseToken),
    Subquery(QueryExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Merge {
    Update(MergeUpdate),
    Insert(MergeInsert),
    InsertRow,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeUpdate {
    pub update_items: Vec<UpdateItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeInsert {
    pub columns: Option<Vec<ParseToken>>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum When {
    Matched(WhenMatched),
    NotMatchedByTarget(WhenNotMatchedByTarget),
    NotMatchedBySource(WhenNotMatchedBySource),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenMatched {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenNotMatchedByTarget {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenNotMatchedBySource {
    pub search_condition: Option<Expr>,
    pub merge: Merge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    Number(String),
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
    Case(CaseExpr),
    GenericFunction(Box<GenericFunctionExpr>),
    Function(FunctionExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseExpr {
    pub case: Option<Box<Expr>>,
    pub when_thens: Vec<(Expr, Expr)>,
    pub r#else: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeExpr {
    pub r#type: Type,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionExpr {
    // list of known functions here
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all
    Array(ArrayFunctionExpr),
    ArrayAgg(ArrayAggFunctionExpr),
    Concat(ConcatFunctionExpr),
    Cast(CastFunctionExpr),
    SafeCast(SafeCastFunctionExpr),
    CurrentDate(CurrentDateFunctionExpr),
    CurrentTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentDateFunctionExpr {
    pub timezone: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayAggFunctionExpr {
    pub arg: Box<GenericFunctionExprArg>,
    pub over: Option<NamedWindowExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayFunctionExpr {
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcatFunctionExpr {
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastFunctionExpr {
    pub expr: Box<Expr>,
    pub r#type: ParameterizedType,
    pub format: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeCastFunctionExpr {
    pub expr: Box<Expr>,
    pub r#type: ParameterizedType,
    pub format: Option<Box<Expr>>,
}

/// Generic function call, whose signature is not yet implemented in the parser
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericFunctionExpr {
    pub name: ParseToken,
    pub arguments: Vec<GenericFunctionExprArg>,
    pub over: Option<NamedWindowExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericFunctionExprArg {
    pub expr: Expr,
    pub aggregate: Option<FunctionAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionAggregate {
    pub distinct: bool,
    pub nulls: Option<FunctionAggregateNulls>,
    pub having: Option<FunctionAggregateHaving>,
    pub order_by: Option<Vec<FunctionAggregateOrderBy>>,
    pub limit: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionAggregateOrderBy {
    pub expr: Box<Expr>,
    pub sort_direction: Option<OrderBySortDirection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionAggregateNulls {
    Ignore,
    Respect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionAggregateHaving {
    pub expr: Box<Expr>,
    pub kind: FunctionAggregateHavingKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionAggregateHavingKind {
    Max,
    Min,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnaryExpr {
    pub operator: ParseToken,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub operator: ParseToken,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingExpr {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayExpr {
    pub r#type: Option<Type>,
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructExpr {
    pub r#type: Option<Type>,
    pub fields: Vec<StructField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    pub expr: Expr,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryExpr {
    Grouping(GroupingQueryExpr),
    Select(SelectQueryExpr),
    SetSelect(SetSelectQueryExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingQueryExpr {
    pub with: Option<With>,
    pub query: Box<QueryExpr>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectQueryExpr {
    pub with: Option<With>,
    pub select: Select,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetSelectQueryExpr {
    pub with: Option<With>,
    pub left_query: Box<QueryExpr>,
    pub set_operator: SetQueryOperator,
    pub right_query: Box<QueryExpr>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SetQueryOperator {
    Union,
    UnionDistinct,
    IntersectDistinct,
    ExceptDistinct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub exprs: Vec<OrderByExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderBySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderByNulls {
    First,
    Last,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub sort_direction: Option<OrderBySortDirection>,
    pub nulls: Option<OrderByNulls>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limit {
    pub count: Box<Expr>,
    pub offset: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct With {
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Cte {
    NonRecursive(NonRecursiveCte),
    Recursive(RecursiveCte),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonRecursiveCte {
    pub name: ParseToken,
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecursiveCte {
    pub name: ParseToken,
    pub base_query: QueryExpr,
    pub recursive_query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub distinct: bool,
    pub table_value: Option<SelectTableValue>,
    pub exprs: Vec<SelectExpr>,
    pub from: Option<From>,
    pub r#where: Option<Where>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,
    pub window: Option<Window>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectTableValue {
    Struct,
    Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectExpr {
    Col(SelectColExpr),
    ColAll(SelectColAllExpr),
    All(SelectAllExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectColExpr {
    pub expr: Expr,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectColAllExpr {
    pub expr: Expr,
    pub except: Option<Vec<ParseToken>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAllExpr {
    pub except: Option<Vec<ParseToken>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct From {
    pub expr: Box<FromExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossJoinExpr {
    pub left: Box<FromExpr>,
    pub right: Box<FromExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinExpr {
    pub kind: JoinKind,
    pub left: Box<FromExpr>,
    pub right: Box<FromExpr>,
    pub cond: JoinCondition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<ParseToken>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnnestExpr {
    pub array: Box<Expr>,
    pub alias: Option<ParseToken>,
    pub with_offset: bool,
    pub offset_alias: Option<ParseToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathExpr {
    pub expr: ParseToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromPathExpr {
    pub path: PathExpr,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingFromExpr {
    pub query: Box<FromExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromGroupingQueryExpr {
    pub query: Box<QueryExpr>,
    pub alias: Option<ParseToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Where {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupByExpr {
    Items(Vec<Expr>),
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBy {
    pub expr: GroupByExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Having {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Qualify {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    pub named_windows: Vec<NamedWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOrderByExpr {
    pub expr: Expr,
    pub asc_desc: Option<OrderBySortDirection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedWindow {
    pub name: ParseToken,
    pub window: NamedWindowExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamedWindowExpr {
    Reference(ParseToken),
    WindowSpec(WindowSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub window_name: Option<ParseToken>,
    pub partition_by: Option<Vec<Expr>>,
    pub order_by: Option<Vec<WindowOrderByExpr>>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: Option<FrameBound>,
    pub end: Option<FrameBound>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(String),
    UnboundedFollowing,
    Following(String),
    CurrentRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameKind {
    Range,
    Rows,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(PartialEq, Clone, Debug, EnumDiscriminants, Serialize, Deserialize)]
#[strum_discriminants(name(TokenTypeVariant))]
pub enum TokenType {
    LeftParen,
    RightParen,
    LeftSquare,
    RightSquare,
    Comma,
    Dot,
    Minus,
    Plus,
    BitwiseNot,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    BitwiseRightShift,
    BitwiseLeftShift,
    Colon,
    Semicolon,
    Slash,
    Star,
    Tick,
    ConcatOperator,
    Bang,
    BangEqual,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    QuotedIdentifier(String),
    Identifier(String),
    String(String),
    RawString(String),
    Bytes(String),
    RawBytes(String),
    Number(String),
    Eof,

    // Reserved Keywords
    All,
    And,
    Any,
    Array,
    As,
    Asc,
    AssertRowsModified,
    At,
    Between,
    By,
    Case,
    Cast,
    Collate,
    Contains,
    Create,
    Cross,
    Cube,
    Current,
    Default,
    Define,
    Desc,
    Distinct,
    Else,
    End,
    Enum,
    Escape,
    Except,
    Exclude,
    Exists,
    Extract,
    False,
    Fetch,
    Following,
    For,
    From,
    Full,
    Group,
    Grouping,
    Groups,
    Hash,
    Having,
    If,
    Ignore,
    In,
    Inner,
    Intersect,
    Interval,
    Into,
    Is,
    Join,
    Lateral,
    Left,
    Like,
    Limit,
    Lookup,
    Merge,
    Natural,
    New,
    No,
    Not,
    Null,
    Nulls,
    Of,
    On,
    Or,
    Order,
    Outer,
    Over,
    Partition,
    Preceding,
    Proto,
    Qualify,
    Range,
    Recursive,
    Respect,
    Right,
    Rollup,
    Rows,
    Select,
    Set,
    Some,
    Struct,
    Tablesample,
    Then,
    To,
    Treat,
    True,
    Union,
    Unnest,
    Using,
    When,
    Where,
    Window,
    With,
    Within,
}

impl TokenTypeVariant {
    pub(crate) fn variant_str(&self) -> &str {
        match self {
            TokenTypeVariant::LeftParen => "(",
            TokenTypeVariant::RightParen => ")",
            TokenTypeVariant::LeftSquare => "[",
            TokenTypeVariant::RightSquare => "]",
            TokenTypeVariant::Comma => ",",
            TokenTypeVariant::Dot => ".",
            TokenTypeVariant::Minus => "-",
            TokenTypeVariant::Plus => "+",
            TokenTypeVariant::BitwiseNot => "~",
            TokenTypeVariant::BitwiseOr => "|",
            TokenTypeVariant::BitwiseAnd => "&",
            TokenTypeVariant::BitwiseXor => "^",
            TokenTypeVariant::BitwiseRightShift => ">>",
            TokenTypeVariant::BitwiseLeftShift => "<<",
            TokenTypeVariant::Colon => ":",
            TokenTypeVariant::Semicolon => ";",
            TokenTypeVariant::Slash => "/",
            TokenTypeVariant::Star => "*",
            TokenTypeVariant::Tick => "`",
            TokenTypeVariant::ConcatOperator => "||",
            TokenTypeVariant::Bang => "!",
            TokenTypeVariant::BangEqual => "!=",
            TokenTypeVariant::Equal => "=",
            TokenTypeVariant::NotEqual => "<>",
            TokenTypeVariant::Greater => ">",
            TokenTypeVariant::GreaterEqual => ">=",
            TokenTypeVariant::Less => "<",
            TokenTypeVariant::LessEqual => "<=",
            TokenTypeVariant::QuotedIdentifier => "QuotedIdentifier",
            TokenTypeVariant::Identifier => "Identifier",
            TokenTypeVariant::String => "String",
            TokenTypeVariant::RawString => "RawString",
            TokenTypeVariant::Bytes => "Bytes",
            TokenTypeVariant::RawBytes => "RawBytes",
            TokenTypeVariant::Number => "Number",
            TokenTypeVariant::Eof => "EOF",

            // Reserved Keywords
            TokenTypeVariant::All => "ALL",
            TokenTypeVariant::And => "AND",
            TokenTypeVariant::Any => "ANY",
            TokenTypeVariant::Array => "ARRAY",
            TokenTypeVariant::As => "AS",
            TokenTypeVariant::Asc => "ASC",
            TokenTypeVariant::AssertRowsModified => "ASSERT_ROWS_MODIFIED",
            TokenTypeVariant::At => "AT",
            TokenTypeVariant::Between => "BETWEEN",
            TokenTypeVariant::By => "BY",
            TokenTypeVariant::Case => "CASE",
            TokenTypeVariant::Cast => "CAST",
            TokenTypeVariant::Collate => "COLLATE",
            TokenTypeVariant::Contains => "CONTAINS",
            TokenTypeVariant::Create => "CREATE",
            TokenTypeVariant::Cross => "CROSS",
            TokenTypeVariant::Cube => "CUBE",
            TokenTypeVariant::Current => "CURRENT",
            TokenTypeVariant::Default => "DEFAULT",
            TokenTypeVariant::Define => "DEFINE",
            TokenTypeVariant::Desc => "DESC",
            TokenTypeVariant::Distinct => "DISTINCT",
            TokenTypeVariant::Else => "ELSE",
            TokenTypeVariant::End => "END",
            TokenTypeVariant::Enum => "ENUM",
            TokenTypeVariant::Escape => "ESCAPE",
            TokenTypeVariant::Except => "EXCEPT",
            TokenTypeVariant::Exclude => "EXCLUDE",
            TokenTypeVariant::Exists => "EXISTS",
            TokenTypeVariant::Extract => "EXTRACT",
            TokenTypeVariant::False => "FALSE",
            TokenTypeVariant::Fetch => "FETCH",
            TokenTypeVariant::Following => "FOLLOWING",
            TokenTypeVariant::For => "FOR",
            TokenTypeVariant::From => "FROM",
            TokenTypeVariant::Full => "FULL",
            TokenTypeVariant::Group => "GROUP",
            TokenTypeVariant::Grouping => "GROUPING",
            TokenTypeVariant::Groups => "GROUPS",
            TokenTypeVariant::Hash => "HASH",
            TokenTypeVariant::Having => "HAVING",
            TokenTypeVariant::If => "IF",
            TokenTypeVariant::Ignore => "IGNORE",
            TokenTypeVariant::In => "IN",
            TokenTypeVariant::Inner => "INNER",
            TokenTypeVariant::Intersect => "INTERSECT",
            TokenTypeVariant::Interval => "INTERVAL",
            TokenTypeVariant::Into => "INTO",
            TokenTypeVariant::Is => "IS",
            TokenTypeVariant::Join => "JOIN",
            TokenTypeVariant::Lateral => "LATERAL",
            TokenTypeVariant::Left => "LEFT",
            TokenTypeVariant::Like => "LIKE",
            TokenTypeVariant::Limit => "LIMIT",
            TokenTypeVariant::Lookup => "LOOKUP",
            TokenTypeVariant::Merge => "MERGE",
            TokenTypeVariant::Natural => "NATURAL",
            TokenTypeVariant::New => "NEW",
            TokenTypeVariant::No => "NO",
            TokenTypeVariant::Not => "NOT",
            TokenTypeVariant::Null => "NULL",
            TokenTypeVariant::Nulls => "NULLS",
            TokenTypeVariant::Of => "OF",
            TokenTypeVariant::On => "ON",
            TokenTypeVariant::Or => "OR",
            TokenTypeVariant::Order => "ORDER",
            TokenTypeVariant::Outer => "OUTER",
            TokenTypeVariant::Over => "OVER",
            TokenTypeVariant::Partition => "PARTITION",
            TokenTypeVariant::Preceding => "PRECEDING",
            TokenTypeVariant::Proto => "PROTO",
            TokenTypeVariant::Qualify => "QUALIFY",
            TokenTypeVariant::Range => "RANGE",
            TokenTypeVariant::Recursive => "RECURSIVE",
            TokenTypeVariant::Respect => "RESPECT",
            TokenTypeVariant::Right => "RIGHT",
            TokenTypeVariant::Rollup => "ROLLUP",
            TokenTypeVariant::Rows => "ROWS",
            TokenTypeVariant::Select => "SELECT",
            TokenTypeVariant::Set => "SET",
            TokenTypeVariant::Some => "SOME",
            TokenTypeVariant::Struct => "STRUCT",
            TokenTypeVariant::Tablesample => "TABLESAMPLE",
            TokenTypeVariant::Then => "THEN",
            TokenTypeVariant::To => "TO",
            TokenTypeVariant::Treat => "TREAT",
            TokenTypeVariant::True => "TRUE",
            TokenTypeVariant::Union => "UNION",
            TokenTypeVariant::Unnest => "UNNEST",
            TokenTypeVariant::Using => "USING",
            TokenTypeVariant::When => "WHEN",
            TokenTypeVariant::Where => "WHERE",
            TokenTypeVariant::Window => "WINDOW",
            TokenTypeVariant::With => "WITH",
            TokenTypeVariant::Within => "WITHIN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub kind: TokenType,
    pub lexeme: String,
    pub line: u32,
    pub col: u32,
}
