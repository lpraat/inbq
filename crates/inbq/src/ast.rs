use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumDiscriminants};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ast {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    Query(QueryStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(Box<UpdateStatement>),
    Truncate(TruncateStatement),
    Merge(Box<MergeStatement>),
    DeclareVar(DeclareVarStatement),
    SetVar(SetVarStatement),
    Block(StatementsBlock),
    CreateSchema(CreateSchemaStatement),
    CreateTable(Box<CreateTableStatement>),
    CreateSqlFunction(CreateSqlFunctionStatement),
    CreateJsFunction(CreateJsFunctionStatement),
    CreateView(CreateViewStatement),
    DropTableStatement(DropTableStatement),
    DropFunctionStatement(DropFunctionStatement),
    If(IfStatement),
    Case(CaseStatement),
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Raise(RaiseStatement),
    Return,
    Call(CallStatement),
    ExecuteImmediate(ExecuteImmediateStatement),
    Loop(LoopStatement),
    Repeat(RepeatStatement),
    While(WhileStatement),
    ForIn(ForInStatement),
    Break,
    Continue,
    Iterate,
    Leave,
    Labeled(LabeledStatement),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropFunctionStatement {
    pub name: PathName,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionArgumentType {
    Standard(Type),
    AnyType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionArgument {
    pub name: Name,
    pub r#type: FunctionArgumentType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSqlFunctionStatement {
    pub replace: bool,
    pub is_temporary: bool,
    pub if_not_exists: bool,
    pub name: PathName,
    pub arguments: Vec<FunctionArgument>,
    pub returns: Option<Type>,
    pub options: Option<Vec<DdlOption>>,
    pub body: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJsFunctionStatement {
    pub replace: bool,
    pub is_temporary: bool,
    pub if_not_exists: bool,
    pub name: PathName,
    pub arguments: Vec<FunctionArgument>,
    pub returns: Type,
    pub is_deterministic: Option<bool>,
    pub options: Option<Vec<DdlOption>>,
    pub body: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchemaStatement {
    pub name: PathName,
    pub if_not_exists: bool,
    pub default_collate: Option<Expr>,
    pub options: Option<Vec<DdlOption>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteImmediateStatement {
    pub sql: Expr,
    pub into_vars: Option<Vec<Name>>,
    pub using_identifiers: Option<Vec<ExecuteImmediateUsingIdentifier>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteImmediateUsingIdentifier {
    pub identifier: Expr,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateViewStatement {
    pub replace: bool,
    pub if_not_exists: bool,
    pub name: PathName,
    pub columns: Option<Vec<ViewColumn>>,
    pub options: Option<Vec<DdlOption>>,
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewColumn {
    pub name: Name,
    pub options: Option<Vec<DdlOption>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlOption {
    pub name: Name,
    pub value: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForInStatement {
    pub var_name: Name,
    pub table_expr: QueryExpr,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileStatement {
    pub condition: Expr,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatStatement {
    pub statements: Vec<Statement>,
    pub until: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopStatement {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledStatement {
    pub statement: Box<Statement>,
    pub start_label: Name,
    pub end_label: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Name {
    Identifier(Identifier),
    QuotedIdentifier(QuotedIdentifier),
}

impl Name {
    pub fn as_str(&self) -> &str {
        match self {
            Name::Identifier(identifier) => &identifier.name,
            Name::QuotedIdentifier(quoted_identifier) => &quoted_identifier.name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathName {
    pub name: String,
    pub parts: Vec<PathPart>,
}

impl std::convert::From<Vec<PathPart>> for PathName {
    fn from(value: Vec<PathPart>) -> Self {
        let name = value
            .iter()
            .map(|p| match p {
                PathPart::Identifier(identifier) => &identifier.name,
                PathPart::QuotedIdentifier(quoted_identifier) => &quoted_identifier.name,
                PathPart::Number(number) => &number.value,
                PathPart::DotSeparator => ".",
                PathPart::SlashSeparator => "/",
                PathPart::DashSeparator => "-",
                PathPart::ColonSeparator => ":",
            })
            .collect::<Vec<&str>>()
            .join("");
        Self { name, parts: value }
    }
}

impl PathName {
    pub fn identifiers(&self) -> Vec<&str> {
        self.parts
            .iter()
            .filter_map(|p| match p {
                PathPart::Identifier(identifier) => Some(identifier.name.as_str()),
                PathPart::QuotedIdentifier(quoted_identifier) => {
                    Some(quoted_identifier.name.as_str())
                }
                _ => None,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PathPart {
    Identifier(Identifier),
    QuotedIdentifier(QuotedIdentifier),
    Number(Number),
    DotSeparator,
    SlashSeparator,
    DashSeparator,
    ColonSeparator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseStatement {
    pub case: Option<Expr>,
    pub when_thens: Vec<CaseWhenThenStatements>,
    pub r#else: Option<Vec<Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseWhenThenStatements {
    pub when: Expr,
    pub then: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallStatement {
    pub procedure_name: PathName,
    pub arguments: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaiseStatement {
    pub message: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfStatement {
    pub r#if: IfBranch,
    pub else_ifs: Option<Vec<IfBranch>>,
    pub r#else: Option<Vec<Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfBranch {
    pub condition: Expr,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementsBlock {
    pub statements: Vec<Statement>,
    pub exception_statements: Option<Vec<Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareVarStatement {
    pub vars: Vec<Name>,
    pub r#type: Option<ParameterizedType>,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetVarStatement {
    pub vars: Vec<SetVariable>,
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SetVariable {
    UserVariable(Name),
    SystemVariable(SystemVariable),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub name: PathName,
    pub schema: Option<Vec<ColumnSchema>>,
    pub constraints: Option<Vec<TableConstraint>>,
    pub default_collate: Option<Expr>,
    pub partition: Option<Expr>,
    pub clustering_columns: Option<Vec<Name>>,
    pub connection: Option<PathName>,
    pub options: Option<Vec<DdlOption>>,
    pub replace: bool,
    pub is_temporary: bool,
    pub if_not_exists: bool,
    pub query: Option<QueryExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKeyNotEnforced(PrimaryKeyConstraintNotEnforced),
    ForeignKeyNotEnforced(ForeignKeyConstraintNotEnforced),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyConstraintNotEnforced {
    pub columns: Vec<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraintNotEnforced {
    pub name: Option<Name>,
    pub columns: Vec<Name>,
    pub reference: ForeignKeyReference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyReference {
    pub table: PathName,
    pub columns: Vec<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub name: PathName,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: Name,
    pub r#type: ParameterizedType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterizedType {
    Array {
        r#type: Box<ParameterizedType>,
    },
    BigNumeric {
        precision: Option<String>,
        scale: Option<String>,
    },
    Bool,
    Bytes {
        max_length: Option<String>,
    },
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Interval,
    Json,
    Numeric {
        precision: Option<String>,
        scale: Option<String>,
    },
    Range {
        r#type: Box<ParameterizedType>,
    },
    String {
        max_length: Option<String>,
    },
    Struct {
        fields: Vec<StructParameterizedFieldType>,
    },
    Time,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructParameterizedFieldType {
    pub name: Name,
    pub r#type: ParameterizedType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Type {
    Array { r#type: Box<Type> },
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
    Range { r#type: Box<Type> },
    String,
    Struct { fields: Vec<StructFieldType> },
    Time,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructFieldType {
    pub name: Option<Name>,
    pub r#type: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatement {
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: PathName,
    pub columns: Option<Vec<Name>>,
    pub values: Option<Vec<Expr>>,
    pub query: Option<QueryExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: PathName,
    pub alias: Option<Name>,
    pub cond: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateItem {
    pub column: Expr,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: PathName,
    pub alias: Option<Name>,
    pub update_items: Vec<UpdateItem>,
    pub from: Option<From>,
    pub r#where: Where,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateStatement {
    pub table: PathName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeStatement {
    pub target_table: PathName,
    pub target_alias: Option<Name>,
    pub source: MergeSource,
    pub source_alias: Option<Name>,
    pub condition: Expr,
    pub whens: Vec<When>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeSource {
    Table(PathName),
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
    pub columns: Option<Vec<Name>>,
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
    Identifier(Identifier),
    QuotedIdentifier(QuotedIdentifier),
    QueryNamedParameter(String),
    QueryPositionalParameter,
    SystemVariable(SystemVariable),
    String(String),
    RawString(String),
    StringConcat(StringConcatExpr),
    Bytes(String),
    RawBytes(String),
    BytesConcat(BytesConcatExpr),
    Numeric(String),
    BigNumeric(String),
    Number(Number),
    Bool(bool),
    With(WithExpr),
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
    Query(Box<QueryExpr>),
    Case(CaseExpr),
    GenericFunction(Box<GenericFunctionExpr>),
    Function(Box<FunctionExpr>),
    QuantifiedLike(QuantifiedLikeExpr),
    Exists(Box<QueryExpr>),
    Unnest(UnnestExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnnestExpr {
    pub array: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithExpr {
    pub vars: Vec<WithExprVar>,
    pub result: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithExprVar {
    pub name: Name,
    pub value: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemVariable {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Number {
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identifier {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotedIdentifier {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringConcatExpr {
    pub strings: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BytesConcatExpr {
    pub bytes: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseExpr {
    pub case: Option<Box<Expr>>,
    pub when_thens: Vec<WhenThen>,
    pub r#else: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenThen {
    pub when: Expr,
    pub then: Expr,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    Coalesce(CoalesceFunctionExpr),
    Concat(ConcatFunctionExpr),
    Cast(CastFunctionExpr),
    SafeCast(SafeCastFunctionExpr),
    Extract(ExtractFunctionExpr),
    If(IfFunctionExpr),

    /// Date
    CurrentDate(CurrentDateFunctionExpr),
    DateDiff(DateDiffFunctionExpr),
    DateTrunc(DateTruncFunctionExpr),

    /// Datetime
    CurrentDatetime(CurrentDatetimeFunctionExpr),
    DatetimeDiff(DatetimeDiffFunctionExpr),
    DatetimeTrunc(DatetimeTruncFunctionExpr),

    /// Timestamp
    CurrentTimestamp,
    TimestampDiff(TimestampDiffFunctionExpr),
    TimestampTrunc(TimestampTruncFunctionExpr),

    /// Time
    CurrentTime(CurrentTimeFunctionExpr),
    TimeDiff(TimeDiffFunctionExpr),
    TimeTrunc(TimeTruncFunctionExpr),

    LastDay(LastDayFunctionExpr),

    Left(LeftFunctionExpr),
    Right(RightFunctionExpr),

    // String
    Normalize(NormalizeFunctionExpr),
    NormalizeAndCasefold(NormalizeAndCasefoldFunctionExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoalesceFunctionExpr {
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeFunctionExpr {
    pub value: Expr,
    pub mode: NormalizationMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeAndCasefoldFunctionExpr {
    pub value: Expr,
    pub mode: NormalizationMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NormalizationMode {
    NFC,
    NFKC,
    NFD,
    NFKD,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentDatetimeFunctionExpr {
    pub timezone: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentTimeFunctionExpr {
    pub timezone: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastDayFunctionExpr {
    pub expr: Expr,
    pub granularity: Option<Granularity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateTruncFunctionExpr {
    pub date: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateDiffFunctionExpr {
    pub start_date: Expr,
    pub end_date: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatetimeTruncFunctionExpr {
    pub datetime: Expr,
    pub granularity: Granularity,
    pub timezone: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatetimeDiffFunctionExpr {
    pub start_datetime: Expr,
    pub end_datetime: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampTruncFunctionExpr {
    pub timestamp: Expr,
    pub granularity: Granularity,
    pub timezone: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampDiffFunctionExpr {
    pub start_timestamp: Expr,
    pub end_timestamp: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTruncFunctionExpr {
    pub time: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeDiffFunctionExpr {
    pub start_time: Expr,
    pub end_time: Expr,
    pub granularity: Granularity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Granularity {
    MicroSecond,
    MilliSecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    WeekWithBegin(WeekBegin),
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
    Date,
    Time,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RightFunctionExpr {
    pub value: Expr,
    pub length: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeftFunctionExpr {
    pub value: Expr,
    pub length: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractFunctionExpr {
    pub part: ExtractFunctionPart,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ExtractFunctionPart {
    MicroSecond,
    MilliSecond,
    Second,
    Minute,
    Hour,
    DayOfWeek,
    Day,
    DayOfYear,
    Week,
    WeekWithBegin(WeekBegin),
    IsoWeek,
    Month,
    Quarter,
    Year,
    IsoYear,
    Date,
    Time,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WeekBegin {
    Sunday,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentDateFunctionExpr {
    pub timezone: Option<Expr>,
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
    pub format: Option<CastFunctionFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeCastFunctionExpr {
    pub expr: Box<Expr>,
    pub r#type: ParameterizedType,
    pub format: Option<CastFunctionFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastFunctionFormat {
    pub format: Expr,
    pub time_zone: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfFunctionExpr {
    pub condition: Box<Expr>,
    pub true_result: Box<Expr>,
    pub false_result: Box<Expr>,
}

/// Generic function call, whose signature is not yet implemented in the parser
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericFunctionExpr {
    pub name: Name,
    pub arguments: Vec<GenericFunctionExprArg>,
    pub over: Option<NamedWindowExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericFunctionExprArg {
    pub name: Option<Name>,
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
    pub nulls: Option<OrderByNulls>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FunctionAggregateNulls {
    Ignore,
    Respect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionAggregateHaving {
    pub expr: Box<Expr>,
    pub kind: FunctionAggregateHavingKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FunctionAggregateHavingKind {
    Max,
    Min,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    BitwiseNot,
    IsNull,
    IsNotNull,
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub operator: BinaryOperator,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Display)]
pub enum BinaryOperator {
    BitwiseNot,
    Star,
    Slash,
    Concat,
    Plus,
    Minus,
    BitwiseLeftShift,
    BitwiseRightShift,
    BitwiseAnd,
    BitwiseXor,
    BitwiseOr,
    Equal,
    LessThan,
    GreaterThan,
    LessThanOrEqualTo,
    GreaterThanOrEqualTo,
    NotEqual,
    Like,
    NotLike,
    QuantifiedLike,
    QuantifiedNotLike,
    Between,
    NotBetween,
    In,
    NotIn,
    IsDistinctFrom,
    IsNotDistinctFrom,
    And,
    Or,
    ArrayIndex,
    FieldAccess,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LikeQuantifier {
    Any,
    Some,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuantifiedLikeExprPattern {
    ExprList { exprs: Vec<Expr> },
    ArrayUnnest { expr: Box<Expr> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantifiedLikeExpr {
    pub quantifier: LikeQuantifier,
    pub pattern: QuantifiedLikeExprPattern,
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
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryExpr {
    Grouping(GroupingQueryExpr),
    Select(Box<SelectQueryExpr>),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OrderBySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    pub name: Name,
    pub query: QueryExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecursiveCte {
    pub name: Name,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectColAllExpr {
    pub expr: Expr,
    pub except: Option<Vec<Name>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAllExpr {
    pub except: Option<Vec<Name>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct From {
    pub expr: Box<FromExpr>,
    pub pivot: Option<Pivot>,
    pub unpivot: Option<Unpivot>,
    pub table_sample: Option<TableSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pivot {
    pub aggregates: Vec<PivotAggregate>,
    pub input_column: Name,
    pub pivot_columns: Vec<PivotColumn>,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PivotAggregate {
    pub expr: Expr,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PivotColumn {
    pub expr: Expr,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unpivot {
    pub nulls: UnpivotNulls,
    pub kind: UnpivotKind,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSample {
    pub percent: Expr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnpivotNulls {
    Include,
    Exclude,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnpivotKind {
    SingleColumn(SingleColumnUnpivot),
    MultiColumn(MultiColumnUnpivot),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleColumnUnpivot {
    pub values_column: Name,
    pub name_column: Name,
    pub columns_to_unpivot: Vec<ColumnToUnpivot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiColumnUnpivot {
    pub values_columns: Vec<Name>,
    pub name_column: Name,
    pub column_sets_to_unpivot: Vec<ColumnSetToUnpivot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnToUnpivot {
    pub name: Name,
    pub alias: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSetToUnpivot {
    pub names: Vec<Name>,
    pub alias: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FromExpr {
    Join(JoinExpr),
    FullJoin(JoinExpr),
    LeftJoin(JoinExpr),
    RightJoin(JoinExpr),
    CrossJoin(CrossJoinExpr),
    Path(FromPathExpr),
    TableFunction(TableFunctionExpr),
    Unnest(FromUnnestExpr),
    GroupingQuery(FromGroupingQueryExpr),
    GroupingFrom(GroupingFromExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFunctionExpr {
    pub name: PathName,
    pub arguments: Vec<TableFunctionArgument>,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableFunctionArgument {
    Table(PathName),
    Expr(Expr),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinCondition {
    On(Expr),
    Using { columns: Vec<Name> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromUnnestExpr {
    pub array: Box<Expr>,
    pub alias: Option<Name>,
    pub with_offset: bool,
    pub offset_alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromPathExpr {
    pub path: PathName,
    pub alias: Option<Name>,
    pub system_time: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingFromExpr {
    pub query: Box<FromExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromGroupingQueryExpr {
    pub query: Box<QueryExpr>,
    pub alias: Option<Name>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Where {
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupByExpr {
    Items { exprs: Vec<Expr> },
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
    pub sort_direction: Option<OrderBySortDirection>,
    pub nulls: Option<OrderByNulls>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedWindow {
    pub name: Name,
    pub window: NamedWindowExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NamedWindowExpr {
    Reference(Name),
    WindowSpec(WindowSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub window_name: Option<Name>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WindowFrameKind {
    Range,
    Rows,
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
    RightArrow,
    QuotedIdentifier(String),
    Identifier(String),
    String(String),
    RawString(String),
    Bytes(String),
    RawBytes(String),
    Number(String),
    QueryNamedParameter(String),
    QueryPositionalParameter,
    SystemVariable(String),
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
            TokenTypeVariant::RightArrow => "=>",
            TokenTypeVariant::QuotedIdentifier => "QuotedIdentifier",
            TokenTypeVariant::Identifier => "Identifier",
            TokenTypeVariant::String => "String",
            TokenTypeVariant::RawString => "RawString",
            TokenTypeVariant::Bytes => "Bytes",
            TokenTypeVariant::RawBytes => "RawBytes",
            TokenTypeVariant::Number => "Number",
            TokenTypeVariant::Eof => "EOF",
            TokenTypeVariant::QueryNamedParameter => "QueryNamedParameter",
            TokenTypeVariant::QueryPositionalParameter => "QueryPositionalParameter",
            TokenTypeVariant::SystemVariable => "SystemVariable",

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
