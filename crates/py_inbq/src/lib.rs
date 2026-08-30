use std::mem::{self, MaybeUninit};

use anyhow::anyhow;
use pyo3::{
    BoundObject,
    exceptions::{PyModuleNotFoundError, PyRuntimeError, PyValueError},
    ffi::c_str,
    intern,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyTuple},
};
use rayon::prelude::*;

use inbq::{
    ast::{
        ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, BinaryOperator,
        BytesConcatExpr, CallStatement, CaseExpr, CaseStatement, CaseWhenThenStatements,
        CastFunctionExpr, CastFunctionFormat, ChainedFunctionExpr, ChainedGenericFunctionExpr,
        CoalesceFunctionExpr, ColumnSchema, ColumnSetToUnpivot, ColumnToUnpivot,
        ConcatFunctionExpr, CreateJsFunctionStatement, CreateSchemaStatement,
        CreateSqlFunctionStatement, CreateTableStatement, CreateViewStatement, CrossJoinExpr, Cte,
        CurrentDateFunctionExpr, CurrentDatetimeFunctionExpr, CurrentTimeFunctionExpr,
        DateDiffFunctionExpr, DateTruncFunctionExpr, DatetimeDiffFunctionExpr,
        DatetimeTruncFunctionExpr, DdlOption, DeclareVarStatement, DeleteStatement,
        DifferentialPrivacy, DifferentialPrivacyOption, DropFunctionStatement, DropSchemaMode,
        DropSchemaStatement, DropTableStatement, DropViewStatement, ExecuteImmediateStatement,
        ExecuteImmediateUsingIdentifier, Expr, ExtractFunctionExpr, ExtractFunctionPart,
        ForInStatement, ForeignKeyConstraintNotEnforced, ForeignKeyReference, FrameBound, FromExpr,
        FromGroupingQueryExpr, FromPathExpr, FromUnnestExpr, FunctionAggregate,
        FunctionAggregateHaving, FunctionAggregateHavingKind, FunctionAggregateNulls,
        FunctionAggregateOrderBy, FunctionArgument, FunctionArgumentType, FunctionExpr,
        GenericFunctionExpr, GenericFunctionExprArg, Granularity, GroupBy, GroupByExpr,
        GroupingExpr, GroupingFromExpr, GroupingQueryExpr, Having, Identifier, IfBranch,
        IfFunctionExpr, IfStatement, InsertStatement, IntervalExpr, IntervalPart, JoinCondition,
        JoinExpr, JoinKind, LabeledStatement, LastDayFunctionExpr, LeftFunctionExpr,
        LikeQuantifier, Limit, LoopStatement, Merge, MergeInsert, MergeSource, MergeStatement,
        MergeUpdate, MultiColumnUnpivot, Name, NamedWindow, NamedWindowExpr, NonRecursiveCte,
        NormalizationMode, NormalizeAndCasefoldFunctionExpr, NormalizeFunctionExpr, Number,
        OrderBy, OrderByExpr, OrderByNulls, OrderBySortDirection, ParameterizedType, PathName,
        PathPart, Pivot, PivotAggregate, PivotColumn, PrimaryKeyConstraintNotEnforced, Qualify,
        QuantifiedLikeExpr, QuantifiedLikeExprPattern, QueryExpr, QueryStatement, QuotedIdentifier,
        RaiseStatement, RangeExpr, RecursiveCte, RepeatStatement, RightFunctionExpr,
        SafeCastFunctionExpr, Select, SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr,
        SelectQueryExpr, SelectTableValue, SetQueryOperator, SetSelectQueryExpr, SetVarStatement,
        SetVariable, SingleColumnUnpivot, Statement, StatementsBlock, StringConcatExpr, StructExpr,
        StructField, StructFieldType, StructParameterizedFieldType, SystemVariable,
        TableConstraint, TableFunctionArgument, TableFunctionExpr, TableOperator, TableSample,
        TimeDiffFunctionExpr, TimeTruncFunctionExpr, TimestampDiffFunctionExpr,
        TimestampTruncFunctionExpr, Token, TokenType, TruncateStatement, Type, UnaryExpr,
        UnaryOperator, UndropSchemaStatement, UnnestExpr, Unpivot, UnpivotKind, UnpivotNulls,
        UpdateItem, UpdateStatement, ViewColumn, WeekBegin, When, WhenMatched,
        WhenNotMatchedBySource, WhenNotMatchedByTarget, WhenThen, Where, WhileStatement, Window,
        WindowFrame, WindowFrameKind, WindowOrderByExpr, WindowSpec, With, WithExpr, WithExprVar,
    },
    lineage::{
        Lineage, RawLineage, RawLineageNode, RawLineageObject, ReadyLineage, ReadyLineageNode,
        ReadyLineageNodeInput, ReadyLineageNodeSideInput, ReadyLineageObject, ReferencedColumns,
        ReferencedNode, ReferencedObject, catalog::Catalog, extract_lineage,
    },
};

struct PyContext<'a> {
    py: Python<'a>,
    inbq_module: Bound<'a, PyModule>,
    ast_nodes: Bound<'a, PyAny>,
    lineage: Bound<'a, PyAny>,
}

impl<'a> PyContext<'a> {
    fn new(py: Python<'a>) -> anyhow::Result<Self> {
        let inbq_module = py
            .import(intern!(py, "inbq"))
            .map_err(|e| PyModuleNotFoundError::new_err(e.to_string()))?;

        let ast_nodes = inbq_module.getattr(intern!(py, "ast_nodes"))?;
        let lineage = inbq_module.getattr(intern!(py, "lineage"))?;
        Ok(Self {
            py,
            inbq_module,
            ast_nodes,
            lineage,
        })
    }

    fn get_ast_class<N>(&self, cls_name: N) -> anyhow::Result<Bound<'a, PyAny>>
    where
        N: IntoPyObject<'a, Target = PyString>,
    {
        Ok(self.ast_nodes.getattr(cls_name)?)
    }

    fn get_lineage_class<N>(&self, cls_name: N) -> anyhow::Result<Bound<'a, PyAny>>
    where
        N: IntoPyObject<'a, Target = PyString>,
    {
        Ok(self.lineage.getattr(cls_name)?)
    }
}

macro_rules! get_ast_class {
    ($py_ctx:expr, $struct:ident) => {
        $py_ctx.get_ast_class(intern!($py_ctx.py, stringify!($struct)))
    };
    ($py_ctx:expr, $enum:ident::$variant:ident) => {
        $py_ctx.get_ast_class(intern!(
            $py_ctx.py,
            concat!(stringify!($enum), "_", stringify!($variant))
        ))
    };
}

macro_rules! get_lineage_class {
    ($py_ctx:expr, $struct:ident) => {
        $py_ctx.get_lineage_class(intern!($py_ctx.py, stringify!($struct)))
    };
    ($py_ctx:expr, $enum:ident::$variant:ident) => {
        $py_ctx.get_lineage_class(intern!(
            $py_ctx.py,
            concat!(stringify!($enum), "_", stringify!($variant))
        ))
    };
}

macro_rules! arg {
    ($py_ctx:expr, $rs_field:expr) => {
        $rs_field.to_py_obj($py_ctx)?
    };
}

trait RsToPyObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>>;
}

fn instantiate_py_class_from_args<'py>(
    py_ctx: &PyContext<'py>,
    cls: Bound<'py, PyAny>,
    args: &[Bound<'py, PyAny>],
) -> anyhow::Result<Bound<'py, PyAny>> {
    let args = PyTuple::new(py_ctx.py, args)?;
    cls.call(args, None).map_err(|e| anyhow!(e))
}

impl<T: RsToPyObject> RsToPyObject for Option<T> {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        if let Some(value) = self {
            value.to_py_obj(py_ctx)
        } else {
            Ok(PyNone::get(py_ctx.py).as_any().to_owned())
        }
    }
}

impl<T: RsToPyObject> RsToPyObject for Box<T> {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        self.as_ref().to_py_obj(py_ctx)
    }
}

impl RsToPyObject for bool {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyBool::new(py_ctx.py, *self).as_any().to_owned())
    }
}

impl RsToPyObject for &str {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyString::new(py_ctx.py, self).as_any().to_owned())
    }
}

impl RsToPyObject for String {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyString::new(py_ctx.py, self).as_any().to_owned())
    }
}

impl<T: RsToPyObject> RsToPyObject for Vec<T> {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let mut py_list = vec![];
        for el in self {
            py_list.push(el.to_py_obj(py_ctx)?);
        }
        Ok(PyList::new(py_ctx.py, py_list)?.as_any().to_owned())
    }
}

impl<T1: RsToPyObject, T2: RsToPyObject> RsToPyObject for (T1, T2) {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyTuple::new(
            py_ctx.py,
            vec![self.0.to_py_obj(py_ctx)?, self.1.to_py_obj(py_ctx)?],
        )?
        .as_any()
        .to_owned())
    }
}

impl RsToPyObject for f32 {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyFloat::new(py_ctx.py, *self as f64).as_any().to_owned())
    }
}

impl RsToPyObject for f64 {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyFloat::new(py_ctx.py, *self).as_any().to_owned())
    }
}

impl RsToPyObject for u16 {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyInt::new(py_ctx.py, self).as_any().to_owned())
    }
}

impl RsToPyObject for u32 {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyInt::new(py_ctx.py, self).as_any().to_owned())
    }
}

impl RsToPyObject for u64 {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyInt::new(py_ctx.py, self).as_any().to_owned())
    }
}

impl RsToPyObject for usize {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        Ok(PyInt::new(py_ctx.py, self).as_any().to_owned())
    }
}

// TODO: below we have a lot of boilerplate code we could autogenerate in the inbq_genpy crate

impl RsToPyObject for TokenType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TokenType::LeftParen => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::LeftParen)?,
                &[],
            ),
            TokenType::RightParen => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::RightParen)?,
                &[],
            ),
            TokenType::LeftSquare => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::LeftSquare)?,
                &[],
            ),
            TokenType::RightSquare => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::RightSquare)?,
                &[],
            ),
            TokenType::Comma => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Comma)?,
                &[],
            ),
            TokenType::Dot => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Dot)?, &[])
            }
            TokenType::Minus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Minus)?,
                &[],
            ),
            TokenType::Plus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Plus)?,
                &[],
            ),
            TokenType::BitwiseNot => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseNot)?,
                &[],
            ),
            TokenType::BitwiseOr => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseOr)?,
                &[],
            ),
            TokenType::BitwiseAnd => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseAnd)?,
                &[],
            ),
            TokenType::BitwiseXor => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseXor)?,
                &[],
            ),
            TokenType::BitwiseRightShift => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseRightShift)?,
                &[],
            ),
            TokenType::BitwiseLeftShift => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseLeftShift)?,
                &[],
            ),
            TokenType::Colon => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Colon)?,
                &[],
            ),
            TokenType::Semicolon => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Semicolon)?,
                &[],
            ),
            TokenType::Slash => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Slash)?,
                &[],
            ),
            TokenType::Star => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Star)?,
                &[],
            ),
            TokenType::Tick => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Tick)?,
                &[],
            ),
            TokenType::ConcatOperator => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::ConcatOperator)?,
                &[],
            ),
            TokenType::Bang => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Bang)?,
                &[],
            ),
            TokenType::BangEqual => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BangEqual)?,
                &[],
            ),
            TokenType::Equal => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Equal)?,
                &[],
            ),
            TokenType::NotEqual => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::NotEqual)?,
                &[],
            ),
            TokenType::Greater => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Greater)?,
                &[],
            ),
            TokenType::GreaterEqual => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::GreaterEqual)?,
                &[],
            ),
            TokenType::Less => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Less)?,
                &[],
            ),
            TokenType::LessEqual => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::LessEqual)?,
                &[],
            ),
            TokenType::RightArrow => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::RightArrow)?,
                &[],
            ),
            TokenType::QuotedIdentifier(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::QuotedIdentifier)?,
                    args,
                )
            }
            TokenType::Identifier(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::Identifier)?,
                    args,
                )
            }
            TokenType::QueryNamedParameter(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::QueryNamedParameter)?,
                    args,
                )
            }
            TokenType::QueryPositionalParameter => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::QueryPositionalParameter)?,
                &[],
            ),
            TokenType::SystemVariable(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::SystemVariable)?,
                    args,
                )
            }
            TokenType::String(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::String)?,
                    args,
                )
            }
            TokenType::RawString(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::RawString)?,
                    args,
                )
            }
            TokenType::Bytes(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::Bytes)?,
                    args,
                )
            }
            TokenType::RawBytes(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::RawBytes)?,
                    args,
                )
            }
            TokenType::Number(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::Number)?,
                    args,
                )
            }
            TokenType::Eof => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Eof)?, &[])
            }
            TokenType::All => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::All)?, &[])
            }
            TokenType::And => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::And)?, &[])
            }
            TokenType::Any => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Any)?, &[])
            }
            TokenType::Array => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Array)?,
                &[],
            ),
            TokenType::As => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::As)?, &[])
            }
            TokenType::Asc => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Asc)?, &[])
            }
            TokenType::AssertRowsModified => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::AssertRowsModified)?,
                &[],
            ),
            TokenType::At => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::At)?, &[])
            }
            TokenType::Between => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Between)?,
                &[],
            ),
            TokenType::By => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::By)?, &[])
            }
            TokenType::Case => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Case)?,
                &[],
            ),
            TokenType::Cast => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Cast)?,
                &[],
            ),
            TokenType::Collate => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Collate)?,
                &[],
            ),
            TokenType::Contains => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Contains)?,
                &[],
            ),
            TokenType::Create => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Create)?,
                &[],
            ),
            TokenType::Cross => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Cross)?,
                &[],
            ),
            TokenType::Cube => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Cube)?,
                &[],
            ),
            TokenType::Current => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Current)?,
                &[],
            ),
            TokenType::Default => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Default)?,
                &[],
            ),
            TokenType::Define => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Define)?,
                &[],
            ),
            TokenType::Desc => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Desc)?,
                &[],
            ),
            TokenType::Distinct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Distinct)?,
                &[],
            ),
            TokenType::Else => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Else)?,
                &[],
            ),
            TokenType::End => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::End)?, &[])
            }
            TokenType::Enum => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Enum)?,
                &[],
            ),
            TokenType::Escape => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Escape)?,
                &[],
            ),
            TokenType::Except => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Except)?,
                &[],
            ),
            TokenType::Exclude => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Exclude)?,
                &[],
            ),
            TokenType::Exists => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Exists)?,
                &[],
            ),
            TokenType::Extract => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Extract)?,
                &[],
            ),
            TokenType::False => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::False)?,
                &[],
            ),
            TokenType::Fetch => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Fetch)?,
                &[],
            ),
            TokenType::Following => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Following)?,
                &[],
            ),
            TokenType::For => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::For)?, &[])
            }
            TokenType::From => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::From)?,
                &[],
            ),
            TokenType::Full => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Full)?,
                &[],
            ),
            TokenType::Group => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Group)?,
                &[],
            ),
            TokenType::Grouping => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Grouping)?,
                &[],
            ),
            TokenType::Groups => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Groups)?,
                &[],
            ),
            TokenType::Hash => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Hash)?,
                &[],
            ),
            TokenType::Having => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Having)?,
                &[],
            ),
            TokenType::If => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::If)?, &[])
            }
            TokenType::Ignore => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Ignore)?,
                &[],
            ),
            TokenType::In => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::In)?, &[])
            }
            TokenType::Inner => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Inner)?,
                &[],
            ),
            TokenType::Intersect => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Intersect)?,
                &[],
            ),
            TokenType::Interval => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Interval)?,
                &[],
            ),
            TokenType::Into => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Into)?,
                &[],
            ),
            TokenType::Is => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Is)?, &[])
            }
            TokenType::Join => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Join)?,
                &[],
            ),
            TokenType::Lateral => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Lateral)?,
                &[],
            ),
            TokenType::Left => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Left)?,
                &[],
            ),
            TokenType::Like => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Like)?,
                &[],
            ),
            TokenType::Limit => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Limit)?,
                &[],
            ),
            TokenType::Lookup => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Lookup)?,
                &[],
            ),
            TokenType::Merge => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Merge)?,
                &[],
            ),
            TokenType::Natural => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Natural)?,
                &[],
            ),
            TokenType::New => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::New)?, &[])
            }
            TokenType::No => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::No)?, &[])
            }
            TokenType::Not => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Not)?, &[])
            }
            TokenType::Null => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Null)?,
                &[],
            ),
            TokenType::Nulls => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Nulls)?,
                &[],
            ),
            TokenType::Of => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Of)?, &[])
            }
            TokenType::On => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::On)?, &[])
            }
            TokenType::Or => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Or)?, &[])
            }
            TokenType::Order => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Order)?,
                &[],
            ),
            TokenType::Outer => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Outer)?,
                &[],
            ),
            TokenType::Over => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Over)?,
                &[],
            ),
            TokenType::Partition => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Partition)?,
                &[],
            ),
            TokenType::Preceding => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Preceding)?,
                &[],
            ),
            TokenType::Proto => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Proto)?,
                &[],
            ),
            TokenType::Qualify => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Qualify)?,
                &[],
            ),
            TokenType::Range => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Range)?,
                &[],
            ),
            TokenType::Recursive => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Recursive)?,
                &[],
            ),
            TokenType::Respect => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Respect)?,
                &[],
            ),
            TokenType::Right => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Right)?,
                &[],
            ),
            TokenType::Rollup => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Rollup)?,
                &[],
            ),
            TokenType::Rows => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Rows)?,
                &[],
            ),
            TokenType::Select => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Select)?,
                &[],
            ),
            TokenType::Set => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::Set)?, &[])
            }
            TokenType::Some => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Some)?,
                &[],
            ),
            TokenType::Struct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Struct)?,
                &[],
            ),
            TokenType::Tablesample => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Tablesample)?,
                &[],
            ),
            TokenType::Then => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Then)?,
                &[],
            ),
            TokenType::To => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TokenType::To)?, &[])
            }
            TokenType::Treat => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Treat)?,
                &[],
            ),
            TokenType::True => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::True)?,
                &[],
            ),
            TokenType::Union => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Union)?,
                &[],
            ),
            TokenType::Unnest => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Unnest)?,
                &[],
            ),
            TokenType::Using => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Using)?,
                &[],
            ),
            TokenType::When => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::When)?,
                &[],
            ),
            TokenType::Where => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Where)?,
                &[],
            ),
            TokenType::Window => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Window)?,
                &[],
            ),
            TokenType::With => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::With)?,
                &[],
            ),
            TokenType::Within => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::Within)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for Token<'_> {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.lexeme),
            arg!(py_ctx, self.line),
            arg!(py_ctx, self.col),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Token)?, args)
    }
}

impl RsToPyObject for Name {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Name::Identifier(identifier) => {
                let args = &[arg!(py_ctx, identifier)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Name::Identifier)?,
                    args,
                )
            }
            Name::QuotedIdentifier(quoted_identifier) => {
                let args = &[arg!(py_ctx, quoted_identifier)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Name::QuotedIdentifier)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for Number {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.value)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Number)?, args)
    }
}

impl RsToPyObject for PathPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            PathPart::Identifier(identifier) => {
                let args = &[arg!(py_ctx, identifier)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, PathPart::Identifier)?,
                    args,
                )
            }
            PathPart::QuotedIdentifier(quoted_identifier) => {
                let args = &[arg!(py_ctx, quoted_identifier)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, PathPart::QuotedIdentifier)?,
                    args,
                )
            }
            PathPart::Number(number) => {
                let args = &[arg!(py_ctx, number)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, PathPart::Number)?,
                    args,
                )
            }
            PathPart::DotSeparator => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::DotSeparator)?,
                &[],
            ),
            PathPart::SlashSeparator => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::SlashSeparator)?,
                &[],
            ),
            PathPart::DashSeparator => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::DashSeparator)?,
                &[],
            ),
            PathPart::ColonSeparator => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::ColonSeparator)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for PathName {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.parts)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, PathName)?, args)
    }
}

impl RsToPyObject for StructFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.r#type)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, StructFieldType)?, args)
    }
}

impl RsToPyObject for Type {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Type::Array { r#type } => {
                let args = &[arg!(py_ctx, r#type)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Array)?, args)
            }
            Type::BigNumeric => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Type::BigNumeric)?,
                &[],
            ),
            Type::Bool => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Bool)?, &[])
            }
            Type::Bytes => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Bytes)?, &[])
            }
            Type::Date => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Date)?, &[])
            }
            Type::Datetime => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Datetime)?, &[])
            }
            Type::Float64 => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Float64)?, &[])
            }
            Type::Geography => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Type::Geography)?,
                &[],
            ),
            Type::Int64 => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Int64)?, &[])
            }
            Type::Interval => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Interval)?, &[])
            }
            Type::Json => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Json)?, &[])
            }
            Type::Numeric => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Numeric)?, &[])
            }
            Type::Range { r#type } => {
                let args = &[arg!(py_ctx, r#type)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Range)?, args)
            }
            Type::String => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::String)?, &[])
            }
            Type::Struct { fields } => {
                let args = &[arg!(py_ctx, fields)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Struct)?, args)
            }
            Type::Time => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Type::Time)?, &[])
            }
            Type::Timestamp => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Type::Timestamp)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for StructParameterizedFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.r#type)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, StructParameterizedFieldType)?,
            args,
        )
    }
}

impl RsToPyObject for ParameterizedType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ParameterizedType::Array { r#type } => {
                let args = &[arg!(py_ctx, r#type)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Array)?,
                    args,
                )
            }
            ParameterizedType::BigNumeric { precision, scale } => {
                let args = &[arg!(py_ctx, precision), arg!(py_ctx, scale)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::BigNumeric)?,
                    args,
                )
            }
            ParameterizedType::Bool => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Bool)?,
                &[],
            ),
            ParameterizedType::Bytes { max_length } => {
                let args = &[arg!(py_ctx, max_length)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Bytes)?,
                    args,
                )
            }
            ParameterizedType::Date => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Date)?,
                &[],
            ),
            ParameterizedType::Datetime => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Datetime)?,
                &[],
            ),
            ParameterizedType::Float64 => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Float64)?,
                &[],
            ),
            ParameterizedType::Geography => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Geography)?,
                &[],
            ),
            ParameterizedType::Int64 => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Int64)?,
                &[],
            ),
            ParameterizedType::Interval => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Interval)?,
                &[],
            ),
            ParameterizedType::Json => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Json)?,
                &[],
            ),
            ParameterizedType::Numeric { precision, scale } => {
                let args = &[arg!(py_ctx, precision), arg!(py_ctx, scale)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Numeric)?,
                    args,
                )
            }
            ParameterizedType::Range { r#type } => {
                let args = &[arg!(py_ctx, r#type)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Range)?,
                    args,
                )
            }
            ParameterizedType::String { max_length } => {
                let args = &[arg!(py_ctx, max_length)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::String)?,
                    args,
                )
            }
            ParameterizedType::Struct { fields } => {
                let args = &[arg!(py_ctx, fields)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Struct)?,
                    args,
                )
            }
            ParameterizedType::Time => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Time)?,
                &[],
            ),
            ParameterizedType::Timestamp => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Timestamp)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for BinaryOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            BinaryOperator::Star => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Star)?,
                &[],
            ),
            BinaryOperator::Slash => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Slash)?,
                &[],
            ),
            BinaryOperator::Concat => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Concat)?,
                &[],
            ),
            BinaryOperator::Plus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Plus)?,
                &[],
            ),
            BinaryOperator::Minus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Minus)?,
                &[],
            ),
            BinaryOperator::BitwiseLeftShift => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseLeftShift)?,
                &[],
            ),
            BinaryOperator::BitwiseRightShift => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseRightShift)?,
                &[],
            ),
            BinaryOperator::BitwiseAnd => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseAnd)?,
                &[],
            ),
            BinaryOperator::BitwiseXor => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseXor)?,
                &[],
            ),
            BinaryOperator::BitwiseOr => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseOr)?,
                &[],
            ),
            BinaryOperator::Equal => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Equal)?,
                &[],
            ),
            BinaryOperator::LessThan => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::LessThan)?,
                &[],
            ),
            BinaryOperator::GreaterThan => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::GreaterThan)?,
                &[],
            ),
            BinaryOperator::LessThanOrEqualTo => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::LessThanOrEqualTo)?,
                &[],
            ),
            BinaryOperator::GreaterThanOrEqualTo => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::GreaterThanOrEqualTo)?,
                &[],
            ),
            BinaryOperator::NotEqual => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotEqual)?,
                &[],
            ),
            BinaryOperator::Like => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Like)?,
                &[],
            ),
            BinaryOperator::NotLike => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotLike)?,
                &[],
            ),
            BinaryOperator::QuantifiedLike => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::QuantifiedLike)?,
                &[],
            ),
            BinaryOperator::QuantifiedNotLike => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::QuantifiedNotLike)?,
                &[],
            ),
            BinaryOperator::Between => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Between)?,
                &[],
            ),
            BinaryOperator::NotBetween => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotBetween)?,
                &[],
            ),
            BinaryOperator::In => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::In)?,
                &[],
            ),
            BinaryOperator::NotIn => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotIn)?,
                &[],
            ),
            BinaryOperator::And => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::And)?,
                &[],
            ),
            BinaryOperator::Or => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Or)?,
                &[],
            ),
            BinaryOperator::ArrayIndex => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::ArrayIndex)?,
                &[],
            ),
            BinaryOperator::FieldAccess => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::FieldAccess)?,
                &[],
            ),
            BinaryOperator::IsDistinctFrom => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::IsDistinctFrom)?,
                &[],
            ),
            BinaryOperator::IsNotDistinctFrom => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::IsNotDistinctFrom)?,
                &[],
            ),
            BinaryOperator::FunctionAccess => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::FunctionAccess)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for BinaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.left),
            arg!(py_ctx, self.operator),
            arg!(py_ctx, self.right),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, BinaryExpr)?, args)
    }
}

impl RsToPyObject for UnaryOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnaryOperator::Plus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::Plus)?,
                &[],
            ),
            UnaryOperator::Minus => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::Minus)?,
                &[],
            ),
            UnaryOperator::BitwiseNot => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::BitwiseNot)?,
                &[],
            ),
            UnaryOperator::IsNull => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNull)?,
                &[],
            ),
            UnaryOperator::IsNotNull => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotNull)?,
                &[],
            ),
            UnaryOperator::IsTrue => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsTrue)?,
                &[],
            ),
            UnaryOperator::IsNotTrue => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotTrue)?,
                &[],
            ),
            UnaryOperator::IsFalse => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsFalse)?,
                &[],
            ),
            UnaryOperator::IsNotFalse => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotFalse)?,
                &[],
            ),
            UnaryOperator::Not => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::Not)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for UnaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.operator), arg!(py_ctx, self.right)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, UnaryExpr)?, args)
    }
}

impl RsToPyObject for GroupingExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, GroupingExpr)?, args)
    }
}

impl RsToPyObject for ArrayExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.r#type), arg!(py_ctx, self.exprs)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ArrayExpr)?, args)
    }
}

impl RsToPyObject for StructField {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, StructField)?, args)
    }
}

impl RsToPyObject for StructExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.r#type), arg!(py_ctx, self.fields)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, StructExpr)?, args)
    }
}

impl RsToPyObject for RangeExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.r#type), arg!(py_ctx, self.value)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, RangeExpr)?, args)
    }
}

impl RsToPyObject for IntervalPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            IntervalPart::Year => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Year)?,
                &[],
            ),
            IntervalPart::Quarter => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Quarter)?,
                &[],
            ),
            IntervalPart::Month => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Month)?,
                &[],
            ),
            IntervalPart::Week => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Week)?,
                &[],
            ),
            IntervalPart::Day => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Day)?,
                &[],
            ),
            IntervalPart::Hour => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Hour)?,
                &[],
            ),
            IntervalPart::Minute => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Minute)?,
                &[],
            ),
            IntervalPart::Second => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Second)?,
                &[],
            ),
            IntervalPart::Millisecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Millisecond)?,
                &[],
            ),
            IntervalPart::Microsecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Microsecond)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for IntervalExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            IntervalExpr::Interval { value, part } => {
                let args = &[arg!(py_ctx, value), arg!(py_ctx, part)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, IntervalExpr::Interval)?,
                    args,
                )
            }
            IntervalExpr::IntervalRange {
                value,
                start_part,
                end_part,
            } => {
                let args = &[
                    arg!(py_ctx, value),
                    arg!(py_ctx, start_part),
                    arg!(py_ctx, end_part),
                ];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, IntervalExpr::IntervalRange)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for WhenThen {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.when), arg!(py_ctx, self.then)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WhenThen)?, args)
    }
}

impl RsToPyObject for CaseExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.case),
            arg!(py_ctx, self.when_thens),
            arg!(py_ctx, self.r#else),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CaseExpr)?, args)
    }
}

impl RsToPyObject for FunctionAggregateNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionAggregateNulls::Ignore => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateNulls::Ignore)?,
                &[],
            ),
            FunctionAggregateNulls::Respect => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateNulls::Respect)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateHavingKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionAggregateHavingKind::Max => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateHavingKind::Max)?,
                &[],
            ),
            FunctionAggregateHavingKind::Min => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateHavingKind::Min)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateHaving {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.kind)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, FunctionAggregateHaving)?,
            args,
        )
    }
}

impl RsToPyObject for OrderBySortDirection {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderBySortDirection::Asc => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, OrderBySortDirection::Asc)?,
                &[],
            ),
            OrderBySortDirection::Desc => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, OrderBySortDirection::Desc)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateOrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.sort_direction),
            arg!(py_ctx, self.nulls),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, FunctionAggregateOrderBy)?,
            args,
        )
    }
}

impl RsToPyObject for FunctionAggregate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.distinct),
            arg!(py_ctx, self.nulls),
            arg!(py_ctx, self.having),
            arg!(py_ctx, self.order_by),
            arg!(py_ctx, self.limit),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, FunctionAggregate)?, args)
    }
}

impl RsToPyObject for GenericFunctionExprArg {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.aggregate),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, GenericFunctionExprArg)?,
            args,
        )
    }
}

impl RsToPyObject for WindowOrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.sort_direction),
            arg!(py_ctx, self.nulls),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WindowOrderByExpr)?, args)
    }
}

impl RsToPyObject for WindowFrameKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            WindowFrameKind::Range => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WindowFrameKind::Range)?,
                &[],
            ),
            WindowFrameKind::Rows => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WindowFrameKind::Rows)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FrameBound {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FrameBound::UnboundedPreceding => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FrameBound::UnboundedPreceding)?,
                &[],
            ),
            FrameBound::Preceding(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FrameBound::Preceding)?,
                    args,
                )
            }
            FrameBound::UnboundedFollowing => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FrameBound::UnboundedFollowing)?,
                &[],
            ),
            FrameBound::Following(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FrameBound::Following)?,
                    args,
                )
            }
            FrameBound::CurrentRow => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FrameBound::CurrentRow)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for WindowFrame {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.start),
            arg!(py_ctx, self.end),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WindowFrame)?, args)
    }
}

impl RsToPyObject for WindowSpec {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.window_name),
            arg!(py_ctx, self.partition_by),
            arg!(py_ctx, self.order_by),
            arg!(py_ctx, self.frame),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WindowSpec)?, args)
    }
}

impl RsToPyObject for NamedWindowExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            NamedWindowExpr::Reference(parse_token) => {
                let args = &[arg!(py_ctx, parse_token)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, NamedWindowExpr::Reference)?,
                    args,
                )
            }
            NamedWindowExpr::WindowSpec(window_spec) => {
                let args = &[arg!(py_ctx, window_spec)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, NamedWindowExpr::WindowSpec)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for GenericFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.arguments),
            arg!(py_ctx, self.over),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, GenericFunctionExpr)?, args)
    }
}

impl RsToPyObject for ChainedGenericFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.function)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, ChainedGenericFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for ChainedFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.function)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ChainedFunctionExpr)?, args)
    }
}

impl RsToPyObject for ArrayFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.query)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ArrayFunctionExpr)?, args)
    }
}

impl RsToPyObject for ArrayAggFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.arg), arg!(py_ctx, self.over)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ArrayAggFunctionExpr)?, args)
    }
}

impl RsToPyObject for ConcatFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.values)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ConcatFunctionExpr)?, args)
    }
}

impl RsToPyObject for CastFunctionFormat {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.format), arg!(py_ctx, self.time_zone)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CastFunctionFormat)?, args)
    }
}

impl RsToPyObject for CastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.r#type),
            arg!(py_ctx, self.format),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CastFunctionExpr)?, args)
    }
}

impl RsToPyObject for SafeCastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.r#type),
            arg!(py_ctx, self.format),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SafeCastFunctionExpr)?, args)
    }
}

impl RsToPyObject for CurrentDateFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.timezone)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CurrentDateFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for IfFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.condition),
            arg!(py_ctx, self.true_result),
            arg!(py_ctx, self.false_result),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, IfFunctionExpr)?, args)
    }
}

impl RsToPyObject for WeekBegin {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            WeekBegin::Sunday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Sunday)?,
                &[],
            ),
            WeekBegin::Monday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Monday)?,
                &[],
            ),
            WeekBegin::Tuesday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Tuesday)?,
                &[],
            ),
            WeekBegin::Wednesday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Wednesday)?,
                &[],
            ),
            WeekBegin::Thursday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Thursday)?,
                &[],
            ),
            WeekBegin::Friday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Friday)?,
                &[],
            ),
            WeekBegin::Saturday => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, WeekBegin::Saturday)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for ExtractFunctionPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ExtractFunctionPart::MicroSecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::MicroSecond)?,
                &[],
            ),
            ExtractFunctionPart::MilliSecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::MilliSecond)?,
                &[],
            ),
            ExtractFunctionPart::Second => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Second)?,
                &[],
            ),
            ExtractFunctionPart::Minute => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Minute)?,
                &[],
            ),
            ExtractFunctionPart::Hour => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Hour)?,
                &[],
            ),
            ExtractFunctionPart::DayOfWeek => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::DayOfWeek)?,
                &[],
            ),
            ExtractFunctionPart::Day => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Day)?,
                &[],
            ),
            ExtractFunctionPart::DayOfYear => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::DayOfYear)?,
                &[],
            ),
            ExtractFunctionPart::Week => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Week)?,
                &[],
            ),
            ExtractFunctionPart::WeekWithBegin(week_begin) => {
                let args = &[arg!(py_ctx, week_begin)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, ExtractFunctionPart::WeekWithBegin)?,
                    args,
                )
            }
            ExtractFunctionPart::IsoWeek => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::IsoWeek)?,
                &[],
            ),
            ExtractFunctionPart::Month => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Month)?,
                &[],
            ),
            ExtractFunctionPart::Quarter => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Quarter)?,
                &[],
            ),
            ExtractFunctionPart::Year => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Year)?,
                &[],
            ),
            ExtractFunctionPart::IsoYear => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::IsoYear)?,
                &[],
            ),
            ExtractFunctionPart::Date => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Date)?,
                &[],
            ),
            ExtractFunctionPart::Time => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Time)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for ExtractFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.part), arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ExtractFunctionExpr)?, args)
    }
}

impl RsToPyObject for LeftFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.value), arg!(py_ctx, self.length)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, LeftFunctionExpr)?, args)
    }
}

impl RsToPyObject for RightFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.value), arg!(py_ctx, self.length)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, RightFunctionExpr)?, args)
    }
}

impl RsToPyObject for Granularity {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Granularity::MicroSecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::MicroSecond)?,
                &[],
            ),
            Granularity::MilliSecond => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::MilliSecond)?,
                &[],
            ),
            Granularity::Second => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Second)?,
                &[],
            ),
            Granularity::Minute => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Minute)?,
                &[],
            ),
            Granularity::Hour => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Hour)?,
                &[],
            ),
            Granularity::Day => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Day)?,
                &[],
            ),
            Granularity::Week => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Week)?,
                &[],
            ),
            Granularity::WeekWithBegin(week_begin) => {
                let args = &[arg!(py_ctx, week_begin)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Granularity::WeekWithBegin)?,
                    args,
                )
            }
            Granularity::IsoWeek => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::IsoWeek)?,
                &[],
            ),
            Granularity::Month => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Month)?,
                &[],
            ),
            Granularity::Quarter => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Quarter)?,
                &[],
            ),
            Granularity::Year => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Year)?,
                &[],
            ),
            Granularity::IsoYear => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::IsoYear)?,
                &[],
            ),
            Granularity::Date => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Date)?,
                &[],
            ),
            Granularity::Time => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::Time)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for DateDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.start_date),
            arg!(py_ctx, self.end_date),
            arg!(py_ctx, self.granularity),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DateDiffFunctionExpr)?, args)
    }
}

impl RsToPyObject for DatetimeDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.start_datetime),
            arg!(py_ctx, self.end_datetime),
            arg!(py_ctx, self.granularity),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, DatetimeDiffFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for TimeDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.start_time),
            arg!(py_ctx, self.end_time),
            arg!(py_ctx, self.granularity),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TimeDiffFunctionExpr)?, args)
    }
}

impl RsToPyObject for TimestampDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.start_timestamp),
            arg!(py_ctx, self.end_timestamp),
            arg!(py_ctx, self.granularity),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, TimestampDiffFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for DateTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.date), arg!(py_ctx, self.granularity)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DateTruncFunctionExpr)?, args)
    }
}

impl RsToPyObject for DatetimeTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.datetime),
            arg!(py_ctx, self.granularity),
            arg!(py_ctx, self.timezone),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, DatetimeTruncFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for TimestampTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.timestamp),
            arg!(py_ctx, self.granularity),
            arg!(py_ctx, self.timezone),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, TimestampTruncFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for TimeTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.time), arg!(py_ctx, self.granularity)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TimeTruncFunctionExpr)?, args)
    }
}

impl RsToPyObject for LastDayFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.granularity)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, LastDayFunctionExpr)?, args)
    }
}

impl RsToPyObject for CurrentDatetimeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.timezone)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CurrentDatetimeFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for CurrentTimeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.timezone)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CurrentTimeFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for NormalizationMode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            NormalizationMode::NFC => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFC)?,
                &[],
            ),
            NormalizationMode::NFKC => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFKC)?,
                &[],
            ),
            NormalizationMode::NFD => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFD)?,
                &[],
            ),
            NormalizationMode::NFKD => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFKD)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for NormalizeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.value), arg!(py_ctx, self.mode)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, NormalizeFunctionExpr)?, args)
    }
}

impl RsToPyObject for NormalizeAndCasefoldFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.value), arg!(py_ctx, self.mode)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, NormalizeAndCasefoldFunctionExpr)?,
            args,
        )
    }
}

impl RsToPyObject for CoalesceFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.exprs)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CoalesceFunctionExpr)?, args)
    }
}

impl RsToPyObject for FunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionExpr::Array(array_function_expr) => {
                let args = &[arg!(py_ctx, array_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Array)?,
                    args,
                )
            }
            FunctionExpr::ArrayAgg(array_agg_function_expr) => {
                let args = &[arg!(py_ctx, array_agg_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::ArrayAgg)?,
                    args,
                )
            }
            FunctionExpr::Concat(concat_function_expr) => {
                let args = &[arg!(py_ctx, concat_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Concat)?,
                    args,
                )
            }
            FunctionExpr::Coalesce(coalesce_function_expr) => {
                let args = &[arg!(py_ctx, coalesce_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Coalesce)?,
                    args,
                )
            }
            FunctionExpr::Cast(cast_function_expr) => {
                let args = &[arg!(py_ctx, cast_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Cast)?,
                    args,
                )
            }
            FunctionExpr::SafeCast(safe_cast_function_expr) => {
                let args = &[arg!(py_ctx, safe_cast_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::SafeCast)?,
                    args,
                )
            }
            FunctionExpr::CurrentDate(current_date_function_expr) => {
                let args = &[arg!(py_ctx, current_date_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentDate)?,
                    args,
                )
            }
            FunctionExpr::CurrentDatetime(current_datetime_function_expr) => {
                let args = &[arg!(py_ctx, current_datetime_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentDatetime)?,
                    args,
                )
            }
            FunctionExpr::CurrentTime(current_time_function_expr) => {
                let args = &[arg!(py_ctx, current_time_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentTime)?,
                    args,
                )
            }
            FunctionExpr::If(if_function_expr) => {
                let args = &[arg!(py_ctx, if_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::If)?,
                    args,
                )
            }
            FunctionExpr::Extract(extract_function_expr) => {
                let args = &[arg!(py_ctx, extract_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Extract)?,
                    args,
                )
            }
            FunctionExpr::Normalize(normalize_function_expr) => {
                let args = &[arg!(py_ctx, normalize_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Normalize)?,
                    args,
                )
            }
            FunctionExpr::NormalizeAndCasefold(normalize_and_casefold_function_expr) => {
                let args = &[arg!(py_ctx, normalize_and_casefold_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::NormalizeAndCasefold)?,
                    args,
                )
            }
            FunctionExpr::Left(left_function_expr) => {
                let args = &[arg!(py_ctx, left_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Left)?,
                    args,
                )
            }
            FunctionExpr::Right(right_function_expr) => {
                let args = &[arg!(py_ctx, right_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Right)?,
                    args,
                )
            }
            FunctionExpr::CurrentTimestamp => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionExpr::CurrentTimestamp)?,
                &[],
            ),
            FunctionExpr::DateDiff(date_diff_function_expr) => {
                let args = &[arg!(py_ctx, date_diff_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DateDiff)?,
                    args,
                )
            }
            FunctionExpr::DatetimeDiff(datetime_diff_function_expr) => {
                let args = &[arg!(py_ctx, datetime_diff_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DatetimeDiff)?,
                    args,
                )
            }
            FunctionExpr::TimestampDiff(timestamp_diff_function_expr) => {
                let args = &[arg!(py_ctx, timestamp_diff_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimestampDiff)?,
                    args,
                )
            }
            FunctionExpr::TimeDiff(time_diff_function_expr) => {
                let args = &[arg!(py_ctx, time_diff_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimeDiff)?,
                    args,
                )
            }
            FunctionExpr::DateTrunc(date_trunc_function_expr) => {
                let args = &[arg!(py_ctx, date_trunc_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DateTrunc)?,
                    args,
                )
            }
            FunctionExpr::DatetimeTrunc(datetime_trunc_function_expr) => {
                let args = &[arg!(py_ctx, datetime_trunc_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DatetimeTrunc)?,
                    args,
                )
            }
            FunctionExpr::TimestampTrunc(timestamp_trunc_function_expr) => {
                let args = &[arg!(py_ctx, timestamp_trunc_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimestampTrunc)?,
                    args,
                )
            }
            FunctionExpr::TimeTrunc(time_trunc_function_expr) => {
                let args = &[arg!(py_ctx, time_trunc_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimeTrunc)?,
                    args,
                )
            }
            FunctionExpr::LastDay(last_day_function_expr) => {
                let args = &[arg!(py_ctx, last_day_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::LastDay)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for LikeQuantifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            LikeQuantifier::Any => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, LikeQuantifier::Any)?,
                &[],
            ),
            LikeQuantifier::Some => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, LikeQuantifier::Some)?,
                &[],
            ),
            LikeQuantifier::All => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, LikeQuantifier::All)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for QuantifiedLikeExprPattern {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            QuantifiedLikeExprPattern::ExprList { exprs } => {
                let args = &[arg!(py_ctx, exprs)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, QuantifiedLikeExprPattern::ExprList)?,
                    args,
                )
            }
            QuantifiedLikeExprPattern::ArrayUnnest { expr } => {
                let args = &[arg!(py_ctx, expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, QuantifiedLikeExprPattern::ArrayUnnest)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for QuantifiedLikeExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.quantifier), arg!(py_ctx, self.pattern)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, QuantifiedLikeExpr)?, args)
    }
}

impl RsToPyObject for StringConcatExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.strings)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, StringConcatExpr)?, args)
    }
}

impl RsToPyObject for BytesConcatExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.bytes)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, BytesConcatExpr)?, args)
    }
}

impl RsToPyObject for Identifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Identifier)?, args)
    }
}

impl RsToPyObject for QuotedIdentifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, QuotedIdentifier)?, args)
    }
}

impl RsToPyObject for SystemVariable {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SystemVariable)?, args)
    }
}

impl RsToPyObject for WithExprVar {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.value)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WithExprVar)?, args)
    }
}

impl RsToPyObject for WithExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.vars), arg!(py_ctx, self.result)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WithExpr)?, args)
    }
}

impl RsToPyObject for UnnestExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.array)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, UnnestExpr)?, args)
    }
}

impl RsToPyObject for Expr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Expr::Binary(binary_expr) => {
                let args = &[arg!(py_ctx, binary_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Binary)?, args)
            }
            Expr::Unary(unary_expr) => {
                let args = &[arg!(py_ctx, unary_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Unary)?, args)
            }
            Expr::Grouping(grouping_expr) => {
                let args = &[arg!(py_ctx, grouping_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Grouping)?,
                    args,
                )
            }
            Expr::Array(array_expr) => {
                let args = &[arg!(py_ctx, array_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Array)?, args)
            }
            Expr::Struct(struct_expr) => {
                let args = &[arg!(py_ctx, struct_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Struct)?, args)
            }
            Expr::Identifier(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Identifier)?,
                    args,
                )
            }
            Expr::QuotedIdentifier(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QuotedIdentifier)?,
                    args,
                )
            }
            Expr::QueryNamedParameter(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QueryNamedParameter)?,
                    args,
                )
            }
            Expr::QueryPositionalParameter => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Expr::QueryPositionalParameter)?,
                &[],
            ),
            Expr::SystemVariable(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::SystemVariable)?,
                    args,
                )
            }
            Expr::String(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::String)?, args)
            }
            Expr::RawString(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::RawString)?,
                    args,
                )
            }
            Expr::Bytes(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Bytes)?, args)
            }
            Expr::RawBytes(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::RawBytes)?,
                    args,
                )
            }
            Expr::StringConcat(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::StringConcat)?,
                    args,
                )
            }
            Expr::BytesConcat(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::BytesConcat)?,
                    args,
                )
            }
            Expr::Numeric(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Numeric)?, args)
            }
            Expr::BigNumeric(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::BigNumeric)?,
                    args,
                )
            }
            Expr::Number(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Number)?, args)
            }
            Expr::Bool(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Bool)?, args)
            }
            Expr::Date(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Date)?, args)
            }
            Expr::Time(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Time)?, args)
            }
            Expr::Datetime(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Datetime)?,
                    args,
                )
            }
            Expr::Timestamp(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Timestamp)?,
                    args,
                )
            }
            Expr::Range(range_expr) => {
                let args = &[arg!(py_ctx, range_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Range)?, args)
            }
            Expr::Interval(interval_expr) => {
                let args = &[arg!(py_ctx, interval_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Interval)?,
                    args,
                )
            }
            Expr::Json(value) => {
                let args = &[arg!(py_ctx, value)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Json)?, args)
            }
            Expr::Default => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Default)?, &[])
            }
            Expr::Null => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Null)?, &[])
            }
            Expr::Star => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Star)?, &[])
            }
            Expr::Query(query_expr) => {
                let args = &[arg!(py_ctx, query_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Query)?, args)
            }
            Expr::Exists(query_expr) => {
                let args = &[arg!(py_ctx, query_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Exists)?, args)
            }
            Expr::Case(case_expr) => {
                let args = &[arg!(py_ctx, case_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Case)?, args)
            }
            Expr::GenericFunction(generic_function_expr) => {
                let args = &[arg!(py_ctx, generic_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::GenericFunction)?,
                    args,
                )
            }
            Expr::ChainedGenericFunction(chained_generic_function_expr) => {
                let args = &[arg!(py_ctx, chained_generic_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::ChainedGenericFunction)?,
                    args,
                )
            }
            Expr::Function(function_expr) => {
                let args = &[arg!(py_ctx, function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::Function)?,
                    args,
                )
            }
            Expr::ChainedFunction(chained_function_expr) => {
                let args = &[arg!(py_ctx, chained_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::ChainedFunction)?,
                    args,
                )
            }
            Expr::QuantifiedLike(quantified_like_expr) => {
                let args = &[arg!(py_ctx, quantified_like_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QuantifiedLike)?,
                    args,
                )
            }
            Expr::With(with_expr) => {
                let args = &[arg!(py_ctx, with_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::With)?, args)
            }
            Expr::Unnest(unnest_expr) => {
                let args = &[arg!(py_ctx, unnest_expr)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Expr::Unnest)?, args)
            }
        }
    }
}

impl RsToPyObject for Limit {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.count), arg!(py_ctx, self.offset)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Limit)?, args)
    }
}

impl RsToPyObject for NonRecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.query)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, NonRecursiveCte)?, args)
    }
}

impl RsToPyObject for RecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.base_query),
            arg!(py_ctx, self.recursive_query),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, RecursiveCte)?, args)
    }
}

impl RsToPyObject for Cte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Cte::NonRecursive(non_recursive_cte) => {
                let args = &[arg!(py_ctx, non_recursive_cte)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Cte::NonRecursive)?,
                    args,
                )
            }
            Cte::Recursive(recursive_cte) => {
                let args = &[arg!(py_ctx, recursive_cte)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Cte::Recursive)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for With {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.ctes)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, With)?, args)
    }
}

impl RsToPyObject for OrderByNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderByNulls::First => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, OrderByNulls::First)?,
                &[],
            ),
            OrderByNulls::Last => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, OrderByNulls::Last)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for OrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.expr),
            arg!(py_ctx, self.sort_direction),
            arg!(py_ctx, self.nulls),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, OrderByExpr)?, args)
    }
}

impl RsToPyObject for OrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.exprs)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, OrderBy)?, args)
    }
}

impl RsToPyObject for GroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.with),
            arg!(py_ctx, self.query),
            arg!(py_ctx, self.order_by),
            arg!(py_ctx, self.limit),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, GroupingQueryExpr)?, args)
    }
}

impl RsToPyObject for SelectTableValue {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectTableValue::Struct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SelectTableValue::Struct)?,
                &[],
            ),
            SelectTableValue::Value => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SelectTableValue::Value)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for SelectColExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SelectColExpr)?, args)
    }
}

impl RsToPyObject for SelectColAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.except)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SelectColAllExpr)?, args)
    }
}

impl RsToPyObject for SelectAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.except)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SelectAllExpr)?, args)
    }
}

impl RsToPyObject for SelectExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectExpr::Col(select_col_expr) => {
                let args = &[arg!(py_ctx, select_col_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, SelectExpr::Col)?,
                    args,
                )
            }
            SelectExpr::ColAll(select_col_all_expr) => {
                let args = &[arg!(py_ctx, select_col_all_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, SelectExpr::ColAll)?,
                    args,
                )
            }
            SelectExpr::All(select_all_expr) => {
                let args = &[arg!(py_ctx, select_all_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, SelectExpr::All)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for JoinKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinKind::Inner => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, JoinKind::Inner)?,
                &[],
            ),
            JoinKind::Left => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, JoinKind::Left)?, &[])
            }
            JoinKind::Right => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, JoinKind::Right)?,
                &[],
            ),
            JoinKind::Full => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, JoinKind::Full)?, &[])
            }
        }
    }
}

impl RsToPyObject for JoinCondition {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinCondition::On(expr) => {
                let args = &[arg!(py_ctx, expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, JoinCondition::On)?,
                    args,
                )
            }
            JoinCondition::Using { columns } => {
                let args = &[arg!(py_ctx, columns)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, JoinCondition::Using)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for JoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.left),
            arg!(py_ctx, self.right),
            arg!(py_ctx, self.cond),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, JoinExpr)?, args)
    }
}

impl RsToPyObject for CrossJoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.left), arg!(py_ctx, self.right)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CrossJoinExpr)?, args)
    }
}

impl RsToPyObject for FromPathExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.path),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.system_time),
            arg!(py_ctx, self.table_operator),
            arg!(py_ctx, self.table_sample),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, FromPathExpr)?, args)
    }
}

impl RsToPyObject for FromUnnestExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.array),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.with_offset),
            arg!(py_ctx, self.offset_alias),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, FromUnnestExpr)?, args)
    }
}

impl RsToPyObject for FromGroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.query),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.table_operator),
            arg!(py_ctx, self.table_sample),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, FromGroupingQueryExpr)?, args)
    }
}

impl RsToPyObject for GroupingFromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.query)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, GroupingFromExpr)?, args)
    }
}

impl RsToPyObject for TableFunctionArgument {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TableFunctionArgument::Table(path_name) => {
                let args = &[arg!(py_ctx, path_name)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableFunctionArgument::Table)?,
                    args,
                )
            }
            TableFunctionArgument::Expr(expr) => {
                let args = &[arg!(py_ctx, expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableFunctionArgument::Expr)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for TableFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.arguments),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.table_operator),
            arg!(py_ctx, self.table_sample),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TableFunctionExpr)?, args)
    }
}

impl RsToPyObject for FromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FromExpr::Join(join_expr) => {
                let args = &[arg!(py_ctx, join_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::Join)?,
                    args,
                )
            }
            FromExpr::FullJoin(join_expr) => {
                let args = &[arg!(py_ctx, join_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::FullJoin)?,
                    args,
                )
            }
            FromExpr::LeftJoin(join_expr) => {
                let args = &[arg!(py_ctx, join_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::LeftJoin)?,
                    args,
                )
            }
            FromExpr::RightJoin(join_expr) => {
                let args = &[arg!(py_ctx, join_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::RightJoin)?,
                    args,
                )
            }
            FromExpr::CrossJoin(cross_join_expr) => {
                let args = &[arg!(py_ctx, cross_join_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::CrossJoin)?,
                    args,
                )
            }
            FromExpr::Path(from_path_expr) => {
                let args = &[arg!(py_ctx, from_path_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::Path)?,
                    args,
                )
            }
            FromExpr::Unnest(unnest_expr) => {
                let args = &[arg!(py_ctx, unnest_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::Unnest)?,
                    args,
                )
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let args = &[arg!(py_ctx, from_grouping_query_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::GroupingQuery)?,
                    args,
                )
            }
            FromExpr::GroupingFrom(grouping_from_expr) => {
                let args = &[arg!(py_ctx, grouping_from_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::GroupingFrom)?,
                    args,
                )
            }
            FromExpr::TableFunction(table_function_expr) => {
                let args = &[arg!(py_ctx, table_function_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::TableFunction)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for PivotColumn {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, PivotColumn)?, args)
    }
}

impl RsToPyObject for PivotAggregate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, PivotAggregate)?, args)
    }
}

impl RsToPyObject for Pivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.aggregates),
            arg!(py_ctx, self.input_column),
            arg!(py_ctx, self.pivot_columns),
            arg!(py_ctx, self.alias),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Pivot)?, args)
    }
}

impl RsToPyObject for UnpivotNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnpivotNulls::Include => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnpivotNulls::Include)?,
                &[],
            ),
            UnpivotNulls::Exclude => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, UnpivotNulls::Exclude)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for ColumnToUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ColumnToUnpivot)?, args)
    }
}

impl RsToPyObject for SingleColumnUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.values_column),
            arg!(py_ctx, self.name_column),
            arg!(py_ctx, self.columns_to_unpivot),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SingleColumnUnpivot)?, args)
    }
}

impl RsToPyObject for ColumnSetToUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.names), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ColumnSetToUnpivot)?, args)
    }
}

impl RsToPyObject for MultiColumnUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.values_columns),
            arg!(py_ctx, self.name_column),
            arg!(py_ctx, self.column_sets_to_unpivot),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, MultiColumnUnpivot)?, args)
    }
}

impl RsToPyObject for UnpivotKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnpivotKind::SingleColumn(single_column_unpivot) => {
                let args = &[arg!(py_ctx, single_column_unpivot)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, UnpivotKind::SingleColumn)?,
                    args,
                )
            }
            UnpivotKind::MultiColumn(multi_column_unpivot) => {
                let args = &[arg!(py_ctx, multi_column_unpivot)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, UnpivotKind::MultiColumn)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for Unpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.nulls),
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.alias),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Unpivot)?, args)
    }
}

impl RsToPyObject for TableSample {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.percent)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TableSample)?, args)
    }
}

impl RsToPyObject for TableOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TableOperator::Pivot(pivot) => {
                let args = &[arg!(py_ctx, pivot)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableOperator::Pivot)?,
                    args,
                )
            }
            TableOperator::Unpivot(unpivot) => {
                let args = &[arg!(py_ctx, unpivot)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableOperator::Unpivot)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for inbq::ast::From {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, From)?, args)
    }
}

impl RsToPyObject for Where {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Where)?, args)
    }
}

impl RsToPyObject for GroupByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            GroupByExpr::Items { exprs } => {
                let args = &[arg!(py_ctx, exprs)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, GroupByExpr::Items)?,
                    args,
                )
            }
            GroupByExpr::All => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, GroupByExpr::All)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for GroupBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, GroupBy)?, args)
    }
}

impl RsToPyObject for Having {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Having)?, args)
    }
}

impl RsToPyObject for Qualify {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Qualify)?, args)
    }
}

impl RsToPyObject for NamedWindow {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.window)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, NamedWindow)?, args)
    }
}

impl RsToPyObject for Window {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.named_windows)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Window)?, args)
    }
}

impl RsToPyObject for DifferentialPrivacyOption {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.value)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, DifferentialPrivacyOption)?,
            args,
        )
    }
}

impl RsToPyObject for DifferentialPrivacy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.options)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DifferentialPrivacy)?, args)
    }
}

impl RsToPyObject for Select {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.differential_privacy),
            arg!(py_ctx, self.distinct),
            arg!(py_ctx, self.table_value),
            arg!(py_ctx, self.exprs),
            arg!(py_ctx, self.from),
            arg!(py_ctx, self.r#where),
            arg!(py_ctx, self.group_by),
            arg!(py_ctx, self.having),
            arg!(py_ctx, self.qualify),
            arg!(py_ctx, self.window),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Select)?, args)
    }
}

impl RsToPyObject for SelectQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.with),
            arg!(py_ctx, self.select),
            arg!(py_ctx, self.order_by),
            arg!(py_ctx, self.limit),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SelectQueryExpr)?, args)
    }
}

impl RsToPyObject for SetQueryOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SetQueryOperator::Union => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::Union)?,
                &[],
            ),
            SetQueryOperator::UnionDistinct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::UnionDistinct)?,
                &[],
            ),
            SetQueryOperator::IntersectDistinct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::IntersectDistinct)?,
                &[],
            ),
            SetQueryOperator::ExceptDistinct => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::ExceptDistinct)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for SetSelectQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.with),
            arg!(py_ctx, self.left_query),
            arg!(py_ctx, self.set_operator),
            arg!(py_ctx, self.right_query),
            arg!(py_ctx, self.order_by),
            arg!(py_ctx, self.limit),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SetSelectQueryExpr)?, args)
    }
}

impl RsToPyObject for QueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            QueryExpr::Grouping(grouping_query_expr) => {
                let args = &[arg!(py_ctx, grouping_query_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, QueryExpr::Grouping)?,
                    args,
                )
            }
            QueryExpr::Select(select_query_expr) => {
                let args = &[arg!(py_ctx, select_query_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, QueryExpr::Select)?,
                    args,
                )
            }
            QueryExpr::SetSelect(set_select_query_expr) => {
                let args = &[arg!(py_ctx, set_select_query_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, QueryExpr::SetSelect)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for QueryStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.query)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, QueryStatement)?, args)
    }
}

impl RsToPyObject for InsertStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.table),
            arg!(py_ctx, self.columns),
            arg!(py_ctx, self.values),
            arg!(py_ctx, self.query),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, InsertStatement)?, args)
    }
}

impl RsToPyObject for DeleteStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.table),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.cond),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DeleteStatement)?, args)
    }
}

impl RsToPyObject for UpdateItem {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.column), arg!(py_ctx, self.expr)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, UpdateItem)?, args)
    }
}

impl RsToPyObject for UpdateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.table),
            arg!(py_ctx, self.alias),
            arg!(py_ctx, self.update_items),
            arg!(py_ctx, self.from),
            arg!(py_ctx, self.r#where),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, UpdateStatement)?, args)
    }
}

impl RsToPyObject for TruncateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.table)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, TruncateStatement)?, args)
    }
}

impl RsToPyObject for MergeSource {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            MergeSource::Table(parse_token) => {
                let args = &[arg!(py_ctx, parse_token)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, MergeSource::Table)?,
                    args,
                )
            }
            MergeSource::Subquery(query_expr) => {
                let args = &[arg!(py_ctx, query_expr)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, MergeSource::Subquery)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for MergeUpdate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.update_items)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, MergeUpdate)?, args)
    }
}

impl RsToPyObject for MergeInsert {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.columns), arg!(py_ctx, self.values)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, MergeInsert)?, args)
    }
}

impl RsToPyObject for Merge {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Merge::Update(merge_update) => {
                let args = &[arg!(py_ctx, merge_update)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Merge::Update)?, args)
            }
            Merge::Insert(merge_insert) => {
                let args = &[arg!(py_ctx, merge_insert)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Merge::Insert)?, args)
            }
            Merge::InsertRow => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Merge::InsertRow)?,
                &[],
            ),
            Merge::Delete => {
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Merge::Delete)?, &[])
            }
        }
    }
}

impl RsToPyObject for WhenMatched {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.search_condition),
            arg!(py_ctx, self.merge),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WhenMatched)?, args)
    }
}

impl RsToPyObject for WhenNotMatchedByTarget {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.search_condition),
            arg!(py_ctx, self.merge),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, WhenNotMatchedByTarget)?,
            args,
        )
    }
}

impl RsToPyObject for WhenNotMatchedBySource {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.search_condition),
            arg!(py_ctx, self.merge),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, WhenNotMatchedBySource)?,
            args,
        )
    }
}

impl RsToPyObject for When {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            When::Matched(when_matched) => {
                let args = &[arg!(py_ctx, when_matched)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, When::Matched)?, args)
            }
            When::NotMatchedByTarget(when_not_matched_by_target) => {
                let args = &[arg!(py_ctx, when_not_matched_by_target)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, When::NotMatchedByTarget)?,
                    args,
                )
            }
            When::NotMatchedBySource(when_not_matched_by_source) => {
                let args = &[arg!(py_ctx, when_not_matched_by_source)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, When::NotMatchedBySource)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for MergeStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.target_table),
            arg!(py_ctx, self.target_alias),
            arg!(py_ctx, self.source),
            arg!(py_ctx, self.source_alias),
            arg!(py_ctx, self.condition),
            arg!(py_ctx, self.whens),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, MergeStatement)?, args)
    }
}

impl RsToPyObject for DeclareVarStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.vars),
            arg!(py_ctx, self.r#type),
            arg!(py_ctx, self.default),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DeclareVarStatement)?, args)
    }
}

impl RsToPyObject for SetVariable {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SetVariable::UserVariable(name) => {
                let args = &[arg!(py_ctx, name)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, SetVariable::UserVariable)?,
                    args,
                )
            }
            SetVariable::SystemVariable(system_variable) => {
                let args = &[arg!(py_ctx, system_variable)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, SetVariable::SystemVariable)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for SetVarStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.vars), arg!(py_ctx, self.exprs)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, SetVarStatement)?, args)
    }
}

impl RsToPyObject for StatementsBlock {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.statements),
            arg!(py_ctx, self.exception_statements),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, StatementsBlock)?, args)
    }
}

impl RsToPyObject for ColumnSchema {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.r#type)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ColumnSchema)?, args)
    }
}

impl RsToPyObject for PrimaryKeyConstraintNotEnforced {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.columns)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, PrimaryKeyConstraintNotEnforced)?,
            args,
        )
    }
}

impl RsToPyObject for ForeignKeyReference {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.table), arg!(py_ctx, self.columns)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ForeignKeyReference)?, args)
    }
}

impl RsToPyObject for ForeignKeyConstraintNotEnforced {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.columns),
            arg!(py_ctx, self.reference),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, ForeignKeyConstraintNotEnforced)?,
            args,
        )
    }
}

impl RsToPyObject for TableConstraint {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TableConstraint::PrimaryKeyNotEnforced(primary_key_constraint_not_enforced) => {
                let args = &[arg!(py_ctx, primary_key_constraint_not_enforced)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableConstraint::PrimaryKeyNotEnforced)?,
                    args,
                )
            }
            TableConstraint::ForeignKeyNotEnforced(foreign_key_constraint_not_enforced) => {
                let args = &[arg!(py_ctx, foreign_key_constraint_not_enforced)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, TableConstraint::ForeignKeyNotEnforced)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for CreateTableStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.schema),
            arg!(py_ctx, self.constraints),
            arg!(py_ctx, self.default_collate),
            arg!(py_ctx, self.partition),
            arg!(py_ctx, self.clustering_columns),
            arg!(py_ctx, self.connection),
            arg!(py_ctx, self.options),
            arg!(py_ctx, self.replace),
            arg!(py_ctx, self.is_temporary),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.query),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CreateTableStatement)?, args)
    }
}

impl RsToPyObject for DropTableStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.if_exists)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DropTableStatement)?, args)
    }
}

impl RsToPyObject for IfBranch {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.condition), arg!(py_ctx, self.statements)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, IfBranch)?, args)
    }
}

impl RsToPyObject for IfStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.r#if),
            arg!(py_ctx, self.else_ifs),
            arg!(py_ctx, self.r#else),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, IfStatement)?, args)
    }
}

impl RsToPyObject for RaiseStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.message)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, RaiseStatement)?, args)
    }
}

impl RsToPyObject for CallStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.procedure_name),
            arg!(py_ctx, self.arguments),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CallStatement)?, args)
    }
}

impl RsToPyObject for CaseWhenThenStatements {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.when), arg!(py_ctx, self.then)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CaseWhenThenStatements)?,
            args,
        )
    }
}

impl RsToPyObject for CaseStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.case),
            arg!(py_ctx, self.when_thens),
            arg!(py_ctx, self.r#else),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CaseStatement)?, args)
    }
}

impl RsToPyObject for LoopStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.statements)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, LoopStatement)?, args)
    }
}

impl RsToPyObject for RepeatStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.statements), arg!(py_ctx, self.until)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, RepeatStatement)?, args)
    }
}

impl RsToPyObject for WhileStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.condition), arg!(py_ctx, self.statements)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, WhileStatement)?, args)
    }
}

impl RsToPyObject for ForInStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.var_name),
            arg!(py_ctx, self.table_expr),
            arg!(py_ctx, self.statements),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ForInStatement)?, args)
    }
}

impl RsToPyObject for LabeledStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.statement),
            arg!(py_ctx, self.start_label),
            arg!(py_ctx, self.end_label),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, LabeledStatement)?, args)
    }
}

impl RsToPyObject for DdlOption {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.value)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DdlOption)?, args)
    }
}

impl RsToPyObject for ViewColumn {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.options)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, ViewColumn)?, args)
    }
}

impl RsToPyObject for CreateViewStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.replace),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.columns),
            arg!(py_ctx, self.options),
            arg!(py_ctx, self.query),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CreateViewStatement)?, args)
    }
}

impl RsToPyObject for ExecuteImmediateUsingIdentifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.identifier), arg!(py_ctx, self.alias)];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, ExecuteImmediateUsingIdentifier)?,
            args,
        )
    }
}

impl RsToPyObject for ExecuteImmediateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.sql),
            arg!(py_ctx, self.into_vars),
            arg!(py_ctx, self.using_identifiers),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, ExecuteImmediateStatement)?,
            args,
        )
    }
}

impl RsToPyObject for CreateSchemaStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.default_collate),
            arg!(py_ctx, self.options),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, CreateSchemaStatement)?, args)
    }
}

impl RsToPyObject for FunctionArgumentType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionArgumentType::Standard(standard_ty) => {
                let args = &[arg!(py_ctx, standard_ty)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionArgumentType::Standard)?,
                    args,
                )
            }
            FunctionArgumentType::AnyType => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, FunctionArgumentType::AnyType)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionArgument {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.r#type)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, FunctionArgument)?, args)
    }
}

impl RsToPyObject for CreateSqlFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.replace),
            arg!(py_ctx, self.is_temporary),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.arguments),
            arg!(py_ctx, self.returns),
            arg!(py_ctx, self.options),
            arg!(py_ctx, self.body),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CreateSqlFunctionStatement)?,
            args,
        )
    }
}

impl RsToPyObject for DropFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.if_exists)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DropFunctionStatement)?, args)
    }
}

impl RsToPyObject for CreateJsFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.replace),
            arg!(py_ctx, self.is_temporary),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.arguments),
            arg!(py_ctx, self.returns),
            arg!(py_ctx, self.is_deterministic),
            arg!(py_ctx, self.options),
            arg!(py_ctx, self.body),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_ast_class!(py_ctx, CreateJsFunctionStatement)?,
            args,
        )
    }
}

impl RsToPyObject for DropViewStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.if_exists)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DropViewStatement)?, args)
    }
}

impl RsToPyObject for DropSchemaMode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            DropSchemaMode::Restrict => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, DropSchemaMode::Restrict)?,
                &[],
            ),
            DropSchemaMode::Cascade => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, DropSchemaMode::Cascade)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for DropSchemaStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.external),
            arg!(py_ctx, self.if_exists),
            arg!(py_ctx, self.mode),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, DropSchemaStatement)?, args)
    }
}

impl RsToPyObject for UndropSchemaStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.if_not_exists),
            arg!(py_ctx, self.options),
        ];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, UndropSchemaStatement)?, args)
    }
}

impl RsToPyObject for Statement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Statement::Query(query_statement) => {
                let args = &[arg!(py_ctx, query_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Query)?,
                    args,
                )
            }
            Statement::Insert(insert_statement) => {
                let args = &[arg!(py_ctx, insert_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Insert)?,
                    args,
                )
            }
            Statement::Delete(delete_statement) => {
                let args = &[arg!(py_ctx, delete_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Delete)?,
                    args,
                )
            }
            Statement::Update(update_statement) => {
                let args = &[arg!(py_ctx, update_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Update)?,
                    args,
                )
            }
            Statement::Truncate(truncate_statement) => {
                let args = &[arg!(py_ctx, truncate_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Truncate)?,
                    args,
                )
            }
            Statement::Merge(merge_statement) => {
                let args = &[arg!(py_ctx, merge_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Merge)?,
                    args,
                )
            }
            Statement::DeclareVar(declare_var_statement) => {
                let args = &[arg!(py_ctx, declare_var_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DeclareVar)?,
                    args,
                )
            }
            Statement::SetVar(set_var_statement) => {
                let args = &[arg!(py_ctx, set_var_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::SetVar)?,
                    args,
                )
            }
            Statement::Block(statements_block) => {
                let args = &[arg!(py_ctx, statements_block)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Block)?,
                    args,
                )
            }
            Statement::CreateSchema(create_schema_statement) => {
                let args = &[arg!(py_ctx, create_schema_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateSchema)?,
                    args,
                )
            }
            Statement::CreateTable(create_table_statement) => {
                let args = &[arg!(py_ctx, create_table_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateTable)?,
                    args,
                )
            }
            Statement::CreateView(create_view_statement) => {
                let args = &[arg!(py_ctx, create_view_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateView)?,
                    args,
                )
            }
            Statement::DropTable(drop_table_statement) => {
                let args = &[arg!(py_ctx, drop_table_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropTable)?,
                    args,
                )
            }
            Statement::DropFunction(drop_function_statement) => {
                let args = &[arg!(py_ctx, drop_function_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropFunction)?,
                    args,
                )
            }
            Statement::DropView(drop_view_statement) => {
                let args = &[arg!(py_ctx, drop_view_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropView)?,
                    args,
                )
            }
            Statement::DropSchema(drop_schema_statement) => {
                let args = &[arg!(py_ctx, drop_schema_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropSchema)?,
                    args,
                )
            }
            Statement::UndropSchema(undrop_schema_statement) => {
                let args = &[arg!(py_ctx, undrop_schema_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::UndropSchema)?,
                    args,
                )
            }
            Statement::If(if_statement) => {
                let args = &[arg!(py_ctx, if_statement)];
                instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Statement::If)?, args)
            }
            Statement::Case(case_statement) => {
                let args = &[arg!(py_ctx, case_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Case)?,
                    args,
                )
            }
            Statement::Raise(raise_statement) => {
                let args = &[arg!(py_ctx, raise_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Raise)?,
                    args,
                )
            }
            Statement::Call(call_statement) => {
                let args = &[arg!(py_ctx, call_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Call)?,
                    args,
                )
            }
            Statement::BeginTransaction => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::BeginTransaction)?,
                &[],
            ),
            Statement::CommitTransaction => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::CommitTransaction)?,
                &[],
            ),
            Statement::RollbackTransaction => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::RollbackTransaction)?,
                &[],
            ),
            Statement::Return => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::Return)?,
                &[],
            ),
            Statement::Loop(loop_statement) => {
                let args = &[arg!(py_ctx, loop_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Loop)?,
                    args,
                )
            }
            Statement::Repeat(repeat_statement) => {
                let args = &[arg!(py_ctx, repeat_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Repeat)?,
                    args,
                )
            }
            Statement::While(while_statement) => {
                let args = &[arg!(py_ctx, while_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::While)?,
                    args,
                )
            }
            Statement::ForIn(for_in_statement) => {
                let args = &[arg!(py_ctx, for_in_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::ForIn)?,
                    args,
                )
            }
            Statement::Break => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::Break)?,
                &[],
            ),
            Statement::Continue => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::Continue)?,
                &[],
            ),
            Statement::Iterate => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::Iterate)?,
                &[],
            ),
            Statement::Leave => instantiate_py_class_from_args(
                py_ctx,
                get_ast_class!(py_ctx, Statement::Leave)?,
                &[],
            ),
            Statement::Labeled(labeled_statement) => {
                let args = &[arg!(py_ctx, labeled_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::Labeled)?,
                    args,
                )
            }
            Statement::ExecuteImmediate(execute_immediate_statement) => {
                let args = &[arg!(py_ctx, execute_immediate_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::ExecuteImmediate)?,
                    args,
                )
            }
            Statement::CreateSqlFunction(create_sql_function_statement) => {
                let args = &[arg!(py_ctx, create_sql_function_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateSqlFunction)?,
                    args,
                )
            }
            Statement::CreateJsFunction(create_js_function_statement) => {
                let args = &[arg!(py_ctx, create_js_function_statement)];
                instantiate_py_class_from_args(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateJsFunction)?,
                    args,
                )
            }
        }
    }
}

impl RsToPyObject for Ast {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.statements)];
        instantiate_py_class_from_args(py_ctx, get_ast_class!(py_ctx, Ast)?, args)
    }
}

impl RsToPyObject for ReadyLineageNodeInput {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.obj_name),
            arg!(py_ctx, self.obj_kind),
            arg!(py_ctx, self.node_name),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageNodeInput)?,
            args,
        )
    }
}

impl RsToPyObject for ReadyLineageNodeSideInput {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.obj_name),
            arg!(py_ctx, self.obj_kind),
            arg!(py_ctx, self.node_name),
            arg!(py_ctx, self.sides),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageNodeSideInput)?,
            args,
        )
    }
}

impl RsToPyObject for ReadyLineageNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.r#type),
            arg!(py_ctx, self.inputs),
            arg!(py_ctx, self.side_inputs),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, ReadyLineageNode)?, args)
    }
}

impl RsToPyObject for ReadyLineageObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.nodes),
        ];
        instantiate_py_class_from_args(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageObject)?,
            args,
        )
    }
}

impl RsToPyObject for ReadyLineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.objects)];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, ReadyLineage)?, args)
    }
}

impl RsToPyObject for RawLineageObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.id),
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.nodes),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, RawLineageObject)?, args)
    }
}

impl RsToPyObject for RawLineageNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.id),
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.source_object),
            arg!(py_ctx, self.inputs),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, RawLineageNode)?, args)
    }
}

impl RsToPyObject for RawLineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.objects),
            arg!(py_ctx, self.lineage_nodes),
            arg!(py_ctx, self.output_lineage),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, RawLineage)?, args)
    }
}

impl RsToPyObject for ReferencedNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.name), arg!(py_ctx, self.referenced_in)];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, ReferencedNode)?, args)
    }
}

impl RsToPyObject for ReferencedObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.name),
            arg!(py_ctx, self.kind),
            arg!(py_ctx, self.nodes),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, ReferencedObject)?, args)
    }
}

impl RsToPyObject for ReferencedColumns {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[arg!(py_ctx, self.objects)];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, ReferencedColumns)?, args)
    }
}

impl RsToPyObject for Lineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let args = &[
            arg!(py_ctx, self.lineage),
            arg!(py_ctx, self.raw_lineage),
            arg!(py_ctx, self.referenced_columns),
        ];
        instantiate_py_class_from_args(py_ctx, get_lineage_class!(py_ctx, Lineage)?, args)
    }
}

#[pyfunction]
fn parse_sql_to_dict(py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
    let rs_ast = inbq::parser::parse_sql(sql).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let json_ast =
        serde_json::to_string(&rs_ast).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let locals = PyDict::new(py);
    locals.set_item(intern!(py, "py_json_ast"), json_ast)?;
    py.run(
        c_str!("import json; out = json.loads(py_json_ast)"),
        None,
        Some(&locals),
    )?;
    let out = locals.get_item(intern!(py, "out"))?.unwrap();
    Ok(out.into())
}

#[pyfunction]
fn parse_sql(py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
    let mut py_ctx = PyContext::new(py).unwrap();
    let rs_ast = inbq::parser::parse_sql(sql)
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .to_py_obj(&mut py_ctx)
        .unwrap();
    Ok(rs_ast.into())
}

#[pyfunction]
#[pyo3(signature = (sqls, parallel=true))]
fn parse_sqls(py: Python<'_>, sqls: Vec<String>, parallel: bool) -> PyResult<Py<PyAny>> {
    let mut py_ctx = PyContext::new(py).unwrap();
    let asts: Vec<Ast> = if parallel {
        sqls.par_iter()
            .map(|sql| inbq::parser::parse_sql(sql).unwrap())
            .collect()
    } else {
        sqls.iter()
            .map(|sql| inbq::parser::parse_sql(sql).unwrap())
            .collect()
    };
    Ok(asts.to_py_obj(&mut py_ctx).unwrap().into())
}

#[pyfunction]
#[pyo3(signature = (sqls, catalog, include_raw=false, parallel=true))]
fn parse_sqls_and_extract_lineage(
    py: Python<'_>,
    sqls: Vec<String>,
    catalog: &Bound<'_, PyDict>,
    include_raw: bool,
    parallel: bool,
) -> PyResult<Py<PyAny>> {
    let locals = PyDict::new(py);
    locals.set_item(intern!(py, "catalog"), catalog)?;
    locals.set_item(intern!(py, "include_raw"), include_raw)?;

    py.run(
        c_str!("import json; catalog_str = json.dumps(catalog)"),
        None,
        Some(&locals),
    )?;
    let catalog_str = locals.get_item(intern!(py, "catalog_str"))?.unwrap();
    let rs_catalog_str: &str = catalog_str.extract()?;
    let rs_catalog =
        serde_json::from_str(rs_catalog_str).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let mut py_ctx = PyContext::new(py).unwrap();

    let asts: Vec<Ast> = py.detach(|| {
        if parallel {
            sqls.par_iter()
                .map(|sql| inbq::parser::parse_sql(sql).unwrap())
                .collect()
        } else {
            sqls.iter()
                .map(|sql| inbq::parser::parse_sql(sql).unwrap())
                .collect()
        }
    });

    let lineages: Vec<Lineage> = py.detach(|| {
        extract_lineage(
            &asts.iter().collect::<Vec<&Ast>>(),
            &rs_catalog,
            include_raw,
            parallel,
        )
        .into_iter()
        .map(|r| r.unwrap())
        .collect()
    });

    let output = (asts, lineages)
        .to_py_obj(&mut py_ctx)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(output.into())
}

#[pyfunction]
fn run_pipeline(
    py: Python<'_>,
    sqls: Vec<String>,
    pipeline: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let pipeline_spec = pipeline.getattr(intern!(py, "spec"))?;

    let config = pipeline_spec.get_item("config")?;
    let raise_exception_on_error: bool = config
        .get_item(intern!(py, "raise_exception_on_error"))?
        .extract()?;
    let parallel: bool = config.get_item(intern!(py, "parallel"))?.extract()?;

    let _parsing_config = pipeline_spec.get_item(intern!(py, "parse"))?;

    let lineage_config = pipeline_spec.get_item(intern!(py, "extract_lineage"));

    // Parsing
    let asts: Vec<anyhow::Result<Ast>> = py.detach(|| {
        if parallel {
            sqls.par_iter()
                .map(|sql| inbq::parser::parse_sql(sql))
                .collect()
        } else {
            sqls.iter()
                .map(|sql| inbq::parser::parse_sql(sql))
                .collect()
        }
    });

    // Lineage
    let lineages: Option<Vec<anyhow::Result<Lineage>>> = if let Ok(lineage_config) = &lineage_config
    {
        let catalog: Catalog = {
            let locals = PyDict::new(py);
            locals.set_item(
                intern!(py, "catalog"),
                lineage_config.get_item(intern!(py, "catalog"))?,
            )?;

            py.run(
                c_str!("import json; catalog_str = json.dumps(catalog)"),
                None,
                Some(&locals),
            )?;
            let py_catalog_str = locals.get_item(intern!(py, "catalog_str"))?.unwrap();
            let rs_catalog_str: &str = py_catalog_str.extract()?;
            serde_json::from_str(rs_catalog_str)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        };

        let include_raw: bool = lineage_config.get_item("include_raw")?.extract()?;

        let closure = |asts: &[anyhow::Result<Ast>]| -> Vec<anyhow::Result<Lineage>> {
            let ok_asts: Vec<(usize, &Ast)> = asts
                .iter()
                .map(|r| r.as_ref())
                .enumerate()
                .filter(|(_, ast)| ast.is_ok())
                .map(|(idx, el)| (idx, el.unwrap()))
                .collect();

            let ko_asts: Vec<(usize, anyhow::Result<Lineage>)> = asts
                .iter()
                .map(|r| r.as_ref())
                .enumerate()
                .filter(|(_, ast)| ast.is_err())
                .map(|(idx, res)| match res {
                    Err(err) => (idx, Err(anyhow!(err.to_string()))),
                    _ => unreachable!(),
                })
                .collect();

            let lineages = extract_lineage(
                &ok_asts.iter().map(|(_, ast)| *ast).collect::<Vec<&Ast>>(),
                &catalog,
                include_raw,
                false,
            );

            let mut output: Vec<MaybeUninit<anyhow::Result<Lineage>>> =
                Vec::with_capacity(asts.len());
            unsafe { output.set_len(asts.len()) };

            for (index, result) in ko_asts {
                output[index].write(result);
            }
            for ((index, _), lin) in ok_asts.into_iter().zip(lineages) {
                output[index].write(lin);
            }

            unsafe { mem::transmute::<_, Vec<anyhow::Result<Lineage>>>(output) }
        };

        let lineages: Vec<anyhow::Result<Lineage>> = py.detach(|| {
            if parallel {
                let n_chunks = std::cmp::max(
                    1,
                    asts.len() / std::thread::available_parallelism().unwrap().get(),
                );
                asts.par_chunks(n_chunks).flat_map(closure).collect()
            } else {
                closure(&asts)
            }
        });

        Some(lineages)
    } else {
        None
    };

    let mut py_ctx = PyContext::new(py).unwrap();

    let py_asts = {
        let mut py_list = Vec::with_capacity(asts.len());
        for ast in &asts {
            match ast {
                Ok(ast) => py_list.push(ast.to_py_obj(&mut py_ctx).unwrap()),
                Err(err) => {
                    if raise_exception_on_error {
                        return Err(PyRuntimeError::new_err(err.to_string()));
                    }
                    let error = err.to_string().to_py_obj(&mut py_ctx).unwrap();
                    py_list.push(
                        py_ctx
                            .inbq_module
                            .getattr(intern!(py, "PipelineError"))?
                            .call(PyTuple::new(py, &[error])?, None)?,
                    );
                }
            }
        }
        PyList::new(py_ctx.py, py_list)?.as_any().to_owned()
    };

    let py_lineages = if let Some(lineages) = &lineages {
        let mut py_list = Vec::with_capacity(asts.len());
        let py_kwargs = PyDict::new(py);
        for lineage in lineages {
            match lineage {
                Ok(lineage) => py_list.push(lineage.to_py_obj(&mut py_ctx).unwrap()),
                Err(err) => {
                    if raise_exception_on_error {
                        return Err(PyRuntimeError::new_err(err.to_string()));
                    }
                    let error = err.to_string().to_py_obj(&mut py_ctx).unwrap();
                    py_list.push(
                        py_ctx
                            .inbq_module
                            .getattr(intern!(py, "PipelineError"))?
                            .call(PyTuple::new(py, &[error])?, Some(&py_kwargs))?,
                    );
                }
            }
        }
        PyList::new(py_ctx.py, py_list)?.as_any().to_owned()
    } else {
        PyNone::get(py).into_bound().as_any().to_owned()
    };

    let pipeline_output = if lineages.is_some() {
        let args = &[py_asts, py_lineages];
        let cls = py_ctx
            .inbq_module
            .getattr(intern!(py, "PipelineParsingLineageOutput"))?;
        cls.call(PyTuple::new(py, args)?, None)?
    } else {
        let args = &[py_asts];
        let cls = py_ctx
            .inbq_module
            .getattr(intern!(py, "PipelineParsingOutput"))?;
        cls.call(PyTuple::new(py, args)?, None)?
    };

    Ok(pipeline_output.into())
}

#[pymodule]
fn _inbq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sql_to_dict, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sqls, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sqls_and_extract_lineage, m)?)?;
    m.add_function(wrap_pyfunction!(run_pipeline, m)?)?;
    Ok(())
}
