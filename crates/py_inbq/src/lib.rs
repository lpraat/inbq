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
        CastFunctionExpr, CastFunctionFormat, CoalesceFunctionExpr, ColumnSchema,
        ColumnSetToUnpivot, ColumnToUnpivot, ConcatFunctionExpr, CreateJsFunctionStatement,
        CreateSchemaStatement, CreateSqlFunctionStatement, CreateTableStatement,
        CreateViewStatement, CrossJoinExpr, Cte, CurrentDateFunctionExpr,
        CurrentDatetimeFunctionExpr, CurrentTimeFunctionExpr, DateDiffFunctionExpr,
        DateTruncFunctionExpr, DatetimeDiffFunctionExpr, DatetimeTruncFunctionExpr, DdlOption,
        DeclareVarStatement, DeleteStatement, DropFunctionStatement, DropTableStatement,
        ExecuteImmediateStatement, ExecuteImmediateUsingIdentifier, Expr, ExtractFunctionExpr,
        ExtractFunctionPart, ForInStatement, ForeignKeyConstraintNotEnforced, ForeignKeyReference,
        FrameBound, FromExpr, FromGroupingQueryExpr, FromPathExpr, FromUnnestExpr,
        FunctionAggregate, FunctionAggregateHaving, FunctionAggregateHavingKind,
        FunctionAggregateNulls, FunctionAggregateOrderBy, FunctionArgument, FunctionArgumentType,
        FunctionExpr, GenericFunctionExpr, GenericFunctionExprArg, Granularity, GroupBy,
        GroupByExpr, GroupingExpr, GroupingFromExpr, GroupingQueryExpr, Having, Identifier,
        IfBranch, IfFunctionExpr, IfStatement, InsertStatement, IntervalExpr, IntervalPart,
        JoinCondition, JoinExpr, JoinKind, LabeledStatement, LastDayFunctionExpr, LeftFunctionExpr,
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
        TableConstraint, TableFunctionArgument, TableFunctionExpr, TableSample,
        TimeDiffFunctionExpr, TimeTruncFunctionExpr, TimestampDiffFunctionExpr,
        TimestampTruncFunctionExpr, Token, TokenType, TruncateStatement, Type, UnaryExpr,
        UnaryOperator, UnnestExpr, Unpivot, UnpivotKind, UnpivotNulls, UpdateItem, UpdateStatement,
        ViewColumn, WeekBegin, When, WhenMatched, WhenNotMatchedBySource, WhenNotMatchedByTarget,
        WhenThen, Where, WhileStatement, Window, WindowFrame, WindowFrameKind, WindowOrderByExpr,
        WindowSpec, With, WithExpr, WithExprVar,
    },
    lineage::{
        Lineage, RawLineage, RawLineageNode, RawLineageObject, ReadyLineage, ReadyLineageNode,
        ReadyLineageNodeInput, ReadyLineageObject, UsedColumns, UsedNode, UsedObject,
        catalog::Catalog, extract_lineage,
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

macro_rules! kwarg {
    ($py_ctx:expr, $py_field:expr, $rs_field:expr) => {
        (
            intern!($py_ctx.py, $py_field),
            $rs_field.to_py_obj($py_ctx)?,
        )
    };
}

static VARIANT_FIELD_NAME: &str = "vty";

trait RsToPyObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>>;
}

fn instantiate_py_class<'py>(
    py_ctx: &PyContext<'py>,
    cls: Bound<'py, PyAny>,
    kwargs: &[(&Bound<'py, PyString>, Bound<'py, PyAny>)],
) -> anyhow::Result<Bound<'py, PyAny>> {
    let py_kwargs = PyDict::new(py_ctx.py);
    for (key, value) in kwargs {
        py_kwargs.set_item(key, value)?;
    }

    cls.call(PyTuple::empty(py_ctx.py), Some(&py_kwargs))
        .map_err(|e| anyhow!(e))
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
            TokenType::LeftParen => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::LeftParen)?, &[])
            }
            TokenType::RightParen => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::RightParen)?, &[])
            }
            TokenType::LeftSquare => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::LeftSquare)?, &[])
            }
            TokenType::RightSquare => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::RightSquare)?, &[])
            }
            TokenType::Comma => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Comma)?, &[])
            }
            TokenType::Dot => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Dot)?, &[])
            }
            TokenType::Minus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Minus)?, &[])
            }
            TokenType::Plus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Plus)?, &[])
            }
            TokenType::BitwiseNot => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::BitwiseNot)?, &[])
            }
            TokenType::BitwiseOr => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::BitwiseOr)?, &[])
            }
            TokenType::BitwiseAnd => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::BitwiseAnd)?, &[])
            }
            TokenType::BitwiseXor => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::BitwiseXor)?, &[])
            }
            TokenType::BitwiseRightShift => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseRightShift)?,
                &[],
            ),
            TokenType::BitwiseLeftShift => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::BitwiseLeftShift)?,
                &[],
            ),
            TokenType::Colon => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Colon)?, &[])
            }
            TokenType::Semicolon => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Semicolon)?, &[])
            }
            TokenType::Slash => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Slash)?, &[])
            }
            TokenType::Star => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Star)?, &[])
            }
            TokenType::Tick => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Tick)?, &[])
            }
            TokenType::ConcatOperator => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::ConcatOperator)?,
                &[],
            ),
            TokenType::Bang => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Bang)?, &[])
            }
            TokenType::BangEqual => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::BangEqual)?, &[])
            }
            TokenType::Equal => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Equal)?, &[])
            }
            TokenType::NotEqual => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::NotEqual)?, &[])
            }
            TokenType::Greater => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Greater)?, &[])
            }
            TokenType::GreaterEqual => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::GreaterEqual)?,
                &[],
            ),
            TokenType::Less => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Less)?, &[])
            }
            TokenType::LessEqual => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::LessEqual)?, &[])
            }
            TokenType::RightArrow => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::RightArrow)?, &[])
            }
            TokenType::QuotedIdentifier(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::QuotedIdentifier)?,
                    kwargs,
                )
            }
            TokenType::Identifier(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::Identifier)?,
                    kwargs,
                )
            }
            TokenType::QueryNamedParameter(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::QueryNamedParameter)?,
                    kwargs,
                )
            }
            TokenType::QueryPositionalParameter => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::QueryPositionalParameter)?,
                &[],
            ),
            TokenType::SystemVariable(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::SystemVariable)?,
                    kwargs,
                )
            }
            TokenType::String(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::String)?, kwargs)
            }
            TokenType::RawString(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TokenType::RawString)?,
                    kwargs,
                )
            }
            TokenType::Bytes(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Bytes)?, kwargs)
            }
            TokenType::RawBytes(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::RawBytes)?, kwargs)
            }
            TokenType::Number(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Number)?, kwargs)
            }
            TokenType::Eof => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Eof)?, &[])
            }
            TokenType::All => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::All)?, &[])
            }
            TokenType::And => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::And)?, &[])
            }
            TokenType::Any => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Any)?, &[])
            }
            TokenType::Array => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Array)?, &[])
            }
            TokenType::As => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::As)?, &[])
            }
            TokenType::Asc => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Asc)?, &[])
            }
            TokenType::AssertRowsModified => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, TokenType::AssertRowsModified)?,
                &[],
            ),
            TokenType::At => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::At)?, &[])
            }
            TokenType::Between => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Between)?, &[])
            }
            TokenType::By => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::By)?, &[])
            }
            TokenType::Case => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Case)?, &[])
            }
            TokenType::Cast => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Cast)?, &[])
            }
            TokenType::Collate => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Collate)?, &[])
            }
            TokenType::Contains => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Contains)?, &[])
            }
            TokenType::Create => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Create)?, &[])
            }
            TokenType::Cross => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Cross)?, &[])
            }
            TokenType::Cube => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Cube)?, &[])
            }
            TokenType::Current => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Current)?, &[])
            }
            TokenType::Default => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Default)?, &[])
            }
            TokenType::Define => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Define)?, &[])
            }
            TokenType::Desc => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Desc)?, &[])
            }
            TokenType::Distinct => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Distinct)?, &[])
            }
            TokenType::Else => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Else)?, &[])
            }
            TokenType::End => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::End)?, &[])
            }
            TokenType::Enum => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Enum)?, &[])
            }
            TokenType::Escape => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Escape)?, &[])
            }
            TokenType::Except => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Except)?, &[])
            }
            TokenType::Exclude => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Exclude)?, &[])
            }
            TokenType::Exists => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Exists)?, &[])
            }
            TokenType::Extract => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Extract)?, &[])
            }
            TokenType::False => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::False)?, &[])
            }
            TokenType::Fetch => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Fetch)?, &[])
            }
            TokenType::Following => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Following)?, &[])
            }
            TokenType::For => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::For)?, &[])
            }
            TokenType::From => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::From)?, &[])
            }
            TokenType::Full => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Full)?, &[])
            }
            TokenType::Group => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Group)?, &[])
            }
            TokenType::Grouping => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Grouping)?, &[])
            }
            TokenType::Groups => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Groups)?, &[])
            }
            TokenType::Hash => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Hash)?, &[])
            }
            TokenType::Having => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Having)?, &[])
            }
            TokenType::If => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::If)?, &[])
            }
            TokenType::Ignore => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Ignore)?, &[])
            }
            TokenType::In => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::In)?, &[])
            }
            TokenType::Inner => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Inner)?, &[])
            }
            TokenType::Intersect => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Intersect)?, &[])
            }
            TokenType::Interval => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Interval)?, &[])
            }
            TokenType::Into => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Into)?, &[])
            }
            TokenType::Is => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Is)?, &[])
            }
            TokenType::Join => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Join)?, &[])
            }
            TokenType::Lateral => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Lateral)?, &[])
            }
            TokenType::Left => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Left)?, &[])
            }
            TokenType::Like => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Like)?, &[])
            }
            TokenType::Limit => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Limit)?, &[])
            }
            TokenType::Lookup => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Lookup)?, &[])
            }
            TokenType::Merge => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Merge)?, &[])
            }
            TokenType::Natural => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Natural)?, &[])
            }
            TokenType::New => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::New)?, &[])
            }
            TokenType::No => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::No)?, &[])
            }
            TokenType::Not => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Not)?, &[])
            }
            TokenType::Null => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Null)?, &[])
            }
            TokenType::Nulls => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Nulls)?, &[])
            }
            TokenType::Of => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Of)?, &[])
            }
            TokenType::On => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::On)?, &[])
            }
            TokenType::Or => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Or)?, &[])
            }
            TokenType::Order => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Order)?, &[])
            }
            TokenType::Outer => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Outer)?, &[])
            }
            TokenType::Over => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Over)?, &[])
            }
            TokenType::Partition => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Partition)?, &[])
            }
            TokenType::Preceding => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Preceding)?, &[])
            }
            TokenType::Proto => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Proto)?, &[])
            }
            TokenType::Qualify => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Qualify)?, &[])
            }
            TokenType::Range => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Range)?, &[])
            }
            TokenType::Recursive => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Recursive)?, &[])
            }
            TokenType::Respect => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Respect)?, &[])
            }
            TokenType::Right => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Right)?, &[])
            }
            TokenType::Rollup => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Rollup)?, &[])
            }
            TokenType::Rows => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Rows)?, &[])
            }
            TokenType::Select => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Select)?, &[])
            }
            TokenType::Set => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Set)?, &[])
            }
            TokenType::Some => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Some)?, &[])
            }
            TokenType::Struct => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Struct)?, &[])
            }
            TokenType::Tablesample => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Tablesample)?, &[])
            }
            TokenType::Then => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Then)?, &[])
            }
            TokenType::To => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::To)?, &[])
            }
            TokenType::Treat => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Treat)?, &[])
            }
            TokenType::True => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::True)?, &[])
            }
            TokenType::Union => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Union)?, &[])
            }
            TokenType::Unnest => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Unnest)?, &[])
            }
            TokenType::Using => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Using)?, &[])
            }
            TokenType::When => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::When)?, &[])
            }
            TokenType::Where => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Where)?, &[])
            }
            TokenType::Window => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Window)?, &[])
            }
            TokenType::With => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::With)?, &[])
            }
            TokenType::Within => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TokenType::Within)?, &[])
            }
        }
    }
}

impl RsToPyObject for Token {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "lexeme", self.lexeme),
            kwarg!(py_ctx, "line", self.line),
            kwarg!(py_ctx, "col", self.col),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Token)?, kwargs)
    }
}

impl RsToPyObject for Name {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Name::Identifier(identifier) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, identifier)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Name::Identifier)?, kwargs)
            }
            Name::QuotedIdentifier(quoted_identifier) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, quoted_identifier)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Name::QuotedIdentifier)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for Number {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "value", self.value)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Number)?, kwargs)
    }
}

impl RsToPyObject for PathPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            PathPart::Identifier(identifier) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, identifier)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, PathPart::Identifier)?,
                    kwargs,
                )
            }
            PathPart::QuotedIdentifier(quoted_identifier) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, quoted_identifier)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, PathPart::QuotedIdentifier)?,
                    kwargs,
                )
            }
            PathPart::Number(number) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, number)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, PathPart::Number)?, kwargs)
            }
            PathPart::DotSeparator => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, PathPart::DotSeparator)?, &[])
            }
            PathPart::SlashSeparator => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::SlashSeparator)?,
                &[],
            ),
            PathPart::DashSeparator => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::DashSeparator)?,
                &[],
            ),
            PathPart::ColonSeparator => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, PathPart::ColonSeparator)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for PathName {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "parts", self.parts),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, PathName)?, kwargs)
    }
}

impl RsToPyObject for StructFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "type_", self.r#type),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, StructFieldType)?, kwargs)
    }
}

impl RsToPyObject for Type {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Type::Array { r#type } => {
                let kwargs = &[kwarg!(py_ctx, "type_", r#type)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Array)?, kwargs)
            }
            Type::BigNumeric => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::BigNumeric)?, &[])
            }
            Type::Bool => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Bool)?, &[]),
            Type::Bytes => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Bytes)?, &[]),
            Type::Date => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Date)?, &[]),
            Type::Datetime => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Datetime)?, &[])
            }
            Type::Float64 => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Float64)?, &[])
            }
            Type::Geography => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Geography)?, &[])
            }
            Type::Int64 => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Int64)?, &[]),
            Type::Interval => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Interval)?, &[])
            }
            Type::Json => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Json)?, &[]),
            Type::Numeric => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Numeric)?, &[])
            }
            Type::Range { r#type } => {
                let kwargs = &[kwarg!(py_ctx, "type_", r#type)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Range)?, kwargs)
            }
            Type::String => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::String)?, &[])
            }
            Type::Struct { fields } => {
                let kwargs = &[kwarg!(py_ctx, "fields", fields)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Struct)?, kwargs)
            }
            Type::Time => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Time)?, &[]),
            Type::Timestamp => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Type::Timestamp)?, &[])
            }
        }
    }
}

impl RsToPyObject for StructParameterizedFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "type_", self.r#type),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, StructParameterizedFieldType)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ParameterizedType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ParameterizedType::Array { r#type } => {
                let kwargs = &[kwarg!(py_ctx, "type_", r#type)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Array)?,
                    kwargs,
                )
            }
            ParameterizedType::BigNumeric { precision, scale } => {
                let kwargs = &[
                    kwarg!(py_ctx, "precision", precision),
                    kwarg!(py_ctx, "scale", scale),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::BigNumeric)?,
                    kwargs,
                )
            }
            ParameterizedType::Bool => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Bool)?,
                &[],
            ),
            ParameterizedType::Bytes { max_length } => {
                let kwargs = &[kwarg!(py_ctx, "max_length", max_length)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Bytes)?,
                    kwargs,
                )
            }
            ParameterizedType::Date => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Date)?,
                &[],
            ),
            ParameterizedType::Datetime => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Datetime)?,
                &[],
            ),
            ParameterizedType::Float64 => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Float64)?,
                &[],
            ),
            ParameterizedType::Geography => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Geography)?,
                &[],
            ),
            ParameterizedType::Int64 => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Int64)?,
                &[],
            ),
            ParameterizedType::Interval => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Interval)?,
                &[],
            ),
            ParameterizedType::Json => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Json)?,
                &[],
            ),
            ParameterizedType::Numeric { precision, scale } => {
                let kwargs = &[
                    kwarg!(py_ctx, "precision", precision),
                    kwarg!(py_ctx, "scale", scale),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Numeric)?,
                    kwargs,
                )
            }
            ParameterizedType::Range { r#type } => {
                let kwargs = &[kwarg!(py_ctx, "type_", r#type)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Range)?,
                    kwargs,
                )
            }
            ParameterizedType::String { max_length } => {
                let kwargs = &[kwarg!(py_ctx, "max_length", max_length)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::String)?,
                    kwargs,
                )
            }
            ParameterizedType::Struct { fields } => {
                let kwargs = &[kwarg!(py_ctx, "fields", fields)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ParameterizedType::Struct)?,
                    kwargs,
                )
            }
            ParameterizedType::Time => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ParameterizedType::Time)?,
                &[],
            ),
            ParameterizedType::Timestamp => instantiate_py_class(
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
            BinaryOperator::BitwiseNot => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseNot)?,
                &[],
            ),
            BinaryOperator::Star => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Star)?, &[])
            }
            BinaryOperator::Slash => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Slash)?, &[])
            }
            BinaryOperator::Concat => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Concat)?, &[])
            }
            BinaryOperator::Plus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Plus)?, &[])
            }
            BinaryOperator::Minus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Minus)?, &[])
            }
            BinaryOperator::BitwiseLeftShift => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseLeftShift)?,
                &[],
            ),
            BinaryOperator::BitwiseRightShift => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseRightShift)?,
                &[],
            ),
            BinaryOperator::BitwiseAnd => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseAnd)?,
                &[],
            ),
            BinaryOperator::BitwiseXor => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseXor)?,
                &[],
            ),
            BinaryOperator::BitwiseOr => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::BitwiseOr)?,
                &[],
            ),
            BinaryOperator::Equal => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Equal)?, &[])
            }
            BinaryOperator::LessThan => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::LessThan)?,
                &[],
            ),
            BinaryOperator::GreaterThan => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::GreaterThan)?,
                &[],
            ),
            BinaryOperator::LessThanOrEqualTo => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::LessThanOrEqualTo)?,
                &[],
            ),
            BinaryOperator::GreaterThanOrEqualTo => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::GreaterThanOrEqualTo)?,
                &[],
            ),
            BinaryOperator::NotEqual => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotEqual)?,
                &[],
            ),
            BinaryOperator::Like => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Like)?, &[])
            }
            BinaryOperator::NotLike => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotLike)?,
                &[],
            ),
            BinaryOperator::QuantifiedLike => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::QuantifiedLike)?,
                &[],
            ),
            BinaryOperator::QuantifiedNotLike => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::QuantifiedNotLike)?,
                &[],
            ),
            BinaryOperator::Between => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::Between)?,
                &[],
            ),
            BinaryOperator::NotBetween => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::NotBetween)?,
                &[],
            ),
            BinaryOperator::In => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::In)?, &[])
            }
            BinaryOperator::NotIn => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::NotIn)?, &[])
            }
            BinaryOperator::And => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::And)?, &[])
            }
            BinaryOperator::Or => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryOperator::Or)?, &[])
            }
            BinaryOperator::ArrayIndex => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::ArrayIndex)?,
                &[],
            ),
            BinaryOperator::FieldAccess => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::FieldAccess)?,
                &[],
            ),
            BinaryOperator::IsDistinctFrom => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::IsDistinctFrom)?,
                &[],
            ),
            BinaryOperator::IsNotDistinctFrom => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, BinaryOperator::IsNotDistinctFrom)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for BinaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "left", self.left),
            kwarg!(py_ctx, "operator", self.operator),
            kwarg!(py_ctx, "right", self.right),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BinaryExpr)?, kwargs)
    }
}

impl RsToPyObject for UnaryOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnaryOperator::Plus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::Plus)?, &[])
            }
            UnaryOperator::Minus => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::Minus)?, &[])
            }
            UnaryOperator::BitwiseNot => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::BitwiseNot)?,
                &[],
            ),
            UnaryOperator::IsNull => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::IsNull)?, &[])
            }
            UnaryOperator::IsNotNull => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotNull)?,
                &[],
            ),
            UnaryOperator::IsTrue => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::IsTrue)?, &[])
            }
            UnaryOperator::IsNotTrue => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotTrue)?,
                &[],
            ),
            UnaryOperator::IsFalse => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::IsFalse)?, &[])
            }
            UnaryOperator::IsNotFalse => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, UnaryOperator::IsNotFalse)?,
                &[],
            ),
            UnaryOperator::Not => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryOperator::Not)?, &[])
            }
        }
    }
}

impl RsToPyObject for UnaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "operator", self.operator),
            kwarg!(py_ctx, "right", self.right),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnaryExpr)?, kwargs)
    }
}

impl RsToPyObject for GroupingExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "expr", self.expr)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupingExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "exprs", self.exprs),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ArrayExpr)?, kwargs)
    }
}

impl RsToPyObject for StructField {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, StructField)?, kwargs)
    }
}

impl RsToPyObject for StructExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "fields", self.fields),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, StructExpr)?, kwargs)
    }
}

impl RsToPyObject for RangeExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "value", self.value),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, RangeExpr)?, kwargs)
    }
}

impl RsToPyObject for IntervalPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            IntervalPart::Year => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Year)?, &[])
            }
            IntervalPart::Quarter => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Quarter)?, &[])
            }
            IntervalPart::Month => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Month)?, &[])
            }
            IntervalPart::Week => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Week)?, &[])
            }
            IntervalPart::Day => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Day)?, &[])
            }
            IntervalPart::Hour => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Hour)?, &[])
            }
            IntervalPart::Minute => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Minute)?, &[])
            }
            IntervalPart::Second => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IntervalPart::Second)?, &[])
            }
            IntervalPart::Millisecond => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, IntervalPart::Millisecond)?,
                &[],
            ),
            IntervalPart::Microsecond => instantiate_py_class(
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
                let kwargs = &[kwarg!(py_ctx, "value", value), kwarg!(py_ctx, "part", part)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, IntervalExpr::Interval)?,
                    kwargs,
                )
            }
            IntervalExpr::IntervalRange {
                value,
                start_part,
                end_part,
            } => {
                let kwargs = &[
                    kwarg!(py_ctx, "value", value),
                    kwarg!(py_ctx, "start_part", start_part),
                    kwarg!(py_ctx, "end_part", end_part),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, IntervalExpr::IntervalRange)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for WhenThen {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "when", self.when),
            kwarg!(py_ctx, "then", self.then),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WhenThen)?, kwargs)
    }
}

impl RsToPyObject for CaseExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "case_", self.case),
            kwarg!(py_ctx, "when_thens", self.when_thens),
            kwarg!(py_ctx, "else_", self.r#else),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CaseExpr)?, kwargs)
    }
}

impl RsToPyObject for FunctionAggregateNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionAggregateNulls::Ignore => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateNulls::Ignore)?,
                &[],
            ),
            FunctionAggregateNulls::Respect => instantiate_py_class(
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
            FunctionAggregateHavingKind::Max => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateHavingKind::Max)?,
                &[],
            ),
            FunctionAggregateHavingKind::Min => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FunctionAggregateHavingKind::Min)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateHaving {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "kind", self.kind),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, FunctionAggregateHaving)?,
            kwargs,
        )
    }
}

impl RsToPyObject for OrderBySortDirection {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderBySortDirection::Asc => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, OrderBySortDirection::Asc)?,
                &[],
            ),
            OrderBySortDirection::Desc => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, OrderBySortDirection::Desc)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateOrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "sort_direction", self.sort_direction),
            kwarg!(py_ctx, "nulls", self.nulls),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, FunctionAggregateOrderBy)?,
            kwargs,
        )
    }
}

impl RsToPyObject for FunctionAggregate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "distinct", self.distinct),
            kwarg!(py_ctx, "nulls", self.nulls),
            kwarg!(py_ctx, "having", self.having),
            kwarg!(py_ctx, "order_by", self.order_by),
            kwarg!(py_ctx, "limit", self.limit),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionAggregate)?, kwargs)
    }
}

impl RsToPyObject for GenericFunctionExprArg {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "aggregate", self.aggregate),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, GenericFunctionExprArg)?,
            kwargs,
        )
    }
}

impl RsToPyObject for WindowOrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "sort_direction", self.sort_direction),
            kwarg!(py_ctx, "nulls", self.nulls),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WindowOrderByExpr)?, kwargs)
    }
}

impl RsToPyObject for WindowFrameKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            WindowFrameKind::Range => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WindowFrameKind::Range)?, &[])
            }
            WindowFrameKind::Rows => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WindowFrameKind::Rows)?, &[])
            }
        }
    }
}

impl RsToPyObject for FrameBound {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FrameBound::UnboundedPreceding => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FrameBound::UnboundedPreceding)?,
                &[],
            ),
            FrameBound::Preceding(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FrameBound::Preceding)?,
                    kwargs,
                )
            }
            FrameBound::UnboundedFollowing => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FrameBound::UnboundedFollowing)?,
                &[],
            ),
            FrameBound::Following(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FrameBound::Following)?,
                    kwargs,
                )
            }
            FrameBound::CurrentRow => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FrameBound::CurrentRow)?, &[])
            }
        }
    }
}

impl RsToPyObject for WindowFrame {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "start", self.start),
            kwarg!(py_ctx, "end", self.end),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WindowFrame)?, kwargs)
    }
}

impl RsToPyObject for WindowSpec {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "window_name", self.window_name),
            kwarg!(py_ctx, "partition_by", self.partition_by),
            kwarg!(py_ctx, "order_by", self.order_by),
            kwarg!(py_ctx, "frame", self.frame),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WindowSpec)?, kwargs)
    }
}

impl RsToPyObject for NamedWindowExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            NamedWindowExpr::Reference(parse_token) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, parse_token)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, NamedWindowExpr::Reference)?,
                    kwargs,
                )
            }
            NamedWindowExpr::WindowSpec(window_spec) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, window_spec)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, NamedWindowExpr::WindowSpec)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for GenericFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "arguments", self.arguments),
            kwarg!(py_ctx, "over", self.over),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GenericFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "query", self.query)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ArrayFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayAggFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "arg", self.arg),
            kwarg!(py_ctx, "over", self.over),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, ArrayAggFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ConcatFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "values", self.values)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ConcatFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for CastFunctionFormat {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "format", self.format),
            kwarg!(py_ctx, "time_zone", self.time_zone),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CastFunctionFormat)?, kwargs)
    }
}

impl RsToPyObject for CastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "format", self.format),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CastFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for SafeCastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "format", self.format),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, SafeCastFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CurrentDateFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "timezone", self.timezone)];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CurrentDateFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for IfFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "condition", self.condition),
            kwarg!(py_ctx, "true_result", self.true_result),
            kwarg!(py_ctx, "false_result", self.false_result),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IfFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for WeekBegin {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            WeekBegin::Sunday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Sunday)?, &[])
            }
            WeekBegin::Monday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Monday)?, &[])
            }
            WeekBegin::Tuesday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Tuesday)?, &[])
            }
            WeekBegin::Wednesday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Wednesday)?, &[])
            }
            WeekBegin::Thursday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Thursday)?, &[])
            }
            WeekBegin::Friday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Friday)?, &[])
            }
            WeekBegin::Saturday => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WeekBegin::Saturday)?, &[])
            }
        }
    }
}

impl RsToPyObject for ExtractFunctionPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ExtractFunctionPart::MicroSecond => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::MicroSecond)?,
                &[],
            ),
            ExtractFunctionPart::MilliSecond => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::MilliSecond)?,
                &[],
            ),
            ExtractFunctionPart::Second => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Second)?,
                &[],
            ),
            ExtractFunctionPart::Minute => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Minute)?,
                &[],
            ),
            ExtractFunctionPart::Hour => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Hour)?,
                &[],
            ),
            ExtractFunctionPart::DayOfWeek => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::DayOfWeek)?,
                &[],
            ),
            ExtractFunctionPart::Day => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Day)?,
                &[],
            ),
            ExtractFunctionPart::DayOfYear => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::DayOfYear)?,
                &[],
            ),
            ExtractFunctionPart::Week => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Week)?,
                &[],
            ),
            ExtractFunctionPart::WeekWithBegin(week_begin) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, week_begin)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, ExtractFunctionPart::WeekWithBegin)?,
                    kwargs,
                )
            }
            ExtractFunctionPart::IsoWeek => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::IsoWeek)?,
                &[],
            ),
            ExtractFunctionPart::Month => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Month)?,
                &[],
            ),
            ExtractFunctionPart::Quarter => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Quarter)?,
                &[],
            ),
            ExtractFunctionPart::Year => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Year)?,
                &[],
            ),
            ExtractFunctionPart::IsoYear => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::IsoYear)?,
                &[],
            ),
            ExtractFunctionPart::Date => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Date)?,
                &[],
            ),
            ExtractFunctionPart::Time => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, ExtractFunctionPart::Time)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for ExtractFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "part", self.part),
            kwarg!(py_ctx, "expr", self.expr),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ExtractFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for LeftFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "value", self.value),
            kwarg!(py_ctx, "length", self.length),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LeftFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for RightFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "value", self.value),
            kwarg!(py_ctx, "length", self.length),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, RightFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for Granularity {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Granularity::MicroSecond => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::MicroSecond)?,
                &[],
            ),
            Granularity::MilliSecond => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Granularity::MilliSecond)?,
                &[],
            ),
            Granularity::Second => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Second)?, &[])
            }
            Granularity::Minute => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Minute)?, &[])
            }
            Granularity::Hour => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Hour)?, &[])
            }
            Granularity::Day => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Day)?, &[])
            }
            Granularity::Week => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Week)?, &[])
            }
            Granularity::WeekWithBegin(week_begin) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, week_begin)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Granularity::WeekWithBegin)?,
                    kwargs,
                )
            }
            Granularity::IsoWeek => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::IsoWeek)?, &[])
            }
            Granularity::Month => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Month)?, &[])
            }
            Granularity::Quarter => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Quarter)?, &[])
            }
            Granularity::Year => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Year)?, &[])
            }
            Granularity::IsoYear => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::IsoYear)?, &[])
            }
            Granularity::Date => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Date)?, &[])
            }
            Granularity::Time => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Granularity::Time)?, &[])
            }
        }
    }
}

impl RsToPyObject for DateDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "start_date", self.start_date),
            kwarg!(py_ctx, "end_date", self.end_date),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, DateDiffFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for DatetimeDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "start_datetime", self.start_datetime),
            kwarg!(py_ctx, "end_datetime", self.end_datetime),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, DatetimeDiffFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for TimeDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "start_time", self.start_time),
            kwarg!(py_ctx, "end_time", self.end_time),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, TimeDiffFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for TimestampDiffFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "start_timestamp", self.start_timestamp),
            kwarg!(py_ctx, "end_timestamp", self.end_timestamp),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, TimestampDiffFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for DateTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "date", self.date),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, DateTruncFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for DatetimeTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "datetime", self.datetime),
            kwarg!(py_ctx, "granularity", self.granularity),
            kwarg!(py_ctx, "timezone", self.timezone),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, DatetimeTruncFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for TimestampTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "timestamp", self.timestamp),
            kwarg!(py_ctx, "granularity", self.granularity),
            kwarg!(py_ctx, "timezone", self.timezone),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, TimestampTruncFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for TimeTruncFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "time", self.time),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, TimeTruncFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for LastDayFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "granularity", self.granularity),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LastDayFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for CurrentDatetimeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "timezone", self.timezone)];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CurrentDatetimeFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CurrentTimeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "timezone", self.timezone)];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CurrentTimeFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for NormalizationMode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            NormalizationMode::NFC => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, NormalizationMode::NFC)?, &[])
            }
            NormalizationMode::NFKC => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFKC)?,
                &[],
            ),
            NormalizationMode::NFD => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, NormalizationMode::NFD)?, &[])
            }
            NormalizationMode::NFKD => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, NormalizationMode::NFKD)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for NormalizeFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "value", self.value),
            kwarg!(py_ctx, "mode", self.mode),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, NormalizeFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for NormalizeAndCasefoldFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "value", self.value),
            kwarg!(py_ctx, "mode", self.mode),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, NormalizeAndCasefoldFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CoalesceFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "exprs", self.exprs)];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CoalesceFunctionExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for FunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionExpr::Array(array_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, array_function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionExpr::Array)?, kwargs)
            }
            FunctionExpr::ArrayAgg(array_agg_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, array_agg_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::ArrayAgg)?,
                    kwargs,
                )
            }
            FunctionExpr::Concat(concat_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, concat_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Concat)?,
                    kwargs,
                )
            }
            FunctionExpr::Coalesce(coalesce_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, coalesce_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Coalesce)?,
                    kwargs,
                )
            }
            FunctionExpr::Cast(cast_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, cast_function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionExpr::Cast)?, kwargs)
            }
            FunctionExpr::SafeCast(safe_cast_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, safe_cast_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::SafeCast)?,
                    kwargs,
                )
            }
            FunctionExpr::CurrentDate(current_date_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    current_date_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentDate)?,
                    kwargs,
                )
            }
            FunctionExpr::CurrentDatetime(current_datetime_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    current_datetime_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentDatetime)?,
                    kwargs,
                )
            }
            FunctionExpr::CurrentTime(current_time_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    current_time_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::CurrentTime)?,
                    kwargs,
                )
            }
            FunctionExpr::If(if_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, if_function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionExpr::If)?, kwargs)
            }
            FunctionExpr::Extract(extract_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, extract_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Extract)?,
                    kwargs,
                )
            }
            FunctionExpr::Normalize(normalize_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, normalize_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::Normalize)?,
                    kwargs,
                )
            }
            FunctionExpr::NormalizeAndCasefold(normalize_and_casefold_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    normalize_and_casefold_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::NormalizeAndCasefold)?,
                    kwargs,
                )
            }
            FunctionExpr::Left(left_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, left_function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionExpr::Left)?, kwargs)
            }
            FunctionExpr::Right(right_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, right_function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionExpr::Right)?, kwargs)
            }
            FunctionExpr::CurrentTimestamp => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FunctionExpr::CurrentTimestamp)?,
                &[],
            ),
            FunctionExpr::DateDiff(date_diff_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, date_diff_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DateDiff)?,
                    kwargs,
                )
            }
            FunctionExpr::DatetimeDiff(datetime_diff_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    datetime_diff_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DatetimeDiff)?,
                    kwargs,
                )
            }
            FunctionExpr::TimestampDiff(timestamp_diff_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    timestamp_diff_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimestampDiff)?,
                    kwargs,
                )
            }
            FunctionExpr::TimeDiff(time_diff_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, time_diff_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimeDiff)?,
                    kwargs,
                )
            }
            FunctionExpr::DateTrunc(date_trunc_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, date_trunc_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DateTrunc)?,
                    kwargs,
                )
            }
            FunctionExpr::DatetimeTrunc(datetime_trunc_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    datetime_trunc_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::DatetimeTrunc)?,
                    kwargs,
                )
            }
            FunctionExpr::TimestampTrunc(timestamp_trunc_function_expr) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    timestamp_trunc_function_expr
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimestampTrunc)?,
                    kwargs,
                )
            }
            FunctionExpr::TimeTrunc(time_trunc_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, time_trunc_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::TimeTrunc)?,
                    kwargs,
                )
            }
            FunctionExpr::LastDay(last_day_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, last_day_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionExpr::LastDay)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for LikeQuantifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            LikeQuantifier::Any => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LikeQuantifier::Any)?, &[])
            }
            LikeQuantifier::Some => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LikeQuantifier::Some)?, &[])
            }
            LikeQuantifier::All => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LikeQuantifier::All)?, &[])
            }
        }
    }
}

impl RsToPyObject for QuantifiedLikeExprPattern {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            QuantifiedLikeExprPattern::ExprList { exprs } => {
                let kwargs = &[kwarg!(py_ctx, "exprs", exprs)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, QuantifiedLikeExprPattern::ExprList)?,
                    kwargs,
                )
            }
            QuantifiedLikeExprPattern::ArrayUnnest { expr } => {
                let kwargs = &[kwarg!(py_ctx, "expr", expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, QuantifiedLikeExprPattern::ArrayUnnest)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for QuantifiedLikeExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "quantifier", self.quantifier),
            kwarg!(py_ctx, "pattern", self.pattern),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, QuantifiedLikeExpr)?, kwargs)
    }
}

impl RsToPyObject for StringConcatExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "strings", self.strings)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, StringConcatExpr)?, kwargs)
    }
}

impl RsToPyObject for BytesConcatExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "bytes", self.bytes)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, BytesConcatExpr)?, kwargs)
    }
}

impl RsToPyObject for Identifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "name", self.name)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Identifier)?, kwargs)
    }
}

impl RsToPyObject for QuotedIdentifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "name", self.name)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, QuotedIdentifier)?, kwargs)
    }
}

impl RsToPyObject for SystemVariable {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "name", self.name)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SystemVariable)?, kwargs)
    }
}

impl RsToPyObject for WithExprVar {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "value", self.value),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WithExprVar)?, kwargs)
    }
}

impl RsToPyObject for WithExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "vars", self.vars),
            kwarg!(py_ctx, "result", self.result),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WithExpr)?, kwargs)
    }
}

impl RsToPyObject for UnnestExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "array", self.array)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnnestExpr)?, kwargs)
    }
}

impl RsToPyObject for Expr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Expr::Binary(binary_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, binary_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Binary)?, kwargs)
            }
            Expr::Unary(unary_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, unary_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Unary)?, kwargs)
            }
            Expr::Grouping(grouping_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, grouping_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Grouping)?, kwargs)
            }
            Expr::Array(array_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, array_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Array)?, kwargs)
            }
            Expr::Struct(struct_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, struct_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Struct)?, kwargs)
            }
            Expr::Identifier(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Identifier)?, kwargs)
            }
            Expr::QuotedIdentifier(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QuotedIdentifier)?,
                    kwargs,
                )
            }
            Expr::QueryNamedParameter(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QueryNamedParameter)?,
                    kwargs,
                )
            }
            Expr::QueryPositionalParameter => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Expr::QueryPositionalParameter)?,
                &[],
            ),
            Expr::SystemVariable(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::SystemVariable)?,
                    kwargs,
                )
            }
            Expr::String(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::String)?, kwargs)
            }
            Expr::RawString(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::RawString)?, kwargs)
            }
            Expr::Bytes(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Bytes)?, kwargs)
            }
            Expr::RawBytes(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::RawBytes)?, kwargs)
            }
            Expr::StringConcat(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::StringConcat)?, kwargs)
            }
            Expr::BytesConcat(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::BytesConcat)?, kwargs)
            }
            Expr::Numeric(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Numeric)?, kwargs)
            }
            Expr::BigNumeric(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::BigNumeric)?, kwargs)
            }
            Expr::Number(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Number)?, kwargs)
            }
            Expr::Bool(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Bool)?, kwargs)
            }
            Expr::Date(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Date)?, kwargs)
            }
            Expr::Time(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Time)?, kwargs)
            }
            Expr::Datetime(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Datetime)?, kwargs)
            }
            Expr::Timestamp(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Timestamp)?, kwargs)
            }
            Expr::Range(range_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, range_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Range)?, kwargs)
            }
            Expr::Interval(interval_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, interval_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Interval)?, kwargs)
            }
            Expr::Json(value) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, value)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Json)?, kwargs)
            }
            Expr::Default => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Default)?, &[])
            }
            Expr::Null => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Null)?, &[]),
            Expr::Star => instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Star)?, &[]),
            Expr::Query(query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, query_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Query)?, kwargs)
            }
            Expr::Exists(query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, query_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Exists)?, kwargs)
            }
            Expr::Case(case_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, case_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Case)?, kwargs)
            }
            Expr::GenericFunction(generic_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, generic_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::GenericFunction)?,
                    kwargs,
                )
            }
            Expr::Function(function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, function_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Function)?, kwargs)
            }
            Expr::QuantifiedLike(quantified_like_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, quantified_like_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Expr::QuantifiedLike)?,
                    kwargs,
                )
            }
            Expr::With(with_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, with_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::With)?, kwargs)
            }
            Expr::Unnest(unnest_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, unnest_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Expr::Unnest)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for Limit {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "count", self.count),
            kwarg!(py_ctx, "offset", self.offset),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Limit)?, kwargs)
    }
}

impl RsToPyObject for NonRecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "query", self.query),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, NonRecursiveCte)?, kwargs)
    }
}

impl RsToPyObject for RecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "base_query", self.base_query),
            kwarg!(py_ctx, "recursive_query", self.recursive_query),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, RecursiveCte)?, kwargs)
    }
}

impl RsToPyObject for Cte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Cte::NonRecursive(non_recursive_cte) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, non_recursive_cte)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Cte::NonRecursive)?, kwargs)
            }
            Cte::Recursive(recursive_cte) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, recursive_cte)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Cte::Recursive)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for With {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "ctes", self.ctes)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, With)?, kwargs)
    }
}

impl RsToPyObject for OrderByNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderByNulls::First => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, OrderByNulls::First)?, &[])
            }
            OrderByNulls::Last => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, OrderByNulls::Last)?, &[])
            }
        }
    }
}

impl RsToPyObject for OrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "sort_direction", self.sort_direction),
            kwarg!(py_ctx, "nulls", self.nulls),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, OrderByExpr)?, kwargs)
    }
}

impl RsToPyObject for OrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "exprs", self.exprs)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, OrderBy)?, kwargs)
    }
}

impl RsToPyObject for GroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "with_", self.with),
            kwarg!(py_ctx, "query", self.query),
            kwarg!(py_ctx, "order_by", self.order_by),
            kwarg!(py_ctx, "limit", self.limit),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupingQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectTableValue {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectTableValue::Struct => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SelectTableValue::Struct)?,
                &[],
            ),
            SelectTableValue::Value => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SelectTableValue::Value)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for SelectColExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectColExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectColAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "except_", self.except),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectColAllExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "except_", self.except)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectAllExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectExpr::Col(select_col_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, select_col_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectExpr::Col)?, kwargs)
            }
            SelectExpr::ColAll(select_col_all_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, select_col_all_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectExpr::ColAll)?, kwargs)
            }
            SelectExpr::All(select_all_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, select_all_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectExpr::All)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for JoinKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinKind::Inner => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinKind::Inner)?, &[])
            }
            JoinKind::Left => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinKind::Left)?, &[])
            }
            JoinKind::Right => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinKind::Right)?, &[])
            }
            JoinKind::Full => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinKind::Full)?, &[])
            }
        }
    }
}

impl RsToPyObject for JoinCondition {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinCondition::On(expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinCondition::On)?, kwargs)
            }
            JoinCondition::Using { columns } => {
                let kwargs = &[kwarg!(py_ctx, "columns", columns)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, JoinCondition::Using)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for JoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "left", self.left),
            kwarg!(py_ctx, "right", self.right),
            kwarg!(py_ctx, "cond", self.cond),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, JoinExpr)?, kwargs)
    }
}

impl RsToPyObject for CrossJoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "left", self.left),
            kwarg!(py_ctx, "right", self.right),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CrossJoinExpr)?, kwargs)
    }
}

impl RsToPyObject for FromPathExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "path", self.path),
            kwarg!(py_ctx, "alias", self.alias),
            kwarg!(py_ctx, "system_time", self.system_time),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromPathExpr)?, kwargs)
    }
}

impl RsToPyObject for FromUnnestExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "array", self.array),
            kwarg!(py_ctx, "alias", self.alias),
            kwarg!(py_ctx, "with_offset", self.with_offset),
            kwarg!(py_ctx, "offset_alias", self.offset_alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromUnnestExpr)?, kwargs)
    }
}

impl RsToPyObject for FromGroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "query", self.query),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, FromGroupingQueryExpr)?,
            kwargs,
        )
    }
}

impl RsToPyObject for GroupingFromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "query", self.query)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupingFromExpr)?, kwargs)
    }
}

impl RsToPyObject for TableFunctionArgument {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TableFunctionArgument::Table(path_name) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, path_name)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TableFunctionArgument::Table)?,
                    kwargs,
                )
            }
            TableFunctionArgument::Expr(expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TableFunctionArgument::Expr)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for TableFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "arguments", self.arguments),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TableFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for FromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FromExpr::Join(join_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, join_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::Join)?, kwargs)
            }
            FromExpr::FullJoin(join_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, join_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::FullJoin)?, kwargs)
            }
            FromExpr::LeftJoin(join_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, join_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::LeftJoin)?, kwargs)
            }
            FromExpr::RightJoin(join_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, join_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::RightJoin)?, kwargs)
            }
            FromExpr::CrossJoin(cross_join_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, cross_join_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::CrossJoin)?, kwargs)
            }
            FromExpr::Path(from_path_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, from_path_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::Path)?, kwargs)
            }
            FromExpr::Unnest(unnest_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, unnest_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FromExpr::Unnest)?, kwargs)
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, from_grouping_query_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::GroupingQuery)?,
                    kwargs,
                )
            }
            FromExpr::GroupingFrom(grouping_from_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, grouping_from_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::GroupingFrom)?,
                    kwargs,
                )
            }
            FromExpr::TableFunction(table_function_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, table_function_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FromExpr::TableFunction)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for PivotColumn {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, PivotColumn)?, kwargs)
    }
}

impl RsToPyObject for PivotAggregate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, PivotAggregate)?, kwargs)
    }
}

impl RsToPyObject for Pivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "aggregates", self.aggregates),
            kwarg!(py_ctx, "input_column", self.input_column),
            kwarg!(py_ctx, "pivot_columns", self.pivot_columns),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Pivot)?, kwargs)
    }
}

impl RsToPyObject for UnpivotNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnpivotNulls::Include => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnpivotNulls::Include)?, &[])
            }
            UnpivotNulls::Exclude => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UnpivotNulls::Exclude)?, &[])
            }
        }
    }
}

impl RsToPyObject for ColumnToUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ColumnToUnpivot)?, kwargs)
    }
}

impl RsToPyObject for SingleColumnUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "values_column", self.values_column),
            kwarg!(py_ctx, "name_column", self.name_column),
            kwarg!(py_ctx, "columns_to_unpivot", self.columns_to_unpivot),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SingleColumnUnpivot)?, kwargs)
    }
}

impl RsToPyObject for ColumnSetToUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "names", self.names),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ColumnSetToUnpivot)?, kwargs)
    }
}

impl RsToPyObject for MultiColumnUnpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "values_columns", self.values_columns),
            kwarg!(py_ctx, "name_column", self.name_column),
            kwarg!(
                py_ctx,
                "column_sets_to_unpivot",
                self.column_sets_to_unpivot
            ),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, MultiColumnUnpivot)?, kwargs)
    }
}

impl RsToPyObject for UnpivotKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            UnpivotKind::SingleColumn(single_column_unpivot) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, single_column_unpivot)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, UnpivotKind::SingleColumn)?,
                    kwargs,
                )
            }
            UnpivotKind::MultiColumn(multi_column_unpivot) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, multi_column_unpivot)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, UnpivotKind::MultiColumn)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for Unpivot {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "nulls", self.nulls),
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Unpivot)?, kwargs)
    }
}

impl RsToPyObject for TableSample {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "percent", self.percent)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TableSample)?, kwargs)
    }
}

impl RsToPyObject for inbq::ast::From {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "expr", self.expr),
            kwarg!(py_ctx, "pivot", self.pivot),
            kwarg!(py_ctx, "unpivot", self.unpivot),
            kwarg!(py_ctx, "table_sample", self.table_sample),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, From)?, kwargs)
    }
}

impl RsToPyObject for Where {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "expr", self.expr)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Where)?, kwargs)
    }
}

impl RsToPyObject for GroupByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            GroupByExpr::Items { exprs } => {
                let kwargs = &[kwarg!(py_ctx, "exprs", exprs)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupByExpr::Items)?, kwargs)
            }
            GroupByExpr::All => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupByExpr::All)?, &[])
            }
        }
    }
}

impl RsToPyObject for GroupBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "expr", self.expr)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, GroupBy)?, kwargs)
    }
}

impl RsToPyObject for Having {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "expr", self.expr)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Having)?, kwargs)
    }
}

impl RsToPyObject for Qualify {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "expr", self.expr)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Qualify)?, kwargs)
    }
}

impl RsToPyObject for NamedWindow {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "window", self.window),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, NamedWindow)?, kwargs)
    }
}

impl RsToPyObject for Window {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "named_windows", self.named_windows)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Window)?, kwargs)
    }
}

impl RsToPyObject for Select {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "distinct", self.distinct),
            kwarg!(py_ctx, "table_value", self.table_value),
            kwarg!(py_ctx, "exprs", self.exprs),
            kwarg!(py_ctx, "from_", self.from),
            kwarg!(py_ctx, "where", self.r#where),
            kwarg!(py_ctx, "group_by", self.group_by),
            kwarg!(py_ctx, "having", self.having),
            kwarg!(py_ctx, "qualify", self.qualify),
            kwarg!(py_ctx, "window", self.window),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Select)?, kwargs)
    }
}

impl RsToPyObject for SelectQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "with_", self.with),
            kwarg!(py_ctx, "select", self.select),
            kwarg!(py_ctx, "order_by", self.order_by),
            kwarg!(py_ctx, "limit", self.limit),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SelectQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for SetQueryOperator {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SetQueryOperator::Union => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::Union)?,
                &[],
            ),
            SetQueryOperator::UnionDistinct => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::UnionDistinct)?,
                &[],
            ),
            SetQueryOperator::IntersectDistinct => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::IntersectDistinct)?,
                &[],
            ),
            SetQueryOperator::ExceptDistinct => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, SetQueryOperator::ExceptDistinct)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for SetSelectQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "with_", self.with),
            kwarg!(py_ctx, "left_query", self.left_query),
            kwarg!(py_ctx, "set_operator", self.set_operator),
            kwarg!(py_ctx, "right_query", self.right_query),
            kwarg!(py_ctx, "order_by", self.order_by),
            kwarg!(py_ctx, "limit", self.limit),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SetSelectQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for QueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            QueryExpr::Grouping(grouping_query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, grouping_query_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, QueryExpr::Grouping)?, kwargs)
            }
            QueryExpr::Select(select_query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, select_query_expr)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, QueryExpr::Select)?, kwargs)
            }
            QueryExpr::SetSelect(set_select_query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, set_select_query_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, QueryExpr::SetSelect)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for QueryStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "query", self.query)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, QueryStatement)?, kwargs)
    }
}

impl RsToPyObject for InsertStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "table", self.table),
            kwarg!(py_ctx, "columns", self.columns),
            kwarg!(py_ctx, "values", self.values),
            kwarg!(py_ctx, "query", self.query),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, InsertStatement)?, kwargs)
    }
}

impl RsToPyObject for DeleteStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "table", self.table),
            kwarg!(py_ctx, "alias", self.alias),
            kwarg!(py_ctx, "cond", self.cond),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, DeleteStatement)?, kwargs)
    }
}

impl RsToPyObject for UpdateItem {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "column", self.column),
            kwarg!(py_ctx, "expr", self.expr),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UpdateItem)?, kwargs)
    }
}

impl RsToPyObject for UpdateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "table", self.table),
            kwarg!(py_ctx, "alias", self.alias),
            kwarg!(py_ctx, "update_items", self.update_items),
            kwarg!(py_ctx, "from_", self.from),
            kwarg!(py_ctx, "where", self.r#where),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, UpdateStatement)?, kwargs)
    }
}

impl RsToPyObject for TruncateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "table", self.table)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, TruncateStatement)?, kwargs)
    }
}

impl RsToPyObject for MergeSource {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            MergeSource::Table(parse_token) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, parse_token)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, MergeSource::Table)?, kwargs)
            }
            MergeSource::Subquery(query_expr) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, query_expr)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, MergeSource::Subquery)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for MergeUpdate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "update_items", self.update_items)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, MergeUpdate)?, kwargs)
    }
}

impl RsToPyObject for MergeInsert {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "columns", self.columns),
            kwarg!(py_ctx, "values", self.values),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, MergeInsert)?, kwargs)
    }
}

impl RsToPyObject for Merge {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Merge::Update(merge_update) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, merge_update)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Merge::Update)?, kwargs)
            }
            Merge::Insert(merge_insert) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, merge_insert)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Merge::Insert)?, kwargs)
            }
            Merge::InsertRow => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Merge::InsertRow)?, &[])
            }
            Merge::Delete => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Merge::Delete)?, &[])
            }
        }
    }
}

impl RsToPyObject for WhenMatched {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "search_condition", self.search_condition),
            kwarg!(py_ctx, "merge", self.merge),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WhenMatched)?, kwargs)
    }
}

impl RsToPyObject for WhenNotMatchedByTarget {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "search_condition", self.search_condition),
            kwarg!(py_ctx, "merge", self.merge),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, WhenNotMatchedByTarget)?,
            kwargs,
        )
    }
}

impl RsToPyObject for WhenNotMatchedBySource {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "search_condition", self.search_condition),
            kwarg!(py_ctx, "merge", self.merge),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, WhenNotMatchedBySource)?,
            kwargs,
        )
    }
}

impl RsToPyObject for When {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            When::Matched(when_matched) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, when_matched)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, When::Matched)?, kwargs)
            }
            When::NotMatchedByTarget(when_not_matched_by_target) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    when_not_matched_by_target
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, When::NotMatchedByTarget)?,
                    kwargs,
                )
            }
            When::NotMatchedBySource(when_not_matched_by_source) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    when_not_matched_by_source
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, When::NotMatchedBySource)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for MergeStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "target_table", self.target_table),
            kwarg!(py_ctx, "target_alias", self.target_alias),
            kwarg!(py_ctx, "source", self.source),
            kwarg!(py_ctx, "source_alias", self.source_alias),
            kwarg!(py_ctx, "condition", self.condition),
            kwarg!(py_ctx, "whens", self.whens),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, MergeStatement)?, kwargs)
    }
}

impl RsToPyObject for DeclareVarStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "vars", self.vars),
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "default", self.default),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, DeclareVarStatement)?, kwargs)
    }
}

impl RsToPyObject for SetVariable {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SetVariable::UserVariable(name) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, name)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, SetVariable::UserVariable)?,
                    kwargs,
                )
            }
            SetVariable::SystemVariable(system_variable) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, system_variable)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, SetVariable::SystemVariable)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for SetVarStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "vars", self.vars),
            kwarg!(py_ctx, "exprs", self.exprs),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, SetVarStatement)?, kwargs)
    }
}

impl RsToPyObject for StatementsBlock {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "statements", self.statements),
            kwarg!(py_ctx, "exception_statements", self.exception_statements),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, StatementsBlock)?, kwargs)
    }
}

impl RsToPyObject for ColumnSchema {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "type_", self.r#type),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ColumnSchema)?, kwargs)
    }
}

impl RsToPyObject for PrimaryKeyConstraintNotEnforced {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "columns", self.columns)];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, PrimaryKeyConstraintNotEnforced)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ForeignKeyReference {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "table", self.table),
            kwarg!(py_ctx, "columns", self.columns),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ForeignKeyReference)?, kwargs)
    }
}

impl RsToPyObject for ForeignKeyConstraintNotEnforced {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "columns", self.columns),
            kwarg!(py_ctx, "reference", self.reference),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, ForeignKeyConstraintNotEnforced)?,
            kwargs,
        )
    }
}

impl RsToPyObject for TableConstraint {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TableConstraint::PrimaryKeyNotEnforced(primary_key_constraint_not_enforced) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    primary_key_constraint_not_enforced
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TableConstraint::PrimaryKeyNotEnforced)?,
                    kwargs,
                )
            }
            TableConstraint::ForeignKeyNotEnforced(foreign_key_constraint_not_enforced) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    foreign_key_constraint_not_enforced
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, TableConstraint::ForeignKeyNotEnforced)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for CreateTableStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "schema", self.schema),
            kwarg!(py_ctx, "constraints", self.constraints),
            kwarg!(py_ctx, "default_collate", self.default_collate),
            kwarg!(py_ctx, "partition", self.partition),
            kwarg!(py_ctx, "clustering_columns", self.clustering_columns),
            kwarg!(py_ctx, "connection", self.connection),
            kwarg!(py_ctx, "options", self.options),
            kwarg!(py_ctx, "replace", self.replace),
            kwarg!(py_ctx, "is_temporary", self.is_temporary),
            kwarg!(py_ctx, "if_not_exists", self.if_not_exists),
            kwarg!(py_ctx, "query", self.query),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CreateTableStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for DropTableStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "if_exists", self.if_exists),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, DropTableStatement)?, kwargs)
    }
}

impl RsToPyObject for IfBranch {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "condition", self.condition),
            kwarg!(py_ctx, "statements", self.statements),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IfBranch)?, kwargs)
    }
}

impl RsToPyObject for IfStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "if_", self.r#if),
            kwarg!(py_ctx, "else_ifs", self.else_ifs),
            kwarg!(py_ctx, "else_", self.r#else),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, IfStatement)?, kwargs)
    }
}

impl RsToPyObject for RaiseStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "message", self.message)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, RaiseStatement)?, kwargs)
    }
}

impl RsToPyObject for CallStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "procedure_name", self.procedure_name),
            kwarg!(py_ctx, "arguments", self.arguments),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CallStatement)?, kwargs)
    }
}

impl RsToPyObject for CaseWhenThenStatements {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "when", self.when),
            kwarg!(py_ctx, "then", self.then),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CaseWhenThenStatements)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CaseStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "case_", self.case),
            kwarg!(py_ctx, "when_thens", self.when_thens),
            kwarg!(py_ctx, "else_", self.r#else),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CaseStatement)?, kwargs)
    }
}

impl RsToPyObject for LoopStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "statements", self.statements)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LoopStatement)?, kwargs)
    }
}

impl RsToPyObject for RepeatStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "statements", self.statements),
            kwarg!(py_ctx, "until", self.until),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, RepeatStatement)?, kwargs)
    }
}

impl RsToPyObject for WhileStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "condition", self.condition),
            kwarg!(py_ctx, "statements", self.statements),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, WhileStatement)?, kwargs)
    }
}

impl RsToPyObject for ForInStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "var_name", self.var_name),
            kwarg!(py_ctx, "table_expr", self.table_expr),
            kwarg!(py_ctx, "statements", self.statements),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ForInStatement)?, kwargs)
    }
}

impl RsToPyObject for LabeledStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "statement", self.statement),
            kwarg!(py_ctx, "start_label", self.start_label),
            kwarg!(py_ctx, "end_label", self.end_label),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, LabeledStatement)?, kwargs)
    }
}

impl RsToPyObject for DdlOption {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "value", self.value),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, DdlOption)?, kwargs)
    }
}

impl RsToPyObject for ViewColumn {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "options", self.options),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, ViewColumn)?, kwargs)
    }
}

impl RsToPyObject for CreateViewStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "replace", self.replace),
            kwarg!(py_ctx, "if_not_exists", self.if_not_exists),
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "columns", self.columns),
            kwarg!(py_ctx, "options", self.options),
            kwarg!(py_ctx, "query", self.query),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, CreateViewStatement)?, kwargs)
    }
}

impl RsToPyObject for ExecuteImmediateUsingIdentifier {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "identifier", self.identifier),
            kwarg!(py_ctx, "alias", self.alias),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, ExecuteImmediateUsingIdentifier)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ExecuteImmediateStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "sql", self.sql),
            kwarg!(py_ctx, "into_vars", self.into_vars),
            kwarg!(py_ctx, "using_identifiers", self.using_identifiers),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, ExecuteImmediateStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CreateSchemaStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "if_not_exists", self.if_not_exists),
            kwarg!(py_ctx, "default_collate", self.default_collate),
            kwarg!(py_ctx, "options", self.options),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CreateSchemaStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for FunctionArgumentType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionArgumentType::Standard(standard_ty) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, standard_ty)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, FunctionArgumentType::Standard)?,
                    kwargs,
                )
            }
            FunctionArgumentType::AnyType => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, FunctionArgumentType::AnyType)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionArgument {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "type_", self.r#type),
        ];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, FunctionArgument)?, kwargs)
    }
}

impl RsToPyObject for CreateSqlFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "replace", self.replace),
            kwarg!(py_ctx, "is_temporary", self.is_temporary),
            kwarg!(py_ctx, "if_not_exists", self.if_not_exists),
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "arguments", self.arguments),
            kwarg!(py_ctx, "returns", self.returns),
            kwarg!(py_ctx, "options", self.options),
            kwarg!(py_ctx, "body", self.body),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CreateSqlFunctionStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for DropFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "if_exists", self.if_exists),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, DropFunctionStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for CreateJsFunctionStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "replace", self.replace),
            kwarg!(py_ctx, "is_temporary", self.is_temporary),
            kwarg!(py_ctx, "if_not_exists", self.if_not_exists),
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "arguments", self.arguments),
            kwarg!(py_ctx, "returns", self.returns),
            kwarg!(py_ctx, "is_deterministic", self.is_deterministic),
            kwarg!(py_ctx, "options", self.options),
            kwarg!(py_ctx, "body", self.body),
        ];
        instantiate_py_class(
            py_ctx,
            get_ast_class!(py_ctx, CreateJsFunctionStatement)?,
            kwargs,
        )
    }
}

impl RsToPyObject for Statement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Statement::Query(query_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, query_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Query)?, kwargs)
            }
            Statement::Insert(insert_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, insert_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Insert)?, kwargs)
            }
            Statement::Delete(delete_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, delete_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Delete)?, kwargs)
            }
            Statement::Update(update_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, update_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Update)?, kwargs)
            }
            Statement::Truncate(truncate_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, truncate_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Truncate)?, kwargs)
            }
            Statement::Merge(merge_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, merge_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Merge)?, kwargs)
            }
            Statement::DeclareVar(declare_var_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, declare_var_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DeclareVar)?,
                    kwargs,
                )
            }
            Statement::SetVar(set_var_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, set_var_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::SetVar)?, kwargs)
            }
            Statement::Block(statements_block) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, statements_block)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Block)?, kwargs)
            }
            Statement::CreateSchema(create_schema_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, create_schema_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateSchema)?,
                    kwargs,
                )
            }
            Statement::CreateTable(create_table_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, create_table_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateTable)?,
                    kwargs,
                )
            }
            Statement::CreateView(create_view_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, create_view_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateView)?,
                    kwargs,
                )
            }
            Statement::DropTableStatement(drop_table_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, drop_table_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropTableStatement)?,
                    kwargs,
                )
            }
            Statement::DropFunctionStatement(drop_function_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, drop_function_statement)];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::DropFunctionStatement)?,
                    kwargs,
                )
            }
            Statement::If(if_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, if_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::If)?, kwargs)
            }
            Statement::Case(case_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, case_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Case)?, kwargs)
            }
            Statement::Raise(raise_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, raise_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Raise)?, kwargs)
            }
            Statement::Call(call_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, call_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Call)?, kwargs)
            }
            Statement::BeginTransaction => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Statement::BeginTransaction)?,
                &[],
            ),
            Statement::CommitTransaction => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Statement::CommitTransaction)?,
                &[],
            ),
            Statement::RollbackTransaction => instantiate_py_class(
                py_ctx,
                get_ast_class!(py_ctx, Statement::RollbackTransaction)?,
                &[],
            ),
            Statement::Return => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Return)?, &[])
            }
            Statement::Loop(loop_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, loop_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Loop)?, kwargs)
            }
            Statement::Repeat(repeat_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, repeat_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Repeat)?, kwargs)
            }
            Statement::While(while_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, while_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::While)?, kwargs)
            }
            Statement::ForIn(for_in_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, for_in_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::ForIn)?, kwargs)
            }
            Statement::Break => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Break)?, &[])
            }
            Statement::Continue => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Continue)?, &[])
            }
            Statement::Iterate => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Iterate)?, &[])
            }
            Statement::Leave => {
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Leave)?, &[])
            }
            Statement::Labeled(labeled_statement) => {
                let kwargs = &[kwarg!(py_ctx, VARIANT_FIELD_NAME, labeled_statement)];
                instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Statement::Labeled)?, kwargs)
            }
            Statement::ExecuteImmediate(execute_immediate_statement) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    execute_immediate_statement
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::ExecuteImmediate)?,
                    kwargs,
                )
            }
            Statement::CreateSqlFunction(create_sql_function_statement) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    create_sql_function_statement
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateSqlFunction)?,
                    kwargs,
                )
            }
            Statement::CreateJsFunction(create_js_function_statement) => {
                let kwargs = &[kwarg!(
                    py_ctx,
                    VARIANT_FIELD_NAME,
                    create_js_function_statement
                )];
                instantiate_py_class(
                    py_ctx,
                    get_ast_class!(py_ctx, Statement::CreateJsFunction)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for Ast {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "statements", self.statements)];
        instantiate_py_class(py_ctx, get_ast_class!(py_ctx, Ast)?, kwargs)
    }
}

impl RsToPyObject for ReadyLineageNodeInput {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "obj_name", self.obj_name),
            kwarg!(py_ctx, "node_name", self.node_name),
        ];
        instantiate_py_class(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageNodeInput)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ReadyLineageNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "type_", self.r#type),
            kwarg!(py_ctx, "input", self.input),
        ];
        instantiate_py_class(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageNode)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ReadyLineageObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "nodes", self.nodes),
        ];
        instantiate_py_class(
            py_ctx,
            get_lineage_class!(py_ctx, ReadyLineageObject)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ReadyLineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "objects", self.objects)];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, ReadyLineage)?, kwargs)
    }
}

impl RsToPyObject for RawLineageObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "id", self.id),
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "nodes", self.nodes),
        ];
        instantiate_py_class(
            py_ctx,
            get_lineage_class!(py_ctx, RawLineageObject)?,
            kwargs,
        )
    }
}

impl RsToPyObject for RawLineageNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "id", self.id),
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "source_object", self.source_object),
            kwarg!(py_ctx, "input", self.input),
        ];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, RawLineageNode)?, kwargs)
    }
}

impl RsToPyObject for RawLineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "objects", self.objects),
            kwarg!(py_ctx, "lineage_nodes", self.lineage_nodes),
            kwarg!(py_ctx, "output_lineage", self.output_lineage),
        ];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, RawLineage)?, kwargs)
    }
}

impl RsToPyObject for UsedNode {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "used_in", self.used_in),
        ];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, UsedNode)?, kwargs)
    }
}

impl RsToPyObject for UsedObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "name", self.name),
            kwarg!(py_ctx, "kind", self.kind),
            kwarg!(py_ctx, "nodes", self.nodes),
        ];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, UsedObject)?, kwargs)
    }
}

impl RsToPyObject for UsedColumns {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[kwarg!(py_ctx, "objects", self.objects)];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, UsedColumns)?, kwargs)
    }
}

impl RsToPyObject for Lineage {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            kwarg!(py_ctx, "lineage", self.lineage),
            kwarg!(py_ctx, "raw_lineage", self.raw_lineage),
            kwarg!(py_ctx, "used_columns", self.used_columns),
        ];
        instantiate_py_class(py_ctx, get_lineage_class!(py_ctx, Lineage)?, kwargs)
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
        let py_kwargs = PyDict::new(py);
        for ast in &asts {
            match ast {
                Ok(ast) => py_list.push(ast.to_py_obj(&mut py_ctx).unwrap()),
                Err(err) => {
                    if raise_exception_on_error {
                        return Err(PyRuntimeError::new_err(err.to_string()));
                    }
                    py_kwargs.set_item(
                        intern!(py, "error"),
                        err.to_string().to_py_obj(&mut py_ctx).unwrap(),
                    )?;
                    py_list.push(
                        py_ctx
                            .inbq_module
                            .getattr(intern!(py, "PipelineError"))?
                            .call(PyTuple::empty(py_ctx.py), Some(&py_kwargs))?,
                    );
                }
            }
        }
        PyList::new(py_ctx.py, py_list)?
    };

    let py_lineages = if let Some(lineages) = lineages {
        let mut py_list = Vec::with_capacity(asts.len());
        let py_kwargs = PyDict::new(py);
        for lineage in &lineages {
            match lineage {
                Ok(lineage) => py_list.push(lineage.to_py_obj(&mut py_ctx).unwrap()),
                Err(err) => {
                    if raise_exception_on_error {
                        return Err(PyRuntimeError::new_err(err.to_string()));
                    }
                    py_kwargs.set_item(
                        intern!(py, "error"),
                        err.to_string().to_py_obj(&mut py_ctx).unwrap(),
                    )?;
                    py_list.push(
                        py_ctx
                            .inbq_module
                            .getattr(intern!(py, "PipelineError"))?
                            .call(PyTuple::empty(py_ctx.py), Some(&py_kwargs))?,
                    );
                }
            }
        }
        PyList::new(py_ctx.py, py_list)?.as_any().to_owned()
    } else {
        PyNone::get(py).into_bound().as_any().to_owned()
    };

    let py_kwargs = PyDict::new(py);
    py_kwargs.set_item(intern!(py, "asts"), py_asts)?;
    py_kwargs.set_item(intern!(py, "lineages"), py_lineages)?;

    let pipeline_output_cls = py_ctx.inbq_module.getattr(intern!(py, "PipelineOutput"))?;
    let pipeline_output = pipeline_output_cls
        .call(PyTuple::empty(py_ctx.py), Some(&py_kwargs))
        .unwrap();
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
