use anyhow::anyhow;
use pyo3::{
    BoundObject, IntoPyObjectExt,
    exceptions::{PyModuleNotFoundError, PyRuntimeError, PyValueError},
    intern,
    prelude::*,
    types::{PyBool, PyDict, PyInt, PyList, PyNone, PyString, PyTuple},
};

use inbq::ast::{
    ArrayAggFunctionExpr, ArrayExpr, ArrayFunctionExpr, Ast, BinaryExpr, CaseExpr,
    CastFunctionExpr, ConcatFunctionExpr, CrossJoinExpr, Cte, CurrentDateFunctionExpr, Expr,
    FrameBound, FromExpr, FromGroupingQueryExpr, FromPathExpr, FunctionAggregate,
    FunctionAggregateHaving, FunctionAggregateHavingKind, FunctionAggregateNulls,
    FunctionAggregateOrderBy, FunctionExpr, GenericFunctionExpr, GenericFunctionExprArg, GroupBy,
    GroupByExpr, GroupingExpr, GroupingFromExpr, GroupingQueryExpr, Having, IntervalExpr,
    IntervalPart, JoinCondition, JoinExpr, JoinKind, Limit, NamedWindow, NamedWindowExpr,
    NonRecursiveCte, OrderBy, OrderByExpr, OrderByNulls, OrderBySortDirection, ParameterizedType,
    ParseToken, PathExpr, Qualify, QueryExpr, QueryStatement, RangeExpr, RecursiveCte,
    SafeCastFunctionExpr, Select, SelectAllExpr, SelectColAllExpr, SelectColExpr, SelectExpr,
    SelectQueryExpr, SelectTableValue, Statement, StructExpr, StructField, StructFieldType,
    StructParameterizedFieldType, Token, TokenType, Type, UnaryExpr, UnnestExpr, WhenThen, Where,
    Window, WindowFrame, WindowFrameKind, WindowOrderByExpr, WindowSpec, With,
};

struct PyContext<'a> {
    py: Python<'a>,
    inbq_module: Bound<'a, PyModule>,
}

impl<'a> PyContext<'a> {
    fn get_class<N>(&self, cls_name: N) -> anyhow::Result<Bound<'a, PyAny>>
    where
        N: IntoPyObject<'a, Target = PyString>,
    {
        Ok(self.inbq_module.getattr(cls_name)?)
    }
}

macro_rules! get_class {
    ($py_ctx:expr, $struct:ident) => {
        $py_ctx.get_class(intern!($py_ctx.py, stringify!($struct)))
    };
    ($py_ctx:expr, $enum:ident::$variant:ident) => {
        $py_ctx.get_class(intern!(
            $py_ctx.py,
            concat!(stringify!($enum), "_", stringify!($variant))
        ))
    };
}

trait RsToPyObject {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>>;
}

fn instantiate_py_class<'py>(
    py_ctx: &PyContext<'py>,
    cls: Bound<'py, PyAny>,
    kwargs: &[(&str, Bound<'py, PyAny>)],
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
        Ok(PyList::new(
            py_ctx.py,
            self.iter().map(|el| el.to_py_obj(py_ctx).unwrap()),
        )?
        .as_any()
        .to_owned())
    }
}

impl RsToPyObject for TokenType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            TokenType::LeftParen => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::LeftParen)?, &[])
            }
            TokenType::RightParen => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::RightParen)?, &[])
            }
            TokenType::LeftSquare => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::LeftSquare)?, &[])
            }
            TokenType::RightSquare => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::RightSquare)?, &[])
            }
            TokenType::Comma => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Comma)?, &[])
            }
            TokenType::Dot => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Dot)?, &[])
            }
            TokenType::Minus => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Minus)?, &[])
            }
            TokenType::Plus => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Plus)?, &[])
            }
            TokenType::BitwiseNot => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::BitwiseNot)?, &[])
            }
            TokenType::BitwiseOr => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::BitwiseOr)?, &[])
            }
            TokenType::BitwiseAnd => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::BitwiseAnd)?, &[])
            }
            TokenType::BitwiseXor => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::BitwiseXor)?, &[])
            }
            TokenType::BitwiseRightShift => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, TokenType::BitwiseRightShift)?,
                &[],
            ),
            TokenType::BitwiseLeftShift => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, TokenType::BitwiseLeftShift)?,
                &[],
            ),
            TokenType::Colon => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Colon)?, &[])
            }
            TokenType::Semicolon => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Semicolon)?, &[])
            }
            TokenType::Slash => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Slash)?, &[])
            }
            TokenType::Star => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Star)?, &[])
            }
            TokenType::Tick => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Tick)?, &[])
            }
            TokenType::ConcatOperator => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::ConcatOperator)?, &[])
            }
            TokenType::Bang => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Bang)?, &[])
            }
            TokenType::BangEqual => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::BangEqual)?, &[])
            }
            TokenType::Equal => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Equal)?, &[])
            }
            TokenType::NotEqual => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::NotEqual)?, &[])
            }
            TokenType::Greater => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Greater)?, &[])
            }
            TokenType::GreaterEqual => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::GreaterEqual)?, &[])
            }
            TokenType::Less => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Less)?, &[])
            }
            TokenType::LessEqual => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::LessEqual)?, &[])
            }
            TokenType::QuotedIdentifier(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, TokenType::QuotedIdentifier)?,
                    kwargs,
                )
            }
            TokenType::Identifier(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Identifier)?, kwargs)
            }
            TokenType::String(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::String)?, kwargs)
            }
            TokenType::RawString(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::RawString)?, kwargs)
            }
            TokenType::Bytes(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Bytes)?, kwargs)
            }
            TokenType::RawBytes(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::RawBytes)?, kwargs)
            }
            TokenType::Number(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Number)?, kwargs)
            }
            TokenType::Eof => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Eof)?, &[])
            }
            TokenType::All => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::All)?, &[])
            }
            TokenType::And => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::And)?, &[])
            }
            TokenType::Any => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Any)?, &[])
            }
            TokenType::Array => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Array)?, &[])
            }
            TokenType::As => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::As)?, &[]),
            TokenType::Asc => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Asc)?, &[])
            }
            TokenType::AssertRowsModified => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, TokenType::AssertRowsModified)?,
                &[],
            ),
            TokenType::At => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::At)?, &[]),
            TokenType::Between => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Between)?, &[])
            }
            TokenType::By => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::By)?, &[]),
            TokenType::Case => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Case)?, &[])
            }
            TokenType::Cast => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Cast)?, &[])
            }
            TokenType::Collate => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Collate)?, &[])
            }
            TokenType::Contains => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Contains)?, &[])
            }
            TokenType::Create => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Create)?, &[])
            }
            TokenType::Cross => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Cross)?, &[])
            }
            TokenType::Cube => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Cube)?, &[])
            }
            TokenType::Current => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Current)?, &[])
            }
            TokenType::Default => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Default)?, &[])
            }
            TokenType::Define => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Define)?, &[])
            }
            TokenType::Desc => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Desc)?, &[])
            }
            TokenType::Distinct => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Distinct)?, &[])
            }
            TokenType::Else => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Else)?, &[])
            }
            TokenType::End => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::End)?, &[])
            }
            TokenType::Enum => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Enum)?, &[])
            }
            TokenType::Escape => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Escape)?, &[])
            }
            TokenType::Except => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Except)?, &[])
            }
            TokenType::Exclude => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Exclude)?, &[])
            }
            TokenType::Exists => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Exists)?, &[])
            }
            TokenType::Extract => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Extract)?, &[])
            }
            TokenType::False => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::False)?, &[])
            }
            TokenType::Fetch => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Fetch)?, &[])
            }
            TokenType::Following => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Following)?, &[])
            }
            TokenType::For => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::For)?, &[])
            }
            TokenType::From => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::From)?, &[])
            }
            TokenType::Full => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Full)?, &[])
            }
            TokenType::Group => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Group)?, &[])
            }
            TokenType::Grouping => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Grouping)?, &[])
            }
            TokenType::Groups => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Groups)?, &[])
            }
            TokenType::Hash => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Hash)?, &[])
            }
            TokenType::Having => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Having)?, &[])
            }
            TokenType::If => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::If)?, &[]),
            TokenType::Ignore => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Ignore)?, &[])
            }
            TokenType::In => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::In)?, &[]),
            TokenType::Inner => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Inner)?, &[])
            }
            TokenType::Intersect => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Intersect)?, &[])
            }
            TokenType::Interval => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Interval)?, &[])
            }
            TokenType::Into => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Into)?, &[])
            }
            TokenType::Is => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Is)?, &[]),
            TokenType::Join => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Join)?, &[])
            }
            TokenType::Lateral => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Lateral)?, &[])
            }
            TokenType::Left => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Left)?, &[])
            }
            TokenType::Like => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Like)?, &[])
            }
            TokenType::Limit => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Limit)?, &[])
            }
            TokenType::Lookup => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Lookup)?, &[])
            }
            TokenType::Merge => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Merge)?, &[])
            }
            TokenType::Natural => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Natural)?, &[])
            }
            TokenType::New => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::New)?, &[])
            }
            TokenType::No => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::No)?, &[]),
            TokenType::Not => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Not)?, &[])
            }
            TokenType::Null => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Null)?, &[])
            }
            TokenType::Nulls => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Nulls)?, &[])
            }
            TokenType::Of => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Of)?, &[]),
            TokenType::On => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::On)?, &[]),
            TokenType::Or => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Or)?, &[]),
            TokenType::Order => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Order)?, &[])
            }
            TokenType::Outer => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Outer)?, &[])
            }
            TokenType::Over => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Over)?, &[])
            }
            TokenType::Partition => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Partition)?, &[])
            }
            TokenType::Preceding => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Preceding)?, &[])
            }
            TokenType::Proto => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Proto)?, &[])
            }
            TokenType::Qualify => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Qualify)?, &[])
            }
            TokenType::Range => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Range)?, &[])
            }
            TokenType::Recursive => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Recursive)?, &[])
            }
            TokenType::Respect => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Respect)?, &[])
            }
            TokenType::Right => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Right)?, &[])
            }
            TokenType::Rollup => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Rollup)?, &[])
            }
            TokenType::Rows => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Rows)?, &[])
            }
            TokenType::Select => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Select)?, &[])
            }
            TokenType::Set => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Set)?, &[])
            }
            TokenType::Some => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Some)?, &[])
            }
            TokenType::Struct => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Struct)?, &[])
            }
            TokenType::Tablesample => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Tablesample)?, &[])
            }
            TokenType::Then => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Then)?, &[])
            }
            TokenType::To => instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::To)?, &[]),
            TokenType::Treat => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Treat)?, &[])
            }
            TokenType::True => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::True)?, &[])
            }
            TokenType::Union => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Union)?, &[])
            }
            TokenType::Unnest => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Unnest)?, &[])
            }
            TokenType::Using => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Using)?, &[])
            }
            TokenType::When => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::When)?, &[])
            }
            TokenType::Where => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Where)?, &[])
            }
            TokenType::Window => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Window)?, &[])
            }
            TokenType::With => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::With)?, &[])
            }
            TokenType::Within => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, TokenType::Within)?, &[])
            }
        }
    }
}

impl RsToPyObject for Token {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("kind", self.kind.to_py_obj(py_ctx)?),
            ("lexeme", self.kind.to_py_obj(py_ctx)?),
            ("line", self.kind.to_py_obj(py_ctx)?),
            ("col", self.kind.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Token)?, kwargs)
    }
}

impl RsToPyObject for ParseToken {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ParseToken::Single(token) => {
                let kwargs = &[("value", token.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParseToken::Single)?, kwargs)
            }
            ParseToken::Multiple(tokens) => {
                let kwargs = &[("value", tokens.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParseToken::Multiple)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for StructFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("type_", self.r#type.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, StructFieldType)?, kwargs)
    }
}

impl RsToPyObject for Type {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Type::Array { r#type } => {
                let kwargs = &[("type_", r#type.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Array)?, kwargs)
            }
            Type::BigNumeric => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::BigNumeric)?, &[])
            }
            Type::Bool => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Bool)?, &[]),
            Type::Bytes => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Bytes)?, &[]),
            Type::Date => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Date)?, &[]),
            Type::Datetime => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Datetime)?, &[])
            }
            Type::Float64 => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Float64)?, &[]),
            Type::Geography => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Geography)?, &[])
            }
            Type::Int64 => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Int64)?, &[]),
            Type::Interval => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Interval)?, &[])
            }
            Type::Json => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Json)?, &[]),
            Type::Numeric => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Numeric)?, &[]),
            Type::Range { r#type } => {
                let kwargs = &[("type_", r#type.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Range)?, kwargs)
            }
            Type::String => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::String)?, &[]),
            Type::Struct { fields } => {
                let kwargs = &[("fields", fields.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Struct)?, kwargs)
            }
            Type::Time => instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Time)?, &[]),
            Type::Timestamp => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, Type::Timestamp)?, &[])
            }
        }
    }
}

impl RsToPyObject for StructParameterizedFieldType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("type_", self.r#type.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(
            py_ctx,
            get_class!(py_ctx, StructParameterizedFieldType)?,
            kwargs,
        )
    }
}

impl RsToPyObject for ParameterizedType {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            ParameterizedType::Array { r#type } => {
                let kwargs = &[("type_", r#type.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::Array)?,
                    kwargs,
                )
            }
            ParameterizedType::BigNumeric { precision, scale } => {
                let kwargs = &[
                    ("precision", precision.to_py_obj(py_ctx)?),
                    ("scale", scale.to_py_obj(py_ctx)?),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::BigNumeric)?,
                    kwargs,
                )
            }
            ParameterizedType::Bool => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Bool)?, &[])
            }
            ParameterizedType::Bytes { max_length } => {
                let kwargs = &[("max_length", max_length.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::Bytes)?,
                    kwargs,
                )
            }
            ParameterizedType::Date => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Date)?, &[])
            }
            ParameterizedType::Datetime => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, ParameterizedType::Datetime)?,
                &[],
            ),
            ParameterizedType::Float64 => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Float64)?, &[])
            }
            ParameterizedType::Geography => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, ParameterizedType::Geography)?,
                &[],
            ),
            ParameterizedType::Int64 => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Int64)?, &[])
            }
            ParameterizedType::Interval => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, ParameterizedType::Interval)?,
                &[],
            ),
            ParameterizedType::Json => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Json)?, &[])
            }
            ParameterizedType::Numeric { precision, scale } => {
                let kwargs = &[
                    ("precision", precision.to_py_obj(py_ctx)?),
                    ("scale", scale.to_py_obj(py_ctx)?),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::Numeric)?,
                    kwargs,
                )
            }
            ParameterizedType::Range { r#type } => {
                let kwargs = &[("type_", r#type.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::Range)?,
                    kwargs,
                )
            }
            ParameterizedType::String { max_length } => {
                let kwargs = &[("max_length", max_length.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::String)?,
                    kwargs,
                )
            }
            ParameterizedType::Struct { fields } => {
                let kwargs = &[("fields", fields.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, ParameterizedType::Struct)?,
                    kwargs,
                )
            }
            ParameterizedType::Time => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, ParameterizedType::Time)?, &[])
            }
            ParameterizedType::Timestamp => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, ParameterizedType::Timestamp)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for BinaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("left", self.left.to_py_obj(py_ctx)?),
            ("operator", self.operator.to_py_obj(py_ctx)?),
            ("right", self.right.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, BinaryExpr)?, kwargs)
    }
}

impl RsToPyObject for UnaryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("operator", self.operator.to_py_obj(py_ctx)?),
            ("right", self.right.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, UnaryExpr)?, kwargs)
    }
}

impl RsToPyObject for GroupingExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GroupingExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("type_", self.r#type.to_py_obj(py_ctx)?),
            ("exprs", self.exprs.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, ArrayExpr)?, kwargs)
    }
}

impl RsToPyObject for StructField {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("alias", self.alias.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, StructField)?, kwargs)
    }
}

impl RsToPyObject for StructExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("type_", self.r#type.to_py_obj(py_ctx)?),
            ("fields", self.fields.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, StructExpr)?, kwargs)
    }
}

impl RsToPyObject for RangeExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("type_", self.r#type.to_py_obj(py_ctx)?),
            ("value", self.value.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, RangeExpr)?, kwargs)
    }
}

impl RsToPyObject for IntervalPart {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            IntervalPart::Year => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Year)?, &[])
            }
            IntervalPart::Quarter => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Quarter)?, &[])
            }
            IntervalPart::Month => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Month)?, &[])
            }
            IntervalPart::Week => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Week)?, &[])
            }
            IntervalPart::Day => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Day)?, &[])
            }
            IntervalPart::Hour => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Hour)?, &[])
            }
            IntervalPart::Minute => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Minute)?, &[])
            }
            IntervalPart::Second => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Second)?, &[])
            }
            IntervalPart::Millisecond => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Millisecond)?, &[])
            }
            IntervalPart::Microsecond => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalPart::Microsecond)?, &[])
            }
        }
    }
}

impl RsToPyObject for IntervalExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            IntervalExpr::Interval { value, part } => {
                let kwargs = &[
                    ("value", value.to_py_obj(py_ctx)?),
                    ("part", part.to_py_obj(py_ctx)?),
                ];
                instantiate_py_class(py_ctx, get_class!(py_ctx, IntervalExpr::Interval)?, kwargs)
            }
            IntervalExpr::IntervalRange {
                value,
                start_part,
                end_part,
            } => {
                let kwargs = &[
                    ("value", value.to_py_obj(py_ctx)?),
                    ("start_part", start_part.to_py_obj(py_ctx)?),
                    ("end_part", end_part.to_py_obj(py_ctx)?),
                ];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, IntervalExpr::IntervalRange)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for WhenThen {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("when", self.when.to_py_obj(py_ctx)?),
            ("then", self.then.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, WhenThen)?, kwargs)
    }
}

impl RsToPyObject for CaseExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("case_", self.case.to_py_obj(py_ctx)?),
            ("when_thens", self.when_thens.to_py_obj(py_ctx)?),
            ("else_", self.r#else.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, CaseExpr)?, kwargs)
    }
}

impl RsToPyObject for FunctionAggregateNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionAggregateNulls::Ignore => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FunctionAggregateNulls::Ignore)?,
                &[],
            ),
            FunctionAggregateNulls::Respect => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FunctionAggregateNulls::Respect)?,
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
                get_class!(py_ctx, FunctionAggregateHavingKind::Max)?,
                &[],
            ),
            FunctionAggregateHavingKind::Min => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FunctionAggregateHavingKind::Min)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for FunctionAggregateHaving {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("kind", self.kind.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionAggregateHaving)?, kwargs)
    }
}

impl RsToPyObject for OrderBySortDirection {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderBySortDirection::Asc => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, OrderBySortDirection::Asc)?, &[])
            }
            OrderBySortDirection::Desc => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, OrderBySortDirection::Desc)?, &[])
            }
        }
    }
}

impl RsToPyObject for FunctionAggregateOrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("sort_direction", self.sort_direction.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(
            py_ctx,
            get_class!(py_ctx, FunctionAggregateOrderBy)?,
            kwargs,
        )
    }
}

impl RsToPyObject for FunctionAggregate {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("distinct", self.distinct.to_py_obj(py_ctx)?),
            ("nulls", self.nulls.to_py_obj(py_ctx)?),
            ("having", self.having.to_py_obj(py_ctx)?),
            ("order_by", self.order_by.to_py_obj(py_ctx)?),
            ("limit", self.limit.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionAggregate)?, kwargs)
    }
}

impl RsToPyObject for GenericFunctionExprArg {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("aggregate", self.aggregate.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GenericFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for WindowOrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("asc_desc", self.asc_desc.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, WindowOrderByExpr)?, kwargs)
    }
}

impl RsToPyObject for WindowFrameKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            WindowFrameKind::Range => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, WindowFrameKind::Range)?, &[])
            }
            WindowFrameKind::Rows => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, WindowFrameKind::Rows)?, &[])
            }
        }
    }
}

impl RsToPyObject for FrameBound {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FrameBound::UnboundedPreceding => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FrameBound::UnboundedPreceding)?,
                &[],
            ),
            FrameBound::Preceding(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FrameBound::Preceding)?, kwargs)
            }
            FrameBound::UnboundedFollowing => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FrameBound::UnboundedFollowing)?,
                &[],
            ),
            FrameBound::Following(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FrameBound::Following)?, kwargs)
            }
            FrameBound::CurrentRow => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, FrameBound::CurrentRow)?, &[])
            }
        }
    }
}

impl RsToPyObject for WindowFrame {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("kind", self.kind.to_py_obj(py_ctx)?),
            ("start", self.start.to_py_obj(py_ctx)?),
            ("end", self.end.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, WindowFrame)?, kwargs)
    }
}

impl RsToPyObject for WindowSpec {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("window_name", self.window_name.to_py_obj(py_ctx)?),
            ("partition_by", self.partition_by.to_py_obj(py_ctx)?),
            ("order_by", self.order_by.to_py_obj(py_ctx)?),
            ("frame", self.frame.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, WindowSpec)?, kwargs)
    }
}

impl RsToPyObject for NamedWindowExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            NamedWindowExpr::Reference(parse_token) => {
                let kwargs = &[("value", parse_token.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, NamedWindowExpr::Reference)?,
                    kwargs,
                )
            }
            NamedWindowExpr::WindowSpec(window_spec) => {
                let kwargs = &[("value", window_spec.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, NamedWindowExpr::WindowSpec)?,
                    kwargs,
                )
            }
        }
    }
}

impl RsToPyObject for GenericFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("arguments", self.arguments.to_py_obj(py_ctx)?),
            ("over", self.over.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GenericFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("query", self.query.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, ArrayFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for ArrayAggFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("arg", self.arg.to_py_obj(py_ctx)?),
            ("over", self.over.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, ArrayAggFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for ConcatFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("values", self.values.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, ConcatFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for CastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("type_", self.r#type.to_py_obj(py_ctx)?),
            ("format", self.format.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, CastFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for SafeCastFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("type_", self.r#type.to_py_obj(py_ctx)?),
            ("format", self.format.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, SafeCastFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for CurrentDateFunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("timezone", self.timezone.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, CurrentDateFunctionExpr)?, kwargs)
    }
}

impl RsToPyObject for FunctionExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FunctionExpr::Array(array_function_expr) => {
                let kwargs = &[("value", array_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionExpr::Array)?, kwargs)
            }
            FunctionExpr::ArrayAgg(array_agg_function_expr) => {
                let kwargs = &[("value", array_agg_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionExpr::ArrayAgg)?, kwargs)
            }
            FunctionExpr::Concat(concat_function_expr) => {
                let kwargs = &[("value", concat_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionExpr::Concat)?, kwargs)
            }
            FunctionExpr::Cast(cast_function_expr) => {
                let kwargs = &[("value", cast_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionExpr::Cast)?, kwargs)
            }
            FunctionExpr::SafeCast(safe_cast_function_expr) => {
                let kwargs = &[("value", safe_cast_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FunctionExpr::SafeCast)?, kwargs)
            }
            FunctionExpr::CurrentDate(current_date_function_expr) => {
                let kwargs = &[("value", current_date_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(
                    py_ctx,
                    get_class!(py_ctx, FunctionExpr::CurrentDate)?,
                    kwargs,
                )
            }
            FunctionExpr::CurrentTimestamp => instantiate_py_class(
                py_ctx,
                get_class!(py_ctx, FunctionExpr::CurrentTimestamp)?,
                &[],
            ),
        }
    }
}

impl RsToPyObject for Expr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Expr::Binary(binary_expr) => {
                let kwargs = &[("value", binary_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Binary)?, kwargs)
            }
            Expr::Unary(unary_expr) => {
                let kwargs = &[(")value", unary_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Unary)?, kwargs)
            }
            Expr::Grouping(grouping_expr) => {
                let kwargs = &[("value", grouping_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Grouping)?, kwargs)
            }
            Expr::Array(array_expr) => {
                let kwargs = &[("value", array_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Array)?, kwargs)
            }
            Expr::Struct(struct_expr) => {
                let kwargs = &[("value", struct_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Struct)?, kwargs)
            }
            Expr::Identifier(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Identifier)?, kwargs)
            }
            Expr::QuotedIdentifier(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::QuotedIdentifier)?, kwargs)
            }
            Expr::String(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::String)?, kwargs)
            }
            Expr::Bytes(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Bytes)?, kwargs)
            }
            Expr::Numeric(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Numeric)?, kwargs)
            }
            Expr::BigNumeric(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::BigNumeric)?, kwargs)
            }
            Expr::Number(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Number)?, kwargs)
            }
            Expr::Bool(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Bool)?, kwargs)
            }
            Expr::Date(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Date)?, kwargs)
            }
            Expr::Time(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Time)?, kwargs)
            }
            Expr::Datetime(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Datetime)?, kwargs)
            }
            Expr::Timestamp(value) => {
                let kwargs = &[("value", value.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Timestamp)?, kwargs)
            }
            Expr::Range(range_expr) => {
                let kwargs = &[("value", range_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Range)?, kwargs)
            }
            Expr::Interval(interval_expr) => {
                let kwargs = &[("value", interval_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Interval)?, kwargs)
            }
            Expr::Json(_) => todo!(),
            Expr::Default => todo!(),
            Expr::Null => todo!(),
            Expr::Star => todo!(),
            Expr::Query(query_expr) => {
                let kwargs = &[("value", query_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Query)?, kwargs)
            }
            Expr::Case(case_expr) => {
                let kwargs = &[("value", case_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Case)?, kwargs)
            }
            Expr::GenericFunction(generic_function_expr) => {
                let kwargs = &[("value", generic_function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::GenericFunction)?, kwargs)
            }
            Expr::Function(function_expr) => {
                let kwargs = &[("value", function_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Expr::Function)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for Limit {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("count", self.count.to_py_obj(py_ctx)?),
            ("offset", self.offset.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Limit)?, kwargs)
    }
}

impl RsToPyObject for NonRecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("query", self.query.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, NonRecursiveCte)?, kwargs)
    }
}

impl RsToPyObject for RecursiveCte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("base_query", self.base_query.to_py_obj(py_ctx)?),
            ("recursive_query", self.recursive_query.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, RecursiveCte)?, kwargs)
    }
}

impl RsToPyObject for Cte {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Cte::NonRecursive(non_recursive_cte) => {
                let kwargs = &[("value", non_recursive_cte.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Cte::NonRecursive)?, kwargs)
            }
            Cte::Recursive(recursive_cte) => {
                let kwargs = &[("value", recursive_cte.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Cte::Recursive)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for With {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("ctes", self.ctes.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, With)?, kwargs)
    }
}

impl RsToPyObject for OrderByNulls {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            OrderByNulls::First => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, OrderByNulls::First)?, &[])
            }
            OrderByNulls::Last => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, OrderByNulls::Last)?, &[])
            }
        }
    }
}

impl RsToPyObject for OrderByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("sort_direction", self.sort_direction.to_py_obj(py_ctx)?),
            ("nulls", self.nulls.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, OrderByExpr)?, kwargs)
    }
}

impl RsToPyObject for OrderBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("exprs", self.exprs.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, OrderBy)?, kwargs)
    }
}

impl RsToPyObject for GroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("with_", self.with.to_py_obj(py_ctx)?),
            ("query", self.query.to_py_obj(py_ctx)?),
            ("order_by", self.order_by.to_py_obj(py_ctx)?),
            ("limit", self.limit.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GroupingQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectTableValue {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectTableValue::Struct => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, SelectTableValue::Struct)?, &[])
            }
            SelectTableValue::Value => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, SelectTableValue::Value)?, &[])
            }
        }
    }
}

impl RsToPyObject for SelectColExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("alias", self.alias.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, SelectColExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectColAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("expr", self.expr.to_py_obj(py_ctx)?),
            ("except_", self.except.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, SelectColAllExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectAllExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("except_", self.except.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, SelectAllExpr)?, kwargs)
    }
}

impl RsToPyObject for SelectExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            SelectExpr::Col(select_col_expr) => {
                let kwargs = &[("value", select_col_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, SelectExpr::Col)?, kwargs)
            }
            SelectExpr::ColAll(select_col_all_expr) => {
                let kwargs = &[("value", select_col_all_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, SelectExpr::ColAll)?, kwargs)
            }
            SelectExpr::All(select_all_expr) => {
                let kwargs = &[("value", select_all_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, SelectExpr::All)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for JoinKind {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinKind::Inner => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinKind::Inner)?, &[])
            }
            JoinKind::Left => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinKind::Left)?, &[])
            }
            JoinKind::Right => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinKind::Right)?, &[])
            }
            JoinKind::Full => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinKind::Full)?, &[])
            }
        }
    }
}

impl RsToPyObject for JoinCondition {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            JoinCondition::On(expr) => {
                let kwargs = &[("value", expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinCondition::On)?, kwargs)
            }
            JoinCondition::Using(parse_tokens) => {
                let kwargs = &[("value", parse_tokens.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, JoinCondition::Using)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for JoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("kind", self.kind.to_py_obj(py_ctx)?),
            ("left", self.left.to_py_obj(py_ctx)?),
            ("right", self.right.to_py_obj(py_ctx)?),
            ("cond", self.cond.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, JoinExpr)?, kwargs)
    }
}

impl RsToPyObject for CrossJoinExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("left", self.left.to_py_obj(py_ctx)?),
            ("right", self.right.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, CrossJoinExpr)?, kwargs)
    }
}

impl RsToPyObject for PathExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, PathExpr)?, kwargs)
    }
}

impl RsToPyObject for FromPathExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("path", self.path.to_py_obj(py_ctx)?),
            ("alias", self.alias.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, FromPathExpr)?, kwargs)
    }
}

impl RsToPyObject for UnnestExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("array", self.array.to_py_obj(py_ctx)?),
            ("alias", self.alias.to_py_obj(py_ctx)?),
            ("with_offset", self.with_offset.to_py_obj(py_ctx)?),
            ("offset_alias", self.offset_alias.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, UnnestExpr)?, kwargs)
    }
}

impl RsToPyObject for FromGroupingQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("query", self.query.to_py_obj(py_ctx)?),
            ("alias", self.alias.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, FromGroupingQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for GroupingFromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("query", self.query.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GroupingFromExpr)?, kwargs)
    }
}

impl RsToPyObject for FromExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            FromExpr::Join(join_expr) => {
                let kwargs = &[("value", join_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::Join)?, kwargs)
            }
            FromExpr::FullJoin(join_expr) => {
                let kwargs = &[("value", join_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::FullJoin)?, kwargs)
            }
            FromExpr::LeftJoin(join_expr) => {
                let kwargs = &[("value", join_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::LeftJoin)?, kwargs)
            }
            FromExpr::RightJoin(join_expr) => {
                let kwargs = &[("value", join_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::RightJoin)?, kwargs)
            }
            FromExpr::CrossJoin(cross_join_expr) => {
                let kwargs = &[("value", cross_join_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::CrossJoin)?, kwargs)
            }
            FromExpr::Path(from_path_expr) => {
                let kwargs = &[("value", from_path_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::Path)?, kwargs)
            }
            FromExpr::Unnest(unnest_expr) => {
                let kwargs = &[("value", unnest_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::Unnest)?, kwargs)
            }
            FromExpr::GroupingQuery(from_grouping_query_expr) => {
                let kwargs = &[("value", from_grouping_query_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::GroupingQuery)?, kwargs)
            }
            FromExpr::GroupingFrom(grouping_from_expr) => {
                let kwargs = &[("value", grouping_from_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, FromExpr::GroupingFrom)?, kwargs)
            }
        }
    }
}

impl RsToPyObject for inbq::ast::From {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, From)?, kwargs)
    }
}

impl RsToPyObject for Where {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Where)?, kwargs)
    }
}

impl RsToPyObject for GroupByExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            GroupByExpr::Items(exprs) => {
                let kwargs = &[("value", exprs.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, GroupByExpr::Items)?, kwargs)
            }
            GroupByExpr::All => {
                instantiate_py_class(py_ctx, get_class!(py_ctx, GroupByExpr::All)?, &[])
            }
        }
    }
}

impl RsToPyObject for GroupBy {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, GroupBy)?, kwargs)
    }
}

impl RsToPyObject for Having {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Having)?, kwargs)
    }
}

impl RsToPyObject for Qualify {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("expr", self.expr.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Qualify)?, kwargs)
    }
}

impl RsToPyObject for NamedWindow {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("name", self.name.to_py_obj(py_ctx)?),
            ("window", self.window.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, NamedWindow)?, kwargs)
    }
}

impl RsToPyObject for Window {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("named_windows", self.named_windows.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Window)?, kwargs)
    }
}

impl RsToPyObject for Select {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("distinct", self.distinct.to_py_obj(py_ctx)?),
            ("table_value", self.table_value.to_py_obj(py_ctx)?),
            ("exprs", self.exprs.to_py_obj(py_ctx)?),
            ("from_", self.from.to_py_obj(py_ctx)?),
            ("where", self.r#where.to_py_obj(py_ctx)?),
            ("group_by", self.group_by.to_py_obj(py_ctx)?),
            ("having", self.having.to_py_obj(py_ctx)?),
            ("qualify", self.qualify.to_py_obj(py_ctx)?),
            ("window", self.window.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Select)?, kwargs)
    }
}

impl RsToPyObject for SelectQueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[
            ("with_", self.with.to_py_obj(py_ctx)?),
            ("select", self.select.to_py_obj(py_ctx)?),
            ("order_by", self.select.to_py_obj(py_ctx)?),
            ("limit", self.limit.to_py_obj(py_ctx)?),
        ];
        instantiate_py_class(py_ctx, get_class!(py_ctx, SelectQueryExpr)?, kwargs)
    }
}

impl RsToPyObject for QueryExpr {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            QueryExpr::Grouping(grouping_query_expr) => {
                let kwargs = &[("value", grouping_query_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, QueryExpr::Grouping)?, kwargs)
            }
            QueryExpr::Select(select_query_expr) => {
                let kwargs = &[("value", select_query_expr.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, QueryExpr::Select)?, kwargs)
            }
            QueryExpr::SetSelect(set_select_query_expr) => todo!(),
        }
    }
}

impl RsToPyObject for QueryStatement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("query", self.query.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, QueryStatement)?, kwargs)
    }
}

impl RsToPyObject for Statement {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        match self {
            Statement::Query(query_statement) => {
                let kwargs = &[("value", query_statement.to_py_obj(py_ctx)?)];
                instantiate_py_class(py_ctx, get_class!(py_ctx, Statement::Query)?, kwargs)
            }
            _ => todo!(),
        }
    }
}

impl RsToPyObject for Ast {
    fn to_py_obj<'py>(&self, py_ctx: &mut PyContext<'py>) -> anyhow::Result<Bound<'py, PyAny>> {
        let kwargs = &[("statements", self.statements.to_py_obj(py_ctx)?)];
        instantiate_py_class(py_ctx, get_class!(py_ctx, Ast)?, kwargs)
    }
}

#[pyfunction]
fn rs_parse_sql_fast(py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
    let inbq_module = py
        .import(intern!(py, "inbq"))
        .map_err(|e| PyModuleNotFoundError::new_err(e.to_string()))?;
    let mut py_ctx = PyContext { py, inbq_module };
    let rs_ast = inbq::parser::parse_sql(sql)
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .to_py_obj(&mut py_ctx)
        .unwrap();
    Ok(rs_ast.into())
}

#[pyfunction]
fn rs_parse_sql(sql: &str) -> PyResult<String> {
    let rs_ast = inbq::parser::parse_sql(sql).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let json_ast =
        serde_json::to_string(&rs_ast).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(json_ast)
}

#[pymodule]
fn _inbq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rs_parse_sql, m)?)?;
    m.add_function(wrap_pyfunction!(rs_parse_sql_fast, m)?)?;
    Ok(())
}
