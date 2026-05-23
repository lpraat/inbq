use inbq::{
    ast::{
        ArrayExpr, Ast, BinaryExpr, BinaryOperator, ChainedFunctionExpr,
        ChainedGenericFunctionExpr, CoalesceFunctionExpr, ConcatFunctionExpr, Expr, FunctionExpr,
        GenericFunctionExpr, GenericFunctionExprArg, GroupingExpr, Identifier, Number, PathName,
        PathPart, QueryExpr, QueryStatement, Select, SelectColExpr, SelectExpr, SelectQueryExpr,
        Statement,
    },
    parser::parse_sql,
    test_utils::TestParsingData,
};

const PARSING_TESTS_FILE: &str = "tests/parsing_tests.toml";

fn test_sql(sql: &str) {
    let ast = parse_sql(sql);
    if let Err(err) = &ast {
        println!("{}", err)
    }
    assert!(ast.is_ok());
}

#[test]
fn test_should_parse() {
    let parsing_test_file =
        std::fs::read_to_string(PARSING_TESTS_FILE).expect("Cannot open parsing test cases");
    let test_parsing_data: TestParsingData =
        toml::from_str(&parsing_test_file).expect("Cannot parse test cases defined in toml");

    for test in test_parsing_data.tests {
        let sql = &test.sql;
        println!("Testing parsing for SQL: {}", sql);
        test_sql(sql);
        test_sql(&sql.to_uppercase());
        test_sql(&sql.to_lowercase());
    }
}

#[test]
fn test_should_not_parse() {
    let sqls = [
        // Cannot use array as quoted identifier
        r#"
      select
        array<struct<int64, `array`<int64>>>[struct(1, [1,2,3])],
      "#,
        // Cannot use range as quoted identifier
        r#"
        select ARRAY<`RANGE`<DATE>>[RANGE<DATE> '[UNBOUNDED, UNBOUNDED)']
      "#,
        // Cannot concatenate string and bytes literals
        r#"
        select "foo" b"foo"
      "#,
        // Cannot group again join op
        r#"
      SELECT * FROM
        (((select * from Produce) inner join (select * from pro) on true))
      "#,
        r#"
      select x
          from ((
              (select 1 as x)
              join
              (select 1 as x)
              using(x)
          )) k
      "#,
        // Cannot pivot grouped from expr
        r#"
      SELECT * FROM
        ((produce inner join pro on true))
        PIVOT(SUM(sales) total_sales, max(product) mp FOR quarter IN ("Q1", "Q2"))
      "#,
    ];
    for sql in sqls {
        println!("Testing parsing error for SQL: {}", sql);
        assert!(parse_sql(sql).is_err())
    }
}

#[test]
fn test_generated_ast() {
    macro_rules! assert_ast_eq {
        ($sql:expr, $left:expr, $right:expr) => {
            let left_val = serde_json::to_value(&$left).unwrap();
            let right_val = serde_json::to_value(&$right).unwrap();
            if left_val != right_val {
                panic!(
                    "ASTs are not equal. SQL:\n`{}`\nLeft:  {}\nRight: {}",
                    &$sql,
                    serde_json::to_string_pretty(&left_val).unwrap(),
                    serde_json::to_string_pretty(&right_val).unwrap()
                );
            }
        };
    }

    let test_cases = vec![
        // dat.f1(x, y) (UDF)
        (
            "select dat.f1(x, y)",
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::Identifier(Identifier {
                                        name: "dat".to_string(),
                                    })),
                                    operator: BinaryOperator::FunctionAccess,
                                    right: Box::new(Expr::GenericFunction(Box::new(
                                        GenericFunctionExpr {
                                            name: PathName {
                                                name: "f1".to_string(),
                                                parts: vec![PathPart::Identifier(Identifier {
                                                    name: "f1".to_string(),
                                                })],
                                            },
                                            arguments: vec![
                                                GenericFunctionExprArg {
                                                    name: None,
                                                    expr: Expr::Identifier(Identifier {
                                                        name: "x".to_string(),
                                                    }),
                                                    aggregate: None,
                                                },
                                                GenericFunctionExprArg {
                                                    name: None,
                                                    expr: Expr::Identifier(Identifier {
                                                        name: "y".to_string(),
                                                    }),
                                                    aggregate: None,
                                                },
                                            ],
                                            over: None,
                                        },
                                    ))),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
        // (dat).f1(x, y) (Chained call via parenthesized receiver)
        (
            "select (dat).f1(x, y)",
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::Grouping(GroupingExpr {
                                        expr: Box::new(Expr::Identifier(Identifier {
                                            name: "dat".to_string(),
                                        })),
                                    })),
                                    operator: BinaryOperator::FunctionAccess,
                                    right: Box::new(Expr::ChainedGenericFunction(Box::new(
                                        ChainedGenericFunctionExpr {
                                            function: GenericFunctionExpr {
                                                name: PathName {
                                                    name: "f1".to_string(),
                                                    parts: vec![PathPart::Identifier(Identifier {
                                                        name: "f1".to_string(),
                                                    })],
                                                },
                                                arguments: vec![
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Grouping(GroupingExpr {
                                                            expr: Box::new(Expr::Identifier(
                                                                Identifier {
                                                                    name: "dat".to_string(),
                                                                },
                                                            )),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "x".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "y".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                ],
                                                over: None,
                                            },
                                        },
                                    ))),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
        // dat.(f1)(x, y) (Chained call via parenthesized function name)
        (
            "select dat.(f1)(x, y)",
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::Identifier(Identifier {
                                        name: "dat".to_string(),
                                    })),
                                    operator: BinaryOperator::FunctionAccess,
                                    right: Box::new(Expr::ChainedGenericFunction(Box::new(
                                        ChainedGenericFunctionExpr {
                                            function: GenericFunctionExpr {
                                                name: PathName {
                                                    name: "f1".to_string(),
                                                    parts: vec![PathPart::Identifier(Identifier {
                                                        name: "f1".to_string(),
                                                    })],
                                                },
                                                arguments: vec![
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "dat".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "x".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "y".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                ],
                                                over: None,
                                            },
                                        },
                                    ))),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
        // "ciao".concat("suffix") (Chained specific function on constant)
        (
            r#"select "ciao".concat("suffix")"#,
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::String("ciao".to_string())),
                                    operator: BinaryOperator::FunctionAccess,
                                    right: Box::new(Expr::ChainedFunction(Box::new(
                                        ChainedFunctionExpr {
                                            function: FunctionExpr::Concat(ConcatFunctionExpr {
                                                values: vec![
                                                    Expr::String("ciao".to_string()),
                                                    Expr::String("suffix".to_string()),
                                                ],
                                            }),
                                        },
                                    ))),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
        // "ciao".f1(x, y) (Chained generic function on constant)
        (
            r#"select "ciao".f1(x, y)"#,
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::String("ciao".to_string())),
                                    operator: BinaryOperator::FunctionAccess,
                                    right: Box::new(Expr::ChainedGenericFunction(Box::new(
                                        ChainedGenericFunctionExpr {
                                            function: GenericFunctionExpr {
                                                name: PathName {
                                                    name: "f1".to_string(),
                                                    parts: vec![PathPart::Identifier(Identifier {
                                                        name: "f1".to_string(),
                                                    })],
                                                },
                                                arguments: vec![
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::String("ciao".to_string()),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "x".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                    GenericFunctionExprArg {
                                                        name: None,
                                                        expr: Expr::Identifier(Identifier {
                                                            name: "y".to_string(),
                                                        }),
                                                        aggregate: None,
                                                    },
                                                ],
                                                over: None,
                                            },
                                        },
                                    ))),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
        // [1,2,3].coalesce([2,3])[0] (Chained function with array subscript indexing)
        (
            "SELECT [1,2,3].coalesce([2,3])[0]",
            Ast {
                statements: vec![Statement::Query(QueryStatement {
                    query: QueryExpr::Select(Box::new(SelectQueryExpr {
                        select: Select {
                            distinct: false,
                            exprs: vec![SelectExpr::Col(SelectColExpr {
                                expr: Expr::Binary(BinaryExpr {
                                    left: Box::new(Expr::Binary(BinaryExpr {
                                        left: Box::new(Expr::Array(ArrayExpr {
                                            r#type: None,
                                            exprs: vec![
                                                Expr::Number(Number {
                                                    value: "1".to_string(),
                                                }),
                                                Expr::Number(Number {
                                                    value: "2".to_string(),
                                                }),
                                                Expr::Number(Number {
                                                    value: "3".to_string(),
                                                }),
                                            ],
                                        })),
                                        operator: BinaryOperator::FunctionAccess,
                                        right: Box::new(Expr::ChainedFunction(Box::new(
                                            ChainedFunctionExpr {
                                                function: FunctionExpr::Coalesce(
                                                    CoalesceFunctionExpr {
                                                        exprs: vec![
                                                            Expr::Array(ArrayExpr {
                                                                r#type: None,
                                                                exprs: vec![
                                                                    Expr::Number(Number {
                                                                        value: "1".to_string(),
                                                                    }),
                                                                    Expr::Number(Number {
                                                                        value: "2".to_string(),
                                                                    }),
                                                                    Expr::Number(Number {
                                                                        value: "3".to_string(),
                                                                    }),
                                                                ],
                                                            }),
                                                            Expr::Array(ArrayExpr {
                                                                r#type: None,
                                                                exprs: vec![
                                                                    Expr::Number(Number {
                                                                        value: "2".to_string(),
                                                                    }),
                                                                    Expr::Number(Number {
                                                                        value: "3".to_string(),
                                                                    }),
                                                                ],
                                                            }),
                                                        ],
                                                    },
                                                ),
                                            },
                                        ))),
                                    })),
                                    operator: BinaryOperator::ArrayIndex,
                                    right: Box::new(Expr::Number(Number {
                                        value: "0".to_string(),
                                    })),
                                }),
                                alias: None,
                            })],
                            ..Default::default()
                        },
                        ..Default::default()
                    })),
                })],
            },
        ),
    ];

    for (sql, expected) in test_cases {
        let parsed = parse_sql(sql).unwrap();
        assert_ast_eq!(sql, parsed, expected);
    }
}
