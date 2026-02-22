use inbq::{parser::parse_sql, test_utils::TestParsingData};

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
