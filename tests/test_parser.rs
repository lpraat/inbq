use anyhow::anyhow;
use inbq::{
    parser::{Parser, Query},
    scanner::Scanner,
};

fn parse_sql(sql: &str) -> anyhow::Result<Query> {
    let mut scanner = Scanner::new(sql);
    let tokens = scanner.scan();
    if scanner.had_error {
        return Err(anyhow!("scanner error"));
    }
    let mut parser = Parser::new(&tokens);
    let ast = parser.parse()?;
    Ok(ast)
}

#[test]
fn test_should_parse() {
    let sqls = [
      "
      select *
      from `project-id.dataset.table`
      ",
        r#"
      select
          --this is a comment
          'pippo' as pippo,
          "zippo" as zippo,
          32.1E4+5 as pi,
          123.456e-67 as b,
          .1E4 as c,
          +58. as d,
          4e2 as e,
          +37 as f,
          -52 as g
      from `project.dataset.table`
      /* this is multiline comment
      aaaa
      bbbb
      */
      where 1=1
      order by c desc
      "#,
        r#"
      select
        * except (id1, id2)
      from table
      "#,
      r#"
      select tmp.s.x[0]
      from tmp
      "#,
        r#"
      with tmp as (with inner_tmp as (select 1 as x) select * from inner_tmp)
      select * from tmp
      "#,
        r#"
      with tmp as (with inner_tmp as (select 1 as x) select * from inner_tmp)
      select *
      "#,
        r#"
      with tmp as (select 1 as x), tmp2 as (select 2 as y union all (select 2 as y))
      select *
      from tmp inner join tmp d using (x) left join tmp as dd on dd.x = d.x
      "#,
        r#"
      WITH RECURSIVE T1 AS ( (SELECT 1 AS n) UNION ALL (SELECT n + 1 AS n FROM T1 WHERE n < 3) )
      SELECT n FROM T1
      "#,
      r#"
      SELECT
        [1,2,3],
        ARRAY[1,2,3][0],
        ARRAY<ARRAY<int64>>[1,2]
      "#,
      r#"
      SELECT
          ARRAY<Array<Array<int64>>>[[[1,2]]],
          1>>4,
          3<<2>>3,
      "#,
      r#"
      select
        array<struct<int64, array<int64>>>[struct(1, [1,2,3])],
        array<struct<`int64`, array<`int64`>>>[struct(1, [1,2,3])]
      "#,
      r#"
      select
        STRUCT(1,2,3),
        STRUCT(1 AS a, 'abc' AS b),
        STRUCT<date>("2011-05-05"),
        STRUCT<`date`>("2011-05-05"),
        STRUCT<x int64>(5 AS x)  -- should be an error, just not a syntax one
      "#,
      r#"
      select
          concat("foo", "bar"),
          concat(tbl.c, ' ', 3)
      from tbl
          
      "#

      // TODO: struct
      // with tmp as (SELECT struct([1,2,3] as x, 2 as y) as s)
      // select tmp.s.x[0]
      // from tmp
      //
      // with tmp as (SELECT struct([1,2,3] as x, 2 as y) as s)
      // select tmp.s.* except (y)
      // from tmp

    ];
    for sql in sqls {
        println!("Testing parser for SQL: {}", sql);
        assert!(parse_sql(sql).is_ok())
    }
}
