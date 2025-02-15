use anyhow::anyhow;
use inbq::{parser::{Parser, Query}, scanner::Scanner};

fn parse_sql(sql: &str) -> anyhow::Result<Query> {
    let mut scanner = Scanner::new(sql);
    let tokens = scanner.scan();
    if scanner.had_error {
        return Err(anyhow!("scanner error"))
    }
    let mut parser = Parser::new(&tokens);
    let ast = parser.parse()?;
    Ok(ast)
}

#[test]
fn test_can_parse() {
    let sqls = [
      r#"
      select *
      from `project.dataset.table`
      "#,

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
      "#
    ];
    for sql in sqls {
        println!("Testing parser for SQL: {}", sql);
        assert!(parse_sql(sql).is_ok())
    }
}
