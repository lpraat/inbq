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
      "#
    ];
    for sql in sqls {
        println!("Testing parser for SQL: {}", sql);
        assert!(parse_sql(sql).is_ok())
    }
}
