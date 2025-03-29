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
      r#"
      select *
      from `project-id.dataset.table`
      "#,
      r#"
      select
          columnA,
          column-a,
          `287column`,
      from
          my-project.mydataset.mytable,
          mydataset.mytable,
          `dataset.inventory`,
          my-table,
          mytable,
          `287mytable`
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
      SELECT ARRAY<STRUCT<warehouse STRING, state string>>
          [('warehouse #1', 'WA'),
           ('warehouse #2', 'CA'),
           ('warehouse #3', 'WA')] col
      "#,
      r#"
      with tmp as (SELECT struct([1,2,3] as x, 2 as y) as s)
      select tmp.s.x[0]
      from tmp
      "#,
      r#"
      with tmp as (SELECT struct([1,2,3] as x, 2 as y) as s)
      select tmp.s.* except (y)
      from tmp
      "#,
      r#"
      select
          concat("foo", "bar"),
          concat(tbl.c, ' ', 3)
      from tbl
      "#,
      r#"
      INSERT dataset.Inventory (product, quantity)
      VALUES('top load washer', 10),
            ('front load washer', 20),
            ('dryer', 30),
            ('refrigerator', 10),
            ('microwave', 20),
            (DEFAULT, 30),
            ('oven', 5);
      select * from dataset.Inventory
      "#,
      r#"
      DELETE dataset.Inventory
      WHERE quantity = 0
      "#,
      r#"
      DELETE dataset.Inventory i
      WHERE i.product NOT IN (SELECT product from dataset.NewArrivals)
      "#,
      r#"
      UPDATE dataset.Inventory
      SET quantity = quantity - 10,
          supply_constrained = DEFAULT
      WHERE product like '%washer%'
      "#,
      r#"
      UPDATE dataset.Inventory
      SET quantity = quantity +
        (SELECT quantity FROM dataset.NewArrivals
         WHERE Inventory.product = NewArrivals.product),
          supply_constrained = false
      WHERE product IN (SELECT product FROM dataset.NewArrivals)
      "#,
      r#"
      UPDATE dataset.Inventory i
      SET quantity = i.quantity + n.quantity,
          supply_constrained = false
      FROM dataset.NewArrivals n
      WHERE i.product = n.product
      "#,
      r#"
      TRUNCATE TABLE dataset.Inventory
      "#,
      r#"
      MERGE dataset.DetailedInventory T
      USING dataset.Inventory S
      ON T.product = S.product
      WHEN NOT MATCHED AND quantity < 20 THEN
        INSERT(product, quantity, supply_constrained, comments)
        VALUES(product, quantity, true, ARRAY<STRUCT<created DATE, comment STRING>>[(DATE('2016-01-01'), 'comment1')])
      WHEN NOT MATCHED THEN
        INSERT(product, quantity, supply_constrained)
        VALUES(product, quantity, false)
      "#,
      r#"
      MERGE dataset.Inventory T
      USING dataset.NewArrivals S
      ON T.product = S.product
      WHEN MATCHED THEN
        UPDATE SET quantity = T.quantity + S.quantity
      WHEN NOT MATCHED THEN
        INSERT (product, quantity) VALUES(product, quantity)
      "#,
      r#"
      MERGE dataset.NewArrivals T
      USING (SELECT * FROM dataset.NewArrivals WHERE warehouse <> 'warehouse #2') S
      ON T.product = S.product
      WHEN MATCHED AND T.warehouse = 'warehouse #1' THEN
        UPDATE SET quantity = T.quantity + 20
      WHEN MATCHED THEN
        DELETE
      "#,
      r#"
      MERGE dataset.Inventory T
      USING dataset.NewArrivals S
      ON FALSE
      WHEN NOT MATCHED AND product LIKE '%washer%' THEN
        INSERT (product, quantity) VALUES(product, quantity)
      WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN
        DELETE
      "#,
      r#"
      MERGE dataset.DetailedInventory T
      USING dataset.Inventory S
      ON T.product = S.product
      WHEN MATCHED AND S.quantity < (SELECT AVG(quantity) FROM dataset.Inventory) THEN
        UPDATE SET comments = ARRAY_CONCAT(comments, ARRAY<STRUCT<created DATE, comment STRING>>[(CAST('2016-02-01' AS DATE), 'comment2')])
      "#,
      r#"
      MERGE dataset.Inventory T
      USING (SELECT product, quantity, state FROM dataset.NewArrivals t1 JOIN dataset.Warehouse t2 ON t1.warehouse = t2.warehouse) S
      ON T.product = S.product
      WHEN MATCHED AND state = 'CA' THEN
        UPDATE SET quantity = T.quantity + S.quantity
      WHEN MATCHED THEN
        DELETE
      "#,
      r#"
      CREATE TEMP TABLE tmp (x `decimal`(3,4), y STRUCT <a ARRAY <STRING>, b BOOL>);
      CREATE OR REPLACE TABLE tmp (x decimal(3,4), y STRUCT <a ARRAY <STRING(255)>, b `BOOL`>);
      "#
    ];
    for sql in sqls {
        println!("Testing parsing for SQL: {}", sql);
        assert!(parse_sql(sql).is_ok())
    }
}

#[test]
fn test_should_not_parse() {
    let sqls = [
      "
      select
        array<struct<int64, `array`<int64>>>[struct(1, [1,2,3])],
      ",
    ];
    for sql in sqls {
        println!("Testing parsing error for SQL: {}", sql);
        assert!(parse_sql(sql).is_err())
    }
}
