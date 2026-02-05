# inbq
A library for parsing BigQuery queries and extracting schema-aware, column-level lineage.

### Features
- Parse BigQuery queries into well-structured ASTs with [easy-to-navigate nodes](#ast-navigation).
- Extract schema-aware, [column-level lineage](#concepts).
- Trace data flow through nested structs and arrays.
- Capture [referenced columns](#referenced-columns) and the specific query components (e.g., select, where, join) they appear in.
- Process both single and multi-statement queries with procedural language constructs.
- Built for speed and efficiency, with lightweight Python bindings that add minimal overhead.


## Python
### Install
`pip install inbq`

### Example (Pipeline API)
```python

import inbq

catalog = {"schema_objects": []}

def add_table(name: str, columns: list[tuple[str, str]]) -> None:
    catalog["schema_objects"].append({
        "name": name,
        "kind": {
            "table": {
                "columns": [{"name": name, "dtype": dtype} for name, dtype in columns]
            }
        }
    })

add_table("project.dataset.out", [("id", "int64"), ("val", "float64")])
add_table("project.dataset.t1", [("id", "int64"), ("x", "float64")])
add_table("project.dataset.t2", [("id", "int64"), ("s", "struct<source string, x float64>")])

query = """
declare default_val float64  default (select min(val) from project.dataset.out);

insert into `project.dataset.out`
select
    id,
    if(x is null or s.x is null, default_val, x + s.x)
from `project.dataset.t1` inner join `project.dataset.t2` using (id)
where s.source = "baz";
"""

pipeline = (
    inbq.Pipeline()
    .config(
        # If the `pipeline` is configured with `raise_exception_on_error=False`,
        # any error that occurs during parsing or lineage extraction is
        # captured and returned as a `inbq.PipelineError`
        raise_exception_on_error=False,
        # No effect with only one query (may provide a speedup with multiple queries)
        parallel=True,
    )
    .parse()
    .extract_lineage(catalog=catalog, include_raw=False)
)
sqls = [query]
pipeline_output = inbq.run_pipeline(sqls, pipeline=pipeline)

# This loop will iterate just once as we have only one query
for i, (ast, output_lineage) in enumerate(
    zip(pipeline_output.asts, pipeline_output.lineages)
):
    assert isinstance(ast, inbq.ast_nodes.Ast), (
        f"Could not parse query `{sqls[i][:20]}...` due to: {ast.error}"
    )

    print(f"{ast=}")

    assert isinstance(output_lineage, inbq.lineage.Lineage), (
        f"Could not extract lineage from query `{sqls[i][:20]}...` due to: {output_lineage.error}"
    )

    print("\nLineage:")
    for lin_obj in output_lineage.lineage.objects:
        print("Inputs:")
        for lin_node in lin_obj.nodes:
            print(
                f"{lin_obj.name}->{lin_node.name} <- {[f'{input_node.obj_name}->{input_node.node_name}' for input_node in lin_node.inputs]}"
            )

        print("\nSide inputs:")
        for lin_node in lin_obj.nodes:
            print(
                f"""{lin_obj.name}->{lin_node.name} <- {[f"{input_node.obj_name}->{input_node.node_name} @ {','.join(input_node.sides)}" for input_node in lin_node.side_inputs]}"""
            )

    print("\nReferenced columns:")
    for ref_obj in output_lineage.referenced_columns.objects:
        for ref_node in ref_obj.nodes:
            print(
                f"{ref_obj.name}->{ref_node.name} referenced in {ref_node.referenced_in}"
            )

# Prints:
# ast=Ast(...)

# Lineage:
# Inputs:
# project.dataset.out->id <- ['project.dataset.t2->id', 'project.dataset.t1->id']
# project.dataset.out->val <- ['project.dataset.t2->s.x', 'project.dataset.t1->x', 'project.dataset.out->val']
#
# Side inputs:
# project.dataset.out->id <- ['project.dataset.t2->s.source @ where', 'project.dataset.t2->id @ join', 'project.dataset.t1->id @ join']
# project.dataset.out->val <- ['project.dataset.t2->s.source @ where', 'project.dataset.t2->id @ join', 'project.dataset.t1->id @ join']
#
# Referenced columns:
# project.dataset.out->val referenced in ['default_var', 'select']
# project.dataset.t1->id referenced in ['join', 'select']
# project.dataset.t1->x referenced in ['select']
# project.dataset.t2->id referenced in ['join', 'select']
# project.dataset.t2->s.x referenced in ['select']
# project.dataset.t2->s.source referenced in ['where']
```

**Note:** What happens if you remove the insert and just keep the select in the query? `inbq` is designed to handle this gracefully. It will return the lineage for the last `SELECT` statement, but since the destination is no longer explicit, the output object (an anonymous query) will be assigned an anonymous identifier (e.g., `!anon_4`). Try it yourself and see how the output changes!

To learn more about the output elements (Lineage, Side Inputs, and Referenced Columns), please see the [Concepts](#concepts) section.

### Example (Individual Functions)
If you don't like the Pipeline API, you can use these functions instead:

#### `parse_sql` and `parse_sql_to_dict`
Parse a single SQL query:
```python
ast = inbq.parse_sql(query)

# You can also get a dictionary representation of the AST
ast_dict = inbq.parse_sql_to_dict(query)
```

#### `parse_sqls`
Parse multiple SQL queries in parallel:

```python
sqls = [query]
asts = inbq.parse_sqls(sqls, parallel=True)
```

#### `parse_sqls_and_extract_lineage`
Parse SQLs and extract lineage in one go:

```python
asts, lineages = inbq.parse_sqls_and_extract_lineage(
    sqls=[query],
    catalog=catalog,
    parallel=True
)
```

### AST Navigation
```python
import inbq
import inbq.ast_nodes as ast_nodes

sql = """
UPDATE proj.dataset.t1
SET quantity = quantity - 10,
    supply_constrained = DEFAULT
WHERE product like '%washer%';

UPDATE proj.dataset.t2
SET quantity = quantity - 10,
WHERE product like '%console%';
"""

ast = inbq.parse_sql(sql)

# Example: find updated tables and columns
for node in ast.find_all(
    ast_nodes.UpdateStatement,
):
    match node:
        case ast_nodes.UpdateStatement(
            table=table,
            alias=_,
            update_items=update_items,
            from_=_,
            where=_,
        ):
            print(f"Found updated table: {table.name}. Updated columns:")
            for update_item in update_items:
                for node in update_item.column.find_all(
                    ast_nodes.Identifier,
                    ast_nodes.QuotedIdentifier
                ):
                    match node:
                        case ast_nodes.Identifier(name=name) | ast_nodes.QuotedIdentifier(name=name):
                            print(f"- {name}")

# Example: find `like` filters
for node in ast.find_all(
    ast_nodes.BinaryExpr,
):
    match node:
        case ast_nodes.BinaryExpr(
            left=left,
            operator=ast_nodes.BinaryOperator_Like(),
            right=right,
        ):
            print(left, "like", right)
```

#### Variants and Variant Types in Python
The AST nodes in Python are auto-generated dataclasses from their Rust definitions.
For instance, a Rust enum `Expr` might be defined as:

```rust
pub enum Expr {
    // ... more variants here ...
    Binary(BinaryExpr),
    Identifier(Identifier),
    // ... more variants here ...
}
```

In Python, this translates to corresponding classes like `Expr_Binary(vty=BinaryExpr)`, `Expr_Identifier(vty=Identifier)`, etc.
The `vty` attribute stands for "variant type" (unit variants do not have a `vty` attribute).
You can search for any type of object using `.find_all()`, whether it's the variant (e.g., `Expr_Identifier`) or the concrete variant type (e.g., `Identifier`).

## Rust
### Install
`cargo add inbq`

### Example
```rust
use inbq::{
    lineage::{
        catalog::{Catalog, Column, SchemaObject, SchemaObjectKind},
        extract_lineage,
    },
    parser::Parser,
    scanner::Scanner,
};

fn column(name: &str, dtype: &str) -> Column {
    Column {
        name: name.to_owned(),
        dtype: dtype.to_owned(),
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let sql = r#"
        declare default_val float64 default (select min(val) from project.dataset.out);

        insert into `project.dataset.out`
        select
            id,
            if(x is null or s.x is null, default_val, x + s.x)
        from `project.dataset.t1` inner join `project.dataset.t2` using (id)
        where s.source = "baz";
    "#;
    let mut scanner = Scanner::new(sql);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let ast = parser.parse()?;
    println!("Syntax Tree: {:?}", ast);

    let data_catalog = Catalog {
        schema_objects: vec![
            SchemaObject {
                name: "project.dataset.out".to_owned(),
                kind: SchemaObjectKind::Table {
                    columns: vec![column("id", "int64"), column("val", "int64")],
                },
            },
            SchemaObject {
                name: "project.dataset.t1".to_owned(),
                kind: SchemaObjectKind::Table {
                    columns: vec![column("id", "int64"), column("x", "float64")],
                },
            },
            SchemaObject {
                name: "project.dataset.t2".to_owned(),
                kind: SchemaObjectKind::Table {
                    columns: vec![
                        column("id", "int64"),
                        column("s", "struct<source string, x float64>"),
                    ],
                },
            },
        ],
    };

    let lineage = extract_lineage(&[&ast], &data_catalog, false, true)
        .pop()
        .unwrap()?;

    println!("\nLineage: {:?}", lineage.lineage);
    println!("\nReferenced columns: {:?}", lineage.referenced_columns);
    Ok(())
}
```


## Command Line Interface
### Install binary
```bash
cargo install inbq
```

### Extract Lineage
1. Prepare your data catalog: create a JSON file (e.g., [catalog.json](./examples/lineage/catalog.json)) that defines the schema for all tables and views referenced in your SQL queries.

2. Run inbq: pass the catalog file and your [SQL file or directory of multiple SQL files](./examples/lineage/query.sql) to the inbq lineage command.
```bash
inbq extract-lineage \
    --pretty \
    --catalog ./examples/lineage/catalog.json  \
    ./examples/lineage/query.sql
```

The output is written to stdout.

## Concepts

### Lineage
Column-level lineage tracks how data flows from a destination column back to its original source columns. A destination column's value is derived from its direct input columns, and this process is applied recursively to trace the lineage back to the foundational source columns. For example, in `with tmp as (select a+b as tmp_c from t) select tmp_c as c from t`, the lineage for column `c` traces back to `a` and `b` as its source columns (the source table is `t`).

### Lineage - Side Inputs
Side inputs are columns that indirectly contribute to the final set of output values. As the name implies, they aren't part of the direct `SELECT` list, but are found in the surrounding clauses that shape the result, such as `WHERE`, `JOIN`, `WINDOW`, etc. Side inputs influence is traced recursively. For example, in the query:
```sql
with cte as (select id, c1 from table1 where f1>10)
select c2 as z
from table2 inner join cte using (id)
```
`table1.f1` is a side input to `z` with sides `join` and `where` (`cte.id`, later used in the join condition, is filtered by `table1.f1`). The other two side inputs are `table1.id` with side `join` and `table2.id` with side `join`.

### Referenced Columns
Referenced columns provide a detailed map of where each input column is mentioned within a query. This is the entry point for a column into the query's logic. From this initial reference, the column can then influence other parts of the query indirectly through subsequent operations.

## Limitations
While this library can parse and extract lineage for most BigQuery syntax, there are some current limitations. For example, the pipe (`|`) syntax and the recently introduced `MATCH_RECOGNIZE` clause are not yet supported. Requests and contributions for unsupported features are welcome.

## Contributing
Here's a brief overview of the project's key modules:
-   `crates/inbq/src/parser.rs`: contains the hand-written top-down parser.
-   `crates/inbq/src/ast.rs`: defines the Abstract Syntax Tree (AST) nodes.
    -   **Note**: If you add or modify AST nodes here, you must regenerate the corresponding Python nodes. You can do this by running `cargo run --bin inbq_genpy`, which will update `crates/py_inbq/python/inbq/ast_nodes.py`.
-   `crates/inbq/src/lineage.rs`: contains the core logic for extracting column-level lineage from the AST.
-   `crates/py_inbq/`: this crate exposes the Rust backend as a Python module via PyO3.
-   `crates/inbq/tests/`: this directory contains the tests. You can add new test cases for parsing and lineage extraction by editing the `.toml` files:
    -   `parsing_tests.toml`
    -   `lineage_tests.toml`
