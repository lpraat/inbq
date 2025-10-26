# inbq
A library for parsing BigQuery queries and extracting schema-aware, column-level lineage. Written in Rust, with Python bindings.

### Features
- Parse BigQuery queries into well-structured ASTs with clear, easy-to-navigate nodes.
- Extract schema-aware column-level lineage.
- Support for both single and multi-statement queries and procedural language constructs.
- Built for speed and efficiency, with lightweight Python bindings that add minimal overhead.

### Work in Progress
- Full lineage tracing through nested structs and arrays (currently partially supported)
- DDL syntax parsing


## Python
### Install
`pip install inbq`

### Example
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
add_table("project.dataset.t2", [("id", "int64"), ("y", "float64")])

query = """
declare default_val float64 default (select min(val) from project.dataset.out);

insert into `project.dataset.out`
select
    id,
    if(x is null or y is null, default_val, x+y)
from `project.dataset.t1` inner join `project.dataset.t2` using (id)
"""

pipeline = (
  inbq.Pipeline()
    .config(raise_exception_on_error=False, parallel=True)
    .parse()
    .extract_lineage(catalog=catalog, include_raw=False)
)
pipeline_output = inbq.run_pipeline(sqls=[query], pipeline=pipeline)

for ast, output_lineage in zip(pipeline_output.asts, pipeline_output.lineages):
    print(f"{ast=}")
    print("Lineage:")
    for object in output_lineage.lineage.objects:
        for node in object.nodes:
            print(f"{object.name}->{node.name} <- {[f'{input_node.obj_name}->{input_node.node_name}' for input_node in node.input]}")

# Prints:
# ast=Ast(...)
# Lineage:
# project.dataset.out->id <- ['project.dataset.t1->id', 'project.dataset.t2->id']
# project.dataset.out->val <- ['project.dataset.out->val', 'project.dataset.t2->y', 'project.dataset.t1->x']
```

## Rust
### Install
`cargo add inbq`

### Example
```rust
use inbq::{
    lineage::{Catalog, Column, SchemaObject, SchemaObjectKind, extract_lineage},
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
            if(x is null or y is null, default_val, x+y)
        from `project.dataset.t1` inner join `project.dataset.t2` using (id)
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
                    columns: vec![column("id", "int64"), column("y", "float64")],
                },
            },
        ],
    };

    let output_lineage = extract_lineage(&[&ast], &data_catalog, false, true)
        .pop()
        .unwrap()?;

    println!("Lineage: {:?}", output_lineage.lineage);
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

2. Run inbq: pass the catalog file and your [SQL file(s)](./examples/lineage/query.sql) to the inbq lineage command.
```bash
inbq extract-lineage \
    --pretty \ # Beautifies output JSON
    --catalog ./examples/lineage/catalog.json  \
    ./examples/lineage/query.sql \ # Path to a single SQL file or a directory of .sql files
```

The output is written to stdout.
