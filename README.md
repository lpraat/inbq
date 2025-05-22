# inbq
A Rust library and command-line tool for extracting schema-aware, column-level lineage (including through nested structs/arrays) from multi-statement BigQuery queries.

## Rust Library
To use `inbq` as a library in your Rust project, add the `inbq` dependency to your `Cargo.toml`:
Add the inbq dependency:
```bash
cargo add inbq
```

Then, in your code:
```rust
use inbq::{
    lineage::{Catalog, Column, SchemaObject, SchemaObjectKind, lineage},
    parser::Parser,
    scanner::Scanner,
};

fn main() -> anyhow::Result<()>{
    let sql = r#"
        insert into proj.dat.out_table
        select
            a,
            s.f1,
            s.f2,
        from proj.dat.in_table
    "#;
    let mut scanner = Scanner::new(sql);
    scanner.scan()?;
    let mut parser = Parser::new(scanner.tokens());
    let ast = parser.parse()?;
    println!("AST: {:?}", ast);

    let data_catalog = Catalog {
        schema_objects: vec![
            SchemaObject {
                name: "proj.dat.in_table".to_owned(),
                kind: SchemaObjectKind::Table,
                columns: vec![
                    // dtype is case insensitive and can be retrieved, for example, using
                    // the INFORMATION_SCHEMA.COLUMNS view (https://cloud.google.com/bigquery/docs/information-schema-columns)
                    Column { name: "a".to_owned(), dtype: "STRING".to_owned() },
                    Column { name: "s".to_owned(), dtype: "STRUCT<f1 INT64, f2 INT64>".to_owned() },
                ]
            },
            SchemaObject {
                name: "proj.dat.out_table".to_owned(),
                kind: SchemaObjectKind::Table,
                columns: vec![
                    Column { name: "a".to_owned(), dtype: "STRING".to_owned() },
                    Column { name: "f1".to_owned(), dtype: "INT64".to_owned() },
                    Column { name: "f2".to_owned(), dtype: "INT64".to_owned() },
                ]
            }
        ],
    };

    let output_lineage = lineage(&ast, &data_catalog)?;
    
    println!();
    println!("Raw lineage: {:?}\n", output_lineage.raw);
    println!();
    println!("Ready (human-friendly) lineage: {:?}", output_lineage.ready);
    // -> Ready (human-friendly) lineage: ReadyLineage { objects: [ReadyLineageObject { name: "proj.dat.out_table", kind: "table", nodes: [
    // ReadyLineageNode { name: "a", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "a" }] },
    // ReadyLineageNode { name: "f1", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "s.f1" }]},
    // ReadyLineageNode { name: "f2", input: [ReadyLineageNodeInput { obj_name: "proj.dat.in_table", node_name: "s.f2" }] }] }] }
    Ok(())
}

```

## Command-line tool
### Install binary
```bash
cargo install inbq
```

### Extract Lineage
1. Prepare your data catalog: create a JSON file (e.g., catalog.json) that defines the schema for all tables and views referenced in your SQL queries.

2. Run inbq: pass the catalog file and your SQL file(s) to the inbq lineage command.
```bash
inbq lineage \
    --catalog ./examples/lineage/catalog.json  \  
    ./examples/lineage/query.sql \ # Path to a single SQL file or a directory of .sql files
    --pretty  # Beautifies output JSON
```

The output is written to stdout.


### Example Output
Given the [catalog.json](./examples/lineage/catalog.json) and [query.sql](./examples/lineage/query.sql) from the repository's `examples` directory:
```sql
CREATE TEMP TABLE patient_vitals AS
SELECT 
  p.patient_id,
  p.demographics.age,
  ARRAY(
    SELECT AS STRUCT 
      reading.measurement_type,
      reading.value,
      d.reference_ranges.normal_min,
      d.reference_ranges.normal_max
    FROM UNNEST(p.vital_signs) AS reading
    JOIN `gcp-health-project.reference.diagnostics` d ON reading.measurement_type = d.test_name
  ) AS processed_vitals
FROM `gcp-health-project.patients.records` p;

INSERT INTO gcp-health-project.analytics.patient_summary
WITH health_summary AS (
  SELECT
    patient_id,
    age,
    COUNT(vital.measurement_type) AS total_measurements,
    COUNTIF(vital.value BETWEEN vital.normal_min AND vital.normal_max) AS normal_measurements
  FROM patient_vitals,
  UNNEST(processed_vitals) AS vital
  GROUP BY patient_id, age, processed_vitals
)
SELECT * FROM health_summary;
```

The output is:
```json
{
  "lineage": {
    "objects": [
      {
        "name": "gcp-health-project.analytics.patient_summary",
        "kind": "table",
        "nodes": [
          {
            "name": "patient_id",
            "input": [
              {
                "obj_name": "gcp-health-project.patients.records",
                "node_name": "patient_id"
              }
            ]
          },
          {
            "name": "age",
            "input": [
              {
                "obj_name": "gcp-health-project.patients.records",
                "node_name": "demographics.age"
              }
            ]
          },
          {
            "name": "total_measurements",
            "input": [
              {
                "obj_name": "gcp-health-project.patients.records",
                "node_name": "vital_signs[].measurement_type"
              }
            ]
          },
          {
            "name": "normal_measurements",
            "input": [
              {
                "obj_name": "gcp-health-project.reference.diagnostics",
                "node_name": "reference_ranges.normal_max"
              },
              {
                "obj_name": "gcp-health-project.reference.diagnostics",
                "node_name": "reference_ranges.normal_min"
              },
              {
                "obj_name": "gcp-health-project.patients.records",
                "node_name": "vital_signs[].value"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

**`node_name` granularity**: the `node_name` field (e.g., `vital_signs[].measurement_type`) indicates the full (possibly nested) field path, including `.` for struct field access and `[]` for array element access. If you want to focus on column-level lineage (ignoring the internal structure of structs/arrays), you can take the part before the first `.` or `[]` of the `node_name` string. For example, `vital_signs[].measurement_type` simplifies to `vital_signs`.

**Interpreting lineage**: you might notice `gcp-health-project.patients.records.vital_signs[].measurement_type` is missing from the `normal_measurements` input lineage, despite its use in creating `processed_vitals`. Here's why:
- While `measurement_type` is crucial for the JOIN logic in the `patient_vitals` CTE (which forms `processed_vitals`), the final COUNTIF for `normal_measurements` only directly uses `vital.value`, `vital.normal_min`, and `vital.normal_max`.
- `inbq` currently traces columns directly selected or transformed into the output field. It does not trace lineage through columns used only in JOIN conditions ("join" lineage) or WHERE clauses ("filter" lineage) for the final selected field. Future enhancements will allow tracing this.


### Lineage Graph
inbq internally builds a comprehensive graph representing the entire lineage flow through all statements and expressions. While the primary JSON output shows input-to-output column lineage for tables in your data catalog, you can inspect the detailed internal graph by setting the RUST_LOG environment:
```bash
RUST_LOG=debug inbq lineage \
    --catalog ./examples/lineage/catalog.json \
    ./examples/lineage/query.sql
```

```bash
...more nodes here...
[4]!anon_0->patient_id <-[[3]p->patient_id]
[4]!anon_0->demographics.age <-[[3]p->demographics.age]
[4]!anon_0->processed_vitals <-[[8]!anon_array_3->!anonymous]
[9]patient_vitals->processed_vitals[] <-[[4]!anon_0->processed_vitals[], [7]!anon_2->!anonymous]
[9]patient_vitals->processed_vitals[].measurement_type <-[[4]!anon_0->processed_vitals[].measurement_type, [7]!anon_2->reading.measurement_type]
[9]patient_vitals->processed_vitals[].value <-[[4]!anon_0->processed_vitals[].value, [7]!anon_2->reading.value]
[9]patient_vitals->processed_vitals[].normal_min <-[[4]!anon_0->processed_vitals[].normal_min, [7]!anon_2->reference_ranges.normal_min]
[9]patient_vitals->processed_vitals[].normal_max <-[[4]!anon_0->processed_vitals[].normal_max, [7]!anon_2->reference_ranges.normal_max]
[9]patient_vitals->patient_id <-[[4]!anon_0->patient_id]
[9]patient_vitals->demographics.age <-[[4]!anon_0->demographics.age]
[9]patient_vitals->processed_vitals <-[[4]!anon_0->processed_vitals]
[10]vital->vital.measurement_type <-[[9]patient_vitals->processed_vitals[].measurement_type, [7]!anon_2->reading.measurement_type]
[10]vital->vital.value <-[[9]patient_vitals->processed_vitals[].value, [7]!anon_2->reading.value]
[10]vital->vital.normal_min <-[[9]patient_vitals->processed_vitals[].normal_min, [7]!anon_2->reference_ranges.normal_min]
[10]vital->vital.normal_max <-[[9]patient_vitals->processed_vitals[].normal_max, [7]!anon_2->reference_ranges.normal_max]
[10]vital->vital <-[[9]patient_vitals->processed_vitals[]]
[11]!anon_5->patient_id <-[[9]patient_vitals->patient_id]
[11]!anon_5->demographics.age <-[[9]patient_vitals->demographics.age]
[11]!anon_5->total_measurements <-[[10]vital->vital.measurement_type]
[11]!anon_5->normal_measurements <-[[10]vital->vital.value, [10]vital->vital.normal_min, [10]vital->vital.normal_max]
[12]health_summary->patient_id <-[[11]!anon_5->patient_id]
[12]health_summary->demographics.age <-[[11]!anon_5->demographics.age]
[12]health_summary->total_measurements <-[[11]!anon_5->total_measurements]
[12]health_summary->normal_measurements <-[[11]!anon_5->normal_measurements]
[13]!anon_6->patient_id <-[[12]health_summary->patient_id]
[13]!anon_6->age <-[[12]health_summary->demographics.age]
[13]!anon_6->total_measurements <-[[12]health_summary->total_measurements]
[13]!anon_6->normal_measurements <-[[12]health_summary->normal_measurements]
[2]gcp-health-project.analytics.patient_summary->patient_id <-[[13]!anon_6->patient_id]
[2]gcp-health-project.analytics.patient_summary->age <-[[13]!anon_6->age]
[2]gcp-health-project.analytics.patient_summary->total_measurements <-[[13]!anon_6->total_measurements]
[2]gcp-health-project.analytics.patient_summary->normal_measurements <-[[13]!anon_6->normal_measurements]
```