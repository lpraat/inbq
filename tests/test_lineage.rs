use std::collections::{HashMap, HashSet};

use inbq::lineage::{Catalog, Column};
use indexmap::IndexMap;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct LineageTest {
    sql: String,
    schema_objects: Vec<SchemaObject>,
    ready_lineage: HashMap<String, HashMap<String, Vec<String>>>,
}

#[derive(Deserialize, Debug)]
struct SchemaObject {
    name: String,
    kind: String,
    columns: Vec<Column>,
}

#[derive(Deserialize, Debug)]
struct LineageTestData {
    tests: Vec<LineageTest>,
}

const LINEAGE_TESTS_FILE: &str = "tests/lineage_tests.toml";

#[test]
fn test_lineage() {
    use inbq::{lineage::lineage, parser::parse_sql};

    let lineage_data_file =
        std::fs::read_to_string(LINEAGE_TESTS_FILE).expect("Cannot open lineage test cases");
    let test_lineage_data: LineageTestData =
        toml::from_str(&lineage_data_file).expect("Cannot parse test cases defined in yaml");

    for test in test_lineage_data.tests {
        println!("Testing parsing for SQL: {}", &test.sql);

        let mut schema_objects: Vec<inbq::lineage::SchemaObject> = vec![];
        for schema_object in test.schema_objects {
            schema_objects.push(inbq::lineage::SchemaObject {
                name: schema_object.name,
                kind: match schema_object.kind.as_str() {
                    "table" => inbq::lineage::SchemaObjectKind::Table,
                    "view" => inbq::lineage::SchemaObjectKind::View,
                    _ => panic!(),
                },
                columns: schema_object.columns,
            });
        }

        let ast =
            parse_sql(&test.sql).unwrap_or_else(|_| panic!("Could not parse sql: {:?}", &test.sql));

        let lineage = lineage(&ast, &Catalog { schema_objects });
        if let Err(err) = &lineage {
            println!("Could not extract lineage due to: {}", err);
        }

        assert!(lineage.is_ok());
        let lineage = lineage.unwrap();

        let target_ready_lineage = test.ready_lineage;
        let ready_lineage = lineage.ready;

        let mut ready_lineage_map: IndexMap<String, IndexMap<String, Vec<String>>> =
            IndexMap::new();
        for obj in ready_lineage.objects {
            ready_lineage_map.insert(
                obj.name,
                obj.nodes
                    .into_iter()
                    .map(|node| {
                        (
                            node.name,
                            node.input
                                .into_iter()
                                .map(|inp| format!("{}->{}", inp.obj_name, inp.node_name))
                                .collect(),
                        )
                    })
                    .collect(),
            );
        }

        assert!(ready_lineage_map.keys().eq(target_ready_lineage.keys()));
        for (obj, nodes) in &ready_lineage_map {
            assert_eq!(
                nodes.keys().collect::<HashSet<_>>(),
                target_ready_lineage[obj].keys().collect::<HashSet<_>>()
            );
            for (node, inputs) in nodes {
                let inputs = inputs.iter().collect::<HashSet<_>>();
                let target_inputs = target_ready_lineage[obj][node]
                    .iter()
                    .collect::<HashSet<_>>();
                assert_eq!(inputs, target_inputs);
            }
        }
    }
}
