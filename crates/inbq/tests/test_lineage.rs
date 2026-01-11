use std::collections::{HashMap, HashSet};

use inbq::lineage::catalog::{Catalog, SchemaObject};
use indexmap::IndexMap;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
struct SideInput {
    name: String,
    sides: Vec<String>,
}

impl PartialEq for SideInput {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sides.iter().collect::<HashSet<_>>() == other.sides.iter().collect()
    }
}

#[derive(Deserialize, Debug)]
struct LineageInputs {
    inputs: Vec<String>,
    side_inputs: Vec<SideInput>,
}

#[derive(Deserialize, Debug)]
struct LineageTest {
    sql: String,
    schema_objects: Vec<SchemaObject>,
    ready_lineage: HashMap<String, HashMap<String, LineageInputs>>,
    referenced_columns: HashMap<String, HashMap<String, Vec<String>>>,
}

#[derive(Deserialize, Debug)]
struct LineageTestData {
    tests: Vec<LineageTest>,
}

const LINEAGE_TESTS_FILE: &str = "tests/lineage_tests.toml";

#[test]
fn test_lineage() {
    use inbq::{lineage::extract_lineage, parser::parse_sql};

    let lineage_data_file =
        std::fs::read_to_string(LINEAGE_TESTS_FILE).expect("Cannot open lineage test cases");
    let test_lineage_data: LineageTestData =
        toml::from_str(&lineage_data_file).expect("Cannot parse test cases defined in toml");

    for test in test_lineage_data.tests {
        println!("Testing lineage for SQL: {}", &test.sql);
        let ast = parse_sql(&test.sql)
            .unwrap_or_else(|err| panic!("Could not parse sql due to: {:?}", &err));

        let mut lineages = extract_lineage(
            &[&ast],
            &Catalog {
                schema_objects: test.schema_objects,
            },
            false,
            false,
        );
        let lineage = lineages.pop().unwrap();
        if let Err(err) = &lineage {
            println!("Could not extract lineage due to: {}", err);
        }

        assert!(lineage.is_ok());
        let lineage = lineage.unwrap();

        let target_ready_lineage = test.ready_lineage;
        let ready_lineage = lineage.lineage;

        // Test ready lineage
        let mut ready_lineage_map: IndexMap<
            String,
            IndexMap<String, (Vec<String>, Vec<SideInput>)>,
        > = IndexMap::new();
        for obj in ready_lineage.objects {
            ready_lineage_map.insert(
                obj.name,
                obj.nodes
                    .into_iter()
                    .map(|node| {
                        (
                            node.name,
                            (
                                node.inputs
                                    .into_iter()
                                    .map(|inp| format!("{}->{}", inp.obj_name, inp.node_name))
                                    .collect(),
                                node.side_inputs
                                    .into_iter()
                                    .map(|inp| SideInput {
                                        name: format!("{}->{}", inp.obj_name, inp.node_name),
                                        sides: inp.sides,
                                    })
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            );
        }

        assert!(
            ready_lineage_map.len() == target_ready_lineage.len()
                && ready_lineage_map
                    .keys()
                    .all(|k| target_ready_lineage.contains_key(k))
        );
        for (obj, nodes) in &ready_lineage_map {
            assert_eq!(
                nodes.keys().collect::<HashSet<_>>(),
                target_ready_lineage[obj].keys().collect::<HashSet<_>>()
            );
            for (node, (inputs, side_inputs)) in nodes {
                let inputs = inputs.iter().collect::<HashSet<_>>();
                let target_inputs = target_ready_lineage[obj][node]
                    .inputs
                    .iter()
                    .collect::<HashSet<_>>();
                assert_eq!(inputs, target_inputs);

                let mut side_inputs = side_inputs.iter().collect::<Vec<_>>();
                side_inputs.sort_by(|s1, s2| s1.name.cmp(&s2.name));

                let mut target_side_inputs = target_ready_lineage[obj][node]
                    .side_inputs
                    .iter()
                    .collect::<Vec<_>>();
                target_side_inputs.sort_by(|s1, s2| s1.name.cmp(&s2.name));

                assert_eq!(side_inputs, target_side_inputs);
            }
        }

        // Test referenced columns
        let target_referenced_columns = test.referenced_columns;
        let referenced_columns = lineage.referenced_columns;

        let mut referenced_columns_map: IndexMap<String, IndexMap<String, Vec<String>>> =
            IndexMap::new();
        for obj in referenced_columns.objects {
            referenced_columns_map.insert(
                obj.name,
                obj.nodes
                    .into_iter()
                    .map(|node| (node.name, node.referenced_in))
                    .collect(),
            );
        }

        assert!(
            referenced_columns_map.len() == target_referenced_columns.len()
                && referenced_columns_map
                    .keys()
                    .all(|k| target_referenced_columns.contains_key(k))
        );
        for (obj, nodes) in &referenced_columns_map {
            assert_eq!(
                nodes.keys().collect::<HashSet<_>>(),
                target_referenced_columns[obj]
                    .keys()
                    .collect::<HashSet<_>>()
            );
            for (referenced_node, referenced_in) in nodes {
                let mut target_referenced_columns =
                    target_referenced_columns[obj][referenced_node].to_vec();
                target_referenced_columns.sort();

                let mut referenced_in = referenced_in.clone();
                referenced_in.sort();

                assert_eq!(referenced_in, target_referenced_columns);
            }
        }
    }
}
