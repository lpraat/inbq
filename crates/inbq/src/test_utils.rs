use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use serde::Deserialize;

use crate::lineage::catalog::SchemaObject;

pub const PARSING_TESTS_FILE: &str = "tests/parsing_tests.toml";
pub const LINEAGE_TESTS_FILE: &str = "tests/lineage_tests.toml";

#[derive(Deserialize, Debug, Clone)]
pub struct TestParsing {
    pub sql: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TestParsingData {
    pub tests: Vec<TestParsing>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TestSideInput {
    pub name: String,
    pub sides: Vec<String>,
}

impl PartialEq for TestSideInput {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sides.iter().collect::<HashSet<_>>() == other.sides.iter().collect()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct TestLineageInputs {
    pub inputs: Vec<String>,
    pub side_inputs: Vec<TestSideInput>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TestLineage {
    pub sql: String,
    pub schema_objects: Vec<SchemaObject>,
    pub ready_lineage: HashMap<String, HashMap<String, TestLineageInputs>>,
    pub referenced_columns: HashMap<String, HashMap<String, Vec<String>>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TestLineageData {
    pub tests: Vec<TestLineage>,
}

impl Display for TestLineageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
