use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaObject {
    /// Project.dataset.name uid
    pub name: String,
    pub kind: SchemaObjectKind,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Catalog {
    pub schema_objects: Vec<SchemaObject>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserFunctionArg {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TableFunctionArgument {
    Standard(TableFunctionStandardArgument),
    Table(TableFunctionTableArgument),
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableFunctionStandardArgument {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableFunctionTableArgument {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObjectKind {
    // Tables
    Table {
        columns: Vec<Column>,
    },
    View {
        columns: Vec<Column>,
    },
    // Routines
    UserSqlFunction {
        /// Each argument dtype can be either defined or ANY TYPE (templated function)
        arguments: Vec<UserFunctionArg>,
        /// Return can be None in templated function (any type can only be used in arguments)
        returns: Option<String>,
        /// Body is required for templated functions
        body: Option<String>,
    },
    UserJsFunction {
        arguments: Vec<UserFunctionArg>,
        returns: String,
    },
    TableFunction {
        arguments: Vec<TableFunctionArgument>,
        returns: Vec<Column>,
    },
}
