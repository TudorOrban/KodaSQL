use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Database {
    pub schemas: Vec<Schema>,
    pub configuration: DatabaseConfiguration,
}

impl Default for Database {
    fn default() -> Self {
        Database {
            schemas: Vec::new(),
            configuration: DatabaseConfiguration::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseConfiguration {
    pub schemas: Vec<String>,
    pub default_schema: String,
}

impl Default for DatabaseConfiguration {
    fn default() -> Self {
        DatabaseConfiguration {
            schemas: Vec::new(),
            default_schema: String::from("NONE"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub tables: Vec<TableSchema>,
    pub configuration: SchemaConfiguration,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaConfiguration {
    pub tables: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
    pub is_indexed: bool,
    pub order: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey,
    DefaultValue(String),
    ForeignKey(ForeignKey)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub local_table: String,
    pub local_columns: Vec<String>,
    pub foreign_table: String,
    pub foreign_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    NoAction,
    Restrict,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Index {
    pub key: String,
    pub offsets: Vec<u64>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RowsIndex {
    pub row_offsets: Vec<u64>
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct InsertedRowColumn {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnValues {
    pub column_name: String,
    pub values: Vec<String>,
}