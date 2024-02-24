use std::collections::HashMap;
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
    pub is_indexed: bool
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
    PrimaryKey
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