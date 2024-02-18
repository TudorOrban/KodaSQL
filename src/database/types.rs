use std::collections::HashMap;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub name: String,
    pub tables: Vec<TableSchema>,
    pub configuration: SchemaConfiguration,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaConfiguration {
    pub tables: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
    pub is_indexed: bool
}


#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Constraint {
    NotNull,
    Unique,
    PrimaryKey
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    pub key: String,
    pub offsets: HashMap<String, u64>
}