use std::collections::HashMap;

use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
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