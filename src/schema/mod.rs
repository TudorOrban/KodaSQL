use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub constraints: Vec<Constraint>
}


#[derive(Serialize, Deserialize, Debug)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}


#[derive(Serialize, Deserialize, Debug)]
pub enum Constraint {
    NotNull,
    Unique
}