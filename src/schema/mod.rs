
pub struct TableSchema {
    name: String,
    columns: Vec<Column>
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>
}



// Supported column data types
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

pub enum Constraint {
    NotNull,
    Unique
}