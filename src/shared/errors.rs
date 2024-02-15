use std::fmt;

#[derive(Debug)]
pub enum Error {
    // Invalid
    InvalidTableName { table_name: String },

    // Missing
    MissingTableName,

    // Not allowed
    TableNameAlreadyExists { table_name: String },

    // Not supported
    UnsupportedColumnDataType { column_name: String, column_type: String },
    UnsupportedConstraint { column_name: String, column_constraint: String }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Invalid
            Error::InvalidTableName { table_name } => write!(f, "The table name {} is invalid.", table_name),
            
            // Missing
            Error::MissingTableName => write!(f, "Missing table name."),

            // Not allowed
            Error::TableNameAlreadyExists { table_name } => write!(f, "Table name {} already exists.", table_name),

            // Not supported
            Error::UnsupportedColumnDataType { column_name, column_type } => write!(f, "The column type {} for column {} is not supported.", column_type, column_name),
            Error::UnsupportedConstraint { column_name, column_constraint } => write!(f, "The constraint {} for column {} is not supported.", column_constraint, column_name),
        }
    }
}

impl std::error::Error for Error {}

// impl From<std::io::Error> for Error {
//     fn from(e: std::io::Error) -> Self {
//         Error::IOError(e)
//     }
// }