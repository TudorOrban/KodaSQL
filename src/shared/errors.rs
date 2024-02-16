use std::fmt;
use std::io::Error as IOError;

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum Error {
    // General
    UnknownError,
    ServerError,

    // Failed
    IOError(std::io::Error),
    CsvError(csv::Error),
    SerdeJsonError(serde_json::Error),
    FailedTableRead { table_name: String },

    // Invalid
    InvalidSQLSyntax,
    InvalidTableName { table_name: String },
    InvalidLimit { limit: String },

    // Missing
    MissingTableName,

    // Not allowed
    TableNameAlreadyExists { table_name: String },

    // Not supported
    GenericUnsupported,

    UnsupportedSelectClause,
    UnsupportedValueType { value: String },
    UnsupportedOperationType { operation: String },

    UnsupportedColumnDataType { column_name: String, column_type: String },
    UnsupportedConstraint { column_name: String, column_constraint: String },

    // Missing
    TableDoesNotExist { table_name: String },
    ColumnDoesNotExist { column_name: String, table_name: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // General
            Error::UnknownError => write!(f, "An unknown error occurred."),
            Error::ServerError => write!(f, "A server error occurred."),

            // Failed
            Error::IOError(e) => write!(f, "IO error: {}", e),
            Error::CsvError(e) => write!(f, "CSV error: {}", e),
            Error::SerdeJsonError(e) => write!(f, "Serde JSON error: {}", e),
            Error::FailedTableRead { table_name } => write!(f, "Failed to read data from table {}", table_name),

            // Invalid
            Error::InvalidSQLSyntax => write!(f, "You have an error in your SQL syntax"),
            Error::InvalidTableName { table_name } => write!(f, "The table name {} is invalid.", table_name),
            Error::InvalidLimit { limit } => write!(f, "The provided limit {} is invalid.", limit),
            
            // Missing
            Error::MissingTableName => write!(f, "Missing table name."),

            // Not allowed
            Error::TableNameAlreadyExists { table_name } => write!(f, "Table name {} already exists.", table_name),

            // Not supported
            Error::GenericUnsupported => write!(f, "You're attempting an SQL operation that is not currently supported."),
            Error::UnsupportedSelectClause => write!(f, "The SELECT clause is not currently supported."),
            Error::UnsupportedValueType { value } => write!(f, "The value {} is not currently supported.", value),
            Error::UnsupportedColumnDataType { column_name, column_type } => write!(f, "The column type {} for column {} is not supported.", column_type, column_name),
            Error::UnsupportedConstraint { column_name, column_constraint } => write!(f, "The constraint {} for column {} is not supported.", column_constraint, column_name),
            Error::UnsupportedOperationType { operation } => write!(f, "The operation {} in the WHERE clause is not currently supported.", operation),

            // Missing
            Error::TableDoesNotExist { table_name } => write!(f, "Table {} does not exist.", table_name),
            Error::ColumnDoesNotExist { column_name, table_name } => write!(f, "Column {} does not exist in table '{}'.", column_name, table_name),
        }
    }
}

// Conversions
impl std::error::Error for Error {}

impl From<IOError> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IOError(error)
    }
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Self {
        Error::CsvError(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::SerdeJsonError(error)
    }
}


// Define a trait for converting errors to a client-friendly error
pub trait ToClientError {
    fn to_client_error(&self) -> ClientError;
}

// Struct for client-friendly errors
#[derive(Serialize, Deserialize)]
pub struct ClientError {
    pub message: String,
    // Add more fields as necessary, like error codes
}

// Implement the trait for your Error enum
impl ToClientError for Error {
    fn to_client_error(&self) -> ClientError {
        match self {
            // Simplify messages for client consumption
            _ => ClientError {
                message: format!("{}", self),
            },
        }
    }
}