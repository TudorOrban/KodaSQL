use std::fmt;
use std::io::Error as IOError;

#[derive(Debug)]
pub enum Error {
    // General
    UnknownError,
    ServerError,

    // Failed
    FailedTableRead { table_name: String },
    IOError(std::io::Error),
    CsvError(csv::Error),

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
            Error::FailedTableRead { table_name } => write!(f, "Failed to read data from table {}", table_name),
            Error::IOError(e) => write!(f, "IO error: {}", e),
            Error::CsvError(e) => write!(f, "CSV error: {}", e),

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
// impl From<std::io::Error> for Error {
//     fn from(e: std::io::Error) -> Self {
//         Error::IOError(e)
//     }
// }