use crate::database::{types::Database, utils::find_database_table};
use crate::shared::errors::Error;
use crate::storage_engine::validation;


pub fn validate_select_query(
    database: &Database,
    table_name: &String,
    columns: &Vec<String>,
    order_column_name: &Option<String>,
) -> Result<(), Error> {
    // Ensure table exists
    let table_schema = match find_database_table(database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    // Ensure selected columns exist
    if !columns.contains(&"*".to_string()) {
        for column in columns {
            validation::common::validate_column_exists(table_schema, column)?;
        }
    }

    // Ensure order column exists
    if let Some(column_name) = order_column_name {
        // TODO: Add type validation
        validation::common::validate_column_exists(table_schema, column_name)?;
    }

    Ok(())
}