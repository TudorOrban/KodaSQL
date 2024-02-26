use crate::{database::{types::Database, utils::find_database_table}, shared::errors::Error};


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
            if !table_schema.columns.iter().any(|col| &col.name == column) {
                return Err(Error::ColumnDoesNotExist { column_name: column.clone(), table_name: table_name.to_string() });
            }
        }
    }

    // Ensure order column exists
    if let Some(column_name) = order_column_name {
        // TODO: Add type validation
        let is_column_valid = !table_schema.columns.iter().any(|col| &col.name == column_name);
        if is_column_valid {
            return Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.to_string() });
        }
    }

    Ok(())
}