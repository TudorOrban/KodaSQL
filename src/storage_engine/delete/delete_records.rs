use sqlparser::ast::{Expr, TableWithJoins};

use crate::{database::{self, database_loader, types::Database, utils::find_database_table}, shared::errors::Error, storage_engine::{filters::filter_column_finder, index::index_updater, select::{record_handler, table_reader}, utils::ast_unwrapper}};

pub async fn delete_records(from: &Vec<TableWithJoins>, filters: &Option<Expr>) -> Result<String, Error> {
    // Unwrap table name
    let table_name = ast_unwrapper::get_table_name_from_from_vector(from)?;
    
    // Prepare: get database blueprint and necessary data from it
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    
    // Perform validation
    validate_delete(&database, &table_name)?;

    // Get columns used for filtering
    let filter_columns = filter_column_finder::find_filter_columns(&filters)?;

    // Use indexes only if all filter columns are indexed
    let use_indexes = filter_column_finder::use_indexes(&filter_columns, table_schema);
    
    // Read from table and filter
    let remaining_rows = if use_indexes {
        table_reader::read_table_with_indexes(&schema_name, &table_name, filters, &filter_columns, false).await?
    } else {
        table_reader::read_table(&schema_name, &table_name, filters, false).await?
    };
    
    // Rewrite CSV file with remaining rows
    record_handler::rewrite_records(&remaining_rows, &schema_name, &table_name)?;

    index_updater::update_indexes_on_update_or_delete(&remaining_rows, &schema_name, &table_name, table_schema)?;
    
    Ok(format!("Success: records have been deleted."))
}


fn validate_delete(database: &Database, table_name: &String) -> Result<(), Error> {
    // Ensure table exists
    let _ = match find_database_table(database, table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    Ok(())
}