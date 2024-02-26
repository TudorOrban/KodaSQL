use std::fs::File;

use csv::ReaderBuilder;
use sqlparser::ast::{Expr, TableWithJoins};

use crate::{database::{database_loader, database_navigator::get_table_data_path, types::Database, utils::find_database_table}, shared::errors::Error, storage_engine::utils::ast_unwrapper};


pub async fn delete_records(from: &Vec<TableWithJoins>, selection: &Option<Expr>) -> Result<String, Error> {
    // Get database blueprint
    let database = database_loader::get_database()?;
    
    // Unwrap table name
    let table_name = ast_unwrapper::get_table_name_from_from(from)?;

    // Perform validation
    validate_delete(&database, &table_name)?;

    // Read from file
    let file_path = get_table_data_path(&database.configuration.default_schema, &table_name);
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(true).from_reader(file);
    

    Ok(format!("Success: records have been deleted."))
}

// fn get_
fn validate_delete(database: &Database, table_name: &String) -> Result<(), Error> {
    // Ensure table exists
    let table_schema = match find_database_table(database, table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    Ok(())
}