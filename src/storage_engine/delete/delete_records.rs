use std::fs::{File, OpenOptions};

use csv::{ReaderBuilder, StringRecord, Writer};
use sqlparser::ast::{Expr, TableWithJoins};

use crate::{database::{self, database_loader, database_navigator::get_table_data_path, types::Database, utils::find_database_table}, shared::errors::Error, storage_engine::{filters::{filter_column_finder, filter_manager}, index::index_reader, select::table_reader, utils::ast_unwrapper}};


pub async fn delete_records(from: &Vec<TableWithJoins>, filters: &Option<Expr>) -> Result<String, Error> {
    // Unwrap table name
    let table_name = ast_unwrapper::get_table_name_from_from(from)?;
    
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
    let filtered_records = if use_indexes {
        table_reader::read_table_with_indexes(&schema_name, &table_name, filters, &filter_columns, false).await?
    } else {
        table_reader::read_table(&schema_name, &table_name, filters, false).await?
    };
    
    rewrite_records(&filtered_records, &database.configuration.default_schema, &table_name)?;

    // TODO: Recalculate index offsets
    
    Ok(format!("Success: records have been deleted."))
}

fn rewrite_records(records: &Vec<StringRecord>, schema_name: &String, table_name: &String) -> Result<(), Error> {
    // Read from file
    let file_path = get_table_data_path(schema_name, table_name);
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&file_path)?;
    let headers = rdr.headers()?.clone(); // Clone the headers to use them later for writing

    // Overwrite original file
    let file = OpenOptions::new().write(true).truncate(true).open(&file_path).map_err(|e| Error::IOError(e))?;
    let mut wtr = Writer::from_writer(file);
    wtr.write_record(&headers)?;

    // Step 4: Write records to the new file
    for record in records {
        wtr.write_record(record.iter())?;
    }

    wtr.flush()?;

    Ok(())
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