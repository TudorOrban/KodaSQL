use sqlparser::ast::{Ident, ObjectName, Query};
use csv::WriterBuilder;
use std::fs::OpenOptions;

use crate::database::database_loader;
use crate::database::database_navigator;
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::storage_engine::index::index_writer;

use super::validator;

pub async fn insert_into_table(name: &ObjectName, columns: &Vec<Ident>, source: &Option<Box<Query>>) -> Result<String, Error> {
    // Get database blueprint
    let database = database_loader::get_database()?;
    
    // Validate insert
    let (table_name, _, complete_inserted_rows) = validator::validate_insert_into(&database, name, columns, source)?;

    // Open CSV file in append mode
    let file_path = database_navigator::get_table_data_path(&database.configuration.default_schema, &table_name);
    let modified_file = OpenOptions::new()
        .write(true).append(true).open(file_path)
        .map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(modified_file);

    // Iterate over complete_inserted_rows and write to CSV
    for row in &complete_inserted_rows {
        let row_value: Vec<String> = row.into_iter().map(|r| r.value.clone()).collect();
        wtr.write_record(&row_value)
            .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;
    }

    wtr.flush().map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Write index offsets
    let table_schema = match find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    index_writer::add_index_offsets_on_insert(&complete_inserted_rows, &database.configuration.default_schema, &table_name, table_schema)?;

    Ok(table_name)
}
