use std::{collections::HashMap, fs::File};

use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::{Assignment, Expr, TableWithJoins};

use crate::{database::{self, database_loader, database_navigator::get_table_data_path, types::{Database, InsertedRowColumn}, utils::find_database_table}, shared::errors::Error, storage_engine::{filters::filter_manager::apply_filters, index::index_updater, select::{record_handler, utils}, utils::ast_unwrapper::{get_new_column_values, get_table_name_from_from}, validation}};


pub async fn update_records(table: &TableWithJoins, assignments: &Vec<Assignment>, filters: &Option<Expr>) -> Result<String, Error> {
    // Unwrap table name and new column values
    let table_name = get_table_name_from_from(table)?;
    let new_column_values = get_new_column_values(assignments)?;
    let columns: Vec<String> = new_column_values.keys().cloned().collect();
    
    // Prepare: get database blueprint and necessary data from it
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    // Validate update 
    validate_update(&database, &table_name, &new_column_values)?;
    
    // Read from file
    let file_path = get_table_data_path(&schema_name, &table_name);
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = match rdr.headers() {
        Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
        Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    };
    let column_indices = utils::get_column_indices(&headers, &columns);

    let mut records = rdr.records().filter_map(Result::ok).collect::<Vec<StringRecord>>();

    for record in &mut records {
        let mut record_fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
    
        let is_hit = apply_filters(record, &headers, (*filters).as_ref())?;
        if is_hit {
            for &column_index in column_indices.iter() {
                if let Some(column_name) = headers.get(column_index) {
                    if let Some(new_value) = new_column_values.get(column_name) {
                        // Update the value at the specified column index
                        if column_index < record_fields.len() {
                            record_fields[column_index] = new_value.clone();
                        }
                    }
                }
            }
        }

        // Construct a new StringRecord from the modified record_fields
        *record = StringRecord::from(record_fields);
    }

    record_handler::rewrite_records(&records, &schema_name, &table_name)?;

    index_updater::update_indexes_on_update_or_delete(&records, &schema_name, &table_name, table_schema)?;

    Ok(String::from("Success: The records have been updated successfully."))
}

fn validate_update(database: &Database, table_name: &String, new_column_values: &HashMap<String, String>) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;

    // Validate columns exist
    let table_schema = match find_database_table(database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    for (key, _) in new_column_values {
        validation::common::validate_column_exists(table_schema, key)?;
    }

    // Validate column types
    // TODO: Fix this mess
    let row: Vec<InsertedRowColumn> = new_column_values.into_iter().map(|(name, value)| {
        InsertedRowColumn { name: name.to_string(), value: value.to_string() }
    }).collect();
    let inserted_rows = vec![row];
    validation::column_types::validate_column_types(table_schema, &inserted_rows)?;
    
    // Validate column constraints
    let validated_inserted_rows = validation::column_constraints::validate_column_constraints(&inserted_rows, &database.configuration.default_schema, table_schema, false)?;
    
    Ok(validated_inserted_rows)
}