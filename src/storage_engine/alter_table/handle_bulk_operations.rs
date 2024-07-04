use std::fs::OpenOptions;

use csv::{StringRecord, WriterBuilder};
use sqlparser::ast::AlterTableOperation;

use crate::database::{self, database_loader, database_navigator::{get_table_data_path, get_table_schema_path}, types::{Column, Database, TableSchema}, utils::get_headers_from_table_schema};
use crate::storage_engine::{index::{index_manager, index_updater}, select::{table_reader, utils::get_column_indices}};
use crate::shared::{errors::Error, file_manager::write_json_into_file};

use super::validator;

/*
 * Function to add, delete or change table columns in bulk.
 * It will be reworked in the future
 */
pub async fn handle_bulk_operations(table_name: &String, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(), Error> {
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    
    // Validate operations and get schema types from it
    let (delete_columns_names, old_column_names, changed_columns, new_columns) = validator::validate_bulk_operations(table_name, table_schema, operations, database)?;
    let new_columns_names: Vec<String> = new_columns.iter().map(|col| col.name.clone()).collect();
    
    // Update table schema
    let mut new_table_schema = update_table_schema_in_bulk(table_schema, &delete_columns_names, &old_column_names, &changed_columns, &new_columns)?;
    let table_schema_file_path = get_table_schema_path(&schema_name, table_name);
    write_json_into_file(&table_schema_file_path, &new_table_schema)?;

    // Get headers from schemas and indices of deleted columns
    let old_headers = get_headers_from_table_schema(&table_schema);
    let new_headers = get_headers_from_table_schema(&new_table_schema);
    let deleted_columns_indices = get_column_indices(&old_headers, &delete_columns_names);

    // Read current records into memory
    let mut records = table_reader::read_table(&schema_name, table_name, &None, true).await?;

    // Update data file
    update_table_data_in_bulk(&mut records, &schema_name, table_name, &new_headers, &deleted_columns_indices, &new_columns_names)?;

    // Create indexes if necessary (deferring offset update to last function)
    for column in changed_columns {
        if column.is_indexed {
            index_manager::create_index(&schema_name, table_name, &column.name, &mut new_table_schema, false, false, &records).await?;
        }
    }

    // Update indexes
    index_updater::update_indexes_on_update_or_delete(&records, &schema_name, table_name, &new_table_schema)?;

    // Reload table schema
    database_loader::reload_table_schema(&schema_name, table_name).await?;

    Ok(())
}

fn update_table_schema_in_bulk(table_schema: &TableSchema, delete_column_names: &Vec<String>, old_column_names: &Vec<String>, changed_columns: &Vec<Column>, new_columns: &Vec<Column>) -> Result<TableSchema, Error> {
    // Remove deleted columns from the schema
    let mut updated_columns: Vec<Column> = table_schema.columns.iter()
        .filter(|col| !delete_column_names.contains(&col.name))
        .cloned().collect();

    // Update definitions for changed columns
    for (old_name, changed_col) in old_column_names.iter().zip(changed_columns.iter()) {
        if let Some(col) = updated_columns.iter_mut().find(|c| c.name == *old_name) {
            col.name = changed_col.name.clone();
            col.data_type = changed_col.data_type.clone();
            col.constraints = changed_col.constraints.clone();
            col.is_indexed = changed_col.is_indexed;
        }
    }

    // Add new columns to the schema
    updated_columns.extend(new_columns.iter().cloned());

    // Adjust order
    for (index, column) in updated_columns.iter_mut().enumerate() {
        column.order = index;
    }

    // Construct a new TableSchema with the updated columns
    let new_table_schema = TableSchema {
        columns: updated_columns,
        ..table_schema.clone()
    };

    Ok(new_table_schema)
}


fn update_table_data_in_bulk(records: &mut Vec<StringRecord>, schema_name: &String, table_name: &String, new_headers: &Vec<String>, deleted_columns_indices: &Vec<usize>, new_columns_names: &Vec<String>) -> Result<(), Error> {
    // Start rewriting CSV file
    let table_data_file_path = get_table_data_path(&schema_name, table_name);
    let modified_file = OpenOptions::new()
        .write(true).truncate(true).create(true).open(table_data_file_path)
        .map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(modified_file);

    // Write new headers
    wtr.write_record(new_headers)
        .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Rewrite records
    for record in records.iter_mut() {
        // Convert StringRecord to Vec<String> for easier manipulation
        let mut fields: Vec<String> = record.iter().map(|field| field.to_string()).collect();

        // Delete columns
        for &index in deleted_columns_indices.iter().rev() {
            if index < fields.len() {
                fields.remove(index);
            }
        }

        // No operation on changed columns

        // Add new columns
        // TODO: Check for default values in the future
        let new_columns_indices: Vec<usize> = (fields.len()..(fields.len() + new_columns_names.len())).collect();
        for _ in new_columns_indices {
            fields.push(String::from("Null"));
        }

        // Update record
        record.clear();
        for field in &fields {
            record.push_field(field);
        }

        // Write record
        wtr.write_record(&*record)
            .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;
    }

    wtr.flush().map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    Ok(())
}