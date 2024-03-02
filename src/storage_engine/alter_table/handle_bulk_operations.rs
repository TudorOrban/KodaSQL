use std::fs::OpenOptions;

use csv::WriterBuilder;
use sqlparser::ast::AlterTableOperation;

use crate::database::{self, database_navigator::{get_table_data_path, get_table_schema_path}, types::{Column, Database, TableSchema}, utils::get_headers_from_table_schema};
use crate::storage_engine::{index::{index_manager, index_updater}, select::{table_reader, utils::get_column_indices}, utils::ast_unwrapper, validation::{self, common::validate_column_exists}};
use crate::shared::{errors::Error, file_manager::write_json_into_file};

/*
 * Work in progress
 */
pub async fn handle_bulk_operations(table_name: &String, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(), Error> {
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    
    // Validate operations and get schema types from it
    let (delete_columns_names, old_column_names, changed_columns, new_columns) = validate_bulk_operations(table_name, table_schema, operations, database)?;
    let new_columns_names: Vec<String> = new_columns.iter().map(|col| col.name.clone()).collect();
    
    // Update table schema
    let mut new_table_schema = update_table_schema_in_bulk(table_schema, &delete_columns_names, &old_column_names, &changed_columns, &new_columns)?;
    let table_schema_file_path = get_table_schema_path(&schema_name, table_name);
    write_json_into_file(&table_schema_file_path, &new_table_schema)?;

    // Get headers from new schema and delete indices
    let old_headers = get_headers_from_table_schema(&table_schema);
    let new_headers = get_headers_from_table_schema(&new_table_schema);
    let deleted_columns_indices = get_column_indices(&old_headers, &delete_columns_names);
    println!("Headers: {:?}, delete indices: {:?}", new_headers, deleted_columns_indices);
    

    // Read current records into memory
    let mut records = table_reader::read_table(&schema_name, table_name, &None, true).await?;

    // Start rewriting table data file
    let table_data_file_path = get_table_data_path(&schema_name, table_name);
    let modified_file = OpenOptions::new()
        .write(true).truncate(true).create(true).open(table_data_file_path)
        .map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(modified_file);

    // Write updated headers
    wtr.write_record(&new_headers)
        .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Rewrite records
    for record in &mut records {
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

        // Convert back to StringRecord
        println!("Old record: {:?}", record);
        record.clear();
        for field in &fields {
            record.push_field(field);
        }
        println!("New record: {:?}", record);

        // Write record
        wtr.write_record(&*record)
            .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;
    }

    wtr.flush().map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Create indexes if necessary (deferring offset update to last function)
    for column in changed_columns {
        if column.is_indexed {
            index_manager::create_index(&schema_name, table_name, &column.name, &mut new_table_schema, false, false, &records).await?;
        }
    }

    // Update indexes
    index_updater::update_indexes_on_update_or_delete(&records, &schema_name, table_name, &new_table_schema)?;

    Ok(())
}

fn validate_bulk_operations(table_name: &String, table_schema: &TableSchema, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(Vec<String>, Vec<String>, Vec<Column>, Vec<Column>), Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;
    
    let delete_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::DropColumn { .. })).cloned().collect();
    let new_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::AddColumn { .. })).cloned().collect();
    let changed_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::ChangeColumn { .. })).cloned().collect();
    
    // Validate columns to be deleted exist
    let mut delete_column_names: Vec<String> = Vec::new();

    for op in delete_columns_ops.iter() {
        if let AlterTableOperation::DropColumn { column_name, .. } = op {
            validate_column_exists(&table_schema, &column_name.value)?;
            delete_column_names.push(column_name.value.clone());
        }
    }

    // Validate columns to be changed exist
    for op in changed_columns_ops.iter() {
        match op {
            AlterTableOperation::ChangeColumn { old_name, .. } => {
                validate_column_exists(&table_schema, &old_name.value)?;
            }
            _ => continue
        }
    }

    // Validate column definitions for columns to be changed
    let (old_changed_column_names, changed_columns_definitions) = ast_unwrapper::get_column_definitions_from_change_columns_ops(&changed_columns_ops);
    let changed_columns = validation::common::validate_column_definitions(&changed_columns_definitions, &(0..changed_columns_definitions.len()).collect())?;
    
    // Validate column definitions for new columns 
    let new_columns_definitions = new_columns_ops.iter().filter_map(|op| {
        if let AlterTableOperation::AddColumn { column_def, .. } = op {
            Some(column_def)
        } else {
            None
        }
    }).cloned().collect();
    let new_columns = validation::common::validate_column_definitions(&new_columns_definitions, &(0..new_columns_definitions.len()).collect())?;

    Ok((delete_column_names, old_changed_column_names, changed_columns, new_columns))
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


// fn update_table_data_in_bulk() -> Result<(), Error> {

//     Ok(())
// }




    // Establishing order of operations
    // let index_after_delete = table_schema.columns.len() - delete_columns_ops.len() - changed_columns_ops.len();
    // let index_after_changed = index_after_delete + changed_columns_ops.len();
    // let index_after_new = index_after_changed + new_columns_ops.len();
    
    // let changed_columns_order: Vec<usize> = (index_after_delete..index_after_changed).collect();
    // let new_columns_order: Vec<usize> = (index_after_changed..index_after_new).collect();