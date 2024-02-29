use csv::StringRecord;
use sqlparser::ast::{AlterTableOperation, ColumnDef, ColumnOptionDef, Ident, ObjectName};

use crate::{database::{self, types::{Column, Database, TableSchema}, utils::get_headers_from_table_schema}, shared::errors::Error, storage_engine::{index::index_updater, select::{table_reader, utils::get_column_indices}, utils::ast_unwrapper, validation::{self, common::{validate_column_doesnt_exist, validate_column_exists}}}};

/*
 * Work in progress
 */
pub async fn handle_bulk_operations(table_name: &String, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(), Error> {
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    let mut headers = get_headers_from_table_schema(&table_schema);
    
    let (changed_columns, new_columns) = validate_bulk_operations(table_name, table_schema, operations, database)?;

    let delete_columns_names: Vec<String> = Vec::new();
    let deleted_columns_indices = get_column_indices(&headers, &delete_columns_names);
    
    let new_columns_names: Vec<String> = Vec::new();

    // Update headers
    for &index in deleted_columns_indices.iter().rev() {
        if index < headers.len() {
            headers.remove(index);
        }
    }

    // for column in changed_columns.iter() {
    //     let column_index = headers.iter().position(|c| c == column.name);
    //     headers[column_index] = column_name;
    // }

    for new_column_name in new_columns_names.iter() {
        headers.push(new_column_name.clone());
    }

    let mut records = table_reader::read_table(&schema_name, table_name, &None, true).await?;

    for record in &mut records {
        // Convert StringRecord to Vec<String> for easier manipulation
        let mut fields: Vec<String> = record.iter().map(|field| field.to_string()).collect();

        // Delete columns
        for &index in deleted_columns_indices.iter().rev() {
            if index < fields.len() {
                fields.remove(index);
            }
        }

        // Add new columns
        // TODO: Check for default values in the future
        let new_columns_indices: Vec<usize> = (fields.len()..(fields.len() + new_columns_names.len())).collect();
        for _ in new_columns_indices {
            fields.push(String::from("Null"));
        }


        // Convert back to StringRecord
        let mut new_record = StringRecord::new();
        for field in fields.iter() {
            new_record.push_field(field);
        }

        // Write record
    }

    // Update indexes
    index_updater::update_indexes_on_update_or_delete(&records, &schema_name, table_name, table_schema)?;


    Ok(())
}

fn validate_bulk_operations(table_name: &String, table_schema: &TableSchema, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(Vec<Column>, Vec<Column>), Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;
    
    let delete_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::DropColumn { .. })).cloned().collect();
    let new_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::AddColumn { .. })).cloned().collect();
    let changed_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::ChangeColumn { .. })).cloned().collect();

    // Establishing order of operations
    let index_after_delete = table_schema.columns.len() - delete_columns_ops.len() - changed_columns_ops.len();
    let index_after_changed = index_after_delete + changed_columns_ops.len();
    let index_after_new = index_after_changed + new_columns_ops.len();
    
    let changed_columns_order: Vec<usize> = (index_after_delete..index_after_changed).collect();
    let new_columns_order: Vec<usize> = (index_after_changed..index_after_new).collect();
    
    // Validate columns to be deleted exist
    for op in delete_columns_ops.iter() {
        match op {
            AlterTableOperation::DropColumn { column_name, if_exists, cascade } => {
                validate_column_exists(&table_schema, &column_name.value)?;
            }
            _ => continue
        }
    }

    // Validate columns to be changed exist
    for op in changed_columns_ops.iter() {
        match op {
            AlterTableOperation::ChangeColumn { old_name, new_name, data_type, options } => {
                validate_column_exists(&table_schema, &old_name.value)?;
            }
            _ => continue
        }
    }

    // Validate column definitions for columns to be changed
    
    let changed_columns_definitions = ast_unwrapper::get_column_definitions_from_alter_table_ops(&new_columns_ops);
    
    
    let changed_columns = validation::common::validate_column_definitions(&changed_columns_definitions, &changed_columns_order)?;


    // Validate column definitions for new columns 
    
    let new_columns_definitions = new_columns_ops.iter().filter_map(|op| {
        if let AlterTableOperation::AddColumn { column_def, .. } = op {
            Some(column_def)
        } else {
            None
        }
    }).cloned().collect();

    let new_columns = validation::common::validate_column_definitions(&new_columns_definitions, &new_columns_order)?;

    


    Ok((changed_columns, new_columns))
}

fn update_table_schema_in_bulk() -> Result<(), Error> {

    Ok(())
}

fn update_table_data_in_bulk() -> Result<(), Error> {

    Ok(())
}