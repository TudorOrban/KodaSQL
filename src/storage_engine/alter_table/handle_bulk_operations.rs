use sqlparser::ast::{AlterTableOperation, ColumnDef, ColumnOptionDef, ObjectName};

use crate::{database::{self, types::{Column, Database, TableSchema}}, shared::errors::Error, storage_engine::validation::{self, common::{validate_column_doesnt_exist, validate_column_exists}}};

pub async fn handle_bulk_operations(table_name: &String, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(), Error> {
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    
    validate_bulk_operations(table_name, table_schema, operations, database)?;

    Ok(())
}

fn validate_bulk_operations(table_name: &String, table_schema: &TableSchema, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(), Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;
    
    let delete_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::DropColumn { .. })).cloned().collect();
    let new_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::AddColumn { .. })).cloned().collect();
    let changed_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::ChangeColumn { .. })).cloned().collect();
    let renamed_columns_ops: Vec<AlterTableOperation> = operations.iter().filter(|op| matches!(**op, AlterTableOperation::RenameColumn { .. })).cloned().collect();

    // Establishing order of operations
    let index_after_delete = table_schema.columns.len() - delete_columns_ops.len() - renamed_columns_ops.len() - changed_columns_ops.len();
    let index_after_rename = index_after_delete + renamed_columns_ops.len();
    let index_after_changed = index_after_rename + changed_columns_ops.len();
    let index_after_new = index_after_changed + new_columns_ops.len();
    
    let changed_columns_order: Vec<usize> = (index_after_rename..index_after_changed).collect();
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

    // Validate columns to be renamed exist
    for op in renamed_columns_ops.iter() {
        match op {
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                validate_column_exists(&table_schema, &old_column_name.value)?;
            }
            _ => continue
        }
    }

    // Validate columns to be changed exist
    for op in renamed_columns_ops.iter() {
        match op {
            AlterTableOperation::ChangeColumn { old_name, new_name, data_type, options } => {
                validate_column_exists(&table_schema, &old_name.value)?;
            }
            _ => continue
        }
    }

    // Validate column definitions for columns to be changed
    
    // let new_columns_definitions = new_columns_ops.iter().filter_map(|op| {
    //     if let AlterTableOperation::ChangeColumn { old_name, new_name, data_type, options } = op {
    //         let mut options: Vec<&ColumnOptionDef> = Vec::new();
    //         // options.push()
    //         let column_def = ColumnDef {
    //             name: new_name.clone(),
    //             data_type: data_type.clone(),
    //             collation: None,
    //             options: &options.clone()
    //         };
    //         Some(column_def)
    //     } else {
    //         None
    //     }
    // }).cloned().collect();

    // let changed_columns = validation::common::validate_column_definitions(&new_columns_definitions, &new_columns_order)?;


    // Validate column definitions for new columns 
    let changed_columns_order: Vec<usize> = (index_after_rename..index_after_changed).collect();
    let new_columns_order: Vec<usize> = (index_after_changed..index_after_new).collect();
    
    let new_columns_definitions = new_columns_ops.iter().filter_map(|op| {
        if let AlterTableOperation::AddColumn { column_def, .. } = op {
            Some(column_def)
        } else {
            None
        }
    }).cloned().collect();

    let new_columns = validation::common::validate_column_definitions(&new_columns_definitions, &new_columns_order)?;

    


    Ok(())
}