use sqlparser::ast::AlterTableOperation;

use crate::{database::types::{Column, Database, TableSchema}, shared::errors::Error, storage_engine::{utils::ast_unwrapper, validation}};


pub fn validate_bulk_operations(table_name: &String, table_schema: &TableSchema, operations: &Vec<AlterTableOperation>, database: &Database) -> Result<(Vec<String>, Vec<String>, Vec<Column>, Vec<Column>), Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;
    
    // Divide operations: delete, new, changed
    let delete_columns_ops: Vec<AlterTableOperation> = operations.iter()
        .filter(|op| matches!(**op, AlterTableOperation::DropColumn { .. })).cloned().collect();
    let new_columns_ops: Vec<AlterTableOperation> = operations.iter()
        .filter(|op| matches!(**op, AlterTableOperation::AddColumn { .. })).cloned().collect();
    let changed_columns_ops: Vec<AlterTableOperation> = operations.iter()
        .filter(|op| matches!(**op, AlterTableOperation::ChangeColumn { .. })).cloned().collect();
    
    // Validate columns to be deleted exist
    let mut delete_column_names: Vec<String> = Vec::new();
    for op in delete_columns_ops.iter() {
        if let AlterTableOperation::DropColumn { column_name, .. } = op {
            validation::common::validate_column_exists(&table_schema, &column_name.value)?;
            delete_column_names.push(column_name.value.clone());
        }
    }

    // Validate columns to be changed exist
    for op in changed_columns_ops.iter() {
        match op {
            AlterTableOperation::ChangeColumn { old_name, .. } => {
                validation::common::validate_column_exists(&table_schema, &old_name.value)?;
            }
            _ => continue
        }
    }

    // Validate column definitions for columns to be changed
    let (old_changed_column_names, changed_columns_definitions) = ast_unwrapper::get_column_definitions_from_change_columns_ops(&changed_columns_ops);
    let changed_columns = validation::common::validate_column_definitions(
        &changed_columns_definitions, 
        &(0..changed_columns_definitions.len()).collect(),
    )?;
    
    // Validate column definitions for new columns 
    let new_columns_definitions = new_columns_ops.iter().filter_map(|op| {
        if let AlterTableOperation::AddColumn { column_def, .. } = op {
            Some(column_def)
        } else {
            None
        }
    }).cloned().collect();
    let new_columns = validation::common::validate_column_definitions(
        &new_columns_definitions, 
        &(0..new_columns_definitions.len()).collect(),
    )?;

    Ok((delete_column_names, old_changed_column_names, changed_columns, new_columns))
}