use sqlparser::ast::{AlterTableOperation, ObjectName, TableConstraint};

use crate::{database::database_loader, shared::errors::Error, storage_engine::foreign_key::foreign_key_manager};

use super::handle_bulk_operations::handle_bulk_operations;


pub async fn dispatch_alter_table_statement(name: &ObjectName, operations: &Vec<AlterTableOperation>) -> Result<String, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    // Get database blueprint
    let database = database_loader::get_database()?;

    // Divide operations into bulk and other
    let bulk_operations: Vec<AlterTableOperation> = operations.into_iter().filter(|op| bulk_operation_strategy(op)).cloned().collect();
    let other_operations: Vec<AlterTableOperation> = operations.iter().filter(|op| !bulk_operation_strategy(op)).cloned().collect();

    // Handle bulk operations
    // handle_bulk_operations(&table_name, &bulk_operations, &database).await?;

    // Handle other operations
    for operation in other_operations {
        match operation {
            // TODO: Add support for RLS, triggers etc
            AlterTableOperation::AddConstraint(table_constraint) => {
                dispatch_add_constraint_statement(&table_name, table_constraint)?;
            }
            _ => return Err(Error::NotSupportedUpdateTableOperation)
        }
    }
    
    Ok(format!("Success: the table has been altered successfully."))
}

fn bulk_operation_strategy(operation: &AlterTableOperation) -> bool {
    match operation {
        AlterTableOperation::AddColumn { .. } => true,
        AlterTableOperation::DropColumn { .. } => true,
        AlterTableOperation::ChangeColumn { .. } => true,
        // TODO: Add AlterColumn and RenameColumn here
        _ => false
    }
}

fn dispatch_add_constraint_statement(table_name: &String, table_constraint: TableConstraint) -> Result<String, Error> {
    println!("Add constraint: {:?}", table_constraint);
    
    match table_constraint {
        TableConstraint::ForeignKey { name, columns, foreign_table, referred_columns, on_delete, on_update, characteristics } => {
            foreign_key_manager::handle_add_foreign_key(name, columns, foreign_table, referred_columns, on_delete, on_update, characteristics)?;
        },
        _ => return Err(Error::UnsupportedConstraint { column_name: table_name.clone(), column_constraint: table_constraint.to_string() })
    }





    Ok(String::from(""))
}