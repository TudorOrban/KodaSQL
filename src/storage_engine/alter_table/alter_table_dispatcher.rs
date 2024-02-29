use sqlparser::ast::{AlterTableOperation, ObjectName};

use crate::{database::database_loader, shared::errors::Error};

use super::handle_bulk_operations::handle_bulk_operations;


pub async fn dispatch_alter_table_statement(name: &ObjectName, operations: &Vec<AlterTableOperation>) -> Result<String, Error> {
    let database = database_loader::get_database()?;

    let bulk_operations: Vec<AlterTableOperation> = operations.into_iter().filter(|op| bulk_operation_policy(op)).cloned().collect();
    let other_operations: Vec<AlterTableOperation> = operations.iter().filter(|op| !bulk_operation_policy(op)).cloned().collect();

    let mut responses: Vec<String> = Vec::new();

    handle_bulk_operations(name, &bulk_operations).await?;



    

    for operation in other_operations {
        match operation {
            // AlterTableOperation::AddColumn { column_keyword, if_not_exists, column_def } => {    
            //     // let response = create_column::create_column().await?;
            //     // responses.push(response);
            // }
            // AlterTableOperation::DropColumn { column_name, if_exists, cascade } => {
            //     // let response = delete_column::delete_column().await?;
            //     // responses.push(response);
            // }
            _ => return Err(Error::NotSupportedUpdateTableOperation)
        }
    }

    
    Ok(format!("Success: the table has been altered successfully."))
}

fn bulk_operation_policy(operation: &AlterTableOperation) -> bool {
    match operation {
        AlterTableOperation::AddColumn { .. } => true,
        AlterTableOperation::DropColumn { .. } => true,
        AlterTableOperation::ChangeColumn { .. } => true,
        AlterTableOperation::RenameColumn { .. } => true,
        _ => false
    }
}