use sqlparser::ast::{AlterTableOperation, ObjectName};

use crate::{database::database_loader, shared::errors::Error};

use super::handle_bulk_operations::handle_bulk_operations;


pub async fn dispatch_alter_table_statement(name: &ObjectName, operations: &Vec<AlterTableOperation>) -> Result<String, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    let database = database_loader::get_database()?;
    // let schema_name = database.configuration.default_schema.clone();
    // let table_schema = match database::utils::find_database_table(&database, &table_name) {
    //     Some(schema) => schema,
    //     None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    // };
    // let mut table_schema_clone = table_schema.clone();

    let bulk_operations: Vec<AlterTableOperation> = operations.into_iter().filter(|op| bulk_operation_strategy(op)).cloned().collect();
    let other_operations: Vec<AlterTableOperation> = operations.iter().filter(|op| !bulk_operation_strategy(op)).cloned().collect();

    let mut responses: Vec<String> = Vec::new();

    handle_bulk_operations(&table_name, &bulk_operations, &database).await?;





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

fn bulk_operation_strategy(operation: &AlterTableOperation) -> bool {
    match operation {
        AlterTableOperation::AddColumn { .. } => true,
        AlterTableOperation::DropColumn { .. } => true,
        AlterTableOperation::ChangeColumn { .. } => true,
        _ => false
    }
}