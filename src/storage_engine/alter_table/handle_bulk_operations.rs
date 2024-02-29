use sqlparser::ast::{AlterTableOperation, ObjectName};

use crate::shared::errors::Error;

pub async fn handle_bulk_operations(name: &ObjectName, operations: &Vec<AlterTableOperation>) -> Result<(), Error> {

    Ok(())
}