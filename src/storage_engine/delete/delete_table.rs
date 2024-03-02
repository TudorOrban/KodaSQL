use std::fs;

use sqlparser::ast::ObjectName;

use crate::{database::{database_loader, database_navigator::{get_schema_configuration_path, get_table_path}, types::SchemaConfiguration}, shared::{errors::Error, file_manager}};


pub async fn delete_table(names: &Vec<ObjectName>) -> Result<String, Error> {
    // Get database blueprint
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    
    let table_name = get_table_name(names)?;

    // Delete table directory
    let table_dir_file_path = get_table_path(&schema_name, &table_name);
    fs::remove_dir_all(table_dir_file_path).map_err(|e| Error::IOError(e))?;

    // Update schema configuration
    let schema_configuration_file_path = get_schema_configuration_path(&schema_name);
    let mut schema_configuration = file_manager::read_json_file::<SchemaConfiguration>(&schema_configuration_file_path)?;
    schema_configuration.tables = schema_configuration.tables.into_iter().filter(|table| table != &table_name).collect();
    file_manager::write_json_into_file(&schema_configuration_file_path, &schema_configuration)?;

    database_loader::reload_schema(&schema_name).await?;
    
    Ok(format!("Success: table {} has been deleted.", table_name))
}

pub fn get_table_name(names: &Vec<ObjectName>) -> Result<String, Error> {
    if let Some(first_name) = names.first() {
        if let Some(first_ident) = first_name.0.first() {
            Ok(first_ident.value.clone())
        } else {
            Err(Error::MissingTableName)
        }
    } else {
        Err(Error::MissingTableName)
    }
}