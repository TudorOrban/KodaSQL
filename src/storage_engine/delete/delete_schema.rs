use std::fs;

use sqlparser::ast::ObjectName;

use crate::{database::{database_navigator::{get_database_configuration_path, get_schema_path}, types::DatabaseConfiguration}, shared::{errors::Error, file_manager}};

pub async fn delete_schema(names: &Vec<ObjectName>) -> Result<String, Error> {
    let schema_name = get_schema_name(names)?;

    // Delete schema directory
    let schema_dir_file_path = get_schema_path(&schema_name);
    fs::remove_dir_all(schema_dir_file_path).map_err(|e| Error::IOError(e))?;

    // Update schema configuration
    let database_configuration_file_path = get_database_configuration_path();
    let mut database_configuration = file_manager::read_json_file::<DatabaseConfiguration>(&database_configuration_file_path)?;
    database_configuration.schemas = database_configuration.schemas.into_iter().filter(|schema| schema != &schema_name).collect();
    file_manager::write_json_into_file(&database_configuration_file_path, &database_configuration)?;

    // TODO: Deal with default schema; reload database 

    Ok(format!("Success: The schema {} has been deleted.", schema_name))
}

pub fn get_schema_name(names: &Vec<ObjectName>) -> Result<String, Error> {
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