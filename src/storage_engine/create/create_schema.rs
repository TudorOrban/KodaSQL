use std::{fs::{self, File}, io::Read};

use sqlparser::ast::SchemaName;

use crate::{database::{database_navigator::{get_database_configuration_path, get_schema_configuration_dir_path, get_schema_configuration_path, get_tables_dir_path}, types::{DatabaseConfiguration, SchemaConfiguration}}, shared::errors::Error};

pub async fn create_schema(schema_name: &SchemaName) -> Result<String, Error> {
    let schema_name_string = get_schema_name_string(schema_name)?;

    create_schema_folders_and_files(&schema_name_string)?;

    update_database_configuration(&schema_name_string)?;
    
    Ok(format!("Success: schema {} created.", schema_name_string))
}

fn get_schema_name_string(schema_name: &SchemaName) -> Result<String, Error> {
    match schema_name {
        SchemaName::Simple(object_name) => {
            if let Some(ident) = object_name.0.first() {
                Ok(ident.value.clone())
            } else {
                Err(Error::GenericUnsupported)
            }
        },
        _ => Err(Error::GenericUnsupported)
    }
}

fn create_schema_folders_and_files(schema_name_string: &String) -> Result<(), Error> {
    // Create folders
    let schema_tables_file_path = get_tables_dir_path(&schema_name_string);
    let schema_configuration_dir_file_path = get_schema_configuration_dir_path(&schema_name_string);
    fs::create_dir_all(schema_tables_file_path).map_err(|e| Error::IOError(e))?;
    fs::create_dir_all(schema_configuration_dir_file_path).map_err(|e| Error::IOError(e))?;

    // Create schema configuration
    let schema_configuration_file_path = get_schema_configuration_path(&schema_name_string);
    let schema_configuration = SchemaConfiguration {
        tables: Vec::new()
    };
    let schema_configuration_string = serde_json::to_string(&schema_configuration).map_err(|e| Error::SerdeJsonError(e))?;
    fs::write(schema_configuration_file_path, schema_configuration_string.as_bytes()).map_err(|e| Error::IOError(e))?;
    Ok(())
}

fn update_database_configuration(schema_name_string: &String) -> Result<(), Error> {
    // Read configuration
    let database_configuration_file_path = get_database_configuration_path();
    let mut file = File::open(&database_configuration_file_path).map_err(|e| Error::IOError(e))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    // Add new schema name
    let mut database_configuration: DatabaseConfiguration = serde_json::from_str(&contents).map_err(|e| Error::SerdeJsonError(e))?;
    database_configuration.schemas.push(schema_name_string.clone());
    let database_configuration_string = serde_json::to_string(&database_configuration).map_err(|e| Error::SerdeJsonError(e))?;

    fs::write(&database_configuration_file_path, database_configuration_string).map_err(|e| Error::IOError(e))?;

    Ok(())
}
