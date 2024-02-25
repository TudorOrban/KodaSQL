use std::fs::{self};
use serde_json::json;
use sqlparser::ast::{ColumnDef, ObjectName};

use crate::database::database_navigator::{get_table_data_path, get_table_path, get_table_schema_path};
use crate::database::types::Database;
use crate::database::utils;
use crate::shared::errors::Error;
use crate::database::database_loader;
use crate::database::types::{Column, TableSchema};
use crate::storage_engine::index::index_manager;
use crate::storage_engine::validation::common;

pub async fn create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<String, Error> {
    // Get database blueprint
    let database = database_loader::get_database()?;
    let default_schema_name = database.configuration.default_schema.clone();

    // Validate query and get table schema
    let table_schema = validate_create_table(&database, name, columns)?;
    
    create_table_folders(&default_schema_name, &table_schema.name).await?;

    create_table_files(&default_schema_name, &table_schema).await?;

    index_manager::create_indexes(&default_schema_name, &table_schema).await?;

    update_schema_configuration(&default_schema_name, &table_schema.name).await
}

fn validate_create_table(database: &Database, name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<TableSchema, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    // Ensure table doesn't already exist
    if common::does_table_exist(database, &table_name) {
        return Err(Error::TableNameAlreadyExists { table_name: table_name });
    }

    // Validate query columns and transform to custom schema types
    let mut schema_columns: Vec<Column> = Vec::new();
    for column in columns {
        let data_type = utils::get_column_custom_data_type(&column.data_type, &column.name.value)?;
        let constraints = utils::get_column_custom_constraints(&column.options, &column.name.value)?;
        let is_indexed = index_manager::index_strategy(&constraints);

        // TODO: Validate there exists exactly one Primary Key

        schema_columns.push(Column {
            name: column.name.value.clone(),
            data_type,
            constraints,
            is_indexed,
        });
    }

    Ok(TableSchema { name: table_name, columns: schema_columns })
}

async fn create_table_folders(schema_name: &String, table_name: &String) -> Result<(), Error> {
    let table_filepath = get_table_path(schema_name, table_name);
    for folder in vec!["table_schema", "data", "indexes"] {
        let folder_filepath = format!("{}/{}", table_filepath, folder);
        fs::create_dir_all(folder_filepath)?;
    }
    Ok(())
}

async fn create_table_files(schema_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    // Table schema file
    let table_schema_filepath = get_table_schema_path(schema_name, &table_schema.name);
    let table_schema_json = json!(table_schema);
    fs::write(&table_schema_filepath, table_schema_json.to_string().as_bytes()).map_err(Error::IOError)?;
    
    // Table data file
    let table_data_filepath = get_table_data_path(schema_name, &table_schema.name);
    let table_data_headers = table_schema.columns.iter().map(|column| column.name.as_str()).collect::<Vec<&str>>().join(",") + "\n";
    fs::write(&table_data_filepath, table_data_headers.as_bytes()).map_err(Error::IOError)?;

    Ok(())
}

async fn update_schema_configuration(schema_name: &String, table_name: &String) -> Result<String, Error> {
    let mut schema_config = database_loader::load_schema_configuration(schema_name).await?;
    
    if !schema_config.tables.contains(table_name) {
        schema_config.tables.push(table_name.clone());
    }

    database_loader::save_schema_configuration(&schema_name, &schema_config).await?;

    // TODO: Reload schema into memory
    
    Ok(String::from(""))
}

/*
 * Unit tests
 */
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::server::load_table_schemas;

//     #[tokio::test]
//     async fn test_validate_create_table() {
//         // Prepare
//         // Load table schema for validation
//         let result = load_table_schemas().await;
//         assert!(result.is_ok());

//         // Mock the necessary data
//         let name = mock_object_name("new_table");
//         let columns = vec![
//             mock_column_def("id", DataType::Int(None)),
//             mock_column_def("name", DataType::Int(None)),
//         ];

//         // Act
//         let result = validate_create_table(&name, &columns).await;
        
//         // Assert
//         assert!(result.is_ok());

//         if let Ok(schema) = result {
//             assert_eq!(schema.name, "new_table");
//             assert_eq!(schema.columns.len(), 2);
//         }
//     }


//     // Utils
//     use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName};

//     fn mock_object_name(name: &str) -> ObjectName {
//         ObjectName(vec![Ident::new(name)])
//     }

//     fn mock_column_def(name: &str, data_type: DataType) -> ColumnDef {
//         ColumnDef {
//             name: Ident::new(name),
//             data_type,
//             collation: None,
//             options: vec![],
//         }
//     }
// }