use std::collections::HashMap;
use std::fs::{self};
use serde_json::json;
use sqlparser::ast::{ColumnDef, ObjectName};

use crate::database::database_navigator::{get_table_data_path, get_table_index_path, get_table_path, get_table_schema_path};
use crate::database::types::{Constraint, Index};
use crate::database::utils;
use crate::shared::errors::Error;
use crate::database::database_loader::DATABASE;
use crate::database::{constants, types::{Column, TableSchema}};
use crate::storage_engine::validation::common::does_table_exist;

pub async fn create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<String, Error> {
    // Get default schema from memory (scoping to minimize lock duration)
    let default_schema_name;
    {
        let database = DATABASE.lock().unwrap();
        default_schema_name = database.configuration.default_schema.clone();
    }

    // Validate query and get table schema
    let table_schema = validate_create_table(name, columns).await?;
    let table_schema_json = json!(table_schema);
    println!("Schema json: {:?}", table_schema_json.to_string());

    // Create table directory
    create_table_folders(&default_schema_name, &table_schema.name).await?;

    create_table_files(&default_schema_name, &table_schema).await?;

    // Create index files for PrimaryKey/Unique columns
    create_indexes(&default_schema_name, table_schema).await
}

async fn validate_create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<TableSchema, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    // Ensure table doesn't already exist
    let database = &*DATABASE.lock().unwrap();
    if does_table_exist(database, &table_name) {
        return Err(Error::TableNameAlreadyExists { table_name: table_name });
    }

    // Validate query columns and transform to custom schema types
    let mut schema_columns: Vec<Column> = Vec::new();
    for column in columns {
        let data_type = utils::get_column_custom_data_type(&column.data_type, &column.name.value)?;
        let constraints = utils::get_column_custom_constraints(&column.options, &column.name.value)?;
        let is_indexed = index_strategy(&constraints);

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
    let table_data_headers = table_schema.columns.iter().map(|column| column.name.as_str()).collect::<Vec<&str>>().join(",");
    fs::write(&table_data_filepath, table_data_headers.as_bytes()).map_err(Error::IOError)?;

    Ok(())
}

async fn create_indexes(schema_name: &String, table_schema: TableSchema) -> Result<String, Error> {
    let indexable_columns = table_schema.columns.iter()
        .filter(|col| index_strategy(&col.constraints));

    for column in indexable_columns {
        let index_filepath = get_table_index_path(schema_name, &table_schema.name, &column.name);
        let index = Index { key: column.name.clone(), offsets: HashMap::new() };
        let index_json = serde_json::to_string(&index)?;
        fs::write(&index_filepath, index_json.as_bytes())?;
    }

    Ok(String::from(""))
}

fn index_strategy(constraints: &Vec<Constraint>) -> bool {
    constraints.contains(&Constraint::PrimaryKey) || constraints.contains(&Constraint::Unique)
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