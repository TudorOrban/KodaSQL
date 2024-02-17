use std::collections::HashMap;
use std::fs::{self};
use serde_json::json;
use sqlparser::ast::{ColumnDef, ObjectName};

use crate::schema::types::{Constraint, Index};
use crate::schema::utils;
use crate::{server::SCHEMAS, shared::errors::Error};
use crate::schema::{constants, types::{Column, TableSchema}};

pub async fn create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<String, Error> {
    // Validate query and get schema
    let schema = validate_create_table(name, columns).await?;
    
    let schema_json = json!(schema);
    println!("Schema json: {:?}", schema_json.to_string());

    // Create schema file
    let schema_filepath = format!("{}/schemas/{}.schema.json", constants::DATABASE_DIR, schema.name);
    fs::write(&schema_filepath, schema_json.to_string().as_bytes()).map_err(Error::IOError)?;

    // Create data file
    let data_filepath = format!("{}/data/{}.csv", constants::DATABASE_DIR, schema.name);
    let data_file_headers = schema.columns.iter().map(|column| column.name.as_str()).collect::<Vec<&str>>().join(",");
    fs::write(&data_filepath, data_file_headers.as_bytes()).map_err(Error::IOError)?;

    // Create index files for PrimaryKey/Unique columns
    create_indexes(schema).await
}

async fn validate_create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<TableSchema, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    // Ensure table doesn't already exist
    let schemas = SCHEMAS.lock().unwrap();
    let does_table_exist = schemas.contains_key(&table_name);
    if does_table_exist {
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

async fn create_indexes(schema: TableSchema) -> Result<String, Error> {
    let indexable_columns = schema.columns.iter()
        .filter(|col| index_strategy(&col.constraints));

    for column in indexable_columns {
        let index_filepath = format!("{}/indexes/{}_{}.index.json", constants::DATABASE_DIR, schema.name, column.name); 
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::load_table_schemas;

    #[tokio::test]
    async fn test_validate_create_table() {
        // Prepare
        // Load table schema for validation
        let result = load_table_schemas().await;
        assert!(result.is_ok());

        // Mock the necessary data
        let name = mock_object_name("new_table");
        let columns = vec![
            mock_column_def("id", DataType::Int(None)),
            mock_column_def("name", DataType::Int(None)),
        ];

        // Act
        let result = validate_create_table(&name, &columns).await;
        
        // Assert
        assert!(result.is_ok());

        if let Ok(schema) = result {
            assert_eq!(schema.name, "new_table");
            assert_eq!(schema.columns.len(), 2);
        }
    }


    // Utils
    use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName};

    fn mock_object_name(name: &str) -> ObjectName {
        ObjectName(vec![Ident::new(name)])
    }

    fn mock_column_def(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident::new(name),
            data_type,
            collation: None,
            options: vec![],
        }
    }
}