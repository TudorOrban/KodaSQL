use std::{error::Error as StdError, fs::File};
use std::io::prelude::*;
use serde_json::json;
use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType, ObjectName};

use crate::{server::SCHEMAS, shared::errors::Error};
use crate::schema::{constants, Column, Constraint as CustomConstraint, DataType as CustomDataType, TableSchema};

pub async fn create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<(), Box<dyn StdError>> {
    // Validate query and get schema
    let schema = validate_create_table(name, columns).await?;
    
    let schema_json = json!(schema);
    println!("Schema json: {:?}", schema_json.to_string());

    // Create schema file
    let schema_filepath = format!("{}/schemas/{}.schema.json", constants::DATABASE_DIR, schema.name);
    let mut schema_file = File::create(schema_filepath).map_err(|e| Box::new(e) as Box<dyn StdError>)?;

    schema_file.write_all(schema_json.to_string().as_bytes())?;
    schema_file.flush()?;

    // Create data file
    let data_filepath = format!("{}/data/{}.csv", constants::DATABASE_DIR, schema.name);
    let data_file_headers = schema.columns.into_iter().map(|column| column.name).collect::<Vec<String>>().join(",");
    
    let mut data_file = File::create(data_filepath).map_err(|e| Box::new(e) as Box<dyn StdError>)?;
    data_file.write_all(data_file_headers.as_bytes())?;
    data_file.flush()?;

    Ok(())
}

pub async fn validate_create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<TableSchema, Error> {
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
        let data_type = get_column_custom_data_type(&column.data_type, &column.name.value)?;
        let constraints = get_column_custom_constraints(&column.options, &column.name.value)?;

        schema_columns.push(Column {
            name: column.name.value.clone(),
            data_type,
            constraints
        });
    }

    Ok(TableSchema { name: table_name, columns: schema_columns })
}

fn get_column_custom_data_type(column_type: &DataType, column_name: &String) -> Result<CustomDataType, Error> {
    match column_type {
        DataType::Int(_) => {
            return Ok(CustomDataType::Integer);
        },
        _ => return Err(Error::UnsupportedColumnDataType { column_name: column_name.clone(), column_type: format!("{:?}", column_type) }),
    }    
}

fn get_column_custom_constraints(column_constraints: &Vec<ColumnOptionDef>, column_name: &String) -> Result<Vec<CustomConstraint>, Error> {
    let mut custom_constraints: Vec<CustomConstraint> = Vec::new();

    for constraint in column_constraints {
        match constraint.option {
            ColumnOption::NotNull => custom_constraints.push(CustomConstraint::NotNull),
            _ => return Err(Error::UnsupportedConstraint { column_name: column_name.clone(), column_constraint: format!("{:?}", constraint.option) })
        }
    }

    Ok(custom_constraints)
}