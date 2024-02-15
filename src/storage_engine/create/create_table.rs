use std::{error::Error, f32::consts::E, fs::File};
use std::io::prelude::*;

use serde_json::json;
use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType, ObjectName};

use crate::schema::{constants, Column, Constraint as CustomConstraint, DataType as CustomDataType, TableSchema};

pub async fn create_table(name: &ObjectName, columns: &Vec<ColumnDef>) -> Result<(), Box<dyn Error>> {
    let first_identifier = name.0.first().ok_or_else(|| "The table name has not been supplied")?;
    let table_name = &first_identifier.value;
    println!("Table name: {:?}", table_name);
    
    // Create table schema
    let mut schema_columns: Vec<Column> = Vec::new();

    println!("Got to for loop");
    for column in columns {
        let data_type = match get_column_custom_data_type(&column.data_type, &column.name.value) {
            Ok(data_type) => data_type,
            Err(e) => {
                println!("Error getting custom data type for column {}: {:?}", column.name.value, e);
                continue; 
            }
        };
    
        let constraints = match get_column_custom_constraints(&column.options, &column.name.value) {
            Ok(constraints) => constraints,
            Err(e) => {
                println!("Error getting custom constraints for column {}: {:?}", column.name.value, e);
                continue;
            }
        };
    
        let new_column = Column {
            name: column.name.value.clone(),
            data_type: data_type,
            constraints: constraints,
        };
    
        println!("Got to after some");
        schema_columns.push(new_column);
    }
    
    let schema = TableSchema {
        name: table_name.clone(),
        columns: schema_columns
    };

    let schema_json = json!(schema);
    println!("Schema json: {:?}", schema_json.to_string());

    // Create schema file
    let filepath = format!("{}/schemas/{}.schema.json", constants::DATABASE_DIR, table_name);
    let mut file = File::create(filepath).map_err(|e| Box::new(e) as Box<dyn Error>)?;

    file.write_all(schema_json.to_string().as_bytes())?;
    file.flush()?;

    Ok(())
}

fn get_column_custom_data_type(column_type: &DataType, column_name: &String) -> Result<CustomDataType, Box<dyn Error>> {
    match column_type {
        DataType::Int(_) => {
            return Ok(CustomDataType::Integer);
        },
        _ => return Err(format!("Column type for {} is not supported.", column_name).into()),
    }    
}

fn get_column_custom_constraints(column_constraints: &Vec<ColumnOptionDef>, column_name: &String) -> Result<Vec<CustomConstraint>, Box<dyn Error>> {
    let mut custom_constraints: Vec<CustomConstraint> = Vec::new();

    for constraint in column_constraints {
        match constraint.option {
            ColumnOption::NotNull => {
                custom_constraints.push(CustomConstraint::NotNull);
            },
            _ => return Err(format!("The constraint for column").into())
        }
    }

    Ok(custom_constraints)
}

pub async fn validate_create_table(table_name: &String, columns: &Vec<ColumnDef>) {
    
}