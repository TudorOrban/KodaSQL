use sqlparser::ast::{Ident, ObjectName, Query};
use std::collections::HashSet;

use crate::database::types::{Constraint, DataType, Database, InsertedRowColumn};
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::database::types::{Column, TableSchema};
use crate::shared::utils::transpose_matrix;
use crate::storage_engine::index::index_reader;
use crate::storage_engine::validation::common::does_table_exist;

use super::utils;

pub fn validate_insert_into(database: &Database, name: &ObjectName, columns: &Vec<Ident>, source: &Option<Box<Query>>) -> Result<(String, Vec<String>, Vec<Vec<InsertedRowColumn>>), Error> {
    // Unwrap table name
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    
    // Validate table exists
    if !does_table_exist(database, &table_name) {
        return Err(Error::TableDoesNotExist { table_name: table_name });
    }

    let table_schema = match find_database_table(database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    
    // Unwrap columns
    let column_names: Vec<String> = columns
        .iter()
        .map(|ident| ident.value.clone())
        .collect();

    let inserted_rows = utils::extract_inserted_rows(source, &column_names)?;

    validate_rows_types(table_schema, &inserted_rows)?;

    let complete_inserted_rows = validate_rows_constraints(&inserted_rows, &database.configuration.default_schema, table_schema)?;

    Ok((table_name, column_names, complete_inserted_rows))
}

// Types
fn validate_rows_types(table_schema: &TableSchema, inserted_rows: &Vec<Vec<InsertedRowColumn>>) -> Result<(), Error> {
    for row in inserted_rows {
        for inserted_column in row {
            if let Some(schema_column) = table_schema.columns.iter().find(|c| c.name == inserted_column.name) {
                validate_value_type(schema_column, &inserted_column.value)?;
            } else {
                return Err(Error::ColumnDoesNotExist {
                    column_name: inserted_column.name.clone(),
                    table_name: table_schema.name.clone(),
                });
            }
        }
    }

    Ok(())
}

fn validate_value_type(column: &Column, insert_value: &String) -> Result<(), Error> {
    match column.data_type {
        DataType::Integer => {
            if insert_value.parse::<i64>().is_err() {
                return Err(Error::ColumnTypeDoesNotMatch {
                    column_name: column.name.clone(),
                });
            }
        },
        DataType::Float => {
            if insert_value.parse::<f64>().is_err() {
                return Err(Error::ColumnTypeDoesNotMatch {
                    column_name: column.name.clone(),
                });
            }
        },
        DataType::Text => {
        },
        DataType::Boolean => {
            if insert_value.parse::<bool>().is_err() {
                return Err(Error::ColumnTypeDoesNotMatch {
                    column_name: column.name.clone(),
                });
            }
        },
    }

    Ok(())
}


// Constraints
fn validate_rows_constraints(inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_schema: &TableSchema) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    let mut complete_inserted_rows_transposed: Vec<Vec<InsertedRowColumn>> = Vec::new();

    for column in table_schema.columns.clone() {
        let inserted_column_values = utils::get_inserted_column_values_from_rows(&inserted_rows, &column.name)?;
        
        let complete_column_values = validate_null_and_default_constraints(&column, &inserted_column_values)?;
        
        validate_uniqueness_constraint(&column, schema_name, table_schema, &complete_column_values)?;
        
        let complete_column_row_values = complete_column_values.iter()
            .map(|value| InsertedRowColumn {
                name: column.name.clone(),
                value: value.clone(),
            })
            .collect::<Vec<InsertedRowColumn>>(); // TODO: Improve this in the future

        complete_inserted_rows_transposed.push(complete_column_row_values); 
    }

    // Transpose 
    let complete_inserted_rows = transpose_matrix(complete_inserted_rows_transposed);

    Ok(complete_inserted_rows)
}

// - Null values
fn validate_null_and_default_constraints(column: &Column, inserted_column_values: &Vec<Option<String>>) -> Result<Vec<String>, Error> {
    let mut complete_column_values: Vec<String> = Vec::new();

    let is_not_null = column.constraints.contains(&Constraint::NotNull);
    let default_value = column.constraints.iter().find_map(|constraint| {
        if let Constraint::DefaultValue(value) = constraint {
            Some(value.clone())
        } else {
            None
        }
    });

    // Insert value if it exists, otherwise default value, otherwise Null if no Not Null constraint
    for inserted_value in inserted_column_values {
        match inserted_value {
            Some(value) => complete_column_values.push(value.clone()),
            None => {
                match &default_value {
                    Some(default_value) => {
                        complete_column_values.push(default_value.clone());
                    },
                    None => {
                        if is_not_null {
                            return Err(Error::ColumnNotNull { column_name: column.name.clone() });
                        } else {
                            complete_column_values.push(String::from("Null"));
                        }
                    }
                }
            }
        }
    }

    Ok(complete_column_values)
}


// - Uniqueness
fn validate_uniqueness_constraint(column: &Column, schema_name: &String, table_schema: &TableSchema, inserted_column_values: &Vec<String>) -> Result<(), Error> {
    let is_unique_constraint = column.constraints.contains(&Constraint::Unique) || column.constraints.contains(&Constraint::PrimaryKey);
    if !is_unique_constraint {
        return Ok(());
    }

    let column_index = index_reader::read_column_index(schema_name, &table_schema.name, &column.name)?;
    let column_values = index_reader::get_column_values_from_index(&column_index, schema_name, &table_schema.name)?;

    validate_column_uniqueness(&inserted_column_values, &column_values, &column.name)?;

    Ok(())
}

fn validate_column_uniqueness(inserted_column_values: &Vec<String>, column_values: &Vec<String>, column_name: &String) -> Result<(), Error> {
    let mut values_set = HashSet::new();

    // Check for duplicates among insert_values
    for value in inserted_column_values {
        if !values_set.insert(value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column_name.clone(), value: value.clone() })
        }
    }

    // Check for duplicates with column values
    for value in column_values {
        if values_set.contains(value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column_name.clone(), value: value.clone() })
        }
    }

    Ok(())
}
