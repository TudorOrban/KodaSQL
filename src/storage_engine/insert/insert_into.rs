use sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Value, Values};
use csv::WriterBuilder;
use std::collections::HashSet;
use std::fs::OpenOptions;

use crate::database::database_loader::get_database;
use crate::database::database_navigator::get_table_data_path;
use crate::database::types::{Constraint, DataType, Database, InsertedRowColumn};
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::database::types::{Column, TableSchema};
use crate::storage_engine::index::index_reader;
use crate::storage_engine::validation::common::does_table_exist;

use super::utils;

pub async fn insert_into_table(name: &ObjectName, columns: &Vec<Ident>, source: &Option<Box<Query>>) -> Result<String, Error> {
    // Get database blueprint
    let database = get_database()?;
    
    // Validate insert
    let (table_name, _, inserted_rows) = validate_insert_into(&database, name, columns, source)?;

    // Open CSV file in append mode
    let file_path = get_table_data_path(&database.configuration.default_schema, &table_name);
    let modified_file = OpenOptions::new()
        .write(true).append(true).open(file_path)
        .map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(modified_file);

    // Iterate over inserted_rows and write to CSV
    for row in inserted_rows {
        let row_value: Vec<String> = row.into_iter().map(|r| r.value).collect();
        wtr.write_record(&row_value)
            .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;
    }

    wtr.flush().map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    Ok(table_name)
}

fn validate_insert_into(database: &Database, name: &ObjectName, columns: &Vec<Ident>, source: &Option<Box<Query>>) -> Result<(String, Vec<String>, Vec<Vec<InsertedRowColumn>>), Error> {
    // Unwrap table name
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    println!("Tablename: {}", table_name);
    
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

    validate_rows_constraints(&inserted_rows, &String::from("schema_1"), table_schema)?;

    Ok((table_name, column_names, inserted_rows))
}

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



fn validate_rows_constraints(inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    for column in table_schema.columns.clone() {
        let is_unique_constraint = column.constraints.contains(&Constraint::Unique) || column.constraints.contains(&Constraint::PrimaryKey);
        if !is_unique_constraint {
            continue;
        }

        let column_index = index_reader::read_column_index(schema_name, &table_schema.name, &column.name)?;
        let column_values = index_reader::get_column_values_from_index(&column_index, schema_name, &table_schema.name)?;
        let inserted_column_values = utils::get_inserted_column_values_from_rows(inserted_rows, &column.name)?;

        validate_column_uniqueness(&inserted_column_values, &column_values, &column.name)?;
    }


    // TODO: Validate other constraints and fill values with NULL, default, AUTO_INCREMENT, NOW etc

    Ok(())
}

fn validate_column_uniqueness(inserted_column_values: &Vec<String>, column_values: &Vec<String>, column_name: &String) -> Result<(), Error> {
    let mut values_set = HashSet::new();

    // Check for duplicates among insert_values
    for value in inserted_column_values {
        if !values_set.insert(value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column_name.clone() })
        }
    }

    // Check for duplicates with column values
    for value in column_values {
        if values_set.contains(value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column_name.clone() })
        }
    }

    Ok(())
}
