use sqlparser::ast::{Expr, Ident, ObjectName, Query, SetExpr, Value, Values};
use csv::WriterBuilder;
use std::fs::OpenOptions;

use crate::database::database_loader::get_database;
use crate::database::database_navigator::get_table_data_path;
use crate::database::types::{DataType, Database, InsertedRowColumn};
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::database::types::{Column, TableSchema};
use crate::storage_engine::validation::common::does_table_exist;

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

    let inserted_rows = extract_inserted_rows(source, &column_names)?;

    validate_rows_types(table_schema, &inserted_rows)?;

    // TODO: Validate constraints and fill unspecified values with NULL or default

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


fn extract_inserted_rows(source: &Option<Box<Query>>, column_names: &[String]) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    let mut all_rows_values = Vec::new();

    if let Some(query) = source {
        if let SetExpr::Values(Values { rows, .. }) = &*query.body {
            for row in rows {
                let mut row_values = Vec::new();
                for (i, expr) in row.iter().enumerate() {
                    let value_str = match expr {
                        Expr::Value(val) => value_to_string(val),
                        _ => "".to_string(),
                    };

                    if let Some(column_name) = column_names.get(i) {
                        row_values.push(InsertedRowColumn {
                            name: column_name.clone(),
                            value: value_str,
                        });
                    } else {
                        return Err(Error::GenericUnsupported);
                    }
                }
                all_rows_values.push(row_values);
            }
        }
    }

    Ok(all_rows_values)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Number(n, _) => n.clone(),
        Value::SingleQuotedString(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        _ => "".to_string(),
    }
}