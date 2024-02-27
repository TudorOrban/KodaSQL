use crate::{database::types::{Column, DataType, InsertedRowColumn, TableSchema}, shared::errors::Error};


pub fn validate_column_types(table_schema: &TableSchema, inserted_rows: &Vec<Vec<InsertedRowColumn>>) -> Result<(), Error> {
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

pub fn validate_value_type(column: &Column, insert_value: &String) -> Result<(), Error> {
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