use std::collections::HashSet;

use crate::{database::types::{Column, Constraint, InsertedRowColumn, TableSchema}, shared::{errors::Error, utils::transpose_matrix}, storage_engine::{index::index_reader, insert::utils}};


pub fn validate_column_constraints(inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_schema: &TableSchema, complete: bool) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    let mut complete_inserted_rows_transposed: Vec<Vec<InsertedRowColumn>> = Vec::new();

    for column in table_schema.columns.clone() {
        let inserted_column_values = utils::get_inserted_column_values_from_rows(&inserted_rows, &column.name)?;
        
        // Skip completing if complete flag is false (for update)
        let complete_column_values = if complete {
            validate_null_and_default_constraints(&column, &inserted_column_values)?
        } else {
            inserted_column_values.into_iter().filter_map(|x| x).collect()
        };

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

    // Get column values
    let column_index = index_reader::read_column_index(schema_name, &table_schema.name, &column.name)?;
    let column_values = index_reader::get_column_values_from_index(&column_index, schema_name, &table_schema.name)?;

    let mut values_set = HashSet::new();

    // Check for duplicates among insert_values
    for value in inserted_column_values {
        if !values_set.insert(value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column.name.clone(), value: value.clone() })
        }
    }

    // Check for duplicates with column values
    for value in column_values {
        if values_set.contains(&value) {
            return Err(Error::ColumnUniquenessNotSatisfied { column_name: column.name.clone(), value: value.clone() })
        }
    }

    // TODO: Return column_values to avoid another read

    Ok(())
}