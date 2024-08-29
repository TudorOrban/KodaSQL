use std::collections::HashSet;

use crate::{database::types::{Column, Constraint, InsertedRowColumn, TableSchema}, shared::{errors::Error, utils::transpose_matrix}, storage_engine::{index::index_reader, insert::utils, select::table_reader}};


pub async fn validate_column_constraints(inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_schema: &TableSchema, complete: bool) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    let mut complete_inserted_rows_transposed: Vec<Vec<InsertedRowColumn>> = Vec::new();

    for column in table_schema.columns.clone() {
        let inserted_column_values = utils::get_inserted_column_values_from_rows(&inserted_rows, &column.name)?;
        
        // Skip completing rows if complete flag is false (for update operation)
        let complete_column_values = if complete {
            validate_null_and_default_constraints(&column, &inserted_column_values).await?
        } else {
            inserted_column_values.into_iter().filter_map(|x| x).collect()
        };

        println!("Complete column values: {:?}", complete_column_values);
        validate_foreign_key_constraint(&column, schema_name, table_schema, &complete_column_values).await?;

        validate_uniqueness_constraint(&column, schema_name, table_schema, &complete_column_values).await?;
        
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
async fn validate_null_and_default_constraints(column: &Column, inserted_column_values: &Vec<Option<String>>) -> Result<Vec<String>, Error> {
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


// - Foreign key
async fn validate_foreign_key_constraint(column: &Column, schema_name: &String, table_schema: &TableSchema, inserted_column_values: &Vec<String>) -> Result<(), Error> {
    let foreign_key = table_schema.foreign_keys.iter().find(|foreign_key| foreign_key.local_columns.contains(&column.name));
    println!("Foreign key: {:?}", foreign_key);
    if let Some(foreign_key) = foreign_key {
        for column_name in &foreign_key.foreign_columns {
            let foreign_column_values = table_reader::read_column_values(schema_name, &foreign_key.foreign_table, column_name).await?;

            for value in inserted_column_values {
                if !foreign_column_values.contains(&value) {
                    return Err(Error::ForeignKeyConstraintNotSatisfied { foreign_key_name: foreign_key.name.clone(), });
                }
            }
        }
    }

    Ok(())
}

// - Uniqueness
async fn validate_uniqueness_constraint(column: &Column, schema_name: &String, table_schema: &TableSchema, inserted_column_values: &Vec<String>) -> Result<(), Error> {
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