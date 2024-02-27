use sqlparser::ast::{Ident, ObjectName, Query};

use crate::database::types::{Database, InsertedRowColumn};
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::storage_engine::validation;
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

    validation::column_types::validate_column_types(table_schema, &inserted_rows)?;

    let complete_inserted_rows = validation::column_constraints::validate_column_constraints(&inserted_rows, &database.configuration.default_schema, table_schema, true)?;

    Ok((table_name, column_names, complete_inserted_rows))
}