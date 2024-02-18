use sqlparser::ast::{ObjectName, Ident, Query, Expr, SelectItem};
use csv::{ReaderBuilder, StringRecord};
use std::fs::File;
use std::io::BufReader;

use crate::database::database_loader::DATABASE;
use crate::shared::errors::Error;
use crate::storage_engine::select::utils;
use crate::database::{constants, types::{Column, TableSchema}};
use crate::storage_engine::validation::common::does_table_exist;

pub async fn insert_into_table(name: &ObjectName, columns: &Vec<Ident>, source: &Option<Box<Query>>) -> Result<String, Error> {
    let result = validate_insert_into(name, columns).await?;



    Ok(result)
}

async fn validate_insert_into(name: &ObjectName, columns: &Vec<Ident>) -> Result<String, Error> {
    // Unwrap table name
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();
    println!("Tablename: {}", table_name);

    // Unwrap columns
    let column_strings: Vec<String> = columns
        .iter()
        .map(|ident| ident.value.clone())
        .collect();
    
    // Validate table exists
    let database = &*DATABASE.lock().unwrap();
    if !does_table_exist(database, &table_name) {
        return Err(Error::TableDoesNotExist { table_name: table_name });
    }

    // Read from schema file and deserialize
    let table_schema_path = format!("{}/schemas/{}.schema.json", constants::DATABASE_DIR, table_name);
    let file = File::open(table_schema_path).map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;
    let reader = BufReader::new(file);
    let table_schema: TableSchema = serde_json::from_reader(reader).map_err(|e| {
        eprintln!("Failed to deserialize schema file: {:?}", e);
        Error::SerdeJsonError(e) 
    })?;

    println!("{:?}", table_schema);

    for column in table_schema.columns {
        if !column_strings.contains(&column.name) {

        }
    }
    // Vaildate column types



    // // Trim spaces in CSV file and find indices
    // let headers = match rdr.headers() {
    //     Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
    //     Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    // };
    // let indices = utils::get_column_indices(&headers, &column_strings);

    // Validate query columns and transform to custom schema types
    // let mut schema_columns: Vec<Column> = Vec::new();

    // for column in columns {
    //     let data_type = get_column_custom_data_type(&column.data_type, &column.name.value)?;
    //     let constraints = get_column_custom_constraints(&column.options, &column.name.value)?;

    //     schema_columns.push(Column {
    //         name: column.name.value.clone(),
    //         data_type,
    //         constraints
    //     });
    // }

    Ok(table_name)
}