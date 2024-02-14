use sqlparser::ast::{SelectItem, TableFactor, Table};
use csv::{ReaderBuilder, StringRecord};
use std::error::Error;
use std::fs::File;

use crate::server::SCHEMAS;


pub async fn read_table(table_name: &String, columns: &Vec<String>) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    println!("Table: {:?}, columns: {:?}", table_name, columns);
    
    // Perform validation before reading the table
    validate_query(table_name, columns).await?;

    // Read from file
    let file_path = format!("data/{}.csv", table_name);
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    
    let headers = rdr.headers()?.clone();

    // Trim spaces
    let trimmed_headers: Vec<String> = headers.iter().map(|header| header.trim().to_string()).collect();

    // Identify indices of the requested columns
    let indices: Vec<usize> = trimmed_headers.iter().enumerate().filter_map(|(i, name)| {
        if columns.contains(&name) {
            Some(i)
        } else {
            None
        }
    }).collect();

    let mut rows: Vec<StringRecord> = Vec::new();

    for result in rdr.records() {
        let record = result?;
        
        // Create a new record only with the selected fields
        let selected_fields: Vec<String> = indices.iter().filter_map(|&i| record.get(i).map(|value| value.trim().to_string())).collect();

        // Construct a StringRecord from selected_fields
        if !selected_fields.is_empty() {
            let record = StringRecord::from(selected_fields);
            rows.push(record);
        }
    }

    Ok(rows)
}


pub async fn validate_query(table_name: &str, columns: &Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let schemas = SCHEMAS.lock().unwrap();

    // Validate table name
    let table_schema = schemas.get(table_name).ok_or_else(|| {
        format!("Table '{}' does not exist.", table_name)
    })?;

    // Assume valid columns if "*" present
    if columns.contains(&"*".to_string()) {
        return Ok(());
    }

    // Validate columns
    for column in columns {
        if !table_schema.columns.iter().any(|col| &col.name == column) {
            return Err(format!("Column '{}' does not exist in table '{}'.", column, table_name).into());
        }
    }

    Ok(())
}