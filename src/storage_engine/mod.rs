use csv::{ReaderBuilder, StringRecord};
use std::error::Error;
use std::fs::File;

pub fn select_columns(table_name: &str, columns: Vec<&str>) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    let file_path = format!("data/{}.csv", table_name);
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    
    let headers = rdr.headers()?.clone();

    // Trim spaces
    let trimmed_headers: Vec<String> = headers.iter().map(|header| header.trim().to_string()).collect();

    // Identify indices of the requested columns
    let indices: Vec<usize> = trimmed_headers.iter().enumerate().filter_map(|(i, name)| {
        if columns.contains(&name.as_str()) {
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
