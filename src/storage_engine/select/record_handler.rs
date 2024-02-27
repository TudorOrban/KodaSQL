use std::{collections::HashMap, fs::OpenOptions};

use csv::{ReaderBuilder, StringRecord, Writer};

use crate::{database::database_navigator::get_table_data_path, shared::errors::Error};

// Select columns
pub fn select_fields(record: &StringRecord, indices: &[usize]) -> StringRecord {
    let selected_fields: Vec<String> = indices.iter()
        .filter_map(|&i| record.get(i).map(|s| s.trim().to_string()))
        .collect();
    StringRecord::from(selected_fields)
}

// Sort
pub fn sort_records(records: &mut Vec<StringRecord>, column_index: usize, ascending: bool) {
    records.sort_by(|a, b| {
        let a_val = a.get(column_index).unwrap_or_default();
        let b_val = b.get(column_index).unwrap_or_default();

        if ascending {
            a_val.cmp(&b_val)
        } else {
            b_val.cmp(&a_val)
        }
    });
}

// Attach column keys to rows and serialize
pub fn format_response(rows: Vec<StringRecord>, selected_headers: Vec<String>, indices: Vec<usize>) -> Result<String, Error> {
    let mut structured_rows: Vec<HashMap<String, String>> = Vec::new();
    
    for row in rows {
        let mut row_map: HashMap<String, String> = HashMap::new();
        indices.iter().enumerate().for_each(|(i, &index)| {
            if let Some(value) = row.get(i) {
                let header = &selected_headers[index]; // Correctly map selected headers based on indices
                row_map.insert(header.clone(), value.to_string());
            }
        });
        structured_rows.push(row_map);
    }

    serde_json::to_string(&structured_rows)
        .map_err(|e| Error::SerdeJsonError(e))
}


pub fn rewrite_records(records: &Vec<StringRecord>, schema_name: &String, table_name: &String) -> Result<(), Error> {
    // Read from file
    let file_path = get_table_data_path(schema_name, table_name);
    let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&file_path)?;
    let headers = rdr.headers()?.clone(); // Clone the headers to use them later for writing

    // Overwrite original file
    let file = OpenOptions::new().write(true).truncate(true).open(&file_path).map_err(|e| Error::IOError(e))?;
    let mut wtr = Writer::from_writer(file);
    wtr.write_record(&headers)?;

    // Step 4: Write records to the new file
    for record in records {
        wtr.write_record(record.iter())?;
    }

    wtr.flush()?;

    Ok(())
}