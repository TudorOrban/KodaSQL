use std::collections::HashMap;

use csv::StringRecord;

use crate::shared::errors::Error;

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
