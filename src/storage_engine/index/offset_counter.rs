use csv::StringRecord;

use crate::database::types::{InsertedRowColumn, TableSchema};


pub fn compute_insertion_offsets(complete_inserted_rows: &Vec<Vec<InsertedRowColumn>>, end_of_file_offset: u64, number_of_columns: usize) -> (Vec<Vec<u64>>, Vec<u64>) {
    let mut current_offset = end_of_file_offset;
    let mut row_offsets = Vec::new();
    let mut column_offsets: Vec<Vec<u64>> = vec![Vec::new(); number_of_columns];

    for row in complete_inserted_rows.iter() {
        let mut row_byte_length: u64 = 0;

        for (col_index, col) in row.iter().enumerate() {
            if col_index > 0 {
                row_byte_length += 1; // For the comma
            }

            // Compute column value offset
            column_offsets[col_index].push(current_offset + row_byte_length);
            row_byte_length += col.value.as_bytes().len() as u64;
        }

        // Compute row offset
        row_byte_length += 1; // For the newline
        current_offset += row_byte_length;
        row_offsets.push(current_offset);
    }

    (column_offsets, row_offsets)
}

pub fn compute_records_offsets(records: &Vec<StringRecord>, table_schema: &TableSchema) -> (Vec<Vec<u64>>, Vec<u64>) {
    let number_of_columns = table_schema.columns.len();
    let mut current_offset = compute_headers_offset(&table_schema);
    let mut row_offsets: Vec<u64> = Vec::new();
    let mut column_offsets: Vec<Vec<u64>> = vec![Vec::new(); number_of_columns];
    row_offsets.push(current_offset);

    for record in records.iter() {
        let mut row_byte_length: u64 = 0;

        for (col_index, field) in record.iter().enumerate() {
            if col_index > 0 {
                row_byte_length += 1; // For the comma
            }

            // Compute column value offset
            column_offsets[col_index].push(current_offset + row_byte_length);
            row_byte_length += field.as_bytes().len() as u64;
        }
        
        // Compute row offset
        row_byte_length += 1; // For the newline
        current_offset += row_byte_length; 
        row_offsets.push(current_offset);
    }

    (column_offsets, row_offsets)
}

pub fn compute_headers_offset(table_schema: &TableSchema) -> u64 {
    let headers = table_schema.columns.iter()
        .map(|col| col.name.clone()).collect::<Vec<String>>().join(",");

    headers.as_bytes().len() as u64 + 1 // For the newline
}