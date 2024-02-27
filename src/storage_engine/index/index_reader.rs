use std::{fs::File, io::{BufReader, BufRead, Read, Seek, SeekFrom}};

use csv::{ReaderBuilder, StringRecord};

use crate::{database::{database_navigator::{get_table_data_path, get_table_index_path}, types::{Index, RowsIndex}}, shared::{errors::Error, file_manager}};

pub fn read_column_index(schema_name: &String, table_name: &String, column_name: &String) -> Result<Index, Error> {
    let file_path = get_table_index_path(schema_name, table_name, column_name);
    let index = file_manager::read_json_file::<Index>(&file_path)?;

    Ok(index)
}

pub fn read_rows_index(schema_name: &String, table_name: &String) -> Result<RowsIndex, Error> {
    let file_path = get_table_index_path(schema_name, table_name, &String::from("row_offsets"));
    let index = file_manager::read_json_file::<RowsIndex>(&file_path)?;

    Ok(index)
}

pub fn get_column_values_from_index(index: &Index, schema_name: &String, table_name: &String) -> Result<Vec<String>, Error> {
    let data_file_path = get_table_data_path(schema_name, table_name);
    let file = File::open(data_file_path)?;
    let mut reader = BufReader::new(file);
    let mut column_values: Vec<String> = Vec::new();

    for offset in &index.offsets {
        reader.seek(SeekFrom::Start(*offset))?;

        let mut column_value = String::new();
        let mut buffer = [0; 1];

        // Read until encountering comma or end of line
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 || buffer[0] == b',' || buffer[0] == b'\n' {
                break;
            }
            column_value.push(buffer[0] as char);
        }

        column_values.push(column_value);
    }

    Ok(column_values)   
}

pub fn get_rows_from_row_offsets(row_offsets: &Vec<u64>, schema_name: &String, table_name: &String) -> Result<Vec<StringRecord>, Error> {
    let data_file_path = get_table_data_path(schema_name, table_name);
    let file = File::open(data_file_path).map_err(|e| Error::IOError(e))?;
    let mut file_reader = BufReader::new(file);
    let mut rows: Vec<StringRecord> = Vec::new();

    for &offset in row_offsets {
        file_reader.seek(SeekFrom::Start(offset)).map_err(|e| Error::IOError(e))?;

        let mut line = String::new();
        file_reader.read_line(&mut line).map_err(|e| Error::IOError(e))?;

        // Parse the line into a StringRecord
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(line.as_bytes());
        for result in rdr.records() {
            let record = result?; // TODO: Better IO error handling
            rows.push(record);
            break;
        }
    }

    Ok(rows)
}

pub fn get_restricted_rows(filter_columns: &Vec<String>, rows_index: &RowsIndex, schema_name: &String, table_name: &String) -> Result<Vec<Vec<String>>, Error> {
    let number_of_rows = rows_index.row_offsets.len() - 1;
    let number_of_columns = filter_columns.len();

    // Initialize restricted_rows with the exact size needed
    let mut restricted_rows: Vec<Vec<String>> = vec![vec![String::new(); number_of_columns]; number_of_rows];

    for (col_index, column_name) in filter_columns.iter().enumerate() {
        let column_index = read_column_index(schema_name, table_name, column_name)?;
        let column_values = get_column_values_from_index(&column_index, schema_name, table_name)?;

        // Populate each row with the column's value
        for (row_index, value) in column_values.into_iter().enumerate() {
            if row_index < restricted_rows.len() {
                restricted_rows[row_index][col_index] = value;
            }
        }
    }

    Ok(restricted_rows)
}

// pub fn get_rows_from_row_offsets(row_offsets: &Vec<u64>, schema_name: &String, table_name: &String) -> Result<Vec<String>, Error> {
//     let data_file_path = get_table_data_path(schema_name, table_name);
//     let file = File::open(data_file_path)?;
//     let mut reader = BufReader::new(file);
//     let mut rows: Vec<String> = Vec::new();
    
//     for row_offset in row_offsets {
//         reader.seek(SeekFrom::Start(*row_offset))?;

//         let mut row = String::new();
//         let mut buffer = [0; 1];

//         // Read until encountering end of line
//         loop {
//             let bytes_read = reader.read(&mut buffer)?;
//             if bytes_read == 0 || buffer[0] == b'\n' {
//                 break;
//             }
//             row.push(buffer[0] as char);
//         }

//         rows.push(row);
//     }

//     Ok(rows)
// }