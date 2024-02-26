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

pub fn get_rows_from_row_offsets(row_offsets: &Vec<u64>, schema_name: &String, table_name: &String) -> Result<Vec<String>, Error> {
    let data_file_path = get_table_data_path(schema_name, table_name);
    let file = File::open(data_file_path)?;
    let mut reader = BufReader::new(file);
    let mut rows: Vec<String> = Vec::new();
    
    for row_offset in row_offsets {
        reader.seek(SeekFrom::Start(*row_offset))?;

        let mut row = String::new();
        let mut buffer = [0; 1];

        // Read until encountering end of line
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 || buffer[0] == b'\n' {
                break;
            }
            row.push(buffer[0] as char);
        }

        rows.push(row);
    }

    Ok(rows)
}


pub fn get_rows_from_row_offsets_2(row_offsets: &Vec<u64>, schema_name: &String, table_name: &String) -> Result<Vec<StringRecord>, Error> {
    let data_file_path = get_table_data_path(schema_name, table_name);
    let file = File::open(data_file_path).map_err(|e| Error::IOError(e))?;
    let mut file_reader = BufReader::new(file);
    let mut rows: Vec<StringRecord> = Vec::new();

    for &offset in row_offsets {
        file_reader.seek(SeekFrom::Start(offset)).map_err(|e| Error::IOError(e))?;

        // Using a temporary buffer to read lines
        let mut line = String::new();
        file_reader.read_line(&mut line).map_err(|e| Error::IOError(e))?;

        // Parse the line into a StringRecord
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(line.as_bytes());
        for result in rdr.records() {
            let record = result?; // Handle potential CSV error
            rows.push(record);
            break; // Assuming each line is a single record
        }
    }

    Ok(rows)
}