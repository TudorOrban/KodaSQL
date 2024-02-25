use std::{fs::File, io::{BufReader, Read, Seek, SeekFrom}};

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

        // Read until encountering comma or end of file
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