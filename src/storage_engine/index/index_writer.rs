use std::fs::{self};

use crate::{database::{database_navigator::{get_table_index_path, get_table_row_index_path}, types::{InsertedRowColumn, TableSchema}}, shared::errors::Error};

use super::{index_reader::{read_column_index, read_rows_index}, offset_counter};

pub fn add_index_offsets_on_insert(complete_inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let mut rows_index = read_rows_index(schema_name, table_name)?;
    let end_of_file_offset = match rows_index.row_offsets.last() {
        Some(&offset) => offset,
        None => 0
    };

    let (all_offsets, mut new_row_offsets) = offset_counter::compute_insertion_offsets(complete_inserted_rows, end_of_file_offset, table_schema.columns.len());

    // Update rows index
    rows_index.row_offsets.append(&mut new_row_offsets);
    let row_index_string = serde_json::to_string(&rows_index)?;
    let row_index_file_path = get_table_row_index_path(&schema_name, &table_name);
    fs::write(row_index_file_path, row_index_string).map_err(|e| Error::IOError(e))?;

    // Update column indexes
    for (column_index, column_offsets) in all_offsets.iter().enumerate() {
        let column = match table_schema.columns.get(column_index) {
            Some(col) => col,
            None => continue
        };
        if !column.is_indexed {
            continue;
        }

        let column_name = &column.name;
        let mut column_index = read_column_index(schema_name, table_name, column_name)?;
        column_index.offsets.append(&mut column_offsets.clone());
        let column_index_string = serde_json::to_string(&column_index)?;
        let column_index_file_path = get_table_index_path(schema_name, table_name, column_name);
        fs::write(column_index_file_path, column_index_string).map_err(|e| Error::IOError(e))?;
    }

  Ok(())
}
