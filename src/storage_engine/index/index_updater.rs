use csv::StringRecord;

use crate::{database::{database_navigator::{get_table_index_path, get_table_row_index_path}, types::{Index, InsertedRowColumn, RowsIndex, TableSchema}}, shared::{errors::Error, file_manager}};

use super::{index_reader::{read_column_index, read_rows_index}, offset_counter};

pub fn add_index_offsets_on_insert(complete_inserted_rows: &Vec<Vec<InsertedRowColumn>>, schema_name: &String, table_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let mut rows_index = read_rows_index(schema_name, table_name)?;
    let end_of_file_offset = match rows_index.row_offsets.last() {
        Some(&offset) => offset,
        None => 0
    };

    let (new_column_value_offsets, mut new_row_offsets) = offset_counter::compute_insertion_offsets(complete_inserted_rows, end_of_file_offset, table_schema.columns.len());

    // Update rows index
    rows_index.row_offsets.append(&mut new_row_offsets);
    let row_index_file_path = get_table_row_index_path(&schema_name, &table_name);
    file_manager::write_json_into_file(&row_index_file_path, &rows_index)?;

    // Update column indexes
    for (column_index, column_offsets) in new_column_value_offsets.iter().enumerate() {
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
        let column_index_file_path = get_table_index_path(schema_name, table_name, column_name);
        file_manager::write_json_into_file(&column_index_file_path, &column_index)?;
    }

  Ok(())
}

pub fn update_indexes_on_delete(records: &Vec<StringRecord>, schema_name: &String, table_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let (new_column_value_offsets, new_row_offsets) = offset_counter::compute_records_offsets(records, table_schema);

    // Update rows index
    let row_index_file_path = get_table_index_path(schema_name, &table_name, &String::from("row_offsets"));
    let rows_index = RowsIndex {
        row_offsets: new_row_offsets
    };
    file_manager::write_json_into_file(&row_index_file_path, &rows_index)?;

    // Update column indexes
    for (column_index, column_offsets) in new_column_value_offsets.iter().enumerate() {
        let column = match table_schema.columns.get(column_index) {
            Some(col) => col,
            None => continue
        };
        if !column.is_indexed {
            continue;
        }

        let column_name = &column.name;
        let column_index = Index {
            key: column_name.clone(),
            offsets: column_offsets.to_vec()
        };
        let column_index_file_path = get_table_index_path(schema_name, &table_name, column_name);
        file_manager::write_json_into_file(&column_index_file_path, &column_index)?;
    }

    Ok(())
}