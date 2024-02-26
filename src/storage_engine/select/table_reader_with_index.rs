use csv::{ReaderBuilder, StringRecord};
use std::collections::HashMap;
use std::fs::File;

use crate::database::database_navigator::get_table_data_path;
use crate::database::types::Database;
use crate::database::utils::find_database_table;
use crate::shared::errors::Error;
use crate::storage_engine::filters::filter_checker::apply_filters;
use crate::storage_engine::filters::filter_manager;
use crate::storage_engine::index::index_reader;
use crate::storage_engine::select::utils;
use crate::storage_engine::types::SelectParameters;

use super::validator;

pub async fn read_table(
    params: SelectParameters,
    database: &Database,
    filter_columns: &Vec<String>,
) -> Result<String, Error> {
    let SelectParameters {table_name, columns, filters, order_column_name, ascending, limit_value: limit } = params;

    let table_schema = match find_database_table(database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    // Perform validation before reading the table
    validator::validate_select_query(database, &table_name, &columns, &order_column_name)?;

    let schema_name = String::from("schema_1");
    let rows_index = index_reader::read_rows_index(&schema_name, &table_name)?;
    
    let restricted_rows = filter_manager::get_restricted_rows(&filter_columns, &rows_index, &schema_name, &table_name)?;
    
    let mut row_offsets: Vec<u64> = Vec::new();

    for (row_index, row) in restricted_rows.iter().enumerate() {
        let is_hit = apply_filters(row, &filter_columns, filters.as_ref())?;
        if is_hit {
            row_offsets.push(rows_index.row_offsets[row_index]);
        }
    }

    println!("WITH INDEX: {:?}\n", row_offsets);
    
    let results = index_reader::get_rows_from_row_offsets_2(&row_offsets, &schema_name, &table_name)?;
    
    // Read from file
    let file_path = get_table_data_path(&database.configuration.default_schema, &table_name);
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = match rdr.headers() {
        Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
        Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    };
    let indices = utils::get_column_indices(&headers, &columns);

    let mut rows: Vec<StringRecord> = results.iter()
        .map(|record| utils::select_fields(record, &indices))
        .collect();

        // Sort
    if let Some(column_name) = order_column_name {
        let column_index = headers.iter().position(|header| header == &column_name)
                                  .ok_or_else(|| Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.clone() })?;
        sort_records(&mut rows, column_index, ascending);
    }

    // Apply limit
    let rows: Vec<StringRecord> = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();
    
    format_response(rows, headers, indices)
}


fn sort_records(records: &mut Vec<StringRecord>, column_index: usize, ascending: bool) {
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
