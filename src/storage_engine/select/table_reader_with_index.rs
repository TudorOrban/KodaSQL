use csv::StringRecord;
use sqlparser::ast::Expr;
use crate::shared::errors::Error;
use crate::storage_engine::filters::filter_manager::apply_filters;
use crate::storage_engine::index::index_reader;
use crate::storage_engine::select::utils;

pub async fn read_table(
    schema_name: &String,
    table_name: &String,
    filters: Option<Expr>,
    filter_columns: &Vec<String>,
    column_indices: &Vec<usize>
) -> Result<Vec<StringRecord>, Error> {
    let rows_index = index_reader::read_rows_index(&schema_name, &table_name)?;
    
    let restricted_rows = index_reader::get_restricted_rows(&filter_columns, &rows_index, &schema_name, &table_name)?;
    
    let mut row_offsets: Vec<u64> = Vec::new();

    for (row_index, row) in restricted_rows.iter().enumerate() {
        let is_hit = apply_filters(row, &filter_columns, filters.as_ref())?;
        if is_hit {
            row_offsets.push(rows_index.row_offsets[row_index]);
        }
    }
    
    let rows = index_reader::get_rows_from_row_offsets(&row_offsets, &schema_name, &table_name)?;
    
    let rows_with_selected_fields: Vec<StringRecord> = rows.iter()
        .map(|record| utils::select_fields(record, column_indices))
        .collect();

    Ok(rows_with_selected_fields)
}

