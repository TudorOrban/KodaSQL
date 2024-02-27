use csv::StringRecord;
use sqlparser::ast::Query;

use crate::{database::{self, database_loader, utils::get_headers_from_table_schema}, shared::errors::Error, storage_engine::{filters::filter_column_finder, select::table_reader, types::SelectParameters, utils::ast_unwrapper}};

use super::{record_handler, utils, validator};

pub async fn handle_select(query: &Query) -> Result<String, Error> {
    let SelectParameters {table_name, columns, filters, order_column_name, ascending, limit_value } = ast_unwrapper::unwrap_select_query(query)?;

    // Prepare: get database blueprint and necessary data from it
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    let headers = get_headers_from_table_schema(table_schema);
    let column_indices = utils::get_column_indices(&headers, &columns);
    
    // Validate query
    validator::validate_select_query(&database, &table_name, &columns, &order_column_name)?;
    
    // Get columns used for filtering
    let filter_columns = filter_column_finder::find_filter_columns(&filters)?;

    // Use indexes only if all filter columns are indexed
    let use_indexes = filter_column_finder::use_indexes(&filter_columns, table_schema);
    
    // Read from table and filter
    let filtered_records = if use_indexes {
        table_reader::read_table_with_indexes(&schema_name, &table_name, &filters, &filter_columns, true).await?
    } else {
        table_reader::read_table(&schema_name, &table_name, &filters, true).await?
    };

    // Select specified columns
    let mut rows_with_selected_fields: Vec<StringRecord> = filtered_records.iter()
        .map(|record| record_handler::select_fields(record, &column_indices))
        .collect();

    // Sort
    if let Some(column_name) = order_column_name {
        let column_index = headers.iter().position(|header| header == &column_name)
                                    .ok_or_else(|| Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_schema.name.clone() })?;
        record_handler::sort_records(&mut rows_with_selected_fields, column_index, ascending);
    }

    // Apply limit
    let rows: Vec<StringRecord> = rows_with_selected_fields.into_iter().take(limit_value.unwrap_or(usize::MAX)).collect();
    
    record_handler::format_response(rows, headers, column_indices)
}