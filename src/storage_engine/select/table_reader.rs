use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;
use std::fs::File;

use crate::database::database_navigator::get_table_data_path;
use crate::shared::errors::Error;
use crate::storage_engine::filters::filter_manager;
use crate::storage_engine::select::utils;

pub async fn read_table(
    schema_name: &String,
    table_name: &String,
    columns: &Vec<String>,
    filters: Option<Expr>,
) -> Result<Vec<StringRecord>, Error> {
    // Read from file
    let file_path = get_table_data_path(schema_name, &table_name);
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = match rdr.headers() {
        Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
        Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    };
    let indices = utils::get_column_indices(&headers, &columns);

    // Perform filtering and select specified fields
    let rows = filter_manager::filter_all_records(&mut rdr, &headers, &filters, &indices)?;
    
    Ok(rows)
}