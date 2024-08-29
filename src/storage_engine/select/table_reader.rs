use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;
use std::fs::File;

use crate::database::database_navigator::{self, get_table_data_path};
use crate::database::types::TableSchema;
use crate::shared::errors::Error;
use crate::shared::file_manager;
use crate::storage_engine::filters::filter_manager;
use crate::storage_engine::index::index_reader;

pub async fn read_table(
    schema_name: &String,
    table_name: &String,
    filters: &Option<Expr>,
    include: bool,
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

    // Perform filtering and select specified fields
    let rows = filter_manager::filter_all_records(&mut rdr, &headers, filters, include)?;
    
    Ok(rows)
}


pub async fn read_table_with_indexes(
    schema_name: &String,
    table_name: &String,
    filters: &Option<Expr>,
    filter_columns: &Vec<String>,
    include: bool,
) -> Result<Vec<StringRecord>, Error> {
    let rows_index = index_reader::read_rows_index(&schema_name, &table_name)?;
    
    let restricted_rows = index_reader::get_restricted_rows(&filter_columns, &rows_index, &schema_name, &table_name)?;
    
    let row_offsets = filter_manager::filter_row_offsets(&restricted_rows, filters, rows_index, filter_columns, include)?;
    
    let rows = index_reader::get_rows_from_row_offsets(&row_offsets, &schema_name, &table_name)?;
    
    Ok(rows)
}

pub async fn read_column_values(
    schema_name: &String,
    table_name: &String,
    column_name: &String,
) -> Result<Vec<String>, Error> {
    let table_schema_file_path = database_navigator::get_table_schema_path(schema_name, &table_name);
    let table_schema = file_manager::read_json_file::<TableSchema>(&table_schema_file_path)?;
    let column = table_schema.columns.iter().find(|col| &col.name == column_name).ok_or(Error::ColumnDoesNotExist { table_name: table_name.clone(), column_name: column_name.clone() })?;

    if column.is_indexed {
        let index = index_reader::read_column_index(&schema_name, &table_name, &column_name)?;
        
        let column_values = index_reader::get_column_values_from_index(&index, &schema_name, &table_name)?;
        
        Ok(column_values)
    } else {
        let rows = read_table(&schema_name, &table_name, &None, true).await?;
        
        let column_index = table_schema.columns.iter().position(|col| &col.name == column_name).unwrap();
        let column_values = rows.iter().map(|row| row.get(column_index).unwrap().to_string()).collect::<Vec<String>>();
        
        Ok(column_values)
    }
}
