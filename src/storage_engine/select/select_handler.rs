use sqlparser::ast::Query;

use crate::{database::{self, database_loader}, shared::errors::Error, storage_engine::{filters::{filter_checker, filter_manager}, select::table_reader, utils::ast_unwrapper::unwrap_select_query}};

use super::table_reader_with_index;

pub async fn handle_query(query: &Query) -> Result<String, Error> {
    let select_parameters = unwrap_select_query(query)?;

    // Decide whether to use indexes
    let filter_columns = filter_manager::find_filter_columns(&select_parameters.filters)?;

    let database = database_loader::get_database()?;
    let table_schema = match database::utils::find_database_table(&database, &select_parameters.table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: select_parameters.table_name.clone() }),
    };

    // Use indexes only if all filter columns are indexed
    let mut use_indexes = true;
    for column_name in &filter_columns {
        let corresp_column = table_schema.columns.iter().find(|column| &column.name == column_name);
        match corresp_column {
            Some(column) => {
                use_indexes = column.is_indexed;
            },
            None => continue,
        }
    }
    
    if use_indexes {
        table_reader_with_index::read_table(select_parameters, &database, &filter_columns).await
    } else {
        table_reader::read_table(select_parameters, &database).await
    }


}