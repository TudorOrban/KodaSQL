use std::{fs::{self, File}, io::{BufReader, Read, Seek, SeekFrom}};

use crate::{database::{database_navigator::{get_table_data_path, get_table_index_path, get_table_row_index_path}, types::{Constraint, Index, RowsIndex, TableSchema}}, shared::errors::Error};


pub async fn create_indexes(schema_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let indexable_columns = table_schema.columns.iter()
        .filter(|col| index_strategy(&col.constraints));

    // Create column indexes
    for column in indexable_columns {
        let index_filepath = get_table_index_path(schema_name, &table_schema.name, &column.name);
        let index = Index { key: column.name.clone(), offsets: Vec::new() };
        let index_json = serde_json::to_string(&index)?;
        fs::write(&index_filepath, index_json.as_bytes())?;
    }

    // Create rows index
    let row_index_filepath = get_table_row_index_path(schema_name, &table_schema.name);
    let row_index = RowsIndex { row_offsets: Vec::new() };
    let row_index_json = serde_json::to_string(&row_index)?;
    fs::write(&row_index_filepath, row_index_json.as_bytes())?;

    Ok(())
}

pub fn index_strategy(constraints: &Vec<Constraint>) -> bool {
    constraints.contains(&Constraint::PrimaryKey) || constraints.contains(&Constraint::Unique)
}