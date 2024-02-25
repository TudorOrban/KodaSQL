use crate::{database::{database_navigator::{get_table_index_path, get_table_row_index_path}, types::{Constraint, Index, RowsIndex, TableSchema}}, shared::{errors::Error, file_manager}};

use super::offset_counter;

pub async fn create_indexes(schema_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let indexable_columns = table_schema.columns.iter()
        .filter(|col| index_strategy(&col.constraints));

    // Create column indexes
    for column in indexable_columns {
        let index_filepath = get_table_index_path(schema_name, &table_schema.name, &column.name);
        let index = Index { key: column.name.clone(), offsets: Vec::new() };
        file_manager::write_json_into_file(&index_filepath, &index)?;
    }

    // Create rows index
    let row_index_filepath = get_table_row_index_path(schema_name, &table_schema.name);
    let row_offsets = offset_counter::compute_headers_offset(table_schema);
    let row_index = RowsIndex { row_offsets: vec![row_offsets] };
    file_manager::write_json_into_file(&row_index_filepath, &row_index)?;

    Ok(())
}

pub fn index_strategy(constraints: &Vec<Constraint>) -> bool {
    constraints.contains(&Constraint::PrimaryKey) || constraints.contains(&Constraint::Unique)
}