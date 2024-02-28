use csv::StringRecord;

use crate::database::database_navigator::{get_table_index_path, get_table_row_index_path, get_table_schema_path};
use crate::database::types::{Constraint, Index, RowsIndex, TableSchema};
use crate::shared::{errors::Error, file_manager};

use super::offset_counter;

pub async fn create_default_indexes(schema_name: &String, table_schema: &TableSchema) -> Result<(), Error> {
    let indexable_columns = table_schema.columns.iter()
        .filter(|col| index_strategy(&col.constraints));

    // Create column indexes
    for column in indexable_columns {
        create_index(
            schema_name, &table_schema.name, &column.name, &mut table_schema.clone(), 
        false, false, &Vec::new(), // No offset computations
        ).await?;
    }

    // Create rows index
    let row_index_filepath = get_table_row_index_path(schema_name, &table_schema.name);
    let row_offsets = offset_counter::compute_headers_offset(table_schema);
    let row_index = RowsIndex { row_offsets: vec![row_offsets] };
    file_manager::write_json_into_file(&row_index_filepath, &row_index)?;

    Ok(())
}

pub async fn create_index(
    schema_name: &String, table_name: &String, column_name: &String, table_schema: &mut TableSchema, 
    update_schema: bool, calculate_offsets: bool, records: &Vec<StringRecord>
) -> Result<(), Error> {
    // Update schema if specified
    if update_schema {
        let table_schema_file_path = get_table_schema_path(schema_name, table_name);
        if let Some(column) = table_schema.columns.iter_mut().find(|c| &c.name == column_name) {
            column.is_indexed = true;
        }
        file_manager::write_json_into_file(&table_schema_file_path, table_schema)?;
    }

    // Create index file, computing column offsets if specified
    let index_file_path = get_table_index_path(schema_name, table_name, column_name);
    let column_order = if let Some(column) = table_schema.columns.iter().find(|c| &c.name == column_name) {
        Ok(column.order)
    } else {
        Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.clone() })
    }?;

    let offsets = if calculate_offsets {
        offset_counter::compute_column_offsets(records, &table_schema, &column_order)
    } else {
        Vec::new()
    };

    let index = Index { key: column_name.clone(), offsets: offsets };
    file_manager::write_json_into_file(&index_file_path, &index)?;

    Ok(())
}

pub fn index_strategy(constraints: &Vec<Constraint>) -> bool {
    constraints.contains(&Constraint::PrimaryKey) || constraints.contains(&Constraint::Unique)
}