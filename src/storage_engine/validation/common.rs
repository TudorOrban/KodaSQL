use sqlparser::ast::ColumnDef;

use crate::{database::{self, types::{Column, Database}}, shared::errors::Error, storage_engine::index::index_manager};

pub fn does_table_exist(database: &Database, table_name: &String) -> bool {
    let default_schema = database.configuration.default_schema.clone();
    if let Some(schema) = database.schemas.iter().find(|s| s.name == default_schema) {
        return schema.tables.iter().any(|table| &table.name == table_name);
    }
    false
}

pub fn validate_table_exists(database: &Database, table_name: &String) -> Result<(), Error> {
    if !does_table_exist(database, table_name) {
        return Err(Error::TableDoesNotExist { table_name: table_name.clone() });
    }

    Ok(())
}

pub fn validate_table_doesnt_exist(database: &Database, table_name: &String) -> Result<(), Error> {
    if does_table_exist(database, table_name) {
        return Err(Error::TableDoesNotExist { table_name: table_name.clone() });
    }

    Ok(())
}

pub fn validate_column_definitions(columns: &Vec<ColumnDef>) -> Result<Vec<Column>, Error> {
    let mut schema_columns: Vec<Column> = Vec::new();
    for (column_index, column_def) in columns.iter().enumerate() {
        let column = validate_column_definition(column_def, column_index)?;
        // TODO: Validate there exists exactly one Primary Key

        schema_columns.push(column);
    }

    Ok(schema_columns)
}

pub fn validate_column_definition(column: &ColumnDef, order: usize) -> Result<Column, Error> {
    let data_type = database::utils::get_column_custom_data_type(&column.data_type, &column.name.value)?;
    let constraints = database::utils::get_column_custom_constraints(&column.options, &column.name.value)?;
    let is_indexed = index_manager::index_strategy(&constraints);

    Ok(Column {
        name: column.name.value.clone(),
        data_type,
        constraints,
        is_indexed,
        order
    })
}