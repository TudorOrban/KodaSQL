use sqlparser::ast::ColumnDef;

use crate::{database::{self, types::{Column, Database, TableSchema}}, shared::errors::Error, storage_engine::index::index_manager};

// Table
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
        return Err(Error::TableNameAlreadyExists { table_name: table_name.clone() });
    }

    Ok(())
}

// Column
pub fn does_column_exist(table_schema: &TableSchema, column_name: &String) -> bool {
    return table_schema.columns.iter().any(|c| &c.name == column_name);
}

pub fn validate_column_exists(table_schema: &TableSchema, column_name: &String) -> Result<(), Error> {
    if !does_column_exist(table_schema, column_name) {
        return Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_schema.name.clone() })
    }

    Ok(())
}

pub fn validate_column_doesnt_exist(table_schema: &TableSchema, column_name: &String) -> Result<(), Error> {
    if does_column_exist(table_schema, column_name) {
        return Err(Error::ColumnNameAlreadyExists { column_name: column_name.clone() })
    }

    Ok(())
}

pub fn validate_columns_exist(table_schema: &TableSchema, column_names: &Vec<String>) -> Result<(), Error> {
    for column_name in column_names {
        validate_column_exists(table_schema, column_name)?;
    }

    Ok(())
}


pub fn validate_columns_dont_exist(table_schema: &TableSchema, column_names: &Vec<String>) -> Result<(), Error> {
    for column_name in column_names {
        validate_column_doesnt_exist(table_schema, column_name)?;
    }

    Ok(())
}

pub fn validate_column_definitions(column_definitions: &Vec<ColumnDef>, order: &Vec<usize>) -> Result<Vec<Column>, Error> {
    let mut schema_columns: Vec<Column> = Vec::new();
    for (column_index, column_def) in column_definitions.iter().enumerate() {
        let column = validate_column_definition(column_def, order[column_index])?;
        // TODO: Validate there exists exactly one Primary Key

        schema_columns.push(column);
    }

    Ok(schema_columns)
}

pub fn validate_column_definition(column_definition: &ColumnDef, order: usize) -> Result<Column, Error> {
    let data_type = database::utils::get_column_custom_data_type(&column_definition.data_type, &column_definition.name.value)?;
    let constraints = database::utils::get_column_custom_constraints(&column_definition.options, &column_definition.name.value)?;
    let is_indexed = index_manager::index_strategy(&constraints);

    Ok(Column {
        name: column_definition.name.value.clone(),
        data_type,
        constraints,
        is_indexed,
        order
    })
}

// pub fn validate_column_definitions(column_definitions: &Vec<ColumnDef>, order: &Vec<usize>) -> Result<Vec<Column>, Error> {
//     let mut columns: Vec<Column> = Vec::new();
    
//     for (col_index, column_definition) in column_definitions.iter().enumerate() {
//         let column = validate_column_definition(column_definition, order[col_index])?;
//         columns.push(column);
//     }

//     Ok(columns)   
// }