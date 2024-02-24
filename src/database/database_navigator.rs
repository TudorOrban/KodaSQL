use crate::database::constants;

pub fn get_database_configuration_path() -> String {
    format!("{}/configuration/configuration.json", constants::DATABASE_DIR)
}

// Schemas
pub fn get_schemas_dir_path() -> String {
    format!("{}/schemas", constants::DATABASE_DIR)
}

pub fn get_schema_path(schema_name: &String) -> String {
    format!("{}/{}", get_schemas_dir_path(), schema_name)
}

pub fn get_schema_configuration_path(schema_name: &String) -> String {
    format!("{}/configuration/configuration.json", get_schema_path(schema_name))
}

// Tables
pub fn get_tables_dir_path(schema_name: &String) -> String {
    format!("{}/tables", get_schema_path(schema_name))
}

pub fn get_table_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/{}", get_tables_dir_path(schema_name), table_name)
}

// Table schema
pub fn get_table_schema_dir_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/table_schema", get_table_path(schema_name, table_name))
}

pub fn get_table_schema_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/{}.schema.json", get_table_schema_dir_path(schema_name, table_name), table_name)
}

// Table data
pub fn get_table_data_dir_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/data", get_table_path(schema_name, table_name))
}

pub fn get_table_data_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/{}.csv", get_table_data_dir_path(schema_name, table_name), table_name)
}

// Table indexes
pub fn get_table_indexes_dir_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/indexes", get_table_path(schema_name, table_name))
}

pub fn get_table_index_path(schema_name: &String, table_name: &String, column_name: &String) -> String {
    format!("{}/{}.index.json", get_table_indexes_dir_path(schema_name, table_name), column_name)
}

pub fn get_table_row_index_path(schema_name: &String, table_name: &String) -> String {
    format!("{}/{}.index.json", get_table_indexes_dir_path(schema_name, table_name), String::from("row_offsets"))
}