use crate::database::types::Database;

pub fn does_table_exist(database: &Database, table_name: &String) -> bool {
    println!("Looking for table in default schema: {}, table: {}", database.configuration.default_schema, table_name);
    let default_schema = database.configuration.default_schema.clone();
    if let Some(schema) = database.schemas.iter().find(|s| s.name == default_schema) {
        println!("Found schema: {}", schema.name);
        return schema.tables.iter().any(|table| &table.name == table_name);
    }
    false
}