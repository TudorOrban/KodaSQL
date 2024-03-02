use lazy_static::lazy_static;
use std::sync::Mutex;

use crate::{database::types::Database, shared::{errors::Error, file_manager}};

use super::{database_navigator::{get_database_configuration_path, get_schema_configuration_path, get_table_schema_path}, types::{DatabaseConfiguration, Schema, SchemaConfiguration, TableSchema}};

lazy_static! {
    pub static ref DATABASE: Mutex<Database> = Mutex::new(Database::default());
}

// Load
pub async fn load_database() -> Result<(), Error> {
    let config = load_database_configuration().await?;
    let schema_names = config.schemas.clone();
    let mut schemas: Vec<Schema> = Vec::new();

    // Load schemas
    for schema_name in schema_names {
        let schema = load_schema(&schema_name).await;
        match schema {
            Ok(schema) => schemas.push(schema),
            Err(_) => continue // Skip schema
        }
    }

    // Load database into global variable
    let mut db = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;
    db.schemas = schemas;
    db.configuration = config;

    Ok(())
}

pub async fn load_database_configuration() -> Result<DatabaseConfiguration, Error> {
    let configuration_path = get_database_configuration_path();
    let configuration = file_manager::read_json_file::<DatabaseConfiguration>(&configuration_path)?;

    Ok(configuration)
}

pub async fn load_schema(schema_name: &String) -> Result<Schema, Error> {
    let config = load_schema_configuration(&schema_name).await?;
    let table_names = config.tables.clone();
    let mut tables: Vec<TableSchema> = Vec::new();

    for table_name in table_names {
        let table = load_table(&schema_name, &table_name).await;
        match table {
            Ok(table) => tables.push(table),
            Err(_) => continue // Skip table
        }
    }

    Ok(Schema {
        name: schema_name.clone(),
        tables: tables,
        configuration: config
    })
}


pub async fn load_schema_configuration(schema_name: &String) -> Result<SchemaConfiguration, Error> {
    let configuration_path = get_schema_configuration_path(&schema_name);
    
    let configuration = file_manager::read_json_file::<SchemaConfiguration>(&configuration_path)?;

    Ok(configuration)
}

pub async fn load_table(schema_name: &String, table_name: &String) -> Result<TableSchema, Error> {
    let table_schema_path = get_table_schema_path(schema_name, &table_name);
    let table_schema = file_manager::read_json_file::<TableSchema>(&table_schema_path)?;
    Ok(table_schema)
}

// Get database
pub fn get_database() -> Result<Database, Error> {
    let db_lock = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;
    Ok(db_lock.clone())
}

// Save
pub async fn save_schema_configuration(schema_name: &String, config: &SchemaConfiguration) -> Result<(), Error> {
    let configuration_path = get_schema_configuration_path(schema_name);
    let content = serde_json::to_string(config).map_err(|_| Error::FailedDatabaseLoading)?;
    tokio::fs::write(&configuration_path, content).await.map_err(|_| Error::FailedDatabaseLoading)?;
    Ok(())
}

// Reload
pub async fn reload_schema(schema_name: &String) -> Result<(), Error> {
    // Load schema
    let updated_schema = load_schema(schema_name).await?;

    // Acquire lock on database
    let mut database = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;

    // Find schema to update
    if let Some(schema) = database.schemas.iter_mut().find(|s| &s.name == schema_name) {
        *schema = updated_schema;
    } else {
        database.schemas.push(updated_schema);
    }

    Ok(())
}

pub async fn reload_table_schema(schema_name: &String, table_name: &String) -> Result<(), Error> {
    // Load table schema
    let updated_table_schema = load_table(schema_name, table_name).await?;

    // Acquire lock on database
    let mut database = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;

    // Find schema and table to update
    if let Some(schema) = database.schemas.iter_mut().find(|s| &s.name == schema_name) {
        if let Some(table) = schema.tables.iter_mut().find(|t| &t.name == table_name) {
            *table = updated_table_schema;
        } else {
            schema.tables.push(updated_table_schema);
        }
    } else {
        return Err(Error::SchemaDoesNotExist { schema_name: schema_name.clone() });
    }

    Ok(())
}
