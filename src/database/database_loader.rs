use lazy_static::lazy_static;
use std::{fs, sync::Mutex};

use crate::{database::types::Database, shared::errors::Error};

use super::{database_navigator::{get_database_configuration_path, get_schema_configuration_path, get_table_schema_path}, types::{DatabaseConfiguration, Schema, SchemaConfiguration, TableSchema}};

lazy_static! {
    pub static ref DATABASE: Mutex<Database> = Mutex::new(Database::default());
}

pub async fn load_database() -> Result<(), Error> {
    let config = load_database_configuration().await?;
    let schema_names = config.schemas.clone();
    let mut schemas: Vec<Schema> = Vec::new();

    // Load schemas
    for schema_name in schema_names {
        let schema = load_schema(schema_name).await;
        match schema {
            Ok(schema) => schemas.push(schema),
            Err(_) => continue // Skip schema
        }
    }

    // Load database into global variable
    let mut db = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;
    db.schemas = schemas;
    db.configuration = config;
    println!("{:?}", db);

    Ok(())
}

pub async fn load_database_configuration() -> Result<DatabaseConfiguration, Error> {
    let configuration_path = get_database_configuration_path();

    // Read the file
    let config_content = fs::read_to_string(configuration_path)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    // Deserialize JSON string
    let config: DatabaseConfiguration = serde_json::from_str(&config_content)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    Ok(config)
}

pub async fn load_schema(schema_name: String) -> Result<Schema, Error> {
    let config = load_schema_configuration(&schema_name).await?;
    let table_names = config.tables.clone();
    let mut tables: Vec<TableSchema> = Vec::new();

    for table_name in table_names {
        let table = load_table(&schema_name, table_name).await;
        match table {
            Ok(table) => tables.push(table),
            Err(_) => continue // Skip table
        }
    }

    Ok(Schema {
        name: schema_name,
        tables: tables,
        configuration: config
    })
}


pub async fn load_schema_configuration(schema_name: &String) -> Result<SchemaConfiguration, Error> {
    let configuration_path = get_schema_configuration_path(&schema_name);

    // Read the file
    let config_content = fs::read_to_string(configuration_path)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    // Deserialize JSON string
    let config: SchemaConfiguration = serde_json::from_str(&config_content)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    Ok(config)
}

pub async fn load_table(schema_name: &String, table_name: String) -> Result<TableSchema, Error> {
    let table_schema_path = get_table_schema_path(schema_name, &table_name);
    
    // Read the file
    let schema_content = fs::read_to_string(table_schema_path)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    // Deserialize JSON string
    let table_schema: TableSchema = serde_json::from_str(&schema_content)
        .map_err(|_| Error::FailedDatabaseLoading)?;

    Ok(table_schema)
}

pub fn get_database() -> Result<Database, Error> {
    let db_lock = DATABASE.lock().map_err(|_| Error::FailedDatabaseLoading)?;
    Ok(db_lock.clone())
}

pub async fn save_schema_configuration(schema_name: &String, config: &SchemaConfiguration) -> Result<(), Error> {
    let configuration_path = get_schema_configuration_path(schema_name);
    let content = serde_json::to_string(config).map_err(|_| Error::FailedDatabaseLoading)?;
    tokio::fs::write(&configuration_path, content).await.map_err(|_| Error::FailedDatabaseLoading)?;
    Ok(())
}