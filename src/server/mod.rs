use lazy_static::lazy_static;
use std::sync::Mutex;
use std::collections::HashMap;
use tokio::net::TcpListener;

use crate::command_dispatcher::handle_request;
use crate::schema::types::TableSchema;
use crate::schema::constants;

lazy_static! {
    pub static ref SCHEMAS: Mutex<HashMap<String, TableSchema>> = Mutex::new(HashMap::new());
}

// Adjust the return type to include TcpListener for further use
pub async fn initialize_server() -> Result<TcpListener, Box<dyn std::error::Error>> {
    load_table_schemas().await?;

    let address = "127.0.0.1:8080";
    let listener = TcpListener::bind(address).await?;
    println!("Server running on {}", address);

    Ok(listener)
}

// New function to handle incoming connections
pub async fn run_server(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                tokio::spawn(async move {
                    handle_request(&mut socket).await;
                });
            },
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
}

// Server initialization
pub async fn load_table_schemas() -> Result<(), Box<dyn std::error::Error>> {
    let data_file_path = format!("{}/schemas", constants::DATABASE_DIR);
    let paths = std::fs::read_dir(data_file_path)?.filter_map(|entry| {
        let entry = entry.ok()?;
        if entry.path().extension()? == "json" && entry.path().file_stem()?.to_str()?.ends_with(".schema") {
            Some(entry.path())
        } else {
            None
        }
    });

    let mut schemas = SCHEMAS.lock().unwrap();

    for path in paths {
        let file = std::fs::File::open(&path)?;
        let schema: TableSchema = serde_json::from_reader(file)?;

        // Extract the table name by trimming the ".schema.json" part of the file name
        let table_name = path.file_stem()
            .and_then(|name| name.to_str())
            .map(|name| name.trim_end_matches(".schema"))
            .unwrap_or_default()
            .to_string();

        schemas.insert(table_name, schema);
    }

    Ok(())
}

/*
 * Unit tests
*/
#[cfg(test)]
mod tests {
    use crate::schema::types::{Column, Constraint, DataType};
    use super::*;

    #[tokio::test]
    async fn test_load_table_schemas() {
            let result = load_table_schemas().await;
            assert!(result.is_ok());

            let schemas = SCHEMAS.lock().unwrap();
            assert!(!schemas.is_empty());

            // Assert the "users" table schema is loaded correctly
            if let Some(user_schema) = schemas.get("users") {
                assert_eq!(user_schema.name, "users");

                let expected_columns = vec![
                    Column {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        constraints: vec![Constraint::NotNull, Constraint::Unique],
                        is_indexed: false
                    },
                    Column {
                        name: "username".to_string(),
                        data_type: DataType::Text,
                        constraints: vec![Constraint::NotNull],
                        is_indexed: false
                    },
                    Column {
                        name: "email".to_string(),
                        data_type: DataType::Text,
                        constraints: vec![Constraint::NotNull],
                        is_indexed: false
                    },
                    Column {
                        name: "age".to_string(),
                        data_type: DataType::Integer,
                        constraints: vec![Constraint::NotNull],
                        is_indexed: false
                    },
                ];

                assert_eq!(user_schema.columns.len(), expected_columns.len());
                for (expected, actual) in expected_columns.iter().zip(user_schema.columns.iter()) {
                    assert_eq!(expected, actual);
                }
            } else {
                panic!("Schema for 'users' table not found");
            }
    }
}
