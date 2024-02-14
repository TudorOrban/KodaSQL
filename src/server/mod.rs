use lazy_static::lazy_static;
use std::sync::Mutex;
use std::collections::HashMap;
use tokio::net::TcpListener;
use crate::command_dispatcher::dispatch_command;

use crate::schema::TableSchema;

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
                    dispatch_command(&mut socket).await;
                });
            },
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
}


pub async fn load_table_schemas() -> Result<(), Box<dyn std::error::Error>> {
    let paths = std::fs::read_dir("schemas")?.filter_map(|entry| {
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