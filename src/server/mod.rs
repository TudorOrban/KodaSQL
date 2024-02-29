use tokio::net::TcpListener;

use crate::database::database_loader::load_database;
use crate::command_dispatcher::request_handler;

pub async fn initialize_server() -> Result<TcpListener, Box<dyn std::error::Error>> {
    load_database().await?;

    let address = "127.0.0.1:8080";
    let listener = TcpListener::bind(address).await?;
    println!("Server running on {}", address);

    Ok(listener)
}

// Start listening for requests and handling them
pub async fn run_server(listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                tokio::spawn(async move {
                    request_handler::handle_request(&mut socket).await;
                });
            },
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
}