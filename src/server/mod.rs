use tokio::net::TcpListener;

use crate::command_dispatcher::handle_request;
use crate::database::database_loader::load_database;

// Adjust the return type to include TcpListener for further use
pub async fn initialize_server() -> Result<TcpListener, Box<dyn std::error::Error>> {
    load_database().await?;

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