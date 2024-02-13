use tokio::net::TcpListener;
use crate::thread_manager::handle_connection;

pub async fn start_listening(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;
    println!("Server running...");

    loop {
        let (socket, _) = listener.accept().await?;
        handle_connection(socket).await;
    }
}