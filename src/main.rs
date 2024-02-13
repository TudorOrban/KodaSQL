use kodasql::connection_manager::start_listening;
use tokio;

#[tokio::main]
async fn main() {
    if let Err(e) = start_listening("127.0.0.1:8080").await {
        println!("Failed to start server: {:?}", e);
    }
}
