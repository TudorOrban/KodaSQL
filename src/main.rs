use kodasql::server::{initialize_server, run_server};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = initialize_server().await?;
    run_server(listener).await;
    Ok(())
}
