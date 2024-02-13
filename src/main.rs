use kodasql::{connection_manager::start_listening, storage_engine::select_columns};
use tokio;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[tokio::main]
async fn main() {
    if let Err(e) = start_listening("127.0.0.1:8080").await {
        println!("Failed to start server: {:?}", e);
    }
    // let result = select_columns("users", vec!["id", "username", "age", "emil"]);
    // println!("Result: {:?}", result);
}
