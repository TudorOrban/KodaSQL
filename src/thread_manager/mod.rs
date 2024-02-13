use tokio::net::TcpStream;
use crate::command_dispatcher::dispatch_command;

pub async fn handle_connection(mut socket: TcpStream) {
    tokio::spawn(async move {
        dispatch_command(&mut socket).await;
    });
}