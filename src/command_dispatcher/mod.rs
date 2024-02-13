use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn dispatch_command(socket: &mut TcpStream) {
    let mut buf = [0; 1024];

    match socket.read(&mut buf).await {
        Ok(n) if n == 0 => return,
        Ok(n) => {
            if let Err(e) = socket.write_all(&buf[0..n]).await {
                println!("Failed to write to socket: {:?}", e);
            }
        }
        Err(e) => {
            println!("Failed to read from socket: {:?}", e);
        }
    }
}