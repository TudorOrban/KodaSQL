use std::io;

use bincode;
use kodasql::network_protocol::types::{MessageType, Request, Response, ResponseStatus};
use tokio::{net::TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt};
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut input_string = String::new();

    loop {
        println!("Enter SQL query (or type 'exit' to quit):");
        input_string.clear(); // Clear the buffer for the next input
        stdin.read_line(&mut input_string).await?;

        if input_string.trim().eq("exit") {
            break;
        }

        let request = Request {
            message_type: MessageType::Query,
            sql: input_string.trim().to_string(),
        };

        // Serialize and send request
        let serialized_request = bincode::serialize(&request).unwrap();
        stream.write_all(&serialized_request).await?;

        // Await and read the response
        let mut buffer = vec![0; 4096];
        let n = stream.read(&mut buffer).await?;

        // Indicate connection was closed on n == 0
        if n == 0 {
            println!("Server closed the connection.");
            break;
        }
        match bincode::deserialize::<Response>(&buffer[..n]) {
            Ok(response) => {
                // Handle response
                println!("Response received: {:?}", response);
                // If you want to specifically handle different response statuses here, you can do so
                match response.status {
                    ResponseStatus::Success => {
                        if let Some(data) = response.data {
                            println!("Data: {}", data);
                        }
                    },
                    ResponseStatus::Error => {
                        if let Some(error) = response.error {
                            println!("Error: {}", error);
                            // Continue even in case of error
                            continue;
                        }
                    },
                    _ => println!("Unknown response status."),
                }
            },
            Err(e) => {
                // Handle deserialization error without terminating the program
                println!("Failed to deserialize response: {}", e);
                // Decide how you want to handle this case. For example, you might want to retry or simply continue.
                continue;
            }
        }
    }

    Ok(())
}