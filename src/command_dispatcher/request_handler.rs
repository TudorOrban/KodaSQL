use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;


use crate::command_dispatcher::statement_dispatcher;
use crate::network_protocol;
use crate::network_protocol::types::{Request, Response, ResponseStatus};
use crate::shared::errors::Error;

pub async fn handle_request(socket: &mut TcpStream) {
    let mut buffer = [0; 4096];

    loop {
        let read_result = socket.read(&mut buffer).await;

        let n = match read_result {
            Ok(0) => {
                // Client closed the connection
                return;
            },
            Ok(n) => n,
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                return;
            },
        };

        let request = match network_protocol::parsing::parse_request(&buffer[..n]) {
            Ok(req) => req,
            Err(e) => {
                let error_response = Response::from(e);
                let response_bytes = network_protocol::parsing::format_response(&error_response);
                if socket.write_all(&response_bytes).await.is_err() {
                    eprintln!("Failed to send error response");
                }
                continue; // Proceed to next iteration to handle more requests
            },
        };

        // Process request and obtain response
        let response = process_request(request).await.map_or_else(
            |e| e.into(),
            |data| Response {
                status: ResponseStatus::Success,
                data: Some(data),
                error: None,
            },
        );

        // Send response back to the client
        let response_bytes = network_protocol::parsing::format_response(&response);
        if socket.write_all(&response_bytes).await.is_err() {
            eprintln!("Failed to write response to socket");
            return;
        }
    }
}


pub async fn process_request(request: Request) -> Result<String, Error> {
    let sql = &request.sql;

    // Parse request into AST
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql);
    println!("AST: {:?}", ast);
    // match error
    let ast = match ast {
        Ok(ast) => ast,
        Err(_e) => return Err(Error::InvalidSQLSyntax),
    };

    // Process AST and dispatch statements
    let mut results = Vec::new();
    for statement in ast {
        let result = statement_dispatcher::dispatch_statement(&statement).await;
        match result {
            Ok(msg) => results.push(msg),
            Err(e) => return Err(e),
        }
    }

    Ok(results.join("\n"))
}