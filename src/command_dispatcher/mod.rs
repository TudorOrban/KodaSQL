use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::dialect::GenericDialect;

use crate::storage_engine::select;

pub async fn dispatch_command(socket: &mut TcpStream) {
    let mut buf = [0; 4096]; // Adjust buffer size as needed

    match socket.read(&mut buf).await {
        Ok(n) if n == 0 => return,
        Ok(n) => {
            let request = match std::str::from_utf8(&buf[0..n]) {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("Error decoding request from UTF-8: {}", e);
                    return;
                },
            };

            // Placeholder:
            // Attempt to find the start of the SQL statement after the headers
            if let Some(sql_start) = request.split("\r\n\r\n").nth(1) {
                let sql = sql_start.trim_matches('"'); // Removing potential double quotes
                println!("Extracted SQL statement: {:?}", sql);

                // Parse request into AST
                let dialect = GenericDialect {};
                let ast = match Parser::parse_sql(&dialect, sql) {
                    Ok(ast) => ast,
                    Err(e) => {
                        eprintln!("Error parsing SQL: {}", e);
                        return;
                    },
                };
    
                // Dispatch based on the parsed AST
                match ast.first().map(|stmt| stmt.clone()) {
                    Some(sqlparser::ast::Statement::Query(_)) => {
                        select::handle_query(&ast[0]).await.unwrap_or_else(|e| eprintln!("{}", e));
                    },
                    _ => eprintln!("Unsupported SQL statement")
                }
    
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    println!("Failed to write to socket: {:?}", e);
                }
            } else {
                eprintln!("Failed to extract SQL statement from request");
            }

            // Optionally, send a response back to the client
            let response = "Command processed\n";
            let _ = socket.write_all(response.as_bytes()).await;
        },
        Err(e) => eprintln!("Failed to read from socket: {}", e),
    }
}
