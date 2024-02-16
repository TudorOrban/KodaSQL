use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;

use crate::network_protocol::parsing::{format_response, parse_request};
use crate::network_protocol::types::{Request, Response, ResponseStatus};
use crate::shared::errors::Error;
use crate::storage_engine::select::handle_select;
use crate::storage_engine::create::create_table;

pub async fn handle_request(socket: &mut TcpStream) {
    let mut buffer = [0; 4096];

    match socket.read(&mut buffer).await {
        Ok(n) if n == 0 => return,
        Ok(n) => {
            match parse_request(&buffer[..n]) {
                Ok(request) => {
                    // Process request
                    let response = match process_request(request).await {
                        Ok(data) => Response { status: ResponseStatus::Success, data: Some(data), error: None },
                        Err(e) => e.into(),
                    };

                    // Serialize response and send to the client
                    let response_bytes = format_response(&response);
                    let _ = socket.write_all(&response_bytes).await;
                },
                Err(e) => {
                    let error_response = Response::from(e);
                    let response_bytes = format_response(&error_response);
                    let _ = socket.write_all(&response_bytes).await;
                }
            }
        },
        Err(e) => {
            let error_response = Response::from(Error::IOError(e));
            let response_bytes = format_response(&error_response);
            let _ = socket.write_all(&response_bytes).await;
        }
    }
}

async fn process_request(request: Request) -> Result<String, Error> {
    let sql = &request.sql;

    // Parse request into AST
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|_| Error::InvalidSQLSyntax)?;

    // Process AST and dispatch statements
    let mut results = Vec::new();
    for statement in ast {
        let result = dispatch_statement(&statement).await;
        match result {
            Ok(msg) => results.push(msg),
            Err(e) => return Err(e),
        }
    }

    Ok(results.join("\n"))
}

async fn dispatch_statement(statement: &Statement) -> Result<String, Error> {
    match statement {
        Statement::Query(statement) => {
            handle_select::handle_query(statement).await
        },
        Statement::CreateTable { or_replace, temporary, external, global, if_not_exists, transient, name, columns, constraints, hive_distribution, hive_formats, table_properties, with_options, file_format, location, query, without_rowid, like, clone, engine, comment, auto_increment_offset, default_charset, collation, on_commit, on_cluster, order_by, partition_by, cluster_by, options, strict } => {
            create_table::create_table(&name, &columns).await
        }
        _ => Err(Error::GenericUnsupported)
    }
}

// let request = match std::str::from_utf8(&buffer[0..n]) {
//     Ok(req) => req,
//     Err(e) => {
//         eprintln!("Error decoding request from UTF-8: {}", e);
//         return;
//     },
// };

// // Handle request
// // , socket, &buffer[0..n]
// handle_request(request).await;

// // Send a response back to the client
// let response = "Command processed\n";
// let _ = socket.write_all(response.as_bytes()).await;

// socket: &mut TcpStream, buffer: &[u8]
pub async fn handle_request_old(request: &str) {
    // Placeholder:
    // Attempt to find the start of the SQL statement after the headers
    if let Some(sql_start) = request.split("\r\n\r\n").nth(1) {
        let sql = sql_start.trim_matches('"'); // Removing potential double quotes
        println!("Extracted SQL statement: {:?}", sql);

        // Parse request into AST
        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, sql) {
            Ok(ast) => ast,
            Err(_) => {
                eprintln!("{}", Error::InvalidSQLSyntax);
                return;
            },
        };
        println!("AST: {:?}", ast);

        // Dispatch based on the parsed AST
        for statement in ast {
            dispatch_statement(&statement).await;
        }

        // if let Err(e) = socket.write_all(buffer).await {
        //     println!("Failed to write to socket: {:?}", e);
        // }
    } else {
        eprintln!("Failed to extract SQL statement from request");
    }
}
 
