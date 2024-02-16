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

        let request = match parse_request(&buffer[..n]) {
            Ok(req) => req,
            Err(e) => {
                let error_response = Response::from(e);
                let response_bytes = format_response(&error_response);
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
        let response_bytes = format_response(&response);
        if socket.write_all(&response_bytes).await.is_err() {
            eprintln!("Failed to write response to socket");
            return;
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