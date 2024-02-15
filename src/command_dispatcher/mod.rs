use sqlparser::keywords::TEMPORARY;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;

use crate::storage_engine::select::handle_select;
use crate::storage_engine::create::create_table;

pub async fn dispatch_command(socket: &mut TcpStream) {
    let mut buf = [0; 4096];

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
                println!("AST: {:?}", ast);
    
                // Dispatch based on the parsed AST
                for statement in ast {
                    dispatch_statement(&statement).await;
                }
    
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    println!("Failed to write to socket: {:?}", e);
                }
            } else {
                eprintln!("Failed to extract SQL statement from request");
            }

            // Send a response back to the client
            let response = "Command processed\n";
            let _ = socket.write_all(response.as_bytes()).await;
        },
        Err(e) => eprintln!("Failed to read from socket: {}", e),
    }
}

pub async fn dispatch_statement(statement: &Statement) {
    match statement {
        Statement::Query(statement) => {
            handle_select::handle_query(statement).await.unwrap_or_else(|e| eprintln!("{}", e));
        },
        Statement::CreateTable { or_replace, temporary, external, global, if_not_exists, transient, name, columns, constraints, hive_distribution, hive_formats, table_properties, with_options, file_format, location, query, without_rowid, like, clone, engine, comment, auto_increment_offset, default_charset, collation, on_commit, on_cluster, order_by, partition_by, cluster_by, options, strict } => {
            match create_table::create_table(&name, &columns).await {
                Ok(_) => println!("Table created successfully."),
                Err(e) => eprintln!("Failed to create table: {}", e),
            }
        }
        _ => eprintln!("Unsupported SQL statement")
    }
}