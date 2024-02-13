use sqlparser::ast::{Expr, Query, Select, SelectItem, Statement};
use std::error::Error;

use crate::storage_engine::table_reader;

pub async fn handle_query(query: &Statement) -> Result<(), Box<dyn Error>> {
    match query {
        Statement::Query(query) => {
            let Query { body, .. } = &**query;
            match &**body {
                sqlparser::ast::SetExpr::Select(select) => {
                    let Select { projection, from, selection, .. } = &**select;

                    let table_name = if !from.is_empty() {
                        &from[0].relation
                    } else {
                        return Err("No table specified in FROM clause".into());
                    };

                    let columns: Vec<String> = projection.iter().filter_map(|item| {
                        match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
                            SelectItem::Wildcard(_) => Some("*".to_string()),
                            _ => None
                        }
                    }).collect();

                    // let filter = match selection {
                    //     Some(expr) => ColumnFilter::from_expr
                    // }

                    let results = table_reader::read_table(&table_name, &columns).await;
                    println!("Query Results: {:?}", results);

                    Ok(())
                },
                _ => Err("Unsupported SET expression in query".into()),
            }
        },
        _ => Err("Unsupported statement type".into()),
    }
}