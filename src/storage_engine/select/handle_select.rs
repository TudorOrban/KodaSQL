use sqlparser::ast::{Expr, Query, Select, SelectItem, TableFactor};
use std::error::Error;

use crate::storage_engine::select::table_reader;

pub async fn handle_query(query: &Query) -> Result<(), Box<dyn Error>> {
    let Query { body, order_by, limit, .. } = query;

    match &**body {
        sqlparser::ast::SetExpr::Select(select) => {
            let Select {
                projection, from, selection, ..
            } = &**select;

            // Unwrap table name
            let table = if !from.is_empty() {
                &from[0].relation
            } else {
                return Err("No table specified in FROM clause".into());
            };
            let table_name = match table {
                TableFactor::Table { name, .. } => {
                    let parts: Vec<String> =
                        name.0.iter().map(|ident| ident.value.clone()).collect();
                    parts.join(".")
                }
                _ => return Err("Unsupported table factor".into()),
            };

            // Unwrap columns
            let columns: Vec<String> = projection
                .iter()
                .filter_map(|item| match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
                    SelectItem::Wildcard(_) => Some("*".to_string()),
                    _ => None,
                })
                .collect();

            // Unwrap ordering parameters
            let mut order_column_name: Option<String> = None;
            let mut ascending = true;

            if let Some(order_by_expr) = order_by.get(0) {
                if let Expr::Identifier(ident) = &order_by_expr.expr {
                    order_column_name = Some(ident.value.clone());
                    ascending = order_by_expr.asc.unwrap_or(true);
                }
            }

            // Unwrap limit
            let mut limit_value: Option<usize> = None;

            if let Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(limit_str, _))) = limit {
                // Attempt to parse the string representation of the limit into an usize
                match limit_str.parse::<usize>() {
                    Ok(num) => limit_value = Some(num),
                    Err(e) => {
                        // Handle the error, e.g., log it or set limit_value to None if parsing fails
                        println!("Error parsing limit value: {:?}", e);
                        limit_value = None;
                    }
                }
            }
            
            // Hit database
            let results = table_reader::read_table(&table_name, &columns, selection, &order_column_name, ascending, limit_value).await;
            println!("Query Results: {:?}", results);

            Ok(())
        }
        _ => Err("Unsupported SET expression in query".into()),
    }
}

// fn unwrap_parameters(from: &Vec<TableWithJoins>, projection: &Vec<SelectItem>, order_by: &Vec<OrderByExpr>) -> Result<(), Box<dyn Error>> {
    
// }