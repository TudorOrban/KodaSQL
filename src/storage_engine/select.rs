use sqlparser::ast::{Expr, Query, Select, SelectItem, Statement, TableFactor};
use std::{error::Error, fs::File, io::BufReader};

use crate::storage_engine::table_reader;

pub async fn handle_query(query: &Query) -> Result<(), Box<dyn Error>> {
    let Query { body, .. } = query;
    match &**body {
        sqlparser::ast::SetExpr::Select(select) => {
            let Select {
                projection,
                from,
                selection,
                ..
            } = &**select;

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

            let columns: Vec<String> = projection
                .iter()
                .filter_map(|item| match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
                    SelectItem::Wildcard(_) => Some("*".to_string()),
                    _ => None,
                })
                .collect();
            
            // let filters = interpret_selection(selection)?;
            
            let results = table_reader::read_table(&table_name, &columns, selection).await;
            println!("Query Results: {:?}", results);

            Ok(())
        }
        _ => Err("Unsupported SET expression in query".into()),
    }
}

fn interpret_selection(selection: &Option<Expr>) -> Result<Option<(String, String)>, Box<dyn Error>> {
    match selection {
        Some(expr) => match &expr {
            Expr::BinaryOp { ref left, op, ref right } => {
                if let (Expr::Identifier(ref ident), Expr::Value(ref value)) = (&**left, &**right) {
                    match op {
                        sqlparser::ast::BinaryOperator::Eq => {
                            let column_name = ident.value.clone();
                            println!("Column name: {:?}, Value: {:?}", column_name, value);
                            let condition_value = match value {
                                sqlparser::ast::Value::Number(n, _) => n.clone(),
                                sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                                _ => return Err("Unsupported value type in selection".into()),
                            };
                            Ok(Some((column_name, condition_value)))
                        },
                        _ => Err("Unsupported operator in selection".into()),
                    }
                } else {
                    Err("Unsupported expression format in selection".into())
                }
            },
            _ => Err("Unsupported expression type in selection".into())
        },
        None => Ok(None)
    }
}