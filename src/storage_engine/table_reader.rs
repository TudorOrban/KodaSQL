use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::{Expr};
use std::error::Error;
use std::fs::File;

use crate::server::SCHEMAS;

pub async fn read_table(
    table_name: &String,
    columns: &Vec<String>,
    expression: &Option<Expr>
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    println!("Table: {:?}, columns: {:?}", table_name, columns);

    // Perform validation before reading the table
    validate_query(table_name, columns).await?;

    // Read from file
    let file_path = format!("data/{}.csv", table_name);
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file
    let headers = rdr.headers()?.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>();

    // Identify indices of the requested columns (accounting for * wildcard)
    let indices: Vec<usize> = if columns.contains(&String::from("*")) {
        (0..headers.len()).collect()
    } else {
        headers
            .iter()
            .enumerate()
            .filter_map(|(i, name)| {
                if columns.contains(&name) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    };

    let rows: Vec<StringRecord> = rdr.records()
        .filter_map(Result::ok) // Filter out any records that couldn't be read
        .filter(|record| apply_filters(record, &headers, expression.as_ref()).unwrap_or(false)) // Apply filters
        .map(|record| StringRecord::from(select_fields(&record, &indices))) // Select fields
        .collect();

    Ok(rows)
}

pub fn apply_filters(record: &StringRecord, trimmed_headers: &Vec<String>, expr_option: Option<&Expr>) -> Result<bool, Box<dyn Error>> {
    match expr_option {
        Some(expr) => match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Handle logical AND
                    sqlparser::ast::BinaryOperator::And => {
                        let left_result = apply_filters(record, trimmed_headers, Some(left))?;
                        let right_result = apply_filters(record, trimmed_headers, Some(right))?;
                        Ok(left_result && right_result)
                    },
                    // Handle logical OR
                    sqlparser::ast::BinaryOperator::Or => {
                        let left_result = apply_filters(record, trimmed_headers, Some(left))?;
                        let right_result = apply_filters(record, trimmed_headers, Some(right))?;
                        Ok(left_result || right_result)
                    },
                    // Handle equality check (Eq)
                    sqlparser::ast::BinaryOperator::Eq => {
                        if let (Expr::Identifier(ident), Expr::Value(value)) = (&**left, &**right) {
                            let column_name = &ident.value;
                            let condition_value = match value {
                                sqlparser::ast::Value::Number(n, _) => n,
                                sqlparser::ast::Value::SingleQuotedString(s) => s,
                                _ => return Err("Unsupported value type in selection".into()),
                            };
                            let value_in_record = record.get(trimmed_headers.iter().position(|r| r == column_name).unwrap()).map(|v| v.trim());
                            Ok(value_in_record == Some(condition_value))
                        } else {
                            Err("Unsupported expression format in selection".into())
                        }
                    },
                    _ => Err("Unsupported operator in selection".into()),
                }
            },
            Expr::Nested(nested_expr) => apply_filters(record, trimmed_headers, Some(nested_expr)),
            _ => Err("Unsupported expression type in selection".into()),
        },
        None => Ok(true), // No filter means the record passes
    }
}

fn select_fields(record: &StringRecord, indices: &[usize]) -> Vec<String> {
    indices.iter()
        .filter_map(|&i| record.get(i).map(|s| s.trim().to_string()))
        .collect()
}


pub async fn validate_query(
    table_name: &str,
    columns: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let schemas = SCHEMAS.lock().unwrap();

    // Validate table name
    let table_schema = schemas
        .get(table_name)
        .ok_or_else(|| format!("Table '{}' does not exist.", table_name))?;

    // Assume valid columns if "*" present
    if columns.contains(&"*".to_string()) {
        return Ok(());
    }

    // Validate columns
    for column in columns {
        if !table_schema.columns.iter().any(|col| &col.name == column) {
            return Err(format!(
                "Column '{}' does not exist in table '{}'.",
                column, table_name
            )
            .into());
        }
    }

    Ok(())
}
