use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;
use std::error::Error;
use std::fs::File;

use crate::server::SCHEMAS;
use crate::storage_engine::select::utils;

pub async fn read_table(
    table_name: &String,
    columns: &Vec<String>,
    filters: &Option<Expr>,
    order_column_name: &Option<String>,
    ascending: bool,
    limit: Option<usize>,
) -> Result<Vec<StringRecord>, Box<dyn Error>> {
    println!("Table: {:?}, columns: {:?}", table_name, columns);

    // Perform validation before reading the table
    validate_query(table_name, columns, order_column_name).await?;

    // Read from file
    let file_path = format!("data/{}.csv", table_name);
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = rdr.headers()?.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>();
    let indices = utils::get_column_indices(&headers, columns);

    // Perform filtering and select specified fields
    let mut rows: Vec<StringRecord> = rdr.records()
        .filter_map(Result::ok) // Filter out any records that couldn't be read
        .filter(|record| apply_filters(record, &headers, filters.as_ref()).unwrap_or(false)) // Apply filters
        .map(|record| StringRecord::from(utils::select_fields(&record, &indices))) // Select fields
        .collect();

    println!("Column name: {:?}, Ascending: {:?}", order_column_name, ascending);
    match order_column_name {
        Some(order_column_name) => sort_records(&mut rows, &headers, order_column_name, ascending),
        None => {}
    }

    // Then apply limit
    let rows: Vec<StringRecord> = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();

    Ok(rows)
}

pub fn apply_filters(record: &StringRecord, headers: &Vec<String>, filters_option: Option<&Expr>) -> Result<bool, Box<dyn Error>> {
    match filters_option {
        Some(expr) => match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Handle logical AND
                    sqlparser::ast::BinaryOperator::And => {
                        let left_result = apply_filters(record, headers, Some(left))?;
                        let right_result = apply_filters(record, headers, Some(right))?;
                        Ok(left_result && right_result)
                    },
                    // Handle logical OR
                    sqlparser::ast::BinaryOperator::Or => {
                        let left_result = apply_filters(record, headers, Some(left))?;
                        let right_result = apply_filters(record, headers, Some(right))?;
                        Ok(left_result || right_result)
                    },
                    // Handle equality check (Eq)
                    sqlparser::ast::BinaryOperator::Eq => {
                        handle_eq(record, headers, left, right)
                    },
                    _ => Err("Unsupported operator in selection".into()),
                }
            },
            Expr::Nested(nested_expr) => apply_filters(record, headers, Some(nested_expr)),
            _ => Err("Unsupported expression type in selection".into()),
        },
        None => Ok(true), // No filter means the record passes
    }
}

fn handle_eq(record: &StringRecord, headers: &Vec<String>, left: &Expr, right: &Expr) -> Result<bool, Box<dyn Error>> {
    if let (Expr::Identifier(ident), Expr::Value(value)) = (left, right) {
        let column_name = &ident.value;
        let condition_value = match value {
            sqlparser::ast::Value::Number(n, _) => n,
            sqlparser::ast::Value::SingleQuotedString(s) => s,
            _ => return Err("Unsupported value type in selection".into()),
        };
        let value_in_record = record.get(headers.iter().position(|r| r == column_name).unwrap()).map(|v| v.trim());
        Ok(value_in_record == Some(condition_value))
    } else {
        Err("Unsupported expression format in selection".into())
    }
}


fn sort_records(records: &mut Vec<StringRecord>, headers: &[String], column_name: &str, ascending: bool) {
    let column_index = headers.iter().position(|header| header == column_name)
        .expect("Column name not found in headers");

    records.sort_by(|a,  b| {
        let a_val = a.get(column_index).unwrap_or_default();
        let b_val = b.get(column_index).unwrap_or_default();

        if ascending {
            a_val.cmp(&b_val)
        } else {
            b_val.cmp(&a_val)
        }
    });
}


pub async fn validate_query(
    table_name: &str,
    columns: &Vec<String>,
    order_column_name: &Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let schemas = SCHEMAS.lock().unwrap();

    let table_schema = schemas.get(table_name)
        .ok_or_else(|| format!("Table '{}' does not exist.", table_name))?;

    if !columns.contains(&"*".to_string()) {
        for column in columns {
            if !table_schema.columns.iter().any(|col| &col.name == column) {
                return Err(format!("Column '{}' does not exist in table '{}'.", column, table_name).into());
            }
        }
    }

    if let Some(column_name) = order_column_name {
        // TODO: Add type validation
        let is_column_valid = !table_schema.columns.iter().any(|col| &col.name == column_name);
        if is_column_valid {
            return Err(format!("Column '{}' does not exist in table '{}'", column_name, table_name).into());
        }
    }

    Ok(())
}
