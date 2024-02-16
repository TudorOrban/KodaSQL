use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs::File;

use crate::server::SCHEMAS;
use crate::shared::errors::Error;
use crate::storage_engine::select::utils;
use crate::schema::constants;

pub async fn read_table(
    table_name: &String,
    columns: &Vec<String>,
    filters: &Option<Expr>,
    order_column_name: &Option<String>,
    ascending: bool,
    limit: Option<usize>,
) -> Result<String, Error> {
    println!("Table: {:?}, columns: {:?}", table_name, columns);

    // Perform validation before reading the table
    validate_query(table_name, columns, order_column_name).await?;

    // Read from file
    let file_path = format!("{}/data/{}.csv", constants::DATABASE_DIR, table_name);
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
     
    // Sort
    match order_column_name {
        Some(order_column_name) => sort_records(&mut rows, &headers, order_column_name, ascending),
        None => {}
    }

    // Apply limit
    let rows: Vec<StringRecord> = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();
        
    prepare_response(rows, headers)
}

// Attach column keys to rows and serialize
pub fn prepare_response(rows: Vec<StringRecord>, headers: Vec<String>) -> Result<String, Error> {
    let mut structured_rows: Vec<HashMap<String, String>> = Vec::new();
    
    for row in rows {
        let mut row_map: HashMap<String, String> = HashMap::new();
        for (i, header) in headers.iter().enumerate() {
            if let Some(value) = row.get(i) {
                row_map.insert(header.clone(), value.to_string());
            }
        }
        structured_rows.push(row_map);
    }

    serde_json::to_string(&structured_rows)
        .map_err(|e| Error::SerdeJsonError(e))
}

pub fn apply_filters(record: &StringRecord, headers: &Vec<String>, filters_option: Option<&Expr>) -> Result<bool, Error> {
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
                    _ => Err(Error::UnsupportedSelectClause),
                }
            },
            Expr::Nested(nested_expr) => apply_filters(record, headers, Some(nested_expr)),
            _ => Err(Error::UnsupportedSelectClause),
        },
        None => Ok(true), // Record passes for no filter
    }
}

fn handle_eq(record: &StringRecord, headers: &Vec<String>, left: &Expr, right: &Expr) -> Result<bool, Error> {
    if let (Expr::Identifier(ident), Expr::Value(value)) = (left, right) {
        let column_name = &ident.value;
        let condition_value = match value {
            sqlparser::ast::Value::Number(n, _) => n,
            sqlparser::ast::Value::SingleQuotedString(s) => s,
            _ => return Err(Error::UnsupportedValueType { value: format!("{:?}", value) }),
        };
        let value_in_record = record.get(headers.iter().position(|r| r == column_name).unwrap()).map(|v| v.trim());
        Ok(value_in_record == Some(condition_value))
    } else {
        Err(Error::UnsupportedSelectClause)
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
) -> Result<(), Error> {
    let schemas = SCHEMAS.lock().unwrap();

    let table_schema = schemas.get(table_name)
        .ok_or_else(|| Error::TableDoesNotExist { table_name: table_name.to_string() })?;

    if !columns.contains(&"*".to_string()) {
        for column in columns {
            if !table_schema.columns.iter().any(|col| &col.name == column) {
                return Err(Error::ColumnDoesNotExist { column_name: column.clone(), table_name: table_name.to_string() });
            }
        }
    }

    if let Some(column_name) = order_column_name {
        // TODO: Add type validation
        let is_column_valid = !table_schema.columns.iter().any(|col| &col.name == column_name);
        if is_column_valid {
            return Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.to_string() });
        }
    }

    Ok(())
}


// /*
//  * Unit tests
// //  */
// #[cfg(test)]
// mod tests {
//     use crate::server::load_table_schemas;
//     use super::*;
//     use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

//     #[tokio::test]
//     async fn test_read_table() {
//         // Prepare
//         // Load table schema for validation
//         let result = load_table_schemas().await;
//         assert!(result.is_ok());

//         // Construct query parameters
//         let table_name = String::from("users");
//         let columns = vec![String::from("id"), String::from("username"), String::from("email"), String::from("age")];

//         // Construct clause: WHERE id = 2 OR (age = 3 AND email = 'johndoe@example.com')
//         let expression = Some(Expr::BinaryOp {
//             left: Box::new(Expr::BinaryOp {
//                 left: Box::new(Expr::Identifier(Ident {
//                     value: "id".to_string(),
//                     quote_style: None,
//                 })),
//                 op: BinaryOperator::Eq,
//                 right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
//             }),
//             op: BinaryOperator::Or,
//             right: Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
//                 left: Box::new(Expr::BinaryOp {
//                     left: Box::new(Expr::Identifier(Ident {
//                         value: "age".to_string(),
//                         quote_style: None,
//                     })),
//                     op: BinaryOperator::Eq,
//                     right: Box::new(Expr::Value(Value::Number("4".to_string(), false))),
//                 }),
//                 op: BinaryOperator::And,
//                 right: Box::new(Expr::BinaryOp {
//                     left: Box::new(Expr::Identifier(Ident {
//                         value: "email".to_string(),
//                         quote_style: None,
//                     })),
//                     op: BinaryOperator::Eq,
//                     right: Box::new(Expr::Value(Value::SingleQuotedString("johndoe@example.com".to_string()))),
//                 }),
//             })))
//         });
//         let order_column_name = Some(String::from("id"));
//         let ascending = true;
//         let limit = Some(10);

//         // Act: call read_table
//         let potential_results = read_table(&table_name, &columns, &expression, &order_column_name, ascending, limit).await;
//         let results = potential_results.expect("An error occurred");
        
//         // Assert: records match
//         let expected_records = vec![
//             StringRecord::from(vec!["1", "John Doe", "johndoe@example.com", "4"]),
//             StringRecord::from(vec!["2", "Jane Doe", "janedoe@example.com", "20"]),
//         ];

//         assert_eq!(results.len(), expected_records.len(), "Number of results does not match expected");

//         for (result, expected) in results.iter().zip(expected_records.iter()) {
//             assert_eq!(result, expected, "Result record does not match expected");
//         }
//     }
// }