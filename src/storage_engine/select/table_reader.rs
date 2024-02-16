use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::fs::File;

use crate::server::SCHEMAS;
use crate::shared::errors::Error;
use crate::storage_engine::select::filters::filter_records;
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
    // Perform validation before reading the table
    validate_query(table_name, columns, order_column_name).await?;

    // Read from file
    let file_path = format!("{}/data/{}.csv", constants::DATABASE_DIR, table_name);
    let file = match File::open(file_path) {
        Ok(file) => file,
        Err(_) => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = match rdr.headers() {
        Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
        Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    };
    let indices = utils::get_column_indices(&headers, columns);

    // Perform filtering and select specified fields
    let mut rows = filter_records(&mut rdr, &headers, filters, table_name, &indices)?;

    // Sort
    if let Some(column_name) = order_column_name {
        let column_index = headers.iter().position(|header| header == column_name)
                                  .ok_or_else(|| Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.clone() })?;
        sort_records(&mut rows, column_index, ascending);
    }

    // Apply limit
    let rows: Vec<StringRecord> = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();
    
    prepare_response(rows, headers)
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

fn sort_records(records: &mut Vec<StringRecord>, column_index: usize, ascending: bool) {
    records.sort_by(|a, b| {
        let a_val = a.get(column_index).unwrap_or_default();
        let b_val = b.get(column_index).unwrap_or_default();

        if ascending {
            a_val.cmp(&b_val)
        } else {
            b_val.cmp(&a_val)
        }
    });
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