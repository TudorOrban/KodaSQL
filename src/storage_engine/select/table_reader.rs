use csv::{ReaderBuilder, StringRecord};
use std::collections::HashMap;
use std::fs::File;

use crate::database::database_navigator::get_table_data_path;
use crate::database::types::Database;
use crate::shared::errors::Error;
use crate::storage_engine::select::filters;
use crate::storage_engine::select::utils;
use crate::storage_engine::types::SelectParameters;

use super::validator;


pub async fn read_table(
    params: SelectParameters,
    database: &Database,
) -> Result<String, Error> {
    let SelectParameters {table_name, columns, filters, order_column_name, ascending, limit_value: limit } = params;

    // Perform validation before reading the table
    validator::validate_select_query(database, &table_name, &columns, &order_column_name)?;

    // Read from file
    let file_path = get_table_data_path(&database.configuration.default_schema, &table_name);
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Trim spaces in CSV file and find indices
    let headers = match rdr.headers() {
        Ok(headers) => headers.iter().map(|h| h.trim().to_string()).collect::<Vec<String>>(),
        Err(_) => return Err(Error::FailedTableRead { table_name: table_name.clone() }),
    };
    let indices = utils::get_column_indices(&headers, &columns);

    println!("Headers: {:?}, columns: {:?}, indices: {:?}", headers, columns, indices);
    // Perform filtering and select specified fields
    let mut rows = filters::filter_all_records(&mut rdr, &headers, &filters, &table_name, &indices)?;

    // Sort
    if let Some(column_name) = order_column_name {
        let column_index = headers.iter().position(|header| header == &column_name)
                                  .ok_or_else(|| Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_name.clone() })?;
        sort_records(&mut rows, column_index, ascending);
    }

    // Apply limit
    let rows: Vec<StringRecord> = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();
    
    format_response(rows, headers, indices)
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
pub fn format_response(rows: Vec<StringRecord>, selected_headers: Vec<String>, indices: Vec<usize>) -> Result<String, Error> {
    let mut structured_rows: Vec<HashMap<String, String>> = Vec::new();
    
    for row in rows {
        let mut row_map: HashMap<String, String> = HashMap::new();
        indices.iter().enumerate().for_each(|(i, &index)| {
            if let Some(value) = row.get(i) {
                let header = &selected_headers[index]; // Correctly map selected headers based on indices
                row_map.insert(header.clone(), value.to_string());
            }
        });
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