use std::fs::File;

use csv::{ReaderBuilder, StringRecord};
use sqlparser::ast::Expr;

use crate::shared::errors::Error;

pub trait RowDataAccess {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String>;
}

impl RowDataAccess for StringRecord {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String> {
        headers.iter().position(|header| header == column_name)
            .and_then(|index| self.get(index).map(|value| value.trim().to_string()))
    }
}

impl RowDataAccess for Vec<String> {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String> {
        headers.iter().position(|header| header == column_name)
            .and_then(|index| self.get(index).cloned())
    }
}

pub fn apply_filters<T: RowDataAccess>(
    row: &T,
    headers: &Vec<String>,
    filters_option: Option<&Expr>,
) -> Result<bool, Error> {
    match filters_option {
        Some(expr) => match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Handle logical AND
                    sqlparser::ast::BinaryOperator::And => {
                        let left_result = apply_filters(row, headers, Some(left))?;
                        let right_result = apply_filters(row, headers, Some(right))?;
                        Ok(left_result && right_result)
                    },
                    // Handle logical OR
                    sqlparser::ast::BinaryOperator::Or => {
                        let left_result = apply_filters(row, headers, Some(left))?;
                        let right_result = apply_filters(row, headers, Some(right))?;
                        Ok(left_result || right_result)
                    },
                    // Handle equality check (Eq)
                    sqlparser::ast::BinaryOperator::Eq => {
                        handle_eq(row, headers, left, right)
                    },
                    _ => Err(Error::UnsupportedOperationType { operation: format!("{:?}", op) }),
                }
            },
            Expr::Nested(nested_expr) => apply_filters(row, headers, Some(nested_expr)),
            _ => Err(Error::UnsupportedSelectClause),
        },
        None => Ok(true)
    }
}


fn handle_eq<T: RowDataAccess>(row: &T, headers: &[String], left: &Expr, right: &Expr) -> Result<bool, Error> {
    if let (Expr::Identifier(ident), Expr::Value(value)) = (left, right) {
        let column_name = &ident.value;
        let condition_value = match value {
            sqlparser::ast::Value::Number(n, _) => n,
            sqlparser::ast::Value::SingleQuotedString(s) => s,
            _ => return Err(Error::UnsupportedValueType { value: format!("{:?}", value) }),
        };
        let value_in_row = row.get_value(column_name, headers);
        Ok(value_in_row == Some(condition_value.clone()))
    } else {
        Err(Error::UnsupportedSelectClause)
    }
}

pub fn test(filters: &Option<Expr>) -> Result<String, Error> {
    let headers: Vec<String> = vec!["id".to_string(), "username".to_string()]; // Example headers
    let dereferenced_filters = (*filters).as_ref();
    
    
    // Example usage with StringRecord
    let file_path = "test_table.csv";
    let file = File::open(file_path).map_err(|e| Error::IOError(e))?;
    let mut rdr: csv::Reader<File> = ReaderBuilder::new().has_headers(false).from_reader(file);
    let mut iter = rdr.records();
    let string_record_result = iter.next().ok_or(Error::GenericUnsupported) // Handle case where no record is found
        .and_then(|res| res.map_err(Error::CsvError))?; // Convert csv::Error if reading the record fails
    
    // Assuming apply_filters can handle a StringRecord directly
    let result1 = apply_filters(&string_record_result, &headers, dereferenced_filters);

    // Example usage with Vec<String>
    let indexed_row: Vec<String> = vec!["1".to_string(), "2".to_string()]; // Example row
    let result2 = apply_filters(&indexed_row, &headers, dereferenced_filters);
    println!("Result 1: {:?}, Result 2: {:?}", result1, result2);

    Ok(String::from("ok"))
}