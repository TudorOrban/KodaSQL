use std::{convert::identity, fs::File};

use csv::{Reader, StringRecord};
use sqlparser::ast::Expr;

use crate::{shared::errors::Error, storage_engine::select::utils};

use super::{operation_handler, types::RowDataAccess};

pub fn filter_all_records(
    rdr: &mut Reader<File>,
    headers: &Vec<String>,
    filters: &Option<Expr>,
    indices: &[usize]
) -> Result<Vec<StringRecord>, Error> {
    rdr.records()
       .filter_map(Result::ok)
       .map(|record| apply_filters(&record, headers, (*filters).as_ref()) // Apply filters
           .and_then(|passes| if passes { Ok(Some(StringRecord::from(utils::select_fields_old(&record, indices)))) } else { Ok(None) }))
       .collect::<Result<Vec<Option<StringRecord>>, Error>>()
       .map(|optional_records| optional_records.into_iter().filter_map(identity).collect())
}

// Used for both StringRecord and Vec<String>, in table_reader and table_reader_with_index respectively
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
                        operation_handler::handle_eq(row, headers, left, right)
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

