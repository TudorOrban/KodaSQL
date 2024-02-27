use std::fs::File;

use csv::{Reader, StringRecord};
use sqlparser::ast::Expr;

use crate::{database::types::RowsIndex, shared::errors::Error};

use super::{operation_handler, types::RowDataAccess};

pub fn filter_all_records(
    rdr: &mut Reader<File>,
    headers: &Vec<String>,
    filters: &Option<Expr>,
    include: bool,
) -> Result<Vec<StringRecord>, Error> {
    rdr.records()
       .filter_map(Result::ok)
       .filter_map(|record| {
           match apply_filters(&record, headers, filters.as_ref()) {
               Ok(passes) if passes == include => Some(Ok(record)), // Include/exclude record based on `include` flag
               Ok(_) => None,
               Err(e) => Some(Err(e)), 
           }
       })
       .collect()
}

pub fn filter_row_offsets(restricted_rows: &Vec<Vec<String>>, filters: &Option<Expr>, rows_index:RowsIndex, filter_columns: &Vec<String>, include: bool) -> Result<Vec<u64>, Error> {
    let mut row_offsets: Vec<u64> = Vec::new();

    for (row_index, row) in restricted_rows.iter().enumerate() {
        let is_hit = apply_filters(row, &filter_columns, filters.as_ref())?;
        if is_hit && include {
            row_offsets.push(rows_index.row_offsets[row_index]);
        } else if !is_hit && !include {
            row_offsets.push(rows_index.row_offsets[row_index]);
        }
    }

    Ok(row_offsets)
}

// Used for T = StringRecord and Vec<String>
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

