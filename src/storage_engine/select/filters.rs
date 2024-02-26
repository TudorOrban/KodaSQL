use csv::{Reader, StringRecord};
use sqlparser::ast::Expr;
use std::convert::identity;
use std::fs::File;
use crate::shared::errors::Error;
use crate::storage_engine::filters::filter_checker::apply_filters;
use crate::storage_engine::select::utils;

pub fn filter_all_records(
    rdr: &mut Reader<File>,
    headers: &Vec<String>,
    filters: &Option<Expr>,
    table_name: &String,
    indices: &[usize]
) -> Result<Vec<StringRecord>, Error> {
    rdr.records()
       .filter_map(Result::ok)
       .map(|record| apply_filters(&record, headers, (*filters).as_ref()) // Apply filters
           .and_then(|passes| if passes { Ok(Some(StringRecord::from(utils::select_fields_old(&record, indices)))) } else { Ok(None) }))
       .collect::<Result<Vec<Option<StringRecord>>, Error>>()
       .map(|optional_records| optional_records.into_iter().filter_map(identity).collect())
}

// fn apply_filters(record: &StringRecord, headers: &Vec<String>, filters_option: Option<&Expr>, table_name: &String) -> Result<bool, Error> {
//     match filters_option {
//         Some(expr) => match expr {
//             Expr::BinaryOp { left, op, right } => {
//                 match op {
//                     // Handle logical AND
//                     sqlparser::ast::BinaryOperator::And => {
//                         let left_result = apply_filters(record, headers, Some(left), table_name)?;
//                         let right_result = apply_filters(record, headers, Some(right), table_name)?;
//                         Ok(left_result && right_result)
//                     },
//                     // Handle logical OR
//                     sqlparser::ast::BinaryOperator::Or => {
//                         let left_result = apply_filters(record, headers, Some(left), table_name)?;
//                         let right_result = apply_filters(record, headers, Some(right), table_name)?;
//                         Ok(left_result || right_result)
//                     },
//                     // Handle equality check (Eq)
//                     sqlparser::ast::BinaryOperator::Eq => {
//                         handle_eq(record, headers, left, right, table_name)
//                     },
//                     _ => Err(Error::UnsupportedOperationType { operation: format!("{:?}", op) }),
//                 }
//             },
//             Expr::Nested(nested_expr) => apply_filters(record, headers, Some(nested_expr), table_name),
//             _ => Err(Error::UnsupportedSelectClause),
//         },
//         None => Ok(true), // Record passes for no filter
//     }
// }

// fn handle_eq(record: &StringRecord, headers: &Vec<String>, left: &Expr, right: &Expr, table_name: &String) -> Result<bool, Error> {
//     if let (Expr::Identifier(ident), Expr::Value(value)) = (left, right) {
//         let column_name = &ident.value;
//         let condition_value = match value {
//             sqlparser::ast::Value::Number(n, _) => n,
//             sqlparser::ast::Value::SingleQuotedString(s) => s,
//             _ => return Err(Error::UnsupportedValueType { value: format!("{:?}", value) }),
//         };

//         let column_pos = headers.iter().position(|r| r == column_name);
//         if column_pos.is_none() {
//             return Err(Error::ColumnDoesNotExist { column_name: column_name.to_string(), table_name: table_name.clone() });
//         }

//         let value_in_record = record.get(column_pos.unwrap()).map(|v| v.trim());
//         Ok(value_in_record == Some(condition_value))
//     } else {
//         Err(Error::UnsupportedSelectClause)
//     }
// }