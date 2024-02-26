use csv::StringRecord;
use sqlparser::ast::{BinaryOperator, Expr};

use crate::{database::types::{RowsIndex, TableSchema}, shared::errors::Error, storage_engine::index::index_reader::{get_column_values_from_index, get_rows_from_row_offsets_2, read_column_index, read_rows_index}};

use super::filter_checker::apply_filters;


// pub fn find_filtered_records(schema_name: &String, table_name: &String, filters: &Option<Expr>, table_schema: &TableSchema, include: bool) -> Result<Vec<StringRecord>, Error> {
    


// }

pub fn get_restricted_rows(filter_columns: &Vec<String>, rows_index: &RowsIndex, schema_name: &String, table_name: &String) -> Result<Vec<Vec<String>>, Error> {
    let number_of_rows = rows_index.row_offsets.len() - 1;
    let number_of_columns = filter_columns.len();

    // Initialize restricted_rows with the exact size needed
    let mut restricted_rows: Vec<Vec<String>> = vec![vec![String::new(); number_of_columns]; number_of_rows];

    for (col_index, column_name) in filter_columns.iter().enumerate() {
        let column_index = read_column_index(schema_name, table_name, column_name)?;
        let column_values = get_column_values_from_index(&column_index, schema_name, table_name)?;

        // Populate each row with the column's value
        for (row_index, value) in column_values.into_iter().enumerate() {
            if row_index < restricted_rows.len() {
                restricted_rows[row_index][col_index] = value;
            }
        }
    }

    Ok(restricted_rows)
}

// fn handle_filter_eq(valid_indices: &mut Vec<usize>, column_values_map: &HashMap<String, Vec<String>>, left: &Expr, right: &Expr) -> Result<(), Error> {
//     if let (Expr::Identifier(ident), Expr::Value(value)) = (left, right) {
//         let column_name = &ident.value;
//         let column_values = &column_values_map[column_name];
        
//         let condition_value = match value {
//             Value::Number(n, _) => n,
//             Value::SingleQuotedString(s) => s,
//             _ => return Err(Error::UnsupportedValueType { value: format!("{:?}", value) }),
//         };

//         for (column_index, column_value) in column_values.iter().enumerate() {
//             if (column_value == condition_value) {
//                 valid_indices.push(column_index); // Not good
//             }
//         }
//     } else {
//         Err(Error::UnsupportedFilter)?
//     }



//     Ok(())
// }

pub fn find_filter_columns(filters_option: &Option<Expr>) -> Result<Vec<String>, Error> {
    let mut filter_columns: Vec<String> = Vec::new();

    identify_columns((*filters_option).as_ref(), &mut filter_columns)?;
    println!("{:?}", filter_columns);

    Ok(filter_columns)
}

fn identify_columns(filters_option: Option<&Expr>, filter_columns: &mut Vec<String>) -> Result<(), Error> {
    match filters_option {
        Some(expr) => match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        identify_columns(Some(left), filter_columns)?;
                        identify_columns(Some(right), filter_columns)?;
                    },
                    BinaryOperator::Or => {
                        identify_columns(Some(left), filter_columns)?;
                        identify_columns(Some(right), filter_columns)?;
                    },
                    BinaryOperator::Eq => {
                        handle_eq(left, right, filter_columns)?;
                    },
                    _ => Err(Error::UnsupportedFilter)?
                }
            },
            Expr::Nested(nested_expr) => identify_columns(Some(&nested_expr), filter_columns)?,
            _ => Err(Error::UnsupportedSelectClause)?,
        },
        None => ()
    }

    Ok(())
}

fn handle_eq(left: &Expr, right: &Expr, filter_columns: &mut Vec<String>) -> Result<(), Error> {
    if let (Expr::Identifier(ident), Expr::Value(_)) = (left, right) {
        let column_name = &ident.value;
        if !filter_columns.contains(column_name) {
            filter_columns.push(column_name.clone());
        }
        Ok(())
    } else {
        Err(Error::UnsupportedFilter)
    }
}
