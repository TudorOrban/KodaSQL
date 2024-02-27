use sqlparser::ast::{BinaryOperator, Expr};

use crate::{database::types::TableSchema, shared::errors::Error};

pub fn use_indexes(filter_columns: &Vec<String>, table_schema: &TableSchema) -> bool {
    // Use indexes only if all filter columns are indexed
    if filter_columns.len() == 0 {
        return false;
    }
    for column_name in filter_columns {
        let corresp_column = table_schema.columns.iter().find(|column| &column.name == column_name);
        
        match corresp_column {
            Some(column) => {
                if !column.is_indexed {
                    return false;
                }
            },
            None => return false,
        }
    }
    true
}

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
