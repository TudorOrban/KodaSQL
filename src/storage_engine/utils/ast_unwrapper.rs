use std::collections::HashMap;

use sqlparser::ast::{Assignment, Expr, OrderByExpr, Query, Select, SelectItem, TableFactor, TableWithJoins, Value};

use crate::{shared::errors::Error, storage_engine::select::types::SelectParameters};


pub fn unwrap_select_query(query: &Query) -> Result<SelectParameters, Error> {
    let mut select_parameters = SelectParameters {
        table_name: String::from(""),
        columns: Vec::new(),
        filters: None,
        order_column_name: None,
        ascending: false,
        limit_value: None
    };
    
    let Query { body, order_by, limit, .. } = query;

    match &**body {
        sqlparser::ast::SetExpr::Select(select) => {
            let Select {
                projection, from, selection, ..
            } = &**select;

            select_parameters.table_name = get_table_name_from_from_vector(from)?;

            select_parameters.columns = get_columns(projection);

            select_parameters.filters = selection.clone();

            let (order_column_name, ascending) = get_ordering(order_by);
            select_parameters.order_column_name = order_column_name;
            select_parameters.ascending = ascending;

            select_parameters.limit_value = get_limit(limit)?; 
        }
        _ => Err(Error::UnsupportedSelectClause)?
    }

    Ok(select_parameters)
}

pub fn get_table_name_from_from_vector(from: &Vec<TableWithJoins>) -> Result<String, Error> {
    let table = if !from.is_empty() {
        get_table_name_from_from(&from[0])?
    } else {
        return Err(Error::MissingTableName)?
    };

    Ok(table)
}

pub fn get_table_name_from_from(from: &TableWithJoins) -> Result<String, Error> {
    let table = &from.relation;

    let table_name = match table {
        TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name.0.iter().map(|ident| ident.value.clone()).collect();
            parts.join(".")
        }
        _ => return Err(Error::GenericUnsupported),
    };

    Ok(table_name)
}

pub fn get_columns(projection: &Vec<SelectItem>) -> Vec<String> {
    projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
            SelectItem::Wildcard(_) => Some("*".to_string()),
            _ => None,
        })
        .collect()
}

pub fn get_ordering(order_by: &Vec<OrderByExpr>) -> (Option<String>, bool) {
    let mut order_column_name: Option<String> = None;
    let mut ascending = true;

    if let Some(order_by_expr) = order_by.get(0) {
        if let Expr::Identifier(ident) = &order_by_expr.expr {
            order_column_name = Some(ident.value.clone());
            ascending = order_by_expr.asc.unwrap_or(true);
        }
    }

    (order_column_name, ascending)
}

pub fn get_limit(limit: &Option<Expr>) -> Result<Option<usize>, Error> {
    let mut limit_value: Option<usize> = None;

    if let Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(limit_str, _))) = limit {
        match limit_str.parse::<usize>() {
            Ok(num) => limit_value = Some(num),
            Err(_) => {
                return Err(Error::InvalidLimit {
                    limit: limit_str.clone(),
                })
            }
        }
    }

    Ok(limit_value)
}

pub fn get_new_column_values(assignments: &Vec<Assignment>) -> Result<HashMap<String, String>, Error> {
    let mut new_column_values: HashMap<String, String> = HashMap::new();

    for assignment in assignments {
        let column_name = assignment.id.first().ok_or(Error::MissingTableName)?.value.clone();
        let column_value = match &assignment.value {
            Expr::Value(Value::SingleQuotedString(value)) => value.clone(),
            Expr::Value(Value::Number(n, _)) => n.clone(),
            _ => return Err(Error::UnsupportedValueType { value: assignment.value.to_string() }),
        };
        new_column_values.insert(column_name, column_value);
    }

    Ok(new_column_values)
}