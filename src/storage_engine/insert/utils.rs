use sqlparser::ast::{Expr, Query, SetExpr, Value, Values};

use crate::{database::types::InsertedRowColumn, shared::errors::Error};


pub fn extract_inserted_rows(source: &Option<Box<Query>>, column_names: &[String]) -> Result<Vec<Vec<InsertedRowColumn>>, Error> {
    let mut all_rows_values = Vec::new();

    if let Some(query) = source {
        if let SetExpr::Values(Values { rows, .. }) = &*query.body {
            for row in rows {
                let mut row_values = Vec::new();
                for (i, expr) in row.iter().enumerate() {
                    let value_str = match expr {
                        Expr::Value(val) => value_to_string(val),
                        _ => "".to_string(),
                    };

                    if let Some(column_name) = column_names.get(i) {
                        row_values.push(InsertedRowColumn {
                            name: column_name.clone(),
                            value: value_str,
                        });
                    } else {
                        return Err(Error::GenericUnsupported);
                    }
                }
                all_rows_values.push(row_values);
            }
        }
    }

    Ok(all_rows_values)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Number(n, _) => n.clone(),
        Value::SingleQuotedString(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        _ => "".to_string(),
    }
}


pub fn get_inserted_column_values_from_rows(rows: &Vec<Vec<InsertedRowColumn>>, column_name: &String) -> Result<Vec<String>, Error> {
    let mut column_values: Vec<String> = Vec::new();

    for row in rows {
        let ins_column = row.into_iter().find(|ins_column| &ins_column.name == column_name);
        match ins_column {
            Some(ins_column) => column_values.push(ins_column.value.clone()),
            None => return Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: String::from("") })
        }
    }

    Ok(column_values)
}