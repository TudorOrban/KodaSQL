use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType, Expr, Value};

use crate::database::types::{Constraint as CustomConstraint, DataType as CustomDataType};
use crate::shared::errors::Error;

use super::types::{Database, Schema, TableSchema};

pub fn get_column_custom_data_type(column_type: &DataType, column_name: &String) -> Result<CustomDataType, Error> {
    match column_type {
        DataType::Int(_) => {
            return Ok(CustomDataType::Integer);
        },
        DataType::Text => {
            return Ok(CustomDataType::Text);
        },
        DataType::Bool => {
            return Ok(CustomDataType::Boolean);
        },
        _ => return Err(Error::UnsupportedColumnDataType { column_name: column_name.clone(), column_type: format!("{:?}", column_type) }),
    }    
}

pub fn get_column_custom_constraints(column_constraints: &Vec<ColumnOptionDef>, column_name: &String) -> Result<Vec<CustomConstraint>, Error> {
    let mut custom_constraints: Vec<CustomConstraint> = Vec::new();

    for constraint in column_constraints {
        match &constraint.option {
            ColumnOption::NotNull => custom_constraints.push(CustomConstraint::NotNull),
            ColumnOption::Unique { is_primary, .. } => {
                if *is_primary {
                    custom_constraints.push(CustomConstraint::PrimaryKey);
                } else {
                    custom_constraints.push(CustomConstraint::Unique);
                }
            },
            ColumnOption::Default(expr) => {
                let default_value = match expr {
                    Expr::Value(Value::Number(n, _)) => n.clone(),
                    Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                    _ => return Err(Error::UnsupportedConstraint { column_name: column_name.clone(), column_constraint: format!("{:?}", expr) }),
                }; // TODO: Ensure default_value type coincides with column type
                custom_constraints.push(CustomConstraint::DefaultValue(default_value))
            }
            _ => return Err(Error::UnsupportedConstraint { column_name: column_name.clone(), column_constraint: format!("{:?}", constraint.option) })
        }
    }

    Ok(custom_constraints)
}

pub fn find_database_schema<'a>(database: &'a Database, schema_name: &str) -> Option<&'a Schema> {
    database.schemas.iter()
        .find(|schema| &schema.name == schema_name)
}


pub fn find_database_table<'a>(database: &'a Database, table_name: &str) -> Option<&'a TableSchema> {
    let default_schema = &database.configuration.default_schema;
    database.schemas.iter()
        .find(|schema| &schema.name == default_schema)
        .and_then(|schema| schema.tables.iter().find(|table| &table.name == table_name))
}

pub fn get_headers_from_table_schema(table_schema: &TableSchema) -> Vec<String> {
    let mut all_columns = table_schema.columns.clone();
    all_columns.sort_by(|a, b| a.order.cmp(&b.order));
    let headers: Vec<String> = all_columns.iter().map(|c| c.name.clone()).collect();

    headers
}