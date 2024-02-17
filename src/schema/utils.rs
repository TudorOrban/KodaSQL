use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

use crate::schema::types::{Constraint as CustomConstraint, DataType as CustomDataType};
use crate::shared::errors::Error;

pub fn get_column_custom_data_type(column_type: &DataType, column_name: &String) -> Result<CustomDataType, Error> {
    match column_type {
        DataType::Int(_) => {
            return Ok(CustomDataType::Integer);
        },
        _ => return Err(Error::UnsupportedColumnDataType { column_name: column_name.clone(), column_type: format!("{:?}", column_type) }),
    }    
}

pub fn get_column_custom_constraints(column_constraints: &Vec<ColumnOptionDef>, column_name: &String) -> Result<Vec<CustomConstraint>, Error> {
    let mut custom_constraints: Vec<CustomConstraint> = Vec::new();

    for constraint in column_constraints {
        match constraint.option {
            ColumnOption::NotNull => custom_constraints.push(CustomConstraint::NotNull),
            ColumnOption::Unique { is_primary, .. } => {
                if is_primary {
                    custom_constraints.push(CustomConstraint::PrimaryKey);
                } else {
                    custom_constraints.push(CustomConstraint::Unique);
                }
            },
            _ => return Err(Error::UnsupportedConstraint { column_name: column_name.clone(), column_constraint: format!("{:?}", constraint.option) })
        }
    }

    Ok(custom_constraints)
}