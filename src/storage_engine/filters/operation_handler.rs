use sqlparser::ast::Expr;

use crate::shared::errors::Error;

use super::types::RowDataAccess;

pub fn handle_eq<T: RowDataAccess>(row: &T, headers: &[String], left: &Expr, right: &Expr) -> Result<bool, Error> {
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