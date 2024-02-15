use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

/*
 * Builder of sqlparser Expressions for ease of testing. Not currently used.
 */
pub struct ExprBuilder {
    expr: Expr
}

impl ExprBuilder {
    pub fn new() -> Self {
        Self { expr: Expr::Value(Value::Null) }
    }

    pub fn binary_op(mut self, left: Expr, op: BinaryOperator, right: Expr) -> Self {
        self.expr = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        self
    }

    pub fn identifier(mut self, value: &str) -> Self {
        self.expr = Expr::Identifier(Ident {
            value: value.to_string(),
            quote_style: None,
        });
        self
    }

    pub fn nested(mut self, expr: Expr) -> Self {
        self.expr = Expr::Nested(Box::new(expr));
        self
    }

    pub fn value_number(mut self, value: &str) -> Self {
        self.expr = Expr::Value(Value::Number(value.to_string(), false));
        self
    }

    pub fn value_string(mut self, value: &str) -> Self {
        self.expr = Expr::Value(Value::SingleQuotedString(value.to_string()));
        self
    }

    pub fn build(self) -> Expr {
        self.expr
    }
}