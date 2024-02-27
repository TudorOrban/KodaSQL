use sqlparser::ast::Expr;

pub struct SelectParameters {
    pub table_name: String,
    pub columns: Vec<String>,
    pub filters: Option<Expr>,
    pub order_column_name: Option<String>,
    pub ascending: bool,
    pub limit_value: Option<usize>,
}