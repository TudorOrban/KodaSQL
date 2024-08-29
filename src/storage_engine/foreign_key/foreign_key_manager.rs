use sqlparser::ast::{ConstraintCharacteristics, Ident, ObjectName, ReferentialAction};

use crate::shared::errors::Error;


pub fn handle_add_foreign_key(
    name: Option<Ident>,
    columns: Vec<Ident>,
    foreign_table: ObjectName,
    referred_columns: Vec<Ident>,
    on_delete: Option<ReferentialAction>,
    on_update: Option<ReferentialAction>,
    characteristics: Option<ConstraintCharacteristics>,
) -> Result<String, Error> {
    

    
    Ok(String::from(""))
}