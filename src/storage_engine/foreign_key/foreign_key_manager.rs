use sqlparser::ast::{Ident, ObjectName, ReferentialAction};

use crate::{database::{database_loader, database_navigator, types::{Database, ForeignKey, TableSchema}, utils::find_database_table}, shared::{errors::Error, file_manager}, storage_engine::utils::ast_unwrapper::get_referential_action};

pub async fn handle_add_foreign_key(
    table_name: &String,
    name: Option<Ident>,
    columns: Vec<Ident>,
    foreign_table: ObjectName,
    referred_columns: Vec<Ident>,
    on_delete: Option<ReferentialAction>,
    on_update: Option<ReferentialAction>,
) -> Result<String, Error> {
    validate_add_foreign_key(table_name, &name, &columns, &foreign_table)?;

    create_foreign_key(table_name, name, columns, foreign_table, referred_columns, on_delete, on_update).await?;

    Ok(String::from("Foreign key added successfully"))
}

async fn create_foreign_key(
    table_name: &String,
    name: Option<Ident>,
    columns: Vec<Ident>,
    foreign_table: ObjectName,
    referred_columns: Vec<Ident>,
    on_delete: Option<ReferentialAction>,
    on_update: Option<ReferentialAction>,
) -> Result<(), Error> {
    let database = database_loader::get_database()?;
    let table_schema = find_database_table(&database, table_name).unwrap();

    let foreign_table_name = foreign_table.0.first().ok_or(Error::MissingTableName)?;

    let foreign_table_schema = database.schemas.iter()
        .find(|schema| schema.name == database.configuration.default_schema)
        .ok_or_else(|| Error::SchemaDoesNotExist { schema_name: database.configuration.default_schema.clone() })?
        .tables.iter()
        .find(|table| table.name == foreign_table_name.value)
        .ok_or_else(|| Error::TableDoesNotExist { table_name: foreign_table_name.value.clone() })?;

    let foreign_key = ForeignKey {
        name: name.map(|ident| ident.value.clone()).unwrap_or_else(|| format!("fk_{}_{}_{}", table_name, foreign_table_name.value, table_schema.foreign_keys.len() + 1)),
        local_table: table_name.clone(),
        local_columns: columns.iter().map(|ident| ident.value.clone()).collect(),
        foreign_table: foreign_table_schema.name.clone(),
        foreign_columns: referred_columns.iter().map(|ident| ident.value.clone()).collect(),
        on_delete: get_referential_action(&on_delete)?,
        on_update: get_referential_action(&on_update)?,
    };

    let mut updated_table_schema = table_schema.clone();
    updated_table_schema.foreign_keys.push(foreign_key);

    // Update table schema file
    let schema_name = database.configuration.default_schema.clone();
    let table_schema_file_path = database_navigator::get_table_schema_path(&schema_name, &table_name);
    file_manager::write_json_into_file(&table_schema_file_path, &updated_table_schema)?;

    database_loader::reload_table_schema(&schema_name, &table_name).await?;

    Ok(())
}

fn validate_add_foreign_key(
    table_name: &String,
    name: &Option<Ident>,
    columns: &Vec<Ident>,
    foreign_table: &ObjectName,
) -> Result<(), Error> {
    let database = database_loader::get_database()?;
    let table_schema = match find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };

    validate_foreign_table(foreign_table, &database)?;

    validate_foreign_key_name(name.clone(), table_schema)?;

    validate_columns(&columns, &table_schema)?;

    Ok(())
}

fn validate_foreign_table(
    foreign_table: &ObjectName,
    database: &Database,
) -> Result<(), Error> {
    let schema_name = database.configuration.default_schema.clone();
    let table_name = foreign_table.0.last().ok_or(Error::MissingTableName)?;

    let schema = match database.schemas.iter().find(|schema| schema.name == schema_name) {
        Some(schema) => schema,
        None => return Err(Error::SchemaDoesNotExist { schema_name: schema_name.clone() }),
    };

    if schema.tables.iter().all(|t| t.name != table_name.value) {
        return Err(Error::TableDoesNotExist { table_name: table_name.value.clone() });
    }

    Ok(())
}

fn validate_foreign_key_name(
    name: Option<Ident>,
    table_schema: &TableSchema,
) -> Result<(), Error> {
    let foreign_key_name = match name {
        Some(name) => name.value,
        None => String::from("")
    };

    let does_key_exist = table_schema.foreign_keys.iter().any(|key| key.name == foreign_key_name);
    if does_key_exist {
        println!("Foreign key already exists: {}", foreign_key_name);
        return Err(Error::ForeignKeyAlreadyExists { foreign_key_name });
    }
    
    Ok(())
}

fn validate_columns(
    columns: &Vec<Ident>,
    table_schema: &TableSchema,
) -> Result<(), Error> {
    let column_names: Vec<String> = columns.iter().map(|ident| ident.value.clone()).collect();
    
    // Ensure all columns exist
    for column_name in column_names.iter() {
        if table_schema.columns.iter().all(|column| column.name != *column_name) {
            return Err(Error::ColumnDoesNotExist { column_name: column_name.clone(), table_name: table_schema.name.clone() });
        }
    }

    // Ensure all columns are unique
    if column_names.iter().collect::<std::collections::HashSet<_>>().len() != column_names.len() {
        return Err(Error::ColumnUniquenessNotSatisfied { column_name: String::from(""), value: String::from("") });
    }

    // Ensure no foreign key already exists
    for column_name in column_names.iter() {
        if table_schema.foreign_keys.iter().any(|key| key.local_columns.contains(&column_name)) {
            return Err(Error::ForeignKeyAlreadyExists { foreign_key_name: String::from("") });
        }
    }

    Ok(())
}