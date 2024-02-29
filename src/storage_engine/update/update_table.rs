use std::fs::OpenOptions;

use csv::WriterBuilder;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, ObjectName};

use crate::{database::{self, database_loader, database_navigator::{get_table_data_path, get_table_schema_path}, types::{Database, TableSchema}, utils::get_headers_from_table_schema}, storage_engine::index::index_updater};
use crate::storage_engine::{index::index_manager, select::table_reader, validation};
use crate::shared::{errors::Error, file_manager};
 
pub async fn update_table(name: &ObjectName, operations: &Vec<AlterTableOperation>) -> Result<String, Error> {
    let first_identifier = name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_identifier.value.clone();

    // Prepare: get database blueprint
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    let table_schema = match database::utils::find_database_table(&database, &table_name) {
        Some(schema) => schema,
        None => return Err(Error::TableDoesNotExist { table_name: table_name.clone() }),
    };
    let mut table_schema_clone = table_schema.clone();

    validate_update_table(&table_name, &database, table_schema)?;

    for operation in operations {
        match operation {
            AlterTableOperation::AddColumn { column_keyword, if_not_exists, column_def } => {    
                add_column(column_def, &schema_name, &table_name, &mut table_schema_clone).await?;
            }
            AlterTableOperation::DropColumn { column_name, if_exists, cascade } => {
                delete_column(column_name, &mut table_schema_clone).await?;
            }
            AlterTableOperation::AddConstraint(r) => {
                
            }
            // AlterTableOperation::AlterColumn { column_name, op }
            _ => return Err(Error::NotSupportedUpdateTableOperation)
        }
    }


    Ok(format!("Success: the table {} has been successfully updated.", table_name))
}

fn validate_update_table(table_name: &String, database: &Database, table_schema: &TableSchema) -> Result<(), Error> {
    // Validate table exists
    validation::common::validate_table_exists(database, table_name)?;

    // TODO: Validate column name uniqueness
    // validation::common::validate_column_doesnt_exist(table_schema, column_name)?;
    // TODO: Distribute validation to each

    Ok(())
}
 
async fn add_column(column: &ColumnDef, schema_name: &String, table_name: &String, table_schema: &mut TableSchema) -> Result<(), Error> {
    // Update schema file
    let schema_column = validation::common::validate_column_definition(column, table_schema.columns.len())?;
    table_schema.columns.push(schema_column.clone());

    let table_schema_file_path = get_table_schema_path(schema_name, table_name);
    file_manager::write_json_into_file(&table_schema_file_path, table_schema)?;

    // Update data file
    let headers = get_headers_from_table_schema(&table_schema);
    
    let mut records = table_reader::read_table(schema_name, table_name, &None, true).await?;
    
    // Rewrite CSV
    let file_path = get_table_data_path(schema_name, table_name);
    let modified_file = OpenOptions::new()
        .write(true).truncate(true).create(true).open(file_path)
        .map_err(|_| Error::TableDoesNotExist { table_name: table_name.clone() })?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(modified_file);

    // Write headers
    wtr.write_record(&headers)
        .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Append Null to each record and write to CSV
    for record in &mut records {
        record.push_field("Null"); // TODO: Check for default value first

        wtr.write_record(&*record)
           .map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;
    }

    wtr.flush().map_err(|_| Error::FailedTableWrite { table_name: table_name.clone() })?;

    // Create index if necessary (deferring offset update to next function)
    if schema_column.is_indexed {
        index_manager::create_index(schema_name, table_name, &schema_column.name, table_schema, true, false, &records).await?;
    }

    // Update indexes
    index_updater::update_indexes_on_update_or_delete(&records, schema_name, table_name, table_schema)?;

    Ok(())
}

async fn delete_column(ident: &Ident, table_schema: &mut TableSchema) -> Result<(), Error> {
    let column_name = &ident.value;
    validation::common::validate_column_exists(table_schema, column_name)?;

    

    Ok(())
}