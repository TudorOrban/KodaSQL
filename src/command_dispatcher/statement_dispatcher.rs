use sqlparser::ast::{ObjectType, Statement};

use crate::shared::errors::Error;
use crate::storage_engine::alter_table::alter_table_dispatcher;
use crate::storage_engine::delete::{delete_records, delete_schema};
use crate::storage_engine::delete::delete_table;
use crate::storage_engine::insert::insert_into;
use crate::storage_engine::select::select_handler;
use crate::storage_engine::create::{create_schema, create_table};
use crate::storage_engine::trigger::create_trigger;
use crate::storage_engine::update::update_records;

pub async fn dispatch_statement(statement: &Statement) -> Result<String, Error> {
    match statement {
        Statement::Query(statement) => {
            select_handler::handle_select(statement).await
        }
        Statement::CreateTable(args) => {
            create_table::create_table(&args.name, &args.columns).await
        }
        Statement::CreateSchema { schema_name, .. } => {
            create_schema::create_schema(schema_name).await
        }
        Statement::Insert(args) => {
            insert_into::insert_into_table(&args.table_name, &args.columns, &args.source).await
        }
        Statement::Drop { object_type, names, .. } => {
            match object_type {
                ObjectType::Schema => {
                    delete_schema::delete_schema(names).await
                },
                ObjectType::Table => {
                    delete_table::delete_table(names).await
                },
                _ => Err(Error::GenericUnsupported)
            }
        }
        Statement::Delete(args) => {
            delete_records::delete_records(&args.from, &args.selection).await
        }
        Statement::Update { table, assignments, selection, .. } => {
            update_records::update_records(table, assignments, selection).await
        }
        Statement::AlterTable { name, operations, .. } => {
            // update_table::update_table(name, operations).await
            alter_table_dispatcher::dispatch_alter_table_statement(name, operations).await
        }
        Statement::CreateTrigger { name, period, events, table_name, exec_body, .. } => {
            create_trigger::create_trigger(&name, &table_name, &period, &events, &exec_body).await
        }
        _ => Err(Error::GenericUnsupported)
    }
}