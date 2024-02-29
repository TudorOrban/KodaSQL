use sqlparser::ast::{AlterTableOperation, ObjectType, Statement};

use crate::shared::errors::Error;
use crate::storage_engine::alter_table::alter_table_dispatcher;
use crate::storage_engine::delete::{delete_records, delete_schema};
use crate::storage_engine::delete::delete_table;
use crate::storage_engine::insert::insert_into;
use crate::storage_engine::select::select_handler;
use crate::storage_engine::create::{create_schema, create_table};
use crate::storage_engine::update::update_records;

pub async fn dispatch_statement(statement: &Statement) -> Result<String, Error> {
    match statement {
        Statement::Query(statement) => {
            select_handler::handle_select(statement).await
        }
        Statement::CreateTable { or_replace, temporary, external, global, if_not_exists, transient, name, columns, constraints, hive_distribution, hive_formats, table_properties, with_options, file_format, location, query, without_rowid, like, clone, engine, comment, auto_increment_offset, default_charset, collation, on_commit, on_cluster, order_by, partition_by, cluster_by, options, strict } => {
            create_table::create_table(&name, &columns).await
        }
        Statement::CreateSchema { schema_name, if_not_exists } => {
            create_schema::create_schema(schema_name).await
        }
        Statement::Insert { or, ignore, into, table_name, table_alias, columns, overwrite, source, partitioned, after_columns, table, on, returning, replace_into, priority } => {
            insert_into::insert_into_table(&table_name, &columns, &source).await
        }
        Statement::Drop { object_type, if_exists, names, cascade, restrict, purge, temporary } => {
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
        Statement::Delete { tables, from, using, selection, returning, order_by, limit } => {
            delete_records::delete_records(from, selection).await
        }
        Statement::Update { table, assignments, from, selection, returning } => {
            update_records::update_records(table, assignments, selection).await
        }
        Statement::AlterTable { name, if_exists, only, operations } => {
            // update_table::update_table(name, operations).await
            alter_table_dispatcher::dispatch_alter_table_statement(name, operations).await
        }
        _ => Err(Error::GenericUnsupported)
    }
}