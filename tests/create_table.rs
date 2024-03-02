
use kodasql::{command_dispatcher::statement_dispatcher, database::{database_loader, database_navigator::{get_table_path, get_table_schema_path}, types::TableSchema}, shared::file_manager};
use sqlparser::{dialect::GenericDialect, parser::Parser};
use std::fs;

#[tokio::test]
pub async fn test_create_table() {
    // Prepare
    database_loader::load_database().await.expect("Failed to load database");

    let schema_name = String::from("schema_1");
    let table_name = String::from("test_create_table");
    let sql_command = format!("CREATE TABLE {} (id INT PRIMARY KEY, username INT UNIQUE NOT NULL, age INT)", table_name);

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, &sql_command).expect("Failed to parse SQL");
    let statement = ast.first().expect("No statements found");

    // Act
    let _ = statement_dispatcher::dispatch_statement(statement).await.expect("Storage engine error");

    // Get results and expected results
    let table_schema_file_path = get_table_schema_path(&schema_name, &table_name);
    let table_schema = file_manager::read_json_file::<TableSchema>(&table_schema_file_path).expect("Table schema not found");

    let expected_table_schema_json = r#"{
        "name": "test_create_table",
        "columns": [
            {
                "name": "id",
                "data_type": "Integer",
                "constraints": ["PrimaryKey"],
                "is_indexed": true,
                "order": 0
            },
            {
                "name": "username",
                "data_type": "Integer",
                "constraints": ["Unique", "NotNull"],
                "is_indexed": true,
                "order": 1
            },
            {
                "name": "age",
                "data_type": "Integer",
                "constraints": [],
                "is_indexed": false,
                "order": 2
            }
        ]
    }
    "#;

    let expected_table_schema: TableSchema = serde_json::from_str(expected_table_schema_json).expect("Failed to deserialize the table schema");
    
    // Clean up
    let table_dir_path = get_table_path(&schema_name, &table_name);
    fs::remove_dir_all(table_dir_path).expect("Failed to delete table");

    // Assert
    assert_eq!(table_schema, expected_table_schema); 

}