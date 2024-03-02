
use std::fs::File;

use csv::StringRecord;
use kodasql::{command_dispatcher::statement_dispatcher, database::{database_loader, database_navigator::get_table_data_path}};
use sqlparser::{dialect::GenericDialect, parser::Parser};

#[tokio::test]
pub async fn test_insert_and_delete_record() {
    // Prepare
    database_loader::load_database().await.expect("Failed to load database");

    let schema_name = String::from("schema_1");
    let table_name = String::from("test_insert_and_delete_record_table");

    let insert_sql_command = format!("INSERT INTO {} (id, username, age) VALUES (3, 'Matt', 14)", table_name);
    let delete_sql_command = format!("DELETE FROM {} WHERE id = 3", table_name);

    let dialect = GenericDialect {};
    let insert_ast = Parser::parse_sql(&dialect, &insert_sql_command).expect("Failed to parse SQL");
    let insert_statement = insert_ast.first().expect("No statements found");
    let delete_ast = Parser::parse_sql(&dialect, &delete_sql_command).expect("Failed to parse SQL");
    let delete_statement = delete_ast.first().expect("No statements found");
    
    let initial_records = vec![
        StringRecord::from(vec!["1", "John", "20"]),
        StringRecord::from(vec!["2", "Mary", "32"]),
    ];

    // Act - insert record
    let _ = statement_dispatcher::dispatch_statement(insert_statement).await.expect("Storage engine error");

    // Get results and expected results
    let table_data_file_path = get_table_data_path(&schema_name, &table_name);
    let file = File::open(&table_data_file_path).expect("Could not open table data file");
    let mut rdr = csv::Reader::from_reader(file);
    let records_with_insertion: Vec<StringRecord> = rdr.records().filter_map(Result::ok).collect::<Vec<StringRecord>>();

    let mut expected_records = initial_records.clone();
    expected_records.push(StringRecord::from(vec!["3", "Matt", "14"])); // Inserted record

    // Assert
    for (record, expected) in records_with_insertion.iter().zip(expected_records.iter()) {
        assert_eq!(record, expected, "Record data does not match expected");
    }

    // Act - delete record
    let _ = statement_dispatcher::dispatch_statement(delete_statement).await.expect("Storage engine error");

    // Get results and expected results
    let modified_file = File::open(&table_data_file_path).expect("Could not open table data file");
    let mut rdr = csv::Reader::from_reader(modified_file);
    let final_records: Vec<StringRecord> = rdr.records().filter_map(Result::ok).collect::<Vec<StringRecord>>();

    // Assert
    for (record, expected) in final_records.iter().zip(initial_records.iter()) {
        assert_eq!(record, expected, "Record data does not match expected");
    }


}