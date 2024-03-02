use std::collections::HashMap;

use kodasql::{command_dispatcher::statement_dispatcher, database::database_loader, shared::errors::Error};
use sqlparser::{dialect::GenericDialect, parser::Parser};


async fn execute_select_statement_and_assert(
    sql_command: &str,
    expected_results_json: &str,
) -> Result<(), Error> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql_command).expect("Failed to parse SQL");
    let statement = ast.first().expect("No statements found");

    let string_result = statement_dispatcher::dispatch_statement(statement).await.expect("Storage engine error");

    // Deserialize both the expected results and the actual results to Vec<HashMap<String, String>>
    let expected_results: Vec<HashMap<String, String>> = serde_json::from_str(expected_results_json).expect("Failed to deserialize expected results");
    let actual_results: Vec<HashMap<String, String>> = serde_json::from_str(&string_result).expect("Failed to deserialize actual results");

    assert_eq!(actual_results, expected_results, "The result rows do not match the expected rows");
    Ok(())
}

/*
 *  test_select_table:
    id,username,email,age
    1,John,john@email.com,4
    2,Mary,mary@email.com,12
    3,Jane,jane@email.com,20
    4,Matt,matt@email.com,34
    5,Andrew,andrew@email.com,21
    6,Mike,mike@email.com,21
    7,Mia,mia@email.com,21
 *
 */

#[tokio::test]
async fn test_select_statements() {
    database_loader::load_database().await.expect("Failed to load database");

    let test_cases = vec![
        // Test nested filters and ordering
        (
            // Statement
            "SELECT id, username FROM test_select_table WHERE id = 2 OR (username = 'Andrew' AND age = 21) ORDER BY id ASC;",
            // Results
            r#"[{"id": "2", "username": "Mary"}, {"id": "5", "username": "Andrew"}]"#,
        ),
        // Test limit
        (
            // Statement
            "SELECT id, age FROM test_select_table WHERE age = 21 LIMIT 2",
            // Results
            r#"[{"id": "5", "age": "21"}, {"id": "6", "age": "21"}]"#,
        ),
        // Test table reader with indexes
        (
            // Statement
            "SELECT id, username FROM test_select_table WHERE id = 2 OR username = 'Jane'",
            // Results
            r#"[{"id": "2", "username": "Mary"}, {"id": "3", "username": "Jane"}]"#
        ),
    ];

    for (sql_command, expected_results_json) in test_cases {
        execute_select_statement_and_assert(sql_command, expected_results_json).await.expect("Test case failed");
    }
}
