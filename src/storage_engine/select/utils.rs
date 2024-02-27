pub fn get_column_indices(headers: &[String], columns: &[String]) -> Vec<usize> {
    if columns.contains(&"*".to_string()) {
        (0..headers.len()).collect()
    } else {
        columns.iter().filter_map(|col| headers.iter().position(|header| header == col)).collect()
    }
}