use csv::StringRecord;

pub fn select_fields_old(record: &StringRecord, indices: &[usize]) -> Vec<String> {
    indices.iter()
        .filter_map(|&i| record.get(i).map(|s| s.trim().to_string()))
        .collect()
}

pub fn select_fields(record: &StringRecord, indices: &[usize]) -> StringRecord {
    let selected_fields: Vec<String> = indices.iter()
        .filter_map(|&i| record.get(i).map(|s| s.trim().to_string()))
        .collect();
    StringRecord::from(selected_fields)
}

pub fn get_column_indices(headers: &[String], columns: &[String]) -> Vec<usize> {
    if columns.contains(&"*".to_string()) {
        (0..headers.len()).collect()
    } else {
        columns.iter().filter_map(|col| headers.iter().position(|header| header == col)).collect()
    }
}