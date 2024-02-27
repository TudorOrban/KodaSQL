use csv::StringRecord;


pub trait RowDataAccess {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String>;
}

impl RowDataAccess for StringRecord {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String> {
        headers.iter().position(|header| header == column_name)
            .and_then(|index| self.get(index).map(|value| value.trim().to_string()))
    }
}

impl RowDataAccess for Vec<String> {
    fn get_value(&self, column_name: &str, headers: &[String]) -> Option<String> {
        headers.iter().position(|header| header == column_name)
            .and_then(|index| self.get(index).cloned())
    }
}