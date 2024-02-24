pub fn add_index_offsets(complete_inserted_rows: &Vec<Vec<String>>, schema_name: &String, table_name: &String, columns: &Vec<String>) -> Result<(), Error> {
  let end_of_file_offset = get_end_of_file_offset(&schema_name, &table_name)?;
  let row_column_shift: Vec<Vec<u32>> = compute_total_shift(complete_inserted_rows, end_of_file_offset: u64);
  let row_index = RowIndex {
      row_offsets = utils::trace(row_columm_shift);
  };
  let row_index_string = serde:to_string(row_index);
  let row_index_file_path = get_row_index_file_path(&schema_name, &table_name);
  fs:write(row_index_file_path, row_index_file_path)?;
  
 
  for column_shift in row_column_shift {
      let column_index = Index {
          // TODO: get and use column_name
          column_offsets: column_shift
      };
      let column_index_file_path = get_column_index_file_path(&schema_name, &table_name, &column_name);
      let column_index = get_column_index(column_index_file_path)?;

      let updated_index: Index = {
           id; column_index.id,
           offsets = concat(current_offsets, columm_shift) 
      };
      let updated_index_json = serde_json::to_json(updated_index)?;
      fs:write(column_index_file_path, updated_index_json)?;
      
  }

  Ok(())
}

fn compute_total_shift(complete_inserted_rows: &Vec<Vec<RowColumn>>, end_of_file_path) -> Vec<Vec<u32>> {
  
};
