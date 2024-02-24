pub fn transpose_matrix<T: Clone>(matrix: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if matrix.is_empty() || matrix[0].is_empty() {
        return vec![];
    }

    let row_count = matrix[0].len();
    let col_count = matrix.len();
    let mut transposed = vec![vec![matrix[0][0].clone(); col_count]; row_count];

    for (i, row) in matrix.iter().enumerate() {
        for (j, item) in row.iter().enumerate() { 
            transposed[j][i] = item.clone(); 
        }
    }

    transposed
}
