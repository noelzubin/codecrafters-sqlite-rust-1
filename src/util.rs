use anyhow::Result;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::creation_sql::Field;
use crate::db::Record;

/// Read nth page from file   
pub fn read_page(file: &File, page_size: u16, page: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0; page_size as usize];
    file.read_exact_at(&mut buffer, page_size as u64 * (page - 1) as u64)?;
    Ok(buffer)
}

/// If the column is an INTEGER PRIMARY KEY then its values will be NULL in the
/// fields and should be picked from row_id.
pub fn get_value_for_record(record: &Record, ind: usize, field: &Field) -> String {
    if field.is_primary_key {
        return record.row_id.clone();
    }

    return record.columns[ind].clone();
}