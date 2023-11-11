use anyhow::Result;
use anyhow::Error;

use crate::db::parse_24bit_be_twos_complement;

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    pub name: String,
    pub table_name: String,
    pub root_page: i64,
    pub sql: String,
}

impl Schema {
    /// Parses a record into a schema
    /// https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
    // Page 1 of a database file is the root page of a table b-tree that holds a special table named "sqlite_schema". This b-tree is known as the "schema table" since it stores the complete database schema. The structure of the sqlite_schema table is as if it had been created using the following SQL:
    // CREATE TABLE sqlite_schema(
    //   type text,
    //   name text,
    //   tbl_name text,
    //   rootpage integer,
    //   sql text
    // );
    pub fn parse_return_option(record: Vec<Vec<u8>>) -> Option<Self> {
        // dbg!(&record);
        let mut items = record.into_iter();
        let kind = items.next()?;
        let name = items.next()?;
        let table_name = items.next()?;
        let root_page: i64 = parse_24bit_be_twos_complement(&items.next()?);
        let sql = items.next()?;

        let schema = Self {
            kind: String::from_utf8_lossy(&kind).to_string(),
            name: String::from_utf8_lossy(&name).to_string(),
            table_name: String::from_utf8_lossy(&table_name).to_string(),
            root_page,
            sql: String::from_utf8_lossy(&sql).to_string(),
        };
        Some(schema)
    }

    // convert Option to Result
    pub fn parse(record: Vec<Vec<u8>>) -> Result<Self> {
        return Schema::parse_return_option(record).ok_or(Error::msg("Failed to parse schema"))
    }
}
