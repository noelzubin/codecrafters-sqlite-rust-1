use anyhow::{bail, Result};
use itertools::Itertools;
use sqlite_starter_rust::db::{get_page_header, parse_schemas,  DB};
use sqlite_starter_rust::select_sql;
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Read database file into database
    let mut file = File::open(&args[1])?;
    let mut database = Vec::new();
    file.read_to_end(&mut database)?;

    // Parse command and act accordingly
    let command = &args[2];

    // On first page first 100 bytes are database header 
    let page_header = get_page_header(&database[100..])?;
    let schemas = parse_schemas(&database, page_header.number_of_cells)?;
    let page_size = u16::from_be_bytes(TryInto::<[u8; 2]>::try_into(&database[16..18]).unwrap());
    let db = DB::new(page_size, schemas);

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size);
            println!("number of tables: {}", db.schemas.len());
        }
        ".tables" => {
            let resp = db.schemas.iter().map(|schema| &schema.table_name).join(" ");
            println!("{}", resp);
        }

        query => {
            let query = select_sql::parse_sql(query)?;
            db.process_query(query, &database)?;
        }
    }

    Ok(())
}
