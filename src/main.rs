use anyhow::{bail, Result};
use std::fs;
mod page;


fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            // Read database into file
            let database: Vec<u8> = fs::read(&args[1])?;

            let page_size = u16::from_be_bytes([database[16], database[17]]);
            let page_header = page::get_page_header(&database[100..])?;
            
            println!("database page size: {}", page_size);
            println!("number of tables: {}", page_header.number_of_cells);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
