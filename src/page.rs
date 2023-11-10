use anyhow::Result;

pub struct PageHeader {
    pub number_of_cells: u16
}

pub fn get_page_header(header_bytes: &[u8]) -> Result<PageHeader> {
    let number_of_cells = u16::from_be_bytes(header_bytes[3..5].try_into()?);
    Ok(PageHeader {
        number_of_cells,    
    })
}

