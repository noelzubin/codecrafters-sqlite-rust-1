use std::vec;

use crate::{
    creation_sql::{parse_create_index, parse_creation, IndexInfo},
    header::{BTreePage, PageHeader},
    record::parse_record,
    schema::Schema,
    select_sql::{SelectClause, Sql},
    varint::parse_varint,
};
use anyhow::Result;
use itertools::Itertools;

// Ideally return size here as well
pub fn get_page_header(header_bytes: &[u8]) -> Result<PageHeader> {
    // A b-tree page is divided into regions in the following order:
    // 1. The 100-byte database file header (found on page 1 only)
    // 2. The 8 or 12 byte b-tree page header
    // 3. The cell pointer array
    // 4. Unallocated space
    // 5. The cell content area
    // 6. The reserved region.

    // Parse page header from database
    PageHeader::parse(header_bytes)
}

pub fn parse_cell_pointers(stream: &[u8], number_of_cells: u16) -> Vec<u16> {
    // Obtain all cell pointers
    // cell pointers are an array of 16 bit offsets that point the cell contents.
    stream
        .chunks_exact(2)
        .take(number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>()
}

pub fn parse_schemas(database: &[u8], number_of_cells: u16) -> Result<Vec<Schema>> {
    let cell_pointers = parse_cell_pointers(&database[108..], number_of_cells);

    // Obtain all records
    let schemas: Result<Vec<Schema>> = cell_pointers
        .into_iter()
        .map(|cell_pointer| {
            let record = parse_btree_leaf_cell_content(cell_pointer, database)?;
            let record = Schema::parse(record)?;
            Ok(record)
        })
        .collect();

    schemas
}

pub fn parse_btree_leaf_cell_content(
    cell_pointer: u16,
    page_stream: &[u8],
) -> Result<Vec<Vec<u8>>> {
    let stream = &page_stream[cell_pointer as usize..];
    let (_payload_size, offset) = parse_varint(stream); // total number of bytes of payload
    let (_rowid, read_bytes) = parse_varint(&stream[offset..]); // integer key (rowid).

    // Now the actual content start
    let record = parse_record(&stream[offset + read_bytes..]);

    record
}

pub fn get_page_size(database: &Vec<u8>) -> Result<u16> {
    let page_size = u16::from_be_bytes(TryInto::<[u8; 2]>::try_into(&database[16..18]).unwrap());
    return Ok(page_size);
}

pub fn get_fields_in_table(tablename: &str, database: &Vec<u8>) -> Result<Vec<String>> {
    let page_header = get_page_header(&database[100..108])?;
    let schemas = parse_schemas(&database, page_header.number_of_cells)?;

    let schema = schemas
        .iter()
        .find(|schema| schema.table_name == tablename)
        .unwrap();

    let (_, create_statement) = parse_creation(schema.sql.as_bytes()).unwrap();

    return Ok(create_statement
        .fields
        .into_iter()
        .map(|field| field.name)
        .collect());
}

// fn get_record_by_rowid(rowid: usize, database: &Vec<u8>, tablename: &str) -> Result<Vec<String>> {
//     let page_header = get_page_header(&database[100..108])?;
//     let schemas = parse_schemas(&database, page_header.number_of_cells)?;

//     let schema = schemas
//         .iter()
//         .find(|schema| schema.table_name == tablename)
//         .unwrap();

//     let page_size = get_page_size(database)?;

//     return Ok(vec![]);
// }

fn parse_page(
    page_size: u16,
    page_number: usize,
    database: &[u8],
    row_collector: &mut Vec<usize>,
) -> Result<()> {
    let start_index = page_size as usize * (page_number - 1) as usize;
    // Get the index page
    let page_header = get_page_header(&database[start_index..])?;

    let value = "eritrea";

    if page_header.page_type == BTreePage::InteriorIndex {
        let cell_pointers =
        // get the index cell pointers
        parse_cell_pointers(&database[(start_index + 12)..], page_header.number_of_cells);

        let mut found_bigger = false;

        for cell_pointer in cell_pointers.iter() {
            let left_child_pointer_start = start_index + *cell_pointer as usize;

            let left_child_pointer_bytes =
                &database[left_child_pointer_start..left_child_pointer_start + 4];

            let left_child_pointer =
                u32::from_be_bytes(left_child_pointer_bytes.try_into().unwrap()) as usize;
            let mut offset = 4;

            let (payload_size, payload_offset) =
                parse_varint(&database[(left_child_pointer_start + offset)..]);
            offset += payload_offset;


            let record = parse_record(&database[(left_child_pointer_start + offset)..]).unwrap();

            let key = String::from_utf8_lossy(&record[0]);

            if value <= &key {
                if found_bigger {
                    return Ok(());
                }

                if value == key {
                    let rowid = record[1].clone();
                    let rowid = parse_24bit_be_twos_complement(&rowid);
                    row_collector.push(rowid as usize);
                } else {
                    found_bigger = true;
                }

                parse_page(
                    page_size,
                    left_child_pointer as usize,
                    database,
                    row_collector,
                )
                .unwrap();
            }
        }

        let right_most_pointer = page_header.right_most_pointer.unwrap();

        parse_page(
            page_size,
            page_header.right_most_pointer.unwrap() as usize,
            database,
            row_collector,
        )
        .unwrap();

        return Ok(());
    }
    if page_header.page_type == BTreePage::LeafIndex {
        let cell_pointers =
            parse_cell_pointers(&database[(start_index + 8)..], page_header.number_of_cells);

        for cell_pointer in &cell_pointers {
            let cell_pointer_start = start_index + *cell_pointer as usize;

            let stream = &database[cell_pointer_start as usize..];

            // Differently from an interior page, there's no pointer.

            //

            let key_record = parse_index_payload(stream)?;

            let key = &key_record[0];

            // warn!("checking {} ", String::from_utf8_lossy(key));

            if key == value.as_bytes() {
                let rowid = key_record[1].clone();
                let rowid = parse_24bit_be_twos_complement(&rowid);

                row_collector.push(rowid as usize);
            }
        }
    }

    Ok(())
}

fn parse_index_payload(stream: &[u8]) -> Result<Vec<Vec<u8>>> {
    let (_payload_size, payload_size_bytes) = parse_varint(stream);
    let key_record = parse_record(&stream[payload_size_bytes..])?;
    Ok(key_record)
}

fn parse_24bit_be_twos_complement(bytes: &[u8]) -> i64 {
    let msb = bytes[0] as i64;
    let middle = bytes[1] as i64;
    let lsb = bytes[2] as i64;

    let value = (msb << 16) | (middle << 8) | lsb;
    if (value & 0x800000) != 0 {
        // The value is negative, perform sign extension
        value | 0xFF_0000_00
    } else {
        value
    }
}

// Get records from the given page.
fn get_them_records(database: &Vec<u8>, page_size: usize, page_number: usize) -> Vec<Vec<String>> {
    // Start index of the page
    let mut start_index = page_size * (page_number - 1);

    if page_number == 1 {
        start_index += 100;
    }

    // get Page header of the current page
    let page_header = get_page_header(&database[start_index..]).unwrap();

    // Get all the cell pointers
    let cell_pointers = parse_cell_pointers(
        &database[(start_index + page_header.size())..],
        page_header.number_of_cells,
    );

    // If it is an interior table. the content of the cell pointer are pointers to the left pages
    if page_header.page_type == BTreePage::InteriorTable {
        return cell_pointers
            .iter()
            .map(|cell_pointer| {
                let left_child_pointer_start = start_index + *cell_pointer as usize;
                let left_child_pointer_bytes =
                    &database[left_child_pointer_start..left_child_pointer_start + 4];
                let left_child_pointer =
                    u32::from_be_bytes(left_child_pointer_bytes.try_into().unwrap()) as usize;
                // Recursively get records from the left child pointer
                return get_them_records(database, page_size, left_child_pointer);
            })
            .flatten()
            .collect();
    }

    // If it is a leaf page. get the records directly
    if page_header.page_type == BTreePage::LeafTable {
        let records = cell_pointers
            .into_iter()
            .map(|cell_pointer| {
                let stream = &database[(start_index + cell_pointer as usize)..];
                let (_payload_size, offset) = parse_varint(stream); // total number of bytes of payload
                let (_rowid, read_bytes) = parse_varint(&stream[offset..]); // integer key (rowid).

                // Now the actual content start
                let record = parse_record(&stream[offset + read_bytes..]);

                record
            })
            .collect::<Result<Vec<_>>>();

        let records: Vec<Vec<String>> = records
            .unwrap()
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| String::from_utf8_lossy(value).into())
                    .collect()
            })
            .collect();

        return records;
    }

    return vec![];
}

pub struct DB {
    pub page_size: u16,
    pub schemas: Vec<Schema>,
}

impl DB {
    pub fn new(page_size: u16, schemas: Vec<Schema>) -> Self {
        Self { page_size, schemas }
    }

    pub fn process_query(&self, query: Sql, database: &Vec<u8>) -> Result<()> {
        // Store whether IndexInfo if you can use one for the query
        let mut idx_info: Option<IndexInfo> = None;

        // If there is a where clause. See if you can use the index.
        if let Some((key, _)) = query.where_clause.clone() {
            // See if you can find a index;
            let index_schema = self
                .schemas
                .iter()
                .find(|schema| schema.kind == "index" && schema.table_name == query.table);

            if let Some(index_schema) = index_schema {
                let (_, index_info) = parse_create_index(index_schema.sql.as_bytes()).unwrap();
                if index_info.column_name == key {
                    idx_info = Some(index_info);
                }
            }
        }

        let records = if let Some(index_info) = idx_info {
            self.get_records_using_index(database, index_info)?
        } else {
            self.get_records_for_schema(&query.table, database)?
        };

        match query.select_clause {
            SelectClause::Columns(columns) => {
                let fields = get_fields_in_table(&query.table, database)?;

                let indexes: Vec<usize> = columns
                    .iter()
                    .map(|col| {
                        fields
                            .iter()
                            .find_position(|field| field == &col)
                            .unwrap()
                            .0
                    })
                    .collect();

                for record in records {
                    if let Some((k, v)) = &query.where_clause {
                        let k_index = fields.iter().find_position(|field| *field == k).unwrap().0;
                        if record[k_index] != *v {
                            continue;
                        }
                    }

                    let resp = indexes.iter().map(|i| record[*i].clone()).join("|");
                    println!("{}", resp);
                }
            }
            SelectClause::FunctionCall(function_name) => {
                if function_name.eq_ignore_ascii_case("COUNT") {
                    println!("{}", records.len());
                }
            }
        }

        return Ok(());
    }

    // Get all records from a schema.
    pub fn get_records_for_schema(
        &self,
        tablename: &str,
        database: &Vec<u8>,
    ) -> Result<Vec<Vec<String>>> {
        let schema = self
            .schemas
            .iter()
            .find(|schema| schema.table_name == tablename)
            .unwrap();

        let records =
            get_them_records(database, self.page_size as usize, schema.root_page as usize);

        return Ok(records);
    }
    pub fn get_records_using_index(
        &self,
        database: &Vec<u8>,
        index_info: IndexInfo,
    ) -> Result<Vec<Vec<String>>> {
        // Get index schema
        let schema = self
            .schemas
            .iter()
            .find(|schema| schema.kind == "index" && schema.name == index_info.index_name)
            .unwrap();

        // get index pages

        // collect all rowIds in this vec
        let mut row_ids: Vec<usize> = Vec::new();
        parse_page(
            self.page_size,
            schema.root_page as usize,
            database,
            &mut row_ids,
        )?;

        // TODO:
        // Ideally now that you have all the rowids, you can get the records from the table by binary searching
        // since the cellpointers are ordered by rowid. skipping that for now.

        return Ok(vec![]);
    }
}
