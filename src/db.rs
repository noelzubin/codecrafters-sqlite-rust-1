use std::{
    collections::HashMap,
    fs::File,
    vec,
};

use crate::{
    creation_sql::{parse_create_index, parse_creation, Field, IndexInfo},
    header::{BTreePage, PageHeader},
    record::parse_record,
    schema::Schema,
    select_sql::{SelectClause, Sql},
    util,
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

fn parse_index_payload(stream: &[u8]) -> Result<Vec<Vec<u8>>> {
    let (_payload_size, payload_size_bytes) = parse_varint(stream);
    let key_record = parse_record(&stream[payload_size_bytes..])?;
    Ok(key_record)
}

pub fn parse_24bit_be_twos_complement(bytes: &[u8]) -> i64 {
    match &bytes.len() {
        3 => i32::from_be_bytes([
            if bytes[0] & 0x80 != 0 { 0xff } else { 0 },
            bytes[0],
            bytes[1],
            bytes[2],
        ]) as i64,
        2 => i16::from_be_bytes([bytes[0], bytes[1]]) as i64,
        1 => bytes[0] as i64,
        _ => panic!("SHOULDNT BE HERE"),
    }
}

pub struct DB {
    pub page_size: u16,
    pub schemas: Vec<Schema>,
    pub file: File,
}

pub struct Record {
    pub row_id: String,
    pub columns: Vec<String>,
}

impl DB {
    /// Create a new instance of the DB.
    pub fn new(page_size: u16, schemas: Vec<Schema>, file: File) -> Self {
        Self {
            page_size,
            schemas,
            file,
        }
    }

    /// Process a query
    /// Tries to use index if possible.
    /// Else does a full scan.
    /// Only supports single equality check in where clause for now.
    pub fn process_query(&self, query: Sql) -> Result<()> {
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

        let fields = self.get_fields_in_table(&query.table)?;

        let records = if let Some(index_info) = idx_info {
            // Get records using index

            let (_k, value) = &query.where_clause.clone().unwrap();
            // Get all matching rowIds
            let row_ids = self.get_row_ids_using_index(index_info, &value)?;

            let schema = self
                .schemas
                .iter()
                .find(|schema| schema.table_name == query.table)
                .unwrap();

            let records: Vec<Record> = row_ids
                .iter()
                .map(|row_id| self.get_record_by_row_id(*row_id as u64, schema.root_page as usize))
                .collect();

            records
        } else {
            // Get records using full scan
            let schema = self
                .schemas
                .iter()
                .find(|schema| schema.table_name == query.table).unwrap();

            let mut records =
                self.get_all_records_for_table(self.page_size as usize, schema.root_page as usize);

            //  filter by where clause
            if let Some((k, v)) = &query.where_clause {
                records = records
                    .into_iter()
                    .filter(|record| {
                        let (ind, field) = &fields[k];
                        let value = util::get_value_for_record(record, *ind, field);
                        value == *v
                    })
                    .collect();
            };

            records
        };

        match query.select_clause {
            SelectClause::Columns(columns) => {
                records.iter().for_each(|record| {
                    let resp = columns
                        .iter()
                        .map(|col| {
                            let (ind, field) = &fields[col];
                            util::get_value_for_record(record, *ind, field)
                        })
                        .join("|");

                    println!("{}", resp);
                });
            }
            SelectClause::FunctionCall(function_name) => {
                if function_name.eq_ignore_ascii_case("COUNT") {
                    println!("{}", records.len());
                }
            }
        }

        return Ok(());
    }

    // Get all rowIds filtered by given value
    fn get_row_ids_using_index(
        &self,
        index_info: IndexInfo,
        value: &str,
    ) -> Result<Vec<usize>> {
        // Get index schema
        let schema = self
            .schemas
            .iter()
            .find(|schema| schema.kind == "index" && schema.name == index_info.index_name)
            .unwrap();

        // collect all rowIds in this vec
        let mut row_ids: Vec<usize> = Vec::new();

        self.parse_index_page(
            self.page_size,
            schema.root_page as usize,
            &mut row_ids,
            value,
        )?;

        return Ok(row_ids);
    }

    /// utility function to read a page from DB
    fn read_page(&self, page_number: usize) -> Result<Vec<u8>> {
        util::read_page(&self.file, self.page_size, page_number)
    }

    /// Get a single record by row_id. Does a btree traversal.
    fn get_record_by_row_id(&self, row_id: u64, page_number: usize) -> Record {
        // Start index of the page
        let page = self.read_page(page_number).unwrap();
        let page_header = get_page_header(&page).unwrap();

        // Get all the cell pointers
        let cell_pointers =
            parse_cell_pointers(&page[page_header.size()..], page_header.number_of_cells);

        // If it is an interior table. the content of the cell pointer are pointers to the left pages
        if page_header.page_type == BTreePage::InteriorTable {
            for cp in cell_pointers.iter() {
                let left_child_pointer_start = *cp as usize;
                let left_child_pointer_bytes =
                    &page[left_child_pointer_start..left_child_pointer_start + 4];
                let left_child_pointer =
                    u32::from_be_bytes(left_child_pointer_bytes.try_into().unwrap()) as usize;
                let (key, _offset) = parse_varint(&page[left_child_pointer_start + 4..]);
                // Recursively get records from the left child pointer
                if row_id <= (key as u64) {
                    return self.get_record_by_row_id(row_id, left_child_pointer);
                }
            }

            return self
                .get_record_by_row_id(row_id, page_header.right_most_pointer.unwrap() as usize);
        }

        // If it is a leaf page. get the records directly
        if page_header.page_type == BTreePage::LeafTable {
            for cell_pointer in cell_pointers.into_iter() {
                let stream = &page[cell_pointer as usize..];
                let (_payload_size, offset) = parse_varint(stream); // total number of bytes of payload
                let (key, read_bytes) = parse_varint(&stream[offset..]); // integer key (rowid).

                if (key as u64) != row_id {
                    continue;
                }

                // Now the actual content start
                let record = parse_record(&stream[offset + read_bytes..]).unwrap();

                let record: Vec<String> = record
                    .iter()
                    .map(|value| String::from_utf8_lossy(value).into())
                    .collect();

                return Record {
                    row_id: key.to_string(),
                    columns: record,
                };
            }
        }

        unreachable!("if row_id exists so should the row");
    }

    /// Get fields in a table indexed by the the field name.
    fn get_fields_in_table(&self, tablename: &str) -> Result<HashMap<String, (usize, Field)>> {
        let schema = &self
            .schemas
            .iter()
            .find(|schema| schema.table_name == tablename)
            .unwrap();

        let (_, create_statement) = parse_creation(schema.sql.as_bytes()).unwrap();

        return Ok(create_statement
            .fields
            .into_iter()
            .enumerate()
            .map(|(ind, field)| (field.name.clone(), (ind, field)))
            .collect());
    }

    // Get records from the given page.
    fn get_all_records_for_table(&self, page_size: usize, page_number: usize) -> Vec<Record> {
        let page = self.read_page(page_number).unwrap();

        // get Page header of the current page
        let page_header = get_page_header(&page).unwrap();

        // Get all the cell pointers
        let cell_pointers =
            parse_cell_pointers(&page[page_header.size()..], page_header.number_of_cells);

        // If it is an interior table. the content of the cell pointer are pointers to the left pages
        if page_header.page_type == BTreePage::InteriorTable {
            let mut records: Vec<Record> = cell_pointers
                .iter()
                .map(|cell_pointer| {
                    let left_child_pointer_start = *cell_pointer as usize;
                    let left_child_pointer_bytes =
                        &page[left_child_pointer_start..left_child_pointer_start + 4];
                    let left_child_pointer =
                        u32::from_be_bytes(left_child_pointer_bytes.try_into().unwrap()) as usize;
                    // Recursively get records from the left child pointer
                    return self.get_all_records_for_table(page_size, left_child_pointer);
                })
                .flatten()
                .collect();

            records.extend(
                self.get_all_records_for_table(page_size, page_header.right_most_pointer.unwrap() as usize),
            );

            return records;
        }

        // If it is a leaf page. get the records directly
        if page_header.page_type == BTreePage::LeafTable {
            let records = cell_pointers
                .into_iter()
                .map(|cell_pointer| {
                    let stream = &page[(cell_pointer as usize)..];
                    let (_payload_size, offset) = parse_varint(stream); // total number of bytes of payload
                    let (row_id, read_bytes) = parse_varint(&stream[offset..]); // integer key (rowid).

                    // Now the actual content start
                    let record = parse_record(&stream[offset + read_bytes..]).unwrap();

                    let record: Vec<String> = record
                        .iter()
                        .map(|value| String::from_utf8_lossy(value).into())
                        .collect();

                    Record {
                        row_id: row_id.to_string(),
                        columns: record,
                    }
                })
                .collect::<Vec<Record>>();

            return records;
        }

        return vec![];
    }

    // Get all records from the index page 
    fn parse_index_page(
        &self,
        page_size: u16,
        page_number: usize,
        row_collector: &mut Vec<usize>,
        value: &str,
    ) -> Result<()> {
        let page = self.read_page(page_number)?;
        // Get the index page
        let page_header = get_page_header(&page)?;


        if page_header.page_type == BTreePage::InteriorIndex {
            let cell_pointers =
        // get the index cell pointers
        parse_cell_pointers(&page[12..], page_header.number_of_cells);

            for cell_pointer in cell_pointers.iter() {
                let left_child_pointer_start = *cell_pointer as usize;

                let left_child_pointer_bytes =
                    &page[left_child_pointer_start..left_child_pointer_start + 4];

                let left_child_pointer =
                    u32::from_be_bytes(left_child_pointer_bytes.try_into().unwrap()) as usize;
                let mut offset = 4;

                let (_payload_size, payload_offset) =
                    parse_varint(&page[(left_child_pointer_start + offset)..]);
                offset += payload_offset;

                let record = parse_record(&page[(left_child_pointer_start + offset)..]).unwrap();

                let key = String::from_utf8_lossy(&record[0]);

                // If value_to_check > cur_key no need to check left tree
                if value > &key {
                    continue;
                }

                // value_to_check == cur_key then check left pointer as well.
                if value == &key {
                    let rowid = record[1].clone();
                    let rowid = parse_24bit_be_twos_complement(&rowid);
                    row_collector.push(rowid as usize);
                }

                self.parse_index_page(page_size, left_child_pointer as usize, row_collector, value)
                    .unwrap();

                // if value_to_check < cur_key. Need to check the left_pointer 1 last time.
                if value < &key {
                    break;
                }
            }

            self.parse_index_page(
                page_size,
                page_header.right_most_pointer.unwrap() as usize,
                row_collector,
                value,
            )
            .expect("Surely there is a right most pointer");

            return Ok(());
        }
        if page_header.page_type == BTreePage::LeafIndex {
            let cell_pointers = parse_cell_pointers(&page[8..], page_header.number_of_cells);

            for cell_pointer in &cell_pointers {
                let cell_pointer_start = *cell_pointer as usize;

                let stream = &page[cell_pointer_start as usize..];

                let key_record = parse_index_payload(stream)?;

                let key = &key_record[0];

                if key == value.as_bytes() {
                    let rowid = key_record[1].clone();
                    let rowid = parse_24bit_be_twos_complement(&rowid);

                    row_collector.push(rowid as usize);
                }
            }
        }

        Ok(())
    }
}
