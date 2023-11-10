use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{alphanumeric1, multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::opt,
    multi::{many0, many1},
    sequence::{delimited, tuple},
    IResult,
};

#[derive(Debug)]
pub struct IndexInfo {
    pub index_name: String, // The name of the index
    pub table_name: String, // the table for which index is created
    pub column_name: String, // The column on which table is created. 
}

// Parse a create index sql query. 
pub fn parse_create_index(input: &[u8]) -> IResult<&[u8], IndexInfo> {
    let (
        remaining_input,
        (_, _, _, _, _, index_name, _, _, _, table_name, _, _, _, column_name, _, _),
    ) = tuple((
        tag_no_case("create"),
        multispace1,
        opt(tuple((tag("unique"), multispace1))),
        tag_no_case("index"),
        multispace1,
        identifier,
        multispace1,
        tag_no_case("on"),
        multispace1,
        identifier,
        multispace0,
        tag("("),
        multispace0,
        identifier,
        multispace0,
        tag(")"),
    ))(input)?;

    Ok((
        remaining_input,
        IndexInfo {
            index_name: index_name,
            table_name,
            column_name,
        },
    ))
}

// match an identifier
// Identifiers with spaces are delimited by double quotes
fn identifier(input: &[u8]) -> IResult<&[u8], String> {
    let (input, name) = alt((
        delimited(
            tag("\""),
            take_while1(|ch| is_sql_identifier(ch) || ch == b' '),
            tag("\""),
        ),
        take_while1(is_sql_identifier),
    ))(input)?;

    let name = String::from_utf8(name.to_vec()).unwrap();

    Ok((input, name))
}

// Parse sql query for the creation of a table
pub fn parse_creation(input: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, _, table, _, _, _, fields, _, _, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(tuple((tag_no_case("IF NOT EXISTS"), multispace1))),
        identifier,
        multispace0,
        tag("("),
        multispace0,
        field_specification_list,
        multispace0,
        tag(")"),
        opt(tag(";")),
    ))(input)?;

    Ok((remaining_input, CreateTableStatement { table, fields }))
}

fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

fn field_specification_list(input: &[u8]) -> IResult<&[u8], Vec<Field>> {
    many1(field_specification)(input)
}

fn field_specification(input: &[u8]) -> IResult<&[u8], Field> {
    let (remaining_input, (column, _, _, _)) = tuple((
        identifier,
        opt(delimited(multispace0, alphanumeric1, multispace0)), // type
        many0(column_constraint),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)?;

    Ok((remaining_input, Field { name: column }))
}

fn column_constraint(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let not_null = delimited(multispace0, tag_no_case("NOT NULL"), multispace0);

    let auto_increment = delimited(multispace0, tag_no_case("AUTOINCREMENT"), multispace0);

    let primary_key = delimited(multispace0, tag_no_case("PRIMARY KEY"), multispace0);

    alt((not_null, auto_increment, primary_key))(input)
}



#[derive(Debug, PartialEq)]

pub struct Field {
    pub name: String,
}

#[derive(Debug, PartialEq)]

pub struct CreateTableStatement {
    pub table: String,

    pub fields: Vec<Field>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() {
        let statement = "CREATE TABLE companies\n(\n\tid integer primary key autoincrement\n, name text, domain text, year_founded text, industry text, \"size range\" text, locality text, country text, current_employees text, total_employees text)";
        let resp = parse_creation(statement.as_bytes()).unwrap();
    }

    #[test]
    fn test_parse_create_index() {
        let statement = "CREATE INDEX idx_companies_country\n\ton companies (country)";
        let resp = parse_create_index(statement.as_bytes()).unwrap();
    }
}