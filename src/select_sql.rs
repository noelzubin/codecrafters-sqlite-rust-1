// Parser for SQL statements using peg   
peg::parser! {
    grammar sql_parser() for str {
        pub rule select_statement() -> Sql
            = kw("SELECT") ws()
            select_clause:select_clause() ws()
            kw("FROM") ws()
            table: identifier()
            where_clause:optional_where_clause()?
            { Sql { select_clause, table, where_clause } }

        rule select_clause() -> SelectClause
            = val2:function_call() { SelectClause::FunctionCall(val2) }
            / val:column_list() { SelectClause::Columns(val) }

        rule function_call() -> String
            = name:identifier() "(*)" { name.to_owned() }

        rule column_list() -> Vec<String> =
            column: (identifier() ** ("," wsz())) { column }
        
        rule quoted_string() -> String =
            "'" value:$([^'\'']*) "'" { value.to_owned() }

        rule optional_where_clause() -> (String, String) =
            ws() kw("WHERE") ws() key:identifier() wsz() "=" wsz() value:quoted_string() { (key.to_owned(), value.to_owned()) }

        rule identifier() -> String =
            s:$(['a'..='z' | 'A'..='Z' | '_']+) { s.to_owned() }

        rule ws() = quiet!{[' ' | '\t']+}

        rule wsz() = quiet!{[' ' | '\t']*}

        rule kw(kw: &'static str) -> () =
            input:$([_]*<{kw.len()}>)
            {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) } }
    }
}

#[derive(Debug, PartialEq)]
pub enum SelectClause {
    Columns(Vec<String>),
    FunctionCall(String),
}

// Final sql statement 
#[derive(Debug, PartialEq)]
pub struct Sql {
    pub select_clause: SelectClause, // What is selected
    pub table: String, // table to select from  
    pub where_clause: Option<(String, String)>, // optional where clause. only support direct string comparison for now  
}

pub fn parse_sql(input: &str) -> Result<Sql, peg::error::ParseError<peg::str::LineCol>> {
    sql_parser::select_statement(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() {
        struct TestCase(&'static str, Sql);

        let test_cases: Vec<TestCase> = vec![
            TestCase(
                "SELECT one FROM apples",
                Sql {
                    select_clause: SelectClause::Columns(vec!["one".to_string()]),
                    table: "apples".to_owned(),
                    where_clause: None,
                },
            ),
            TestCase(
                "SELECT one FROM apples WHERE key = 'value'",
                Sql {
                    select_clause: SelectClause::Columns(vec!["one".to_string()]),
                    table: "apples".to_owned(),
                    where_clause: Some(("key".to_owned(), "value".to_owned())),
                },
            ),
            TestCase(
                "SELECT one(*) FROM apples",
                Sql {
                    select_clause: SelectClause::FunctionCall("one".to_string()),
                    table: "apples".to_owned(),
                    where_clause: None,
                },
            ),
            TestCase(
                "SELECT one, two FROM apples",
                Sql {
                    select_clause: SelectClause::Columns(vec![
                        "one".to_string(),
                        "two".to_string(),
                    ]),
                    table: "apples".to_owned(),
                    where_clause: None,
                },
            ),
            TestCase(
                "select one, two fRoM apples",
                Sql {
                    select_clause: SelectClause::Columns(vec![
                        "one".to_string(),
                        "two".to_string(),
                    ]),
                    table: "apples".to_owned(),
                    where_clause: None,
                },
            ),
        ];

        for tc in test_cases {
            println!("\n\nrunning tests for [{}]", tc.0);
            assert_eq!(parse_sql(&tc.0).unwrap(), tc.1,);
        }
    }
}
