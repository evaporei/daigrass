use std::fs::{self, File};
use std::io::{self, BufRead, Seek, SeekFrom};
use std::path::Path;

fn buf_reader<P>(filename: P) -> io::Result<io::BufReader<File>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file))
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    buf_reader(filename).map(|b| b.lines())
}

type Parts = Vec<String>;

#[derive(Debug, Default)]
struct Query {
    projection: Option<Parts>, // fields/attributes
    selection: Option<Parts>,  // conditions
    scan: Option<Parts>,       // tables
}

use serde_json::Value;

impl From<Value> for Query {
    fn from(json: Value) -> Self {
        let mut query = Query::default();
        for clause in json.as_array().unwrap() {
            if clause[0] == "PROJECTION" {
                // lol
                query.projection = Some(
                    clause[1]
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|a| a.as_str().unwrap().to_owned())
                        .collect(),
                );
            }
            if clause[0] == "SELECTION" {
                // lol
                query.selection = Some(
                    clause[1]
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|a| a.as_str().unwrap().to_owned())
                        .collect(),
                );
            }
            if clause[0] == "SCAN" {
                // lol
                query.scan = Some(
                    clause[1]
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|a| a.as_str().unwrap().to_owned())
                        .collect(),
                );
            }
        }
        query
    }
}

struct FileScan {
    offset: usize,
    table: String,
    file: io::BufReader<File>,
}

impl FileScan {
    fn new(table: &str) -> Self {
        let mut file = buf_reader(format!("./ml-20m/{table}.csv")).unwrap();
        let mut trash = vec![];
        let offset = file.read_until(b'\n', &mut trash).unwrap();

        Self {
            offset,
            table: table.to_owned(),
            file,
        }
    }
}

trait Offset: Iterator<Item = Row> {
    fn offset(&self) -> usize;
    // move elsewhere
    fn table(&self) -> &str;
}

impl Offset for FileScan {
    fn offset(&self) -> usize {
        self.offset
    }

    fn table(&self) -> &str {
        &self.table
    }
}

// Tuple
type Row = Vec<String>;

impl Iterator for FileScan {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let mut raw = vec![];
        let read = self.file.read_until(b'\n', &mut raw).unwrap();
        if read == 0 {
            return None;
        }
        self.offset += read;

        // TODO: abstract
        Some(
            raw.split(|b| *b == b',')
                .map(|bytes| std::str::from_utf8(bytes))
                .map(Result::unwrap)
                .map(|s| s.trim())
                .map(|s| s.to_owned())
                .collect(),
        )
    }
}

#[derive(Clone, Debug)]
struct Schema {
    table: String,
    fields: Vec<String>,
}

impl Schema {
    fn new(table: &str) -> Self {
        Self {
            // omaga
            fields: read_lines(format!("./ml-20m/{table}.csv"))
                .unwrap()
                .next()
                .expect("at least one line in file (the schema)")
                .unwrap()
                .split(',')
                .map(|s| s.to_owned())
                .collect(),
            table: table.to_owned(),
        }
    }
}

/// For now the projection retrieves the fields
/// in the order of the schema (first line in csv).
struct Projector<'a> {
    source: &'a mut dyn Iterator<Item = Row>,
    projection: Vec<String>,
    idxs: Vec<usize>,
}

impl<'a> Projector<'a> {
    fn new(
        projection: Vec<String>,
        source: &'a mut dyn Iterator<Item = Row>,
        schema: &Schema,
    ) -> Self {
        Self {
            idxs: projection
                .iter()
                .map(|p| {
                    schema
                        .fields
                        .iter()
                        .position(|f| f == p)
                        // we can remove later if we want silent filter (if not found)
                        // which can be useful once we have multiple scanners (multi-table queries)
                        .expect(&format!(
                            "'{p}' field not found in table '{}'",
                            schema.table
                        ))
                })
                .collect(),
            source,
            projection,
        }
    }
}

impl<'a> Iterator for Projector<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.source.next()?;

        if self.projection.is_empty() {
            return Some(row);
        }

        Some(
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| self.idxs.contains(i))
                .map(|(_, s)| s)
                .collect(),
        )
    }
}

struct Selector<'a> {
    selection: Vec<String>,
    source: &'a mut dyn Iterator<Item = Row>,
    idx: usize,
    // ugly but it works
    ran: bool,
}

impl<'a> Selector<'a> {
    fn new(
        selection: Vec<String>,
        source: &'a mut dyn Iterator<Item = Row>,
        schema: &Schema,
    ) -> Self {
        Self {
            idx: schema
                .fields
                .iter()
                .position(|f| f == &selection[0])
                // we can remove later if we want silent filter (if not found)
                // which can be useful once we have multiple scanners (multi-table queries)
                .expect(&format!(
                    "'{}' field not found in table '{}'",
                    selection[0], schema.table
                )),
            selection,
            source,
            ran: false,
        }
    }
}

impl<'a> Iterator for Selector<'a> {
    // look at me, I'm different o-o
    type Item = Vec<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ran {
            return None;
        }
        self.ran = true;

        let mut results = vec![];
        while let Some(row) = self.source.next() {
            match &self.selection[1][..] {
                "EQUALS" => {
                    if row[self.idx] == self.selection[2] {
                        results.push(row);
                    }
                }
                _ => {
                    // TODO: should print only on ::new
                    println!(
                        "warn: selection operation '{}' not supported",
                        self.selection[1]
                    );
                }
            }
        }
        Some(results)
    }
}

type Field = String;

struct IndexBuilder<'a> {
    source: &'a mut dyn Offset,
    idx: usize,
    ran: bool,
}

impl<'a> IndexBuilder<'a> {
    fn new(field: &str, source: &'a mut dyn Offset, schema: &Schema) -> Self {
        Self {
            idx: schema
                .fields
                .iter()
                .position(|f| f == field)
                // we can remove later if we want silent filter (if not found)
                // which can be useful once we have multiple scanners (multi-table queries)
                .expect(&format!(
                    "'{}' field not found in table '{}'",
                    field, schema.table
                )),
            source,
            ran: false,
        }
    }
}

use std::collections::BTreeMap;

struct Index {
    ptrs: BTreeMap<Field, usize>,
    table: String,
}

impl Index {
    fn new(ptrs: BTreeMap<Field, usize>, table: &str) -> Self {
        Index {
            ptrs,
            table: table.into(),
        }
    }

    fn search(&self, value: &str) -> Option<Row> {
        let mut file = buf_reader(format!("./ml-20m/{}.csv", self.table)).unwrap();
        file.seek(SeekFrom::Start(*self.ptrs.get(value).unwrap() as u64))
            .unwrap();
        let mut row = vec![];
        let _ = file.read_until(b'\n', &mut row).unwrap();
        // TODO: abstract
        Some(
            row.split(|b| *b == b',')
                .map(|bytes| std::str::from_utf8(bytes))
                .map(Result::unwrap)
                .map(|s| s.trim())
                .map(|s| s.to_owned())
                .collect(),
        )
    }
}

impl<'a> Iterator for IndexBuilder<'a> {
    // I'm different too!
    type Item = Index;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ran {
            return None;
        }
        self.ran = true;

        let mut results = BTreeMap::new();
        while let Some(row) = self.source.next() {
            results.insert(row[self.idx].clone(), self.source.offset());
        }
        Some(Index::new(results, self.source.table()))
    }
}

fn main() {
    let query = fs::read_to_string("query.json").unwrap();
    let json: Value = serde_json::from_str(&query).unwrap();
    let query = Query::from(json);

    let scan = query
        .scan
        .expect("there should be at least one table in the scan list");
    // TODO: multiple scanners
    let mut scanner = FileScan::new(&scan[0]);
    let schema = Schema::new(scanner.table());

    let results: Vec<Row> = match (query.selection, query.projection) {
        (Some(selection), Some(projection)) => {
            let mut selector = Selector::new(selection, &mut scanner, &schema)
                .into_iter()
                .flatten();
            let projector = Projector::new(projection, &mut selector, &schema);
            projector.into_iter().collect()
        }
        (Some(selection), None) => {
            let selector = Selector::new(selection, &mut scanner, &schema);
            selector.into_iter().flatten().collect()
        }
        (None, Some(projection)) => {
            let projector = Projector::new(projection, &mut scanner, &schema);
            projector.into_iter().collect()
        }
        (None, None) => scanner.into_iter().collect(),
    };

    println!("results:");
    println!("{results:?}");

    let mut scanner = FileScan::new(&scan[0]);
    // meh, this is a vec... bleeping FromIterator
    let index: Vec<Index> = IndexBuilder::new("movieId", &mut scanner, &schema)
        .into_iter()
        .collect();
    // oops I'm returning 5001
    println!("bin search: {:?}", index[0].search("5000"));
}