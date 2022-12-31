use std::{
    mem
};
use parquet::{
        file::{reader::{SerializedFileReader, FileReader}, metadata::ParquetMetaData},
        schema::parser::parse_message_type,
        record::{Row,
            RowAccessor,
            reader::RowIter
        }
};


fn get_parquet_iter<'a>(path: &'a str, message_type: Option<&'a str>) -> Option<(RowIter<'a>, ParquetMetaData)> {
//    let proj = parse_message_type(message_type).ok();
    let proj = message_type.map(|mt| parse_message_type(mt).unwrap());
    println!(" The type = {:?}", proj);
    let reader = SerializedFileReader::try_from(path.to_owned()).unwrap();
    let parquet_metadata = reader.metadata();
    // clone needed to get a copy, as we currently only have a reference to an Arc<ParquetMetaData>
    let parquet_metadata  = parquet_metadata.clone();

    println!("Opened file with metadata {:?}", reader.metadata());
    let res = RowIter::from_file_into(Box::new(reader))
            .project(proj);
    if res.is_err() {
        println!(" failed with error: {:?}", res.err());
        return None;
    } 

    let row_iter = res.unwrap();

    Some((row_iter, parquet_metadata.clone()))
}



pub struct RowIterExt<'a> {
    row_iter: RowIter<'a>,
    metadata: ParquetMetaData,
    head: Option<Row>
}

impl<'a> RowIterExt<'a> {
    pub fn new(path: &'a str) -> Self {
        if let Some((mut row_iter, metadata)) = get_parquet_iter(path, None) {

            let head = row_iter.next();
            RowIterExt {
                row_iter,
                metadata,
                head
            }
        } else {
            panic!("Failed to create iterator for {}", path);
        }
    }

    // pub fn next(&mut self) -> Option<Row> {
    //     self.row_iter.next()
    // }

    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    pub fn head(&self) -> &Option<Row> {
        &self.head
    }

    pub fn update_head (&mut self) -> (Row, bool) {
        let mut head = self.row_iter.next();
        mem::swap(&mut self.head, &mut head);
        (head.unwrap(), self.head.is_none())
    }

    pub fn drain<F>(&mut self, row_proc: &mut F) where
        F: FnMut(Row) {
        loop {
            let (head, ready) = self.update_head();
            row_proc(head);
            if ready {
                break;
            }
        }

    }
}




pub fn read_parquet_rowiter(path: &str, max_rows: Option<usize>, message_type: &str) {
    let max_rows = max_rows.or(Some(1000000000)).unwrap();

    let (res, _) = get_parquet_iter(path, Some(message_type)).unwrap();

    let mut sum = 0;
    let mut last_idx = 0;
    for (i, row) in res.enumerate() {
//        println!("result {i}:  {row:?}");
        if let Ok(amount) = row.get_int(1) {
            println!("{i} has amount={amount}");
            sum += amount;
        }

        if i > max_rows { break; }
        last_idx = i;
    }

    println!("iterated over {last_idx}  fields with total amount = {sum}");
}




