use std::mem;
use parquet::{
        schema::{
            parser::parse_message_type,
            types::Type},
        record::{Row,
            reader::RowIter
        }
};
use crate::parquet_reader::{ParquetReaderEnum, get_parquet_reader};



pub struct RowIterExt<'a> {
    row_iter: RowIter<'a>,
    schema: Type,
    head: Option<Row>
}

impl<'a> RowIterExt<'a> {
    pub fn new(path: &'a str) -> Self {
        if let Some((mut row_iter, schema)) = get_parquet_iter(path, None) {

            let head = row_iter.next();
            RowIterExt {
                row_iter,
                schema,
                head
            }
        } else {
            panic!("Failed to create iterator for {}", path);
        }
    }

    pub fn schema(&self) -> &Type {
        &self.schema
    }


    pub fn head(&self) -> &Option<Row> {
        &self.head
    }

    pub fn update_head (&mut self) -> (Row, bool) {
        let mut head = self.row_iter.next();
        mem::swap(&mut self.head, &mut head);
        (head.unwrap(), self.head.is_none())
    }

    // get a vector of at most 'max_rows' rows or None
    pub fn take(&mut self, max_rows: u64) -> Option<Vec<Row>> {
        assert!(max_rows > 0); 
        if self.head == None {
            return None
        }
        let mut data = Vec::new();
        data.push(self.head.take().unwrap());
        let mut num_rows = 0;
        while num_rows < max_rows {
            if let Some(row) = self.row_iter.next() {
                data.push(row);
                num_rows += 1;
            } else {
                return Some(data); // we are ready. All data is consumed.
            }
        }
        // set the new head again, as not all data is consumed.
        self.head = self.row_iter.next();
        return Some(data);
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



/// get a projection from a message_type if String exists and parses to a valid parquet Schema (Type)
fn get_projection<'a>(message_type: Option<&'a str>) -> Option<Type> {
    message_type.map(|mt| parse_message_type(mt).unwrap())
}

/// create an iterator over the data of a Parquet-file.
/// If string is prefixed by 'mem:' this will be an in memory buffer, if is is prefixed by 's3:' it will be a s3-object. Otherswise it will be a path on the local file system. 
fn get_parquet_iter<'a>(path: &'a str, message_type: Option<&'a str>) -> Option<(RowIter<'a>, Type)> {
    //    let proj = parse_message_type(message_type).ok();
    let proj = get_projection(message_type);

    let reader = get_parquet_reader(path);

    let schema = if let Some(projection) = proj.as_ref() {
        projection.clone()
    } else {
        // no projection set, so return the full metadata
        reader.metadata().file_metadata().schema().clone()
    };
    

    let row_iter = match reader {
            ParquetReaderEnum::File(reader) => {
                RowIter::from_file_into(Box::new(reader))
            }
            ParquetReaderEnum::S3(reader) => RowIter::from_file_into(Box::new(reader))
        }.project(proj);  // make the mapping to the right schema

    if row_iter.is_err() {
        println!("Opening {path} failed with error: {:?}", row_iter.err());
        return None;
    } 

    Some((row_iter.unwrap(), schema))
}




/// run over a parquet row_iter and read all rows up to a maximum and return these as a vector
pub fn read_rows(path: &str, max_rows: Option<usize>, message_type: &str) -> Vec<Row>{
    let max_rows = max_rows.or(Some(1_000_000_000)).unwrap();

    let (res, _) = get_parquet_iter(path, Some(message_type)).unwrap();

    res.collect()
}

use itertools::Itertools;

/// run over a parquet row_iter and read all rows up to a maximum and return these as a vector with step-size applied.
pub fn read_rows_stepped(path: &str, step_size: usize, max_rows: Option<usize>, message_type: &str) -> Vec<Row>{
    let max_rows = max_rows.or(Some(1_000_000_000)).unwrap();

    let (res, _) = get_parquet_iter(path, Some(message_type)).unwrap();

    res.step(step_size).collect()
}


pub mod ttest {

    use super::get_parquet_iter;
    use parquet::record::{
        Row,
        RowAccessor};

    /// run over a parquet row_iter and read all rows up to a maximum.
    pub fn read_parquet_rowiter(path: &str, max_rows: Option<usize>, message_type: &str) -> Vec<Row>{
        let max_rows = max_rows.or(Some(1_000_000_000)).unwrap();

        let (res, _) = get_parquet_iter(path, Some(message_type)).unwrap();

        let mut data = Vec::new();

    let mut sum = 0;
    let mut last_idx = 0;
        for (i, row) in res.enumerate() {
            println!("\nresult {i}:  {row:?}   already accumulated {sum}");
            if let Ok(amount) = row.get_int(1) {
                println!("{i} has amount={amount}");
                sum += amount;
            }
            data.push(row);

            if i > max_rows { break; }
            last_idx = i;
        }

        println!("iterated over {last_idx}  rows with total amount = {sum}");

        data
    }

}

