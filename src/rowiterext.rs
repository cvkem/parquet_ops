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



/// create an iterator over the data of a Parquet-file.
/// If string is prefixed by 'mem:' this will be an in memory buffer, if is is prefixed by 's3:' it will be a s3-object. Otherswise it will be a path on the local file system. 
fn get_parquet_iter<'a>(path: &'a str, message_type: Option<&'a str>) -> Option<(RowIter<'a>, ParquetMetaData)> {
    //    let proj = parse_message_type(message_type).ok();
        let proj = message_type.map(|mt| parse_message_type(mt).unwrap());
        println!(" The type = {:?}", proj);

        // we differentiate at this level for the different types of inputs as lower levels can not handle this more generic
        // as the Associated types are in the way on ChunkReader, and also on SerializedFileReader as it wants to see the generic.
        // handling it at this level introduces some source-code-duplication, but that is manageable.
        let (ri_res, parquet_metadata) = match path.split(':').next().unwrap() {
            prefix if path.len() == prefix.len() => {
                    let reader = SerializedFileReader::try_from(path.to_owned()).unwrap();
                    let parquet_metadata = reader.metadata().clone();
                    let ri_res = RowIter::from_file_into(Box::new(reader))
                        .project(proj);
                    (ri_res, parquet_metadata)        
                }
            "mem" =>panic!("prefix 'mem:'can best be handled via temp-files, or all data should be incoded in the path-string"),
            "s3" => panic!("{path}: S3 still has to be implemented."),
            prefix => panic!("get_parquet_iter not implemented for prefix {prefix} of path {path}")
            };

        if ri_res.is_err() {
            println!("Opening {path} failed with error: {:?}", ri_res.err());
            return None;
        } 
    
        let row_iter = ri_res.unwrap();
    
        Some((row_iter, parquet_metadata))
    }
    

// failed experiment: Associated Type is manditory, so I can not generalize
//
// use parquet::file::reader::ChunkReader;
// use bytes::Bytes;
// use std::io::BufReader;
// use std::fs;

// fn create_SerializedFileReader(path: &str) -> Box<SerializedFileReader> {
//      match path.split(':').next().unwrap() {
//         path => {
//                 let file = fs::OpenOptions::new()
//                     .read(true)
//                     .open(path)
//                     .unwrap();
//                 let chunk_reader = ChunkReader::new(file);
//                 let SF_reader = SerializedFileReader::new(chunk_reader);
//                 Box::new(SF_reader)
//             },
// //        "mem" => Box::new(Bytes::new()), // how to get this filled up?
// //        "s3" => println!("{s}: S3"),
//         prefix => panic!("Unknown prefix '{prefix}' on file {path}")
//     }
// }
    


pub fn read_parquet_rowiter(path: &str, max_rows: Option<usize>, message_type: &str) -> Vec<Row>{
    let max_rows = max_rows.or(Some(1000000000)).unwrap();

    let (res, _) = get_parquet_iter(path, Some(message_type)).unwrap();

    let mut data = Vec::new();

//    let mut sum = 0;
//    let mut last_idx = 0;
    for (i, row) in res.enumerate() {
        // println!("result {i}:  {row:?}");
        // if let Ok(amount) = row.get_int(1) {
        //     println!("{i} has amount={amount}");
        //     sum += amount;
        // }
        data.push(row);

        if i > max_rows { break; }
//        last_idx = i;
    }

//    println!("iterated over {last_idx}  fields with total amount = {sum}");

    data
}




