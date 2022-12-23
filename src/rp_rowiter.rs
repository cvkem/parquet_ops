use std::path::Path;
use parquet::{
        file::{reader::{SerializedFileReader, FileReader}, metadata::ParquetMetaData},
        schema::parser::parse_message_type,
        record::{Row,
            RowAccessor,
            reader::RowIter
        }
};


fn get_parquet_iter<'a>(path: &'a Path, message_type: Option<&'a str>) -> Option<(RowIter<'a>, ParquetMetaData)> {
//    let proj = parse_message_type(message_type).ok();
    let proj = message_type.map(|mt| parse_message_type(mt).unwrap());
    println!(" The type = {:?}", proj);
//    let path = get_test_path("nested_maps.snappy.parquet");
//    let reader = SerializedFileReader::try_from(path.as_path()).unwrap();
//    let path = "./sample.parquet".to_owned();
//    let path = "/tmp/data/sample_1.parquet".to_owned();
    let reader = SerializedFileReader::try_from(path.to_string_lossy().into_owned()).unwrap();
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


pub fn read_parquet_rowiter(path: &Path, max_rows: Option<usize>, message_type: &str) {
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


pub fn merge_parquet(path_1: &Path, path_2: &Path, smaller: fn(&Row, &Row) -> bool) {
    if let Some((row_iter_1, parquet_meta_1)) = get_parquet_iter(path_1, None) {
        if let Some((row_iter_2, parquet_meta_2)) = get_parquet_iter(path_2, None) {

            let mut row_iters = vec![row_iter_1, row_iter_2];
            let mut heads = Vec::new();
            row_iters.iter_mut().for_each(|ri| heads.push(ri.next()));

            loop {
                let min_pos = heads.iter().enumerate().fold(None, 
                    |acc: Option<(usize, &Row)>, (idx, val)| {

                        if let Some(val) = val {
                            if let Some(acc) = acc {
                                if smaller(val, acc.1) {
                                    Some((idx, val))
                                } else {
                                    Some(acc)
                                }
                            } else {
                                Some((idx, val))
                            }
                        } else {
                            acc
                        }
                    });

                    if min_pos.is_none() {
                        break;
                    }
                let (idx, val) = min_pos.unwrap();

                println!("Next element from iter:{idx} having value id={}, acc={} and amount={}.", val.get_long(0).unwrap(), val.get_string(1).unwrap(), val.get_int(2).unwrap());

                // advance the right row-iter
                heads[idx] = row_iters[idx].next();

            }
        } else {
            panic!("Failed to read file-2.")
        }

    } else {
        panic!("Failed to read file-1.")
    }

}

