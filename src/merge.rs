use std::{
    fs,
    path::Path,
    sync::Arc};
use parquet::{
//        file::{reader::{SerializedFileReader, FileReader}, metadata::ParquetMetaData},
//        schema::parser::parse_message_type,
        record::{Row,
            RowAccessor
        }
};

use super::rowiterext::RowIterExt;
use super::writer::RowWriter;


pub fn merge_parquet(paths: Vec<&Path>, smaller: fn(&Row, &Row) -> bool) {

//    let mut merged_rows = Vec::new();

    let mut row_iters: Vec<RowIterExt> = paths
    .iter()
    .map(|p| RowIterExt::new(p))
    .filter(|rie| rie.head().is_some())
    .collect();

    if row_iters.len() < 1 {
        panic!("Nothing to merge");
    }
    let schema = Arc::new(row_iters[0].metadata().file_metadata().schema().clone());
    let mut row_writer = RowWriter::<fs::File>::new(Path::new("merged.parquet"), schema, 10000).unwrap();

    let mut row_processor = |row: Row| {
        if row.get_long(0).unwrap() % 10000 == 0 {
            println!("Row with id={}, acc={} and amount={}.", row.get_long(0).unwrap(), row.get_string(1).unwrap(), row.get_int(2).unwrap());
        }
        row_writer.append_row(row);
//        merged_rows.push(row);
    };

    loop {
        match row_iters.len() {
            0 => break,  // we are ready
            1 => {
                    row_iters[0].drain(&mut row_processor);
                    row_iters.remove(0);
            },
            _ => {
                if let Some((min_pos, _)) = row_iters
                        .iter()
                        .enumerate()
                        .reduce(|acc, other| {
                            if smaller(&acc.1.head().as_ref().unwrap(), &other.1.head().as_ref().unwrap()) {
                                acc
                            } else {
                                other
                            }
                        }) {
                    let (head, ready) = row_iters[min_pos].update_head();         
                    row_processor(head);
                    if  ready {
                        let _ = row_iters.swap_remove(min_pos);
                    }
                } else {
                    panic!("Could not find element while row_iters is not empty.")
                }
            }
        }
    }

    // const last_n: usize = 5;
    // println!("\nShowing last {last_n} rows");
    // merged_rows.into_iter()
    //     .rev()
    //     .take(last_n)
    //     .rev()
    //     .for_each(|row| println!("Row with id={}, acc={} and amount={}.", row.get_long(0).unwrap(), row.get_string(1).unwrap(), row.get_int(2).unwrap()))
    row_writer.close();
}


// // The more imperative (and less general) implementation. Complex due to the deeply nested if-then
// pub fn merge_parquet(path_1: &Path, path_2: &Path, smaller: fn(&Row, &Row) -> bool) {
//     if let Some((row_iter_1, parquet_meta_1)) = get_parquet_iter(path_1, None) {
//         if let Some((row_iter_2, parquet_meta_2)) = get_parquet_iter(path_2, None) {

//             let mut row_iters = vec![row_iter_1, row_iter_2];
//             let mut heads = Vec::new();
//             row_iters.iter_mut().for_each(|ri| heads.push(ri.next()));

//             loop {
//                 let min_pos = heads.iter().enumerate().fold(None, 
//                     |acc: Option<(usize, &Row)>, (idx, val)| {

//                         if let Some(val) = val {
//                             if let Some(acc) = acc {
//                                 if smaller(val, acc.1) {
//                                     Some((idx, val))
//                                 } else {
//                                     Some(acc)
//                                 }
//                             } else {
//                                 Some((idx, val))
//                             }
//                         } else {
//                             acc
//                         }
//                     });

//                     if min_pos.is_none() {
//                         break;
//                     }
//                 let (idx, val) = min_pos.unwrap();

//                 println!("Next element from iter:{idx} having value id={}, acc={} and amount={}.", val.get_long(0).unwrap(), val.get_string(1).unwrap(), val.get_int(2).unwrap());

//                 // advance the right row-iter
//                 heads[idx] = row_iters[idx].next();

//             }
//         } else {
//             panic!("Failed to read file-2.")
//         }

//     } else {
//         panic!("Failed to read file-1.")
//     }

// }

