use parquet::record::{Row, RowAccessor};
use std::sync::Arc;

use super::rowiterext::RowIterExt;
use super::rowwritebuffer::RowWriteBuffer;

use super::REPORT_APPEND_STEP;

pub fn merge_parquet_fake(_paths: Vec<&str>, merged_path: &str, _smaller: fn(&Row, &Row) -> bool) {
    use crate::ttypes::{get_test_schema, test_parquet_row};

    let num_extra_columns: i16 = 135;
    let num_rows: u64 = 20; // 20_000;
    let schema = get_test_schema(num_extra_columns);

    let mut row_writer = RowWriteBuffer::new(merged_path, schema, 10000).unwrap();

    println!("Fill merge_data with fake data (to circumvent the opening of multiple files)");
    (0..num_rows).for_each(|id| row_writer.append_row(test_parquet_row(id, num_extra_columns)));

    println!("Closing the RowWriteBuffer (merge_fake)");
    row_writer.close();
}

pub fn merge_parquet(paths: Vec<&str>, merged_path: &str, smaller: fn(&Row, &Row) -> bool) {
    // use crate::barrier::Barrier;
    // let mut barriers = Barrier::new(0xeeee, 10_000);

    // barriers.add_barrier();
    // let mut b1: u64 = 1;

    // println!("TMP: About to open row_iterators [press NewLine]");
    // let mut null = Default::default();
    // std::io::stdin().read_line(&mut null);

    let mut row_iters: Vec<RowIterExt> = paths
        .iter()
        .map(|p| RowIterExt::new(p))
        .filter(|rie| rie.head().is_some())
        .collect();

    if row_iters.len() < 1 {
        panic!("Nothing to merge");
    }

    // println!("TMP: About to open row_writeBuffer [press NewLine]");
    // let mut null = Default::default();
    // std::io::stdin().read_line(&mut null);

    // barriers.add_barrier();
    // let mut b2: u64 = 2;

    let schema = Arc::new(row_iters[0].schema().clone());
    let mut row_writer = RowWriteBuffer::new(merged_path, schema, 10000).unwrap();

    let mut row_processor = |row: Row| {
        if row.get_long(0).unwrap() % REPORT_APPEND_STEP == 0 {
            println!(
                "Row with id={}, acc={} and amount={}.",
                row.get_long(0).unwrap(),
                row.get_string(1).unwrap(),
                row.get_int(2).unwrap()
            );
        }
        row_writer.append_row(row);
    };

    // println!("TMP: About to start the Loop [press NewLine]");
    // let mut null = Default::default();
    // std::io::stdin().read_line(&mut null);

    // barriers.add_barrier();
    // let mut b3: u64 = 3;
    // let mut b4: u64 = 0;

    let _report = true;

    loop {
        match row_iters.len() {
            0 => break, // we are ready
            1 => {
                println!("TMP: DRAINING the last one");
                row_iters[0].drain(&mut row_processor);
                row_iters.remove(0);
            }
            _ => {
                if let Some((min_pos, _)) = row_iters.iter().enumerate().reduce(|acc, other| {
                    if smaller(
                        &acc.1.head().as_ref().unwrap(),
                        &other.1.head().as_ref().unwrap(),
                    ) {
                        acc
                    } else {
                        other
                    }
                }) {
                    let (head, ready) = row_iters[min_pos].update_head();
                    row_processor(head);
                    if ready {
                        println!(
                            "TMP: RowIter at {min_pos} is Ready, so closing 1 out of {}",
                            row_iters.len()
                        );

                        let _ = row_iters.swap_remove(min_pos);
                    }
                } else {
                    panic!("Could not find element while row_iters is not empty.")
                }
            }
        }

        // if report {
        //     println!("TMP: Handled first iteration and written an item [press NewLine]");
        //     let mut null = Default::default();
        //     std::io::stdin().read_line(&mut null);

        //     report = false;
        // }
        // barriers.add_barrier();
        // b4 = 4;
    }

    println!("Closing the RowWriteBuffer  row_writer.close()");
    row_writer.close();
    // println!("Closed\nUseless memory for checks contains {} (expect 10)", b1 + b2 + b3 + b4);
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
