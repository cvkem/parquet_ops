use parquet::record::{Row, RowAccessor};
use parquet_ops;
use std::any::type_name;
use std::time::Instant;
use std::{env, fs, io::Read};

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;

fn smaller_test(row_1: &Row, row_2: &Row) -> bool {
    let k1 = row_1.get_long(0).unwrap();
    let k2 = row_2.get_long(0).unwrap();
    k1 <= k2
}

fn main() {
    let action = env::args().next().unwrap_or("UNKNOWN".to_owned());

    let path_1 = paths::PATH_1;

    let timer = Instant::now();

    parquet_ops::read_parquet_rowiter(path_1, None, parquet_ops::MESSAGE_TYPE);

    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);

    println!("\n Now reading all data again (all columns).");

    let timer = Instant::now();

    let output = parquet_ops::read_rows(path_1, None, parquet_ops::MESSAGE_TYPE);

    let elapsed = timer.elapsed();

    println!(
        "Action '{}' with duration {:?} and returned a vector of {}",
        &action,
        &elapsed,
        output.len()
    );

    println!("\n Now reading all data again (only a single string column).");

    let timer = Instant::now();

    let output = parquet_ops::read_rows_stepped(path_1, 50, parquet_ops::ACCOUNT_ONLY_TYPE);

    let elapsed = timer.elapsed();

    println!(
        "Action '{}' with duration {:?} and returned a vector of {}",
        &action,
        &elapsed,
        output.len()
    );

    let str_output = output
        .iter()
        .map(|x| x.get_string(0).unwrap())
        .fold(String::new(), |l, r| {
            if l.len() == 0 {
                r.to_owned()
            } else {
                l + ", " + r
            }
        });
    println!("The output:\n{}", str_output);

    // restructure to check output file of merge (not created yet)
    let mut bytes = [0_u8; 10];
    if let Err(err) = fs::File::open(&path_1).unwrap().read(&mut bytes) {
        println!("Failed to open {path_1:?}. Obtained error: {err}");
    };
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    println!(
        "First 10 bytes are: {:?}",
        std::str::from_utf8(&bytes[0..7])
    );
}
