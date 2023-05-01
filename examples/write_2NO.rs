use parquet_ops;
use std::time::Instant;
use std::{env, fs, io::Read, path::Path, thread};

// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;

fn get_u64_from_string(s: &str, err_msg: &str) -> Option<u64> {
    Some(s.replace("_", "").parse::<u64>().expect(err_msg))
}

const DEFAULT_NUM_EXTRA_COLUMNS: u64 = 135;

fn main() {
    //let action = env::args().next().unwrap_or("UNKNOWN".to_owned());

    let args: Vec<String> = env::args().collect();

    let action = "writ";
    let path_1 = paths::PATH_1;
    let path_2 = paths::PATH_2;

    let timer = Instant::now();

    let num_recs = if args.len() > 1 {
        get_u64_from_string(
            &args[1],
            "first argument should be 'num_recs' (a positive integer).",
        )
    } else {
        None
    };
    let group_size = if args.len() > 2 {
        get_u64_from_string(
            &args[2],
            "Second argument should be 'group_size' (a positive integer).",
        )
    } else {
        None
    };
    let num_extra_columns = if args.len() > 3 {
        get_u64_from_string(
            &args[3],
            "Third argument should be 'num_extra_columns' (a positive integer).",
        )
        .unwrap()
    } else {
        DEFAULT_NUM_EXTRA_COLUMNS
    } as usize;

    assert!(
        num_extra_columns < 1000,
        "Number of extra colums > 1000, which is excessive."
    );

    let num_recs_cpy = num_recs.clone();
    let group_size_cpy = group_size.clone();

    println!("Creating file with even-rows in {:?}", &path_1);
    let even_handle = thread::spawn(move || {
        parquet_ops::write_parquet(
            &path_1,
            num_extra_columns,
            num_recs,
            group_size,
            Some(|i| i % 2 == 0),
        )
        .unwrap()
    });

    println!("Creating file with odd-rows in {:?}", &path_2);
    let odd_handle = thread::spawn(move || {
        parquet_ops::write_parquet(
            &path_2,
            num_extra_columns,
            num_recs_cpy,
            group_size_cpy,
            Some(|i| i % 2 != 0),
        )
        .unwrap()
    });

    even_handle.join();
    odd_handle.join();

    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);

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
