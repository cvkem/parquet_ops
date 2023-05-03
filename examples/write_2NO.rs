use parquet_ops;
use std::time::Instant;
use std::{env, fs, io::Read, path::Path, thread};

// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;

fn get_u64_from_string(s: &str, err_msg: &str) -> u64 {
    s.replace("_", "").parse::<u64>().expect(err_msg)
}

const DEFAULT_NUM_EXTRA_COLUMNS: u64 = 135;
const DEFAULT_STORE: &str = "local"; // "s3"

fn main() {
    let action = "writ";

    let mut args = env::args();
    println!("Program name = {}", args.next().unwrap());
    println!("Works on local files, unless you provide argument 's3' as first argument, as it operates on bucket 'parquet-exp' in s3.");
    println!("Usage: cargo run --example write_2NO <s3|local>  <num_recs>  <group_size>  <num_extra_columns>  <ordered (Y/N)");
    let store = args.next().unwrap_or(DEFAULT_STORE.to_owned());

    let (path_1, path_2) = if store == "s3" {
        let rewrite_s3 = |p: &str| {
            let mut s = "s3:parquet-exp:".to_owned();
            s.push_str(&p[2..]);
            s.to_owned()
        };
        (rewrite_s3(paths::PATH_1), rewrite_s3(paths::PATH_2))
    } else {
        (paths::PATH_1.to_owned(), paths::PATH_2.to_owned())
    };

    let num_recs = args.next().map(|s| {
        get_u64_from_string(
            &s,
            "first argument should be 'num_recs' (a positive integer).",
        )
    });
    let group_size = args.next().map(|s| {
        get_u64_from_string(
            &s,
            "Second argument should be 'group_size' (a positive integer).",
        )
    });
    let num_extra_columns = args.next().map_or(DEFAULT_NUM_EXTRA_COLUMNS, |s| {
        get_u64_from_string(
            &s,
            "Third argument should be 'num_extra_columns' (a positive integer).",
        )
    }) as usize;
    let ordered = args.next().map_or(true, |s| s.to_uppercase() == "Y");

    assert!(
        num_extra_columns < 1000,
        "Number of extra colums > 1000, which is excessive."
    );

    let timer = Instant::now();

    let num_recs_cpy = num_recs.clone();
    let group_size_cpy = group_size.clone();

    {
        let path_even = path_1.to_owned();
        println!("Creating file with even-rows in {:?}", &path_even);
        let even_handle = thread::spawn(move || {
            parquet_ops::write_parquet(
                &path_even,
                num_extra_columns,
                num_recs,
                group_size,
                Some(|i| i % 2 == 0),
                ordered,
            )
            .unwrap()
        });

        let path_odd = path_2.to_owned();
        println!("Creating file with odd-rows in {:?}", &path_odd);
        let odd_handle = thread::spawn(move || {
            parquet_ops::write_parquet(
                &path_odd,
                num_extra_columns,
                num_recs_cpy,
                group_size_cpy,
                Some(|i| i % 2 != 0),
                ordered,
            )
            .unwrap()
        });

        even_handle.join();
        odd_handle.join();
    }

    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);

    if store != "s3" {
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
    };
}
