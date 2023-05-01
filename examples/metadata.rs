use std::any::type_name;
use std::time::Instant;
use std::{env, fs, io::Read};
// use parquet::record::{Row,
//     RowAccessor};
use parquet_ops::{self, get_parquet_metadata};

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;

fn main() {
    let path = env::args()
        .skip(1)
        .next()
        .unwrap_or(paths::PATH_1.to_owned());

    println!("About to inspect metadata of file {path}");

    let timer = Instant::now();

    parquet_ops::show_parquet_metadata(&get_parquet_metadata(&path));

    let elapsed = timer.elapsed();

    println!(
        "Action Metadata for file '{}' with duration {:?}",
        &path, &elapsed
    );

    // restructure to check output file of merge (not created yet)
    let mut bytes = [0_u8; 10];
    if let Err(err) = fs::File::open(&path).unwrap().read(&mut bytes) {
        println!("Failed to open {path:?}. Obtained error: {err}");
    };
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    println!(
        "First 10 bytes are: {:?}",
        std::str::from_utf8(&bytes[0..7])
    );
}
