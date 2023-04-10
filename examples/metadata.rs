use std::{
    env,
    fs, 
    path::Path, 
    io::Read};
use std::time::Instant;
use std::any::type_name;
use parquet::record::{Row,
    RowAccessor};
use parquet_ops;

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;


fn main() {
    let action = env::args().next().unwrap_or("UNKNOWN".to_owned());

    let path_1 = Path::new(paths::PATH_1);

    let timer = Instant::now();

    parquet_ops::show_parquet_metadata(&path_1);
        
    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);


    // restructure to check output file of merge (not created yet)
    let mut bytes = [0_u8; 10];
    if let Err(err) = fs::File::open(&path_1).unwrap().read(&mut bytes) {
        println!("Failed to open {path_1:?}. Obtained error: {err}");
    };
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    println!("First 10 bytes are: {:?}", std::str::from_utf8(&bytes[0..7]));
    }
