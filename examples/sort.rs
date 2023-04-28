use std::cmp::Ordering;
use std::{
    fs, 
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


mod paths;


fn comparator(row_1: &Row, row_2: &Row) -> Ordering {
    let k1 = row_1.get_long(0).unwrap();
    let k2 = row_2.get_long(0).unwrap();

    // reverse ordering
//    k2.cmp(&k1)
    k1.cmp(&k2)
}

fn main() {
    let action = "sorting";
    let path_1 = paths::PATH_1;
    let sorted_path = "sorted.parquet";

    let timer = Instant::now();

    parquet_ops::sort_multistage(path_1, sorted_path, |r1, r2| comparator(r1, r2));
    
    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", action, &elapsed);


    // restructure to check output file of merge (not created yet)
    let mut bytes = [0_u8; 10];
    if let Err(err) = fs::File::open(sorted_path).unwrap().read(&mut bytes) {
        println!("Failed to open {sorted_path:?}. Obtained error: {err}");
    };
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    println!("First 10 bytes are: {:?}", std::str::from_utf8(&bytes[0..7]));
}
