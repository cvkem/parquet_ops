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
use s3_file;


// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}


pub const PATH_1: &str = "./sample_even.parquet";
pub const PATH_2: &str = "./sample_odd.parquet";




fn smaller_test(row_1: &Row, row_2: &Row) -> bool {
    let k1 = row_1.get_long(0).unwrap();
    let k2 = row_2.get_long(0).unwrap();
    k1 <= k2
}

#[tokio::main]
async fn main() {
    let action = env::args().next().unwrap_or("UNKNOWN".to_owned());

    let path_1 = "s3:parquet-exp:sample_2MB.parquet";

    let timer = Instant::now();

    parquet_ops::read_parquet_rowiter(path_1, None, parquet_ops::MESSAGE_TYPE);
    
    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);
}
