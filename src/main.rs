use std::{ 
    env, 
    fs, path::Path, 
    io::Read};
use std::time::Instant;
use std::any::type_name;

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}





mod rp_rowiter;
use crate::rp_rowiter::read_parquet_rowiter;

mod code;
use crate::code::{
    MESSAGE_TYPE,
    read_parquet_metadata,
    write_parquet};


fn get_u64_from_string(s: &str, err_msg: &str) -> Option<u64> {
    Some(s
        .replace("_", "")
        .parse::<u64>()
        .expect(err_msg))
}




fn main() {

    let args: Vec<String> = env::args().collect();

    let path1 = Path::new("./sample_even.parquet");
    let path2 = Path::new("./sample_odd.parquet");

    let action = if args.len() > 1 { args[1].to_owned() } else { "write".to_owned() };

    let timer = Instant::now();

    match &*action.to_lowercase() {
        "write" => {
            let num_recs = if args.len() > 2 { get_u64_from_string(&args[2], "first argument should be 'num_recs' (a positive integer).") } else { None };
            let group_size = if args.len() > 3 { get_u64_from_string(&args[3], "second argument should be 'group_size' (a positive integer).") } else { None };
        
            println!("Creating file in {:?}", &path1);        
            write_parquet(&path1, num_recs, group_size, Some(|i| i % 2 == 0)).unwrap();        
            println!("Creating file in {:?}", &path2);        
            write_parquet(&path2, num_recs, group_size, Some(|i| i % 2 != 0)).unwrap();        
        },
        "meta" => read_parquet_metadata(&path1),
        "read" => {
//            let acc_name = Some(if args.len() > 2 { args[2].to_owned() } else { "aafqlr".to_owned() }); // exists at end of file with 1_000_000 records.
//            block_on(read_parquet(&path, acc_name));
        read_parquet_rowiter(path1, None, MESSAGE_TYPE);
        }
        _ => panic!("Unknown action: expecting 'write', 'meta', or 'read' as first argument. Found action: '{action}'")
    }
    let elapsed = timer.elapsed();

    println!("Action '{}' with duration {:?}", &action, &elapsed);



    let mut bytes = [0_u8; 10];
    if let Err(err) = fs::File::open(&path1).unwrap().read(&mut bytes) {
        println!("Failed to open {path1:?}. Obtained error: {err}");
    };
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    println!("First 10 bytes are: {:?}", std::str::from_utf8(&bytes[0..7]));
    }
