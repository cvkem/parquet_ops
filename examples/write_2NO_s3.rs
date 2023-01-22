use std::{
    env,
    fs,  
    io::Read,
    path::Path,
    thread
};
use std::time::Instant;
use parquet_ops;


// only needed for Rust 2015
//extern crate parquet_ops;

mod paths;



fn get_u64_from_string(s: &str, err_msg: &str) -> Option<u64> {
    Some(s
        .replace("_", "")
        .parse::<u64>()
        .expect(err_msg))
}

const NUM_EXTRA_COLUMNS: usize = 135;

const PARALLEL: bool = false;

#[tokio::main]
async fn main() {
    let action = env::args().next().unwrap_or("UNKNOWN".to_owned());

    let args: Vec<String> = env::args().collect();

//    let action = "writ";
    let mut path_1 = "s3:parquet-exp:".to_owned();
    path_1.push_str(&paths::PATH_1[2..]); // ignore the initial './'
    let mut path_2 = "s3:parquet-exp:".to_owned();
    path_2.push_str(&paths::PATH_2[2..]);

    let timer = Instant::now();

    let num_recs = if args.len() > 1 { get_u64_from_string(&args[1], "first argument should be 'num_recs' (a positive integer).") } else { None };
    let group_size = if args.len() > 2 { get_u64_from_string(&args[2], "second argument should be 'group_size' (a positive integer).") } else { None };

    let num_recs_cpy = num_recs.clone();
    let group_size_cpy = group_size.clone();

    let mut even_handle: Option<thread::JoinHandle<()>> = None;
    println!("Creating file with even-rows in {:?}", &path_1);
    if PARALLEL {
        even_handle = Some(thread::spawn(move || {
            // a runtime is needed for this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                parquet_ops::write_parquet(&path_1, NUM_EXTRA_COLUMNS, num_recs, group_size, Some(|i| i % 2 == 0)).unwrap()
            });
        }));    
    } else {
        parquet_ops::write_parquet(&path_1, NUM_EXTRA_COLUMNS, num_recs, group_size, Some(|i| i % 2 == 0)).unwrap()
    }
 
    println!("Creating file with odd-rows in {:?} on the main thread", &path_2);        
// //    let odd_handle = thread::spawn(move || parquet_ops::write_parquet_s3(&path_2, NUM_EXTRA_COLUMNS, num_recs_cpy, group_size_cpy, Some(|i| i % 2 != 0)).unwrap());        
    parquet_ops::write_parquet(&path_2, NUM_EXTRA_COLUMNS, num_recs_cpy, group_size_cpy, Some(|i| i % 2 != 0)).expect("write odd vlaue on main failed");        

    if PARALLEL {
        if let Some(handle) = even_handle {
            handle.join().expect("Failed to join even-handle");
        }
    }
//    odd_handle.join().expect("Failed to join odd-handle");

    let elapsed = timer.elapsed();


    println!("Action '{}' with duration {:?}", &action, &elapsed);
}
