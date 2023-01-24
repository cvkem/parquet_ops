use std::{
    env,
    fs, 
    io::Read,
    sync::atomic::{
        AtomicUsize,
        Ordering},
    thread,
    time::Instant};
use std::any::type_name;
use parquet::record::{Row,
    RowAccessor};
use parquet_ops;
use tokio;

#[allow(dead_code)]
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


fn the_merge(path_1: &str, path_2: &str, merged_path: &str) {

    println!("merged '{}' and '{}' into '{}'.", &path_1, &path_2, merged_path);
    let timer = Instant::now();

    parquet_ops::merge_parquet(vec![path_1, path_2], merged_path, smaller_test);

    let elapsed = timer.elapsed();


    println!("merged '{}' and '{}' into '{}' with duration {:?}.", &path_1, &path_2, merged_path, &elapsed);

}


#[tokio::main]
async fn main() {
    let mut args = env::args();
    println!("Program name = {}", args.next().unwrap());
    println!("Works on local files, unless you provide argument 's3' as first argument, as it operates on bucket 'parquet-exp' in s3.");
    let action = args.next().unwrap_or("UNKNOWN".to_owned());

    if action == "s3" {

//         let rt = tokio::runtime::Builder::new_multi_thread()
//             .enable_all()
// //            .thread_name("CvK-tokio-merge")
//             .thread_name_fn(|| {
//                 static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
//                 let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
//                 format!("CvK-merge-pool-{}", id)
//              })
//             .on_thread_start(|| {
//                 println!("thread started with name '{:?}'", thread::current().name());
//             })
//              .build()
//             .unwrap();
//         rt.block_on(async {
//             the_merge(paths::PATH_1_S3, paths::PATH_2_S3, paths::MERGED_S3)
//         });
        the_merge(paths::PATH_1_S3, paths::PATH_2_S3, paths::MERGED_S3)

    } else { // operate on local file-system

        let (path_1, path_2, merged_path) = (paths::PATH_1, paths::PATH_2, paths::MERGED);

        the_merge(path_1, path_2, merged_path);

        // restructure to check output file of merge (not created yet)
        let mut bytes = [0_u8; 10];
        if let Err(err) = fs::File::open(&path_1).unwrap().read(&mut bytes) {
            println!("Failed to open {path_1:?}. Obtained error: {err}");
        };
        assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
        println!("First 10 bytes are: {:?}", std::str::from_utf8(&bytes[0..7]));
    }

}
