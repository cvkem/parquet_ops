use std::path::Path;
use parquet::{
        file::reader::{SerializedFileReader, FileReader},
        schema::parser::parse_message_type,
        record::reader::RowIter,
        record::RowAccessor
};


fn get_parquet_iter<'a>(path: &'a Path, message_type: &'a str) -> Option<RowIter<'a>> {
    let proj = parse_message_type(message_type).ok();
    println!(" The type = {:?}", proj);
//    let path = get_test_path("nested_maps.snappy.parquet");
//    let reader = SerializedFileReader::try_from(path.as_path()).unwrap();
//    let path = "./sample.parquet".to_owned();
//    let path = "/tmp/data/sample_1.parquet".to_owned();
    let reader = SerializedFileReader::try_from(path.to_string_lossy().into_owned()).unwrap();

    println!("Opened file with metadata {:?}", reader.metadata());
    let res = RowIter::from_file_into(Box::new(reader))
            .project(proj);
    if res.is_err() {
        println!(" failed with error: {:?}", res.err());
        return None;
    } 

    let row_iter = res.unwrap();
    Some(row_iter)
}


pub fn read_parquet_rowiter(path: &Path, max_rows: Option<usize>, message_type: &str) {
    let max_rows = max_rows.or(Some(1000000000)).unwrap();

    let res = get_parquet_iter(path, message_type).unwrap();

    let mut sum = 0;
    let mut last_idx = 0;
    for (i, row) in res.enumerate() {
//        println!("result {i}:  {row:?}");
        if let Ok(amount) = row.get_int(1) {
            println!("{i} has amount={amount}");
            sum += amount;
        }

        if i > max_rows { break; }
        last_idx = i;
    }

    println!("iterated over {last_idx}  fields with total amount = {sum}");
}


pub fn merge_parquet(p1: &Path, p2: &Path) {

}

