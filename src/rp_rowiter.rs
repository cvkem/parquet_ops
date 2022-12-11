use parquet::{
        file::reader::SerializedFileReader,
        schema::parser::parse_message_type,
        record::reader::RowIter,
        record::RowAccessor
};



pub fn read_parquet_rowiter(max_rows: Option<usize>, message_type: &str) {
    let max_rows = max_rows.or(Some(1000000000)).unwrap();

    let proj = parse_message_type(message_type).ok();
//    let path = get_test_path("nested_maps.snappy.parquet");
//    let reader = SerializedFileReader::try_from(path.as_path()).unwrap();
    let path = "./sample.parquet".to_owned();
//    let path = "/tmp/data/sample_1.parquet".to_owned();
    let reader = SerializedFileReader::try_from(path).unwrap();
    let res = RowIter::from_file_into(Box::new(reader)).project(proj);
    if res.is_err() {
        println!(" failed with error: {:?}", res.err());
        return;
    } 

    let res= res.unwrap();

    let mut sum = 0;
    let mut last_idx = 0;
    for (i, row) in res.enumerate() {
//        println!("result {i}:  {row:?}");
        if let Ok(amount) = row.get_int(1) {
            sum += amount;
        }

        if i > max_rows { break; }
        last_idx = i;
    }

    println!("iterated over {last_idx}  fields with total amount = {sum}");
}

