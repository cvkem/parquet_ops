use std::{fs, path::Path, sync::Arc, io, cmp};
use std::time::Instant;
use parquet::{
    data_type::{Int32Type, ByteArrayType, ByteArray},
    file::{
        properties::WriterProperties,
        writer::SerializedFileWriter,
        reader::SerializedFileReader,
        reader::FileReader
    },
    schema::parser::parse_message_type,
};


use std::any::type_name;

// return the type of a ref as a static string
fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

const MESSAGE_TYPE: &str = "
message schema {
  REQUIRED BINARY account (UTF8);
  REQUIRED INT32 amount;
}
";

const NUM_TX_PER_ACCOUNT: u64 = 10;

// extract a label from a number
fn make_label(idx: u64) -> String {
    let mut idx = idx / NUM_TX_PER_ACCOUNT;  // first part is amount.
    let mut stack = Vec::new();
    for _i in 1..7 {
        let val =  (idx % 26) as u32 + ('a' as u32);
        stack.push(char::from_u32(val).unwrap());
        idx /= 26;
    }
    let s: String = stack.into_iter().rev().collect();
    s
}


// extract an amount from a number
fn find_amount(idx: u64) -> i32 {
    (idx % NUM_TX_PER_ACCOUNT) as i32 - (NUM_TX_PER_ACCOUNT as i32)/2 + 1
}


fn write_parquet_row_group(writer: &mut SerializedFileWriter<fs::File>, start: u64, end: u64) {
    let mut row_group_writer = writer.next_row_group().unwrap();
    let mut col_nr = 0;
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        match col_nr {
            0 => {
                let distinct: u64 = 26;
                let values: Vec<_> = (start..end)
                    .map(|i| {
//                        let bs = ;
                        ByteArray::from(make_label(i).as_bytes().to_vec())
                    } )
                    .collect();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch_with_statistics(&values, None, None, Some(&(values[0])), Some(&(values[values.len()-1])), None) //Some(distinct))
                    .expect("writing String column");
            },
            1 => {
                    let distinct = 1000 as u64;
                    let values: Vec<i32> =  (start..end)
                        .map(|i| find_amount(i))
                        .collect();
                col_writer
                        .typed::<Int32Type>()
                        .write_batch_with_statistics(&values, None, None, Some(&-499), Some(&500), Some(distinct))
                        .expect("writing i32 column");
                },
            _ => panic!("incorrect column number")
            }
            col_nr += 1;
            col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();

}

fn write_parquet(path: &Path) -> Result<(), io::Error> {
    let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let now = Instant::now();

    let mut start = 0;
    let end = 100;
    let group_size = 60;
    let mut ng = 1;
    while start < end {
        let group_end = cmp::min(start+group_size, end);

        write_parquet_row_group(&mut writer, start, group_end);

        let last = group_end -1;
        println!("End of group {} in {:?} has value {} is account {} with amount {}", ng, now.elapsed(), last, make_label(last), find_amount(last));
        start += group_size;
        ng += 1;
    }

    writer.close().unwrap();
    Ok(())
}


const SHOW_FIRST_GROUP_ONLY: bool = true;

fn read_parquet_metadata(path: &Path) {
    if let Ok(file) = fs::File::open(path) {

        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let file_metadata = parquet_metadata.file_metadata();

        println!("For path={:?} found file-metadata:\n {:?}", &path, &file_metadata);

        let rows = file_metadata.num_rows();
        
        for (idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
            println!("  rowgroup: {} has meta {:?}", idx, rg);
            if SHOW_FIRST_GROUP_ONLY {
                println!(" {} groups in total, but only first shown.", parquet_metadata.row_groups().len());
                break;
            }
        }

        let fields = parquet_metadata.file_metadata().schema().get_fields();

        println!("And fields:\n{:?}", &fields)
    }
}

fn main() {

    // for i in 0..100 {
    //     println!("{}  ->  {}", i, make_label(i));
    // }
    // return;
    let path = Path::new("./sample.parquet");

    println!("Creating file in {:?}", &path);

    write_parquet(&path).unwrap();

    read_parquet_metadata(&path);

    let bytes = fs::read(&path).unwrap();
    assert_eq!(&bytes[0..4], &[b'P', b'A', b'R', b'1']);
    }
