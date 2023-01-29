use std::{ 
    cmp,
    io,
    time::Instant};

use super::ttypes;

use super::parquetwriter::{self, ParquetWriter};





pub fn write_parquet(path: &str, extra_columns: usize, num_recs: Option<u64>, group_size: Option<u64>,
    selection: Option<fn(&u64) -> bool>) -> Result<(), io::Error> {

    let now = Instant::now();
    let mut pw = parquetwriter::get_parquet_writer(path, extra_columns);

    let mut row_writer = RowWriteBuffer::new(merged_path, schema, 10000).unwrap();

    let mut row_processor = |row: Row| {
        if row.get_long(0).unwrap() % REPORT_APPEND_STEP == 0 {
            println!("Row with id={}, acc={} and amount={}.", row.get_long(0).unwrap(), row.get_string(1).unwrap(), row.get_int(2).unwrap());
        }
        row_writer.append_row(row);
    };
    
    let mut start = 0;
    let end = num_recs.unwrap_or(1000);
    let group_size = group_size.unwrap_or(60);
    let mut ng = 1;
    while start < end {
        let group_end = cmp::min(start+group_size, end);

        if ttypes::NESTED {
            // match type to call the right type of writer
            match &mut pw {
                ParquetWriter::FileWriter(ref mut writer) => write_parquet_row_group_nested(writer, start, group_end),
                ParquetWriter::S3Writer(ref mut writer) => write_parquet_row_group_nested(writer, start, group_end)
            }
        } else {
            match &mut pw {
                ParquetWriter::FileWriter(ref mut writer) => write_parquet_row_group(writer, start, group_end, selection),
                ParquetWriter::S3Writer(ref mut writer) => write_parquet_row_group(writer, start, group_end, selection)
            }
        }

        let last = group_end -1;
        println!("End of group {} in {:?} has value {} is account {} with amount {}", ng, now.elapsed(), last, ttypes::make_label(last), ttypes::find_amount(last));
        start += group_size;
        ng += 1;
    }

    match pw {
        ParquetWriter::FileWriter(writer) => writer.close().unwrap(),
        ParquetWriter::S3Writer(writer) => writer.close().unwrap()
    };

    Ok(())
}
