// types used for testing

pub const MESSAGE_TYPE: &str = "
message schema {
    REQUIRED INT64 id;
    REQUIRED BINARY account (UTF8);
    REQUIRED INT32 amount;
    REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
    REQUIRED BINARY extra_00 (UTF8);} 
";

// const NESTED_MESSAGE_TYPE: &str = "
// message schema {
//   REQUIRED BINARY account (UTF8);
//   REPEATED INT32 amount;
// }
// ";


macro_rules! MESSAGE_FORMAT {() => ("message schema {{
  REQUIRED INT64 id;
  REQUIRED BINARY account (UTF8);
  REQUIRED INT32 amount;
  REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
  {}
}} 
")}

macro_rules! NESTED_MESSAGE_FORMAT {() => ("message schema {{
  REQUIRED BINARY account (UTF8);
  REPEATED INT32 amount;
  {}
}}
")}

/// get a schema for test-data with 'num_extra_columns'  additional column named extra_XY.
pub fn get_schema_str(num_extra_columns: usize) -> String {
let columns = (0..num_extra_columns)
//    .iter()
  .map(|idx| format!("REQUIRED BINARY extra_{:02?} (UTF8);", idx))
  .collect::<Vec<String>>()
  .join("\n    ");

  format!(MESSAGE_FORMAT!(), columns)
}

/// get a nested schema for test-data with 'num_extra_columns'  additional column named extra_XY.
pub fn get_nested_schema_str(num_extra_columns: usize) -> String {
  let columns = (0..num_extra_columns)
  //    .iter()
    .map(|idx| format!("REPEATED BINARY extra_{:02?} (UTF8);", idx))
    .collect::<Vec<String>>()
    .join("\n    ");
  
    format!(NESTED_MESSAGE_FORMAT!(), columns)
  }
  



pub const NESTED: bool = false;


const NUM_TX_PER_ACCOUNT: u64 = 10;

// extract a label from a number
pub fn make_label(idx: u64) -> String {
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
pub fn find_amount(idx: u64) -> i32 {
    (idx % NUM_TX_PER_ACCOUNT) as i32 - (NUM_TX_PER_ACCOUNT as i32)/2 + 1
}

const BASE: u64 = 123456789;

pub fn find_text(col: i16, idx: u64) -> String {
    let mut seed = col as u64 * BASE + idx;
    let mut chars = Vec::new();
    for _i in 0..100 {
        let val =  (seed % 26) as u32 + ('a' as u32);
        chars.push(char::from_u32(val).unwrap());
        seed /= 2;
    } 
    chars.into_iter().collect()
}


const TIMEBASE: i64 = 1644537600;   // epoch-secs on 11-12-2022 
const TIME_MULTIPLIER: i64 = 10_000; // step-size is 10 milliseconds

// find a time starting on dec 11 2022 and increasing by 10 Milliseconds
pub fn find_time(idx: u64) -> i64 {
  ((idx as i64) + TIMEBASE) * TIME_MULTIPLIER
}