// types used for testing

pub const MESSAGE_TYPE: &str = "
message schema {
    REQUIRED INT64 id;
    REQUIRED BINARY account (UTF8);
    REQUIRED INT32 amount;
    REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
    REQUIRED BINARY garbage_00 (UTF8);} 
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

// const NESTED_MESSAGE_TYPE: &str = "
// message schema {
//   REQUIRED BINARY account (UTF8);
//   REPEATED INT32 amount;
// }
// ";

pub fn get_schema_str(num_extra_columns: usize) -> String {
let columns = (0..num_extra_columns)
//    .iter()
  .map(|idx| format!("REQUIRED BINARY extra_{:02?} (UTF8);", idx))
  .collect::<Vec<String>>()
  .join("\n    ");

  format!(MESSAGE_FORMAT!(), columns)
}


pub const LONG_MESSAGE_TYPE: &str = "
message schema {
  REQUIRED INT64 id;
  REQUIRED BINARY account (UTF8);
  REQUIRED INT32 amount;
  REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
  REQUIRED BINARY garbage_00 (UTF8);
  REQUIRED BINARY garbage_01 (UTF8);
  REQUIRED BINARY garbage_02 (UTF8);
  REQUIRED BINARY garbage_03 (UTF8);
  REQUIRED BINARY garbage_04 (UTF8);
  REQUIRED BINARY garbage_05 (UTF8);
  REQUIRED BINARY garbage_06 (UTF8);
  REQUIRED BINARY garbage_07 (UTF8);
  REQUIRED BINARY garbage_08 (UTF8);
  REQUIRED BINARY garbage_09 (UTF8);
  REQUIRED BINARY garbage_10 (UTF8);
  REQUIRED BINARY garbage_11 (UTF8);
  REQUIRED BINARY garbage_12 (UTF8);
  REQUIRED BINARY garbage_13 (UTF8);
  REQUIRED BINARY garbage_14 (UTF8);
  REQUIRED BINARY garbage_15 (UTF8);
  REQUIRED BINARY garbage_16 (UTF8);
  REQUIRED BINARY garbage_17 (UTF8);
  REQUIRED BINARY garbage_18 (UTF8);
  REQUIRED BINARY garbage_19 (UTF8);
}
";

pub const LONG_NESTED_MESSAGE_TYPE: &str = "
message schema {
  REQUIRED BINARY account (UTF8);
  REPEATED INT32 amount;
  REPEATED BINARY garbage_00 (UTF8);
  REPEATED BINARY garbage_01 (UTF8);
  REPEATED BINARY garbage_02 (UTF8);
  REPEATED BINARY garbage_03 (UTF8);
  REPEATED BINARY garbage_04 (UTF8);
  REPEATED BINARY garbage_05 (UTF8);
  REPEATED BINARY garbage_06 (UTF8);
  REPEATED BINARY garbage_07 (UTF8);
  REPEATED BINARY garbage_08 (UTF8);
  REPEATED BINARY garbage_09 (UTF8);
  REPEATED BINARY garbage_10 (UTF8);
  REPEATED BINARY garbage_11 (UTF8);
  REPEATED BINARY garbage_12 (UTF8);
  REPEATED BINARY garbage_13 (UTF8);
  REPEATED BINARY garbage_14 (UTF8);
  REPEATED BINARY garbage_15 (UTF8);
  REPEATED BINARY garbage_16 (UTF8);
  REPEATED BINARY garbage_17 (UTF8);
  REPEATED BINARY garbage_18 (UTF8);
  REPEATED BINARY garbage_19 (UTF8);
}
";
