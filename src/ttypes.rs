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

#[allow(dead_code)]
pub const LONG_MESSAGE_TYPE: &str = "
message schema {
  REQUIRED INT64 id;
  REQUIRED BINARY account (UTF8);
  REQUIRED INT32 amount;
  REQUIRED INT64 datetime (TIMESTAMP(MILLIS,true));
  REQUIRED BINARY extra_00 (UTF8);
  REQUIRED BINARY extra_01 (UTF8);
  REQUIRED BINARY extra_02 (UTF8);
  REQUIRED BINARY extra_03 (UTF8);
  REQUIRED BINARY extra_04 (UTF8);
  REQUIRED BINARY extra_05 (UTF8);
  REQUIRED BINARY extra_06 (UTF8);
  REQUIRED BINARY extra_07 (UTF8);
  REQUIRED BINARY extra_08 (UTF8);
  REQUIRED BINARY extra_09 (UTF8);
  REQUIRED BINARY extra_10 (UTF8);
  REQUIRED BINARY extra_11 (UTF8);
  REQUIRED BINARY extra_12 (UTF8);
  REQUIRED BINARY extra_13 (UTF8);
  REQUIRED BINARY extra_14 (UTF8);
  REQUIRED BINARY extra_15 (UTF8);
  REQUIRED BINARY extra_16 (UTF8);
  REQUIRED BINARY extra_17 (UTF8);
  REQUIRED BINARY extra_18 (UTF8);
  REQUIRED BINARY extra_19 (UTF8);
}
";

pub const LONG_NESTED_MESSAGE_TYPE: &str = "
message schema {
  REQUIRED BINARY account (UTF8);
  REPEATED INT32 amount;
  REPEATED BINARY extra_00 (UTF8);
  REPEATED BINARY extra_01 (UTF8);
  REPEATED BINARY extra_02 (UTF8);
  REPEATED BINARY extra_03 (UTF8);
  REPEATED BINARY extra_04 (UTF8);
  REPEATED BINARY extra_05 (UTF8);
  REPEATED BINARY extra_06 (UTF8);
  REPEATED BINARY extra_07 (UTF8);
  REPEATED BINARY extra_08 (UTF8);
  REPEATED BINARY extra_09 (UTF8);
  REPEATED BINARY extra_10 (UTF8);
  REPEATED BINARY extra_11 (UTF8);
  REPEATED BINARY extra_12 (UTF8);
  REPEATED BINARY extra_13 (UTF8);
  REPEATED BINARY extra_14 (UTF8);
  REPEATED BINARY extra_15 (UTF8);
  REPEATED BINARY extra_16 (UTF8);
  REPEATED BINARY extra_17 (UTF8);
  REPEATED BINARY extra_18 (UTF8);
  REPEATED BINARY extra_19 (UTF8);
}
";
