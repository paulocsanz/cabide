# Cabide

**Typed file based database library**

Specified type will be (de)serialized from/to the file

If the type changes to have different field order, field types or if more fields are added deserialization may be broken,
please keep the type unchanged or migrate the database first

Free blocks in the middle of the file will be cached and prefered, but no data is fragmented over them

## Dependencies

- Rust compiler (https://rustup.rs/)

## Docs

This is a library that provides access to Cabide and HashCabide, two file based databases each with its API documented with `cargo doc`

`cargo doc --open`

## Example

```rust
use serde::{Serialize, Deserialize};
use cabide::Cabide;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Data {
    name: String,
    maybe_number: Option<u8>,
}

// Opens file pre-filling it, creates it since it's first run
let mut cbd: Cabide<Data> = Cabide::new("test.file", Some(1000))?;
assert_eq!(cbd.blocks()?, 1000);

for _ in 0..100 {
    let data = random_data();
    let primary_key = cbd.write(&data)?;
    assert_eq!(cbd.read(primary_key)?, data);
}

cbd.remove(40)?;
cbd.remove(30)?;
cbd.remove(35)?;

// Since there are empty blocks in the middle of the file we re-use one of them
// (the last one to be available that fits the data)
assert_eq!(cbd.write(&random_data())?, 35);
```
