use serde::{Serialize, Deserialize};
use cabide::Cabide;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    name: String,
    age: u8,
}

fn main() -> Result<(), cabide::Error> {
    let mut cbd: Cabide<Data> = Cabide::new("select.file", None)?;
    
    // Add things from csv
    for _ in 0..100 {
        cbd.write(&Data { name: "iuro".to_owned(), age: 4 })?;
    }

    assert_eq!(cbd.read(10)?, Data { name: "iuro".to_owned(), age: 4 });

    Ok(())
}
