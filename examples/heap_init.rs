use cabide::Cabide;
use csv::Reader;
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    uhe: u64,
    cenario: u64,
    estagio: String,
    geracao: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Truncates file to clear it for this run
    File::create("gh_head.db")?;

    let mut cbd: Cabide<Data> = Cabide::new("gh_head.db", None)?;

    let mut csv = Reader::from_reader(File::open("data/gh_head.csv")?);
    for data in csv.deserialize() {
        let data = data?;
        let block = cbd.write(&data)?;
        assert_eq!(cbd.read(block)?, data);
    }

    Ok(())
}
