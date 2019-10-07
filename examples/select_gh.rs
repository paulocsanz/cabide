use serde::{Serialize, Deserialize};
use csv::Reader;
use std::fs::File;
use cabide::Cabide;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    uhe: u64,
    cenario: u64,
    estagio: String,
    geracao: f64
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Truncates file to clear it for this run
    File::create("select.file")?; 

    let mut cbd: Cabide<Data> = Cabide::new("select.file", None)?;

    let mut csv = Reader::from_reader(File::open("examples/gh.csv")?);
    for data in csv.deserialize() {
        let data = data?;
        let block = cbd.write(&data)?;
        assert_eq!(cbd.read(block)?, data);
    }

    Ok(())
}
