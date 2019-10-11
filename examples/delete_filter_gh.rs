use cabide::Cabide;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    uhe: u64,
    cenario: u64,
    estagio: String,
    geracao: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cbd: Cabide<Data> = Cabide::new("select.file", None)?;

    // Edit function passed to filter to change delete condition
    let results = cbd.remove_with(|student| student.estagio == "2017-01-01");

    for result in results.iter() {
        println!("Found {} from {}", result.uhe, result.estagio);
    }

    // TODO op reporting
    println!();
    println!("used blocks: {}", cbd.blocks()?);
    println!("read blocks: {}", 1);

    Ok(())
}
