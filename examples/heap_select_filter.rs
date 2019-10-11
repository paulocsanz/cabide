use cabide::Cabide;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{Ordering};
use cabide::READ_BLOCKS_COUNT;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    uhe: u64,
    cenario: u64,
    estagio: String,
    geracao: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cbd: Cabide<Data> = Cabide::new("gh_head.db", None)?;

    // Edit function passed to filter to change select condition
    let results = cbd.filter(|student| student.estagio == "2017-01-01");

    for result in results.iter() {
        println!("Found {} from {}", result.uhe, result.estagio);
    }

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
