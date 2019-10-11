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

const DATA_COUNT: usize = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cbd: Cabide<Data> = Cabide::new("gh_head.db", None)?;
    println!();
    println!("Inserting {} entries", DATA_COUNT);
    println!("used blocks pre insert: {}", cbd.blocks()?);

    for _i in 0..DATA_COUNT {
        let mut _entry = Data {
            uhe: rand::random::<u64>(),
            cenario: rand::random::<u64>(),
            estagio: String::from("2017-08-01"),
            geracao: rand::random::<f64>(),
        };
        &cbd.write(&_entry);
    }

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
