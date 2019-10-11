use cabide::Cabide;
use cabide::READ_BLOCKS_COUNT;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    uhe: u64,
    cenario: u64,
    estagio: String,
    geracao: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cbd: Cabide<Data> = Cabide::new("gh_head.db", None)?;

    let mut _data = Data {
        uhe: rand::random::<u64>(),
        cenario: rand::random::<u64>(),
        estagio: String::from("2017-08-01"),
        geracao: rand::random::<f64>(),
    };
    println!();
    println!("used blocks pre insert: {}", cbd.blocks()?);

    let _result = &cbd.write(&_data);

    // TODO op reporting
    println!();
    println!("Used blocks postinsert: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
