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

    let result = &cbd.read(0);

    match  result {
        Ok(_v) => println!("Found {} from {}", _v.uhe, _v.estagio),
        Err(_e) => println!("Found nothing"),
    }

    println!();
    println!("used blocks: {}", 1);
    println!("read blocks: {}", 1);

    Ok(())
}
