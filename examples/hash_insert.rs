use cabide::HashCabide;
use cabide::READ_BLOCKS_COUNT;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    dre: String,
    nome: String,
    data_inicio: String,
    data_fim: String,
    cr: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hash_fn = |a: &Data| -> u8 { a.nome.chars().next().unwrap_or('\0') as u8 };

    let mut cbd: HashCabide<Data> = HashCabide::new("alunos_head.db", Box::new(hash_fn))?;

    let mut _data = Data {
        dre: String::from("118000000"),
        nome: String::from("yeahboi"),
        data_inicio: String::from("2017-08-01"),
        data_fim: String::from("2017-08-22"),
        cr: rand::random::<f32>(),
    };

    println!();
    println!("Used blocks pre insert: {}", cbd.blocks()?);

    // Change read() arg to desired id
    let _result = &cbd.write(&_data)?;

    println!("Wrote!");

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
