use cabide::{HashCabide};
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

    // Change read() arg to desired id
    let result = &cbd.read(('A' as u8, 0));

    match result {
        Ok(_v) => println!("Found {} from {}", _v.nome, _v.data_inicio),
        Err(_e) => println!("Found nothing"),
    }

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
