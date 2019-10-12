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

    // Change block id and initial letter
    let ids: [(u8, u64); 2] = [('A' as u8, 0), ('B' as u8, 0)];

    for id in ids.iter() {
        let result = &cbd.read(*id);
        match result {
            Ok(_v) => println!("Found DRE: {} Name: {}", _v.dre, _v.nome),
            Err(_e) => println!("Found nothing"),
        }
    }

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
