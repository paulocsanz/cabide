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

    // Edit function passed to filter to change select condition
    let results = cbd.remove_with(|student| student.data_inicio == "2018-06-16");

    for result in results.iter() {
        println!("Found DRE:{} NAME:{}", result.dre, result.nome);
    }

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
