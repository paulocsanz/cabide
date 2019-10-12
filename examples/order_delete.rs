use cabide::OrderCabide;
use serde::{Deserialize, Serialize};
use cabide::READ_BLOCKS_COUNT;
use std::sync::atomic::Ordering;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Data {
    dre: String,
    nome: String,
    data_inicio: String,
    data_fim: String,
    cr: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let extract_name = |a: &Data| a.nome.clone();
    let (buffer, ordered, temp) = (
        "alunos_head_buff.db",
        "alunos_head_ordered.db",
        "alunos_head_ordered.temp.db",
    );
    let mut cbd = OrderCabide::new(
        buffer,
        ordered,
        temp,
        Box::new(extract_name),
        Box::new(Ord::cmp),
    )?;


    let result = &cbd.remove(|entry| entry.dre == "143670124");

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));


    Ok(())
}
