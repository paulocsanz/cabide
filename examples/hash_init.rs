use cabide::HashCabide;
use csv::Reader;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;

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

    let _ = fs::create_dir("alunos_head.db");
    let mut cbd: HashCabide<Data> = HashCabide::new("alunos_head.db", Box::new(hash_fn))?;

    let mut csv = Reader::from_reader(File::open("data/alunos_head.csv")?);
    for data in csv.deserialize() {
        let data = data?;
        let block = cbd.write(&data)?;
        assert_eq!(cbd.read(block)?, data);
    }

    Ok(())
}
