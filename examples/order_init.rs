use cabide::OrderCabide;
use csv::Reader;
use serde::{Deserialize, Serialize};
use std::fs::File;

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

    let mut csv = Reader::from_reader(File::open("data/alunos_head.csv")?);
    for data in csv.deserialize() {
        cbd.write(&data?)?;
    }

    Ok(())
}
