use cabide::OrderCabide;
use serde::{Deserialize, Serialize};
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;
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




    let mut _name: String = thread_rng().sample_iter(&Alphanumeric).take(thread_rng().gen_range(4, 12)).collect();
    let _surname: String = thread_rng().sample_iter(&Alphanumeric).take(thread_rng().gen_range(4, 12)).collect();

    let _fullname = format!("X{} {}", _name, _surname);

    let mut _data = Data {
        dre: format!("{}", thread_rng().gen_range(100_000_000, 999_999_999)),
        nome: _fullname,
        data_inicio: String::from("2015-08-01"),
        data_fim: String::from("2019-08-01"),
        cr: (rand::random::<f32>() * 100.).round() / 10.,
    };


    println!();
    println!("{:?}", _data);
    println!("used blocks pre insert: {}", cbd.blocks()?);

    cbd.write(&_data)?;

    println!();
    println!("Used blocks postinsert: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));


    Ok(())
}
