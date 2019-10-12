use cabide::HashCabide;
use cabide::READ_BLOCKS_COUNT;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use rand::Rng;
use rand::thread_rng;
use rand::distributions::Alphanumeric;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Data {
    dre: String,
    nome: String,
    data_inicio: String,
    data_fim: String,
    cr: f32,
}

const DATA_COUNT: usize = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hash_fn = |a: &Data| -> u8 { a.nome.chars().next().unwrap_or('\0') as u8 };

    let mut cbd: HashCabide<Data> = HashCabide::new("alunos_head.db", Box::new(hash_fn))?;

    println!();
    println!("Inserting {} entries", DATA_COUNT);
    println!("used blocks pre insert: {}", cbd.blocks()?);

    for _i in 0..DATA_COUNT {
    
        let mut _name: String = thread_rng().sample_iter(&Alphanumeric).take(thread_rng().gen_range(4, 12)).collect();
        let _surname: String = thread_rng().sample_iter(&Alphanumeric).take(thread_rng().gen_range(4, 12)).collect();

        let _fullname = format!("{} {}", _name, _surname);
        
        let mut _data = Data {
            dre: format!("{}", thread_rng().gen_range(100_000_000, 999_999_999)),
            nome: _fullname,
            data_inicio: String::from("2017-08-01"),
            data_fim: String::from("2017-08-22"),
            cr:  (rand::random::<f32>() * 100.).round() / 10.,
        };
        cbd.write(&_data)?;
    }


    // Change read() arg to desired id

    println!("Wrote!");

    println!();
    println!("Used blocks: {}", cbd.blocks()?);
    println!("Read blocks: {}", READ_BLOCKS_COUNT.load(Ordering::Relaxed));

    Ok(())
}
