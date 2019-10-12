use crate::{Cabide, Error};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

pub struct HashCabide<T> {
    folder: PathBuf,
    cabides: HashMap<u8, Cabide<T>>,
    hash_function: Box<dyn Fn(&T) -> u8>,
}

impl<T> HashCabide<T> {
    pub fn new<P>(folder: P, hash_function: Box<dyn Fn(&T) -> u8>) -> Result<Self, Error>
    where
        P: Into<PathBuf>,
    {
        let (folder, mut cabides) = (folder.into(), HashMap::default());
        for value in 0..255 {
            let path = folder.join(value.to_string());
            if path.is_file() {
                cabides.insert(value, Cabide::new(path, None)?);
            }
        }

        Ok(Self {
            folder,
            cabides,
            hash_function,
        })
    }

    #[inline]
    pub fn blocks(&self) -> Result<u64, Error> {
        let mut blocks = 0;
        for cabide in self.cabides.values() {
            blocks += cabide.blocks()?;
        }
        Ok(blocks)
    }
}

impl<T: Serialize> HashCabide<T> {
    #[inline]
    pub fn write(&mut self, obj: &T) -> Result<(u8, u64), Error> {
        let hash = (self.hash_function)(obj);
        let block = if let Some(cabide) = self.cabides.get_mut(&hash) {
            cabide.write(obj)?
        } else {
            let mut cabide = Cabide::new(self.folder.join(hash.to_string()), None)?;
            let block = cabide.write(obj)?;
            self.cabides.insert(hash, cabide);
            block
        };
        Ok((hash, block))
    }
}

impl<T> HashCabide<T>
where
    for<'de> T: Deserialize<'de>,
{
    #[inline]
    pub fn read(&mut self, (hash, block): (u8, u64)) -> Result<T, Error> {
        self.cabides
            .get_mut(&hash)
            .ok_or(Error::NotExistant)?
            .read(block)
    }

    #[inline]
    pub fn filter(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = vec![];
        for cabide in self.cabides.values_mut() {
            vec.extend(cabide.filter(&filter));
        }
        vec
    }

    #[inline]
    pub fn remove(&mut self, (hash, block): (u8, u64)) -> Result<T, Error> {
        self.cabides
            .get_mut(&hash)
            .ok_or(Error::NotExistant)?
            .remove(block)
    }
}
