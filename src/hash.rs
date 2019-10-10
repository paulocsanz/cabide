use crate::{Cabide, Error};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf};

pub struct HashCabide<T> {
    folder: PathBuf,
    cabides: HashMap<u8, Cabide<T>>,
    hash_function: Box<dyn Fn(&T) -> u8>,
}

impl<T> HashCabide<T> {
    pub fn new(folder: impl Into<PathBuf>, hash_function: Box<dyn Fn(&T) -> u8>) -> Self {
        Self {
            folder: folder.into(),
            cabides: HashMap::default(),
            hash_function,
        }
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
    pub fn write(&mut self, obj: &T) -> Result<u64, Error> {
        let hash = (self.hash_function)(obj);
        let mut written = 0;
        if let Some(cabide) = self.cabides.get_mut(&hash) {
            written += cabide.write(obj)?;
        } else {
            let mut cabide = Cabide::new(self.folder.join(hash.to_string()), None)?;
            written += cabide.write(obj)?;
            self.cabides.insert(hash, cabide);
        }
        Ok(written)
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
    pub fn remove(&mut self, (hash, block): (u8, u64)) -> Result<T, Error> {
        self.cabides
            .get_mut(&hash)
            .ok_or(Error::NotExistant)?
            .remove(block)
    }
}
