use crate::{Cabide, Error};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fs, path::Path, path::PathBuf};

static BUFFER_MAX_BLOCKS: u64 = 200;

pub struct OrderCabide<T> {
    unordered_buffer: Cabide<T>,
    main: (Cabide<T>, PathBuf),
    sort_temp: (Cabide<T>, PathBuf),
    order_function: Box<dyn Fn(&T, &T) -> Ordering>,
}

impl<T> OrderCabide<T> {
    pub fn new(
        buffer: impl AsRef<Path>,
        main: impl Into<PathBuf>,
        sort_temp: impl Into<PathBuf>,
        order_function: Box<dyn Fn(&T, &T) -> Ordering>,
    ) -> Result<Self, Error> {
        let (main, sort_temp) = (main.into(), sort_temp.into());
        Ok(Self {
            unordered_buffer: Cabide::new(buffer, None)?,
            main: (Cabide::new(&main, None)?, main),
            sort_temp: (Cabide::new(&sort_temp, None)?, sort_temp),
            order_function,
        })
    }

    #[inline]
    pub fn blocks(&self) -> Result<u64, Error> {
        Ok(self.unordered_buffer.blocks()? + self.main.0.blocks()?)
    }
}

impl<T> OrderCabide<T>
where
    for<'de> T: Serialize + Deserialize<'de>,
{
    #[inline]
    pub fn write(&mut self, obj: &T) -> Result<(), Error> {
        self.unordered_buffer.write(obj)?;

        if self.unordered_buffer.blocks()? >= BUFFER_MAX_BLOCKS {
            let mut main = self.main.0.filter(|_| true);
            main.extend(self.unordered_buffer.filter(|_| true));
            main.sort_by(&self.order_function);

            self.sort_temp.0.truncate()?;
            for obj in main {
                self.sort_temp.0.write(&obj)?;
            }

            fs::copy(&self.sort_temp.1, &self.main.1)?;
            self.unordered_buffer.truncate()?;
        }
        Ok(())
    }
}

impl<T> OrderCabide<T>
where
    for<'de> T: Deserialize<'de>,
{
    // Binary search is a must tho, this is currently O(N), which misses the point of the order

    #[inline]
    pub fn first(&mut self, filter: impl Fn(&T) -> bool) -> Option<T> {
        self.unordered_buffer
            .first(&filter)
            .or_else(|| self.main.0.first(filter))
    }

    #[inline]
    pub fn filter(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = self.unordered_buffer.filter(&filter);
        vec.extend(self.main.0.filter(filter));
        vec
    }

    #[inline]
    pub fn remove_with(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = self.unordered_buffer.remove_with(&filter);
        vec.extend(self.main.0.remove_with(filter));
        vec
    }
}
