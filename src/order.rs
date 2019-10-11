use crate::{Cabide, Error};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, path::Path};

static BUFFER_MAX_BLOCKS: u64 = 200;

pub struct OrderCabide<T> {
    unordered_buffer: Cabide<T>,
    main: Cabide<T>,
    order_function: Box<dyn Fn(&T, &T) -> Ordering>,
}

impl<T> OrderCabide<T> {
    pub fn new<P1, P2>(
        buffer: P1,
        main: P2,
        order_function: Box<dyn Fn(&T, &T) -> Ordering>,
    ) -> Result<Self, Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        Ok(Self {
            unordered_buffer: Cabide::new(buffer, None)?,
            main: Cabide::new(main, None)?,
            order_function,
        })
    }

    #[inline]
    pub fn blocks(&self) -> Result<u64, Error> {
        Ok(self.unordered_buffer.blocks()? + self.main.blocks()?)
    }
}

impl<T> OrderCabide<T>
where
    for<'de> T: Serialize + Deserialize<'de> + Clone,
{
    #[inline]
    pub fn write(&mut self, obj: &T) -> Result<(), Error> {
        self.unordered_buffer.write(obj)?;
        if self.unordered_buffer.blocks()? >= BUFFER_MAX_BLOCKS {
            let buffer = self.unordered_buffer.remove_with(|_| true);
            let mut main = self.main.remove_with(|_| true);
            main.extend_from_slice(&buffer);
            main.sort_by(&self.order_function);
            for obj in main {
                // If this errors data will go cray cray, things will get corrupted
                // lets ignore the error so most data can be recovered
                let _ = self.main.write(&obj);
            }
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
            .or_else(|| self.main.first(filter))
    }

    #[inline]
    pub fn filter(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = self.unordered_buffer.filter(&filter);
        vec.extend(self.main.filter(filter));
        vec
    }

    #[inline]
    pub fn remove_with(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = self.unordered_buffer.remove_with(&filter);
        vec.extend(self.main.remove_with(filter));
        vec
    }
}
