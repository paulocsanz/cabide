use crate::{Cabide, Error};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fs, path::Path, path::PathBuf};

static BUFFER_MAX_BLOCKS: u64 = 200;

pub struct OrderCabide<T, F, G, OrderField>
where
    F: Fn(&T) -> OrderField,
    G: Fn(&OrderField, &OrderField) -> Ordering,
{
    unordered_buffer: Cabide<T>,
    main: (Cabide<T>, PathBuf),
    sort_temp: (Cabide<T>, PathBuf),
    extract_order_field: F,
    order_function: G,
}

impl<T, F, G, OrderField> OrderCabide<T, F, G, OrderField>
where
    F: Fn(&T) -> OrderField,
    G: Fn(&OrderField, &OrderField) -> Ordering,
{
    pub fn new(
        buffer: impl AsRef<Path>,
        main: impl Into<PathBuf>,
        sort_temp: impl Into<PathBuf>,
        extract_order_field: F,
        order_function: G,
    ) -> Result<Self, Error> {
        let (main, sort_temp) = (main.into(), sort_temp.into());
        Ok(Self {
            unordered_buffer: Cabide::new(buffer, None)?,
            main: (Cabide::new(&main, None)?, main),
            sort_temp: (Cabide::new(&sort_temp, None)?, sort_temp),
            extract_order_field,
            order_function,
        })
    }

    #[inline]
    pub fn blocks(&self) -> Result<u64, Error> {
        Ok(self.unordered_buffer.blocks()? + self.main.0.blocks()?)
    }
}

impl<T, F, G, OrderField> OrderCabide<T, F, G, OrderField>
where
    for<'de> T: Serialize + Deserialize<'de>,
    F: Fn(&T) -> OrderField,
    G: Fn(&OrderField, &OrderField) -> Ordering,
{
    #[inline]
    pub fn write(&mut self, obj: &T) -> Result<(), Error> {
        self.unordered_buffer.write(obj)?;

        if self.unordered_buffer.blocks()? >= BUFFER_MAX_BLOCKS {
            let mut main = self.main.0.filter(|_| true);
            main.extend(self.unordered_buffer.filter(|_| true));
            main.sort_by(|t1, t2| {
                let f1 = (self.extract_order_field)(t1);
                let f2 = (self.extract_order_field)(t2);
                (self.order_function)(&f1, &f2)
            });

            self.sort_temp.0.truncate()?;
            for obj in main {
                self.sort_temp.0.write(&obj)?;
            }

            fs::copy(&self.sort_temp.1, &self.main.1)?;
            self.unordered_buffer.truncate()?;
            self.sort_temp.0.truncate()?;
        }
        Ok(())
    }
}

#[derive(PartialEq)]
enum Going {
    Left,
    Right,
}

impl<T, F, G, OrderField> OrderCabide<T, F, G, OrderField>
where
    for<'de> T: Deserialize<'de> + std::fmt::Debug,
    F: Fn(&T) -> OrderField,
    G: Fn(&OrderField, &OrderField) -> Ordering,
{
    pub fn first(&mut self, order_by: impl Fn(&OrderField) -> Ordering) -> Option<T> {
        let (unordered_buffer, extract_order_field) =
            (&mut self.unordered_buffer, &self.extract_order_field);
        unordered_buffer
            .first(|data| order_by(&(extract_order_field)(data)) == Ordering::Equal)
            .or_else(|| {
                let blocks = self.main.0.blocks().ok()?;
                let mut block = blocks / 2;
                let mut has_found_something = false;
                let mut going = Going::Right;
                loop {
                    if let Ok(data) = self.main.0.read(block) {
                        has_found_something = true;
                        match order_by(&(self.extract_order_field)(&data)) {
                            Ordering::Equal => return Some(data),
                            Ordering::Less => {
                                going = Going::Right;
                                if block == blocks {
                                    return None;
                                } else {
                                    let missing = blocks - block;
                                    block = block.saturating_add(missing / 2);
                                }
                            }
                            Ordering::Greater => {
                                going = Going::Left;
                                if block == 0 {
                                    return None;
                                } else {
                                    block = block.saturating_sub(block / 2);
                                }
                            }
                        }
                    } else if going == Going::Left {
                        if block == 0 {
                            return None;
                        } else {
                            block = block.saturating_sub(1);
                        }
                    } else {
                        if block == blocks {
                            if has_found_something {
                                return None;
                            } else {
                                going = Going::Left;
                                block = blocks / 2;
                            }
                        } else {
                            block = block.saturating_add(1);
                        }
                    }
                }
            })
    }

    pub fn filter(&mut self, order_by: impl Fn(&OrderField) -> Ordering) -> Vec<T> {
        let (unordered_buffer, extract_order_field) =
            (&mut self.unordered_buffer, &self.extract_order_field);
        let mut vec = unordered_buffer
            .filter(|data| order_by(&(extract_order_field)(data)) == Ordering::Equal);

        let blocks = self.main.0.blocks().unwrap_or(0);
        let mut block = blocks / 2;
        let mut has_found_something = false;
        let mut going = Going::Right;
        loop {
            if let Ok(data) = self.main.0.read(block) {
                has_found_something = true;
                match order_by(&(self.extract_order_field)(&data)) {
                    Ordering::Equal => vec.push(data),
                    Ordering::Less => {
                        going = Going::Right;
                        if block == blocks {
                            return vec;
                        } else {
                            let missing = blocks - block;
                            block = block.saturating_add(missing / 2);
                        }
                    }
                    Ordering::Greater => {
                        going = Going::Left;
                        if block == 0 {
                            return vec;
                        } else {
                            block = block.saturating_sub(block / 2);
                        }
                    }
                }
            } else if going == Going::Left {
                if block == 0 {
                    return vec;
                } else {
                    block = block.saturating_sub(1);
                }
            } else {
                if block == blocks {
                    if has_found_something {
                        return vec;
                    } else {
                        going = Going::Left;
                        block = blocks / 2;
                    }
                } else {
                    block = block.saturating_add(1);
                }
            }
        }
    }

    pub fn remove(&mut self, order_by: impl Fn(&OrderField) -> Ordering) -> Vec<T> {
        let (unordered_buffer, extract_order_field) =
            (&mut self.unordered_buffer, &self.extract_order_field);
        let mut vec = unordered_buffer
            .remove_with(|data| order_by(&(extract_order_field)(data)) == Ordering::Equal);

        let blocks = self.main.0.blocks().unwrap_or(0);
        let mut block = blocks / 2;
        let mut has_found_something = false;
        let mut going = Going::Right;
        loop {
            if let Ok(data) = self.main.0.remove(block) {
                has_found_something = true;
                match order_by(&(self.extract_order_field)(&data)) {
                    Ordering::Equal => vec.push(data),
                    Ordering::Less => {
                        going = Going::Right;
                        if block == blocks {
                            return vec;
                        } else {
                            let missing = blocks - block;
                            block = block.saturating_add(missing / 2);
                        }
                    }
                    Ordering::Greater => {
                        going = Going::Left;
                        if block == 0 {
                            return vec;
                        } else {
                            block = block.saturating_sub(block / 2);
                        }
                    }
                }
            } else if going == Going::Left {
                if block == 0 {
                    return vec;
                } else {
                    block = block.saturating_sub(1);
                }
            } else {
                if block == blocks {
                    if has_found_something {
                        return vec;
                    } else {
                        going = Going::Left;
                        block = blocks / 2;
                    }
                } else {
                    block = block.saturating_add(1);
                }
            }
        }
    }
}
