//! Typed file based database
//!
//! ```rust
//! use serde::{Serialize, Deserialize};
//! use cabide::Cabide;
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq)]
//! struct Data {
//!     name: String,
//!     maybe_number: Option<u8>,
//! }
//!
//! # use rand::{distributions::*, random, thread_rng};
//! # fn random_data() -> Data {
//! #     // this is slow af since it doesn't cache thread_rng
//! #     Data {
//! #         name: (0..5)
//! #             .map(|_| Alphanumeric.sample(&mut thread_rng()))
//! #             .collect(),
//! #         maybe_number: Some(random()).filter(|_| random()),
//! #     }
//! # }
//! # fn main() -> Result<(), cabide::Error> {
//! // Opens file pre-filling it, creates it since it's first run
//! # std::fs::File::create("test.file")?;
//! let mut cbd: Cabide<Data> = Cabide::new("test.file", Some(1000))?;
//! assert_eq!(cbd.blocks()?, 1000);
//!
//! // Since random_data only returns a data that fits in one block it writes continuously from last block
//! for i in 0..100 {
//!     // In this case each object only uses one block
//!     let data = random_data();
//!     assert_eq!(cbd.write(&data)?, i);
//!     assert_eq!(cbd.read(i)?, data);
//! }
//!
//! cbd.remove(40)?;
//! cbd.remove(30)?;
//! cbd.remove(35)?;
//!
//! // Since there are empty blocks in the middle of the file we re-use one of them
//! // (the last one to be available that fits the data)
//! assert_eq!(cbd.write(&random_data())?, 35);
//! # std::fs::remove_file("test.file")?;
//! # Ok(())
//! # }
//! ```

mod error;
mod protocol;

pub use crate::error::Error;
use crate::protocol::{END_BYTE, BLOCK_SIZE, CONTENT_SIZE, Metadata};

use bincode::{serialize, deserialize_from};
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::{collections::BTreeMap, fs::File, fs::OpenOptions, marker::PhantomData, path::Path};

/// Abstracts typed database binded to a specific file
///
/// Specified type will be (de)serialized from/to the file
///
/// If the type changes to have different field order, field types or if more fields are added deserialization may be broken, please keep the type unchanged or migrate the database first
///
/// Free blocks in the middle of the file will be cached and prefered, but no data is fragmented over them
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use cabide::Cabide;
///
/// #[derive(Debug, Serialize, Deserialize, PartialEq)]
/// struct Data {
///     name: String,
///     maybe_number: Option<u8>,
/// }
///
/// # use rand::{distributions::*, random, thread_rng};
/// # fn random_data() -> Data {
/// #     // this is slow af since it doesn't cache thread_rng
/// #     Data {
/// #         name: (0..5)
/// #             .map(|_| Alphanumeric.sample(&mut thread_rng()))
/// #             .collect(),
/// #         maybe_number: Some(random()).filter(|_| random()),
/// #     }
/// # }
/// # fn main() -> Result<(), cabide::Error> {
/// // Opens file pre-filling it, creates it since it's first run
/// # std::fs::File::create("test.file")?;
/// let mut cbd: Cabide<Data> = Cabide::new("test.file", Some(1000))?;
/// assert_eq!(cbd.blocks()?, 1000);
///
/// // Since random_data only returns a data that fits in one block it writes continuously from last block
/// for i in 0..100 {
///     let data = random_data();
///     assert_eq!(cbd.write(&data)?, i);
///     assert_eq!(cbd.read(i)?, data);
/// }
///
/// cbd.remove(40)?;
/// cbd.remove(30)?;
/// cbd.remove(35)?;
///
/// // Since there are empty blocks in the middle of the file we re-use one of them
/// // (the last one to be available that fits the data)
/// assert_eq!(cbd.write(&random_data())?, 35);
/// # std::fs::remove_file("test.file")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Cabide<T> {
    /// File which typed database is binded to
    file: File,
    /// Caches number of next empty block
    next_block: u64,
    /// (number of continuous empty blocks -> list of "starting block"s)
    empty_blocks: BTreeMap<usize, Vec<u64>>,
    /// Marks that database must contain a single type
    _marker: PhantomData<T>,
}

impl<T> Cabide<T> {
    /// Binds database to specified file, creating it if non existent
    ///
    /// Pads file to have specified number of blocks, pre-filling it
    ///
    /// If file already exists empty blocks in the middle of it will be cached and prefered,
    /// next empty block number will be identified and cached too
    ///
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// // Opens file without pre-filling it, creates it since it's first run
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test2.file")?;
    /// let mut cbd: Cabide<u8> = Cabide::new("test2.file", None)?;
    /// assert_eq!(cbd.blocks()?, 0);
    ///
    /// // Re-opens file now pre-filling it
    /// cbd = Cabide::new("test2.file", Some(1000))?;
    /// assert_eq!(cbd.blocks()?, 1000);
    ///
    /// // Re-opens the file asking for less blocks than available, only to be ignored
    /// cbd = Cabide::new("test2.file", Some(30))?;
    /// assert_eq!(cbd.blocks()?, 1000);
    /// # std::fs::remove_file("test2.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<P>(filename: P, mut blocks: Option<u64>) -> Result<Self, Error>
    where
        P: AsRef<Path>
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(filename)?;
        let (mut next_block, mut empty_blocks) = (0, BTreeMap::default());

        let current_length = file.metadata()?.len();
        // If file already has data we need to parse it to generate an up-to-date Cabide
        if current_length > 0 {
            next_block = ((current_length as f64) / (BLOCK_SIZE as f64)).ceil() as u64;

            // If less pre-filled blocks than currently exist are asked for we ignore them
            blocks = blocks.filter(|blocks| next_block.saturating_sub(1) < *blocks);

            // Holds empty blocks chain
            let mut empty_block = None;

            // We need to find the empty blocks in the middle of the file
            for curr_block in 0..next_block {
                let mut metadata = [0];

                file.seek(SeekFrom::Start(curr_block * BLOCK_SIZE))?;
                if Read::by_ref(&mut file).take(1).read(&mut metadata)? == 0 {
                    // EOF
                    break;
                }

                if let Some((current, mut size)) = empty_block.take() {
                    if metadata[0] == Metadata::Empty as u8 {
                        // Free blocks chain keeps going
                        size += 1;
                        empty_block = Some((current, size));
                    } else {
                        // Free blocks chain ended, we must store it
                        empty_blocks
                            .entry(size)
                            .and_modify(|vec: &mut Vec<u64>| vec.push(current))
                            .or_insert_with(|| vec![current]);
                    }
                } else if metadata[0] == Metadata::Empty as u8 {
                    // First block of empty chain
                    empty_block = Some((curr_block, 1));
                }
            }
        }

        // Pre-fills the file if desired
        if let Some(blocks) = blocks {
            // `set_len` works assuming that `Metadata::Empty` is 0
            // So we assert it at compile time
            const _METADATA_EMPTY_MUST_BE_ZERO: u8 = 0 - (Metadata::Empty as u8);

            file.set_len(blocks * BLOCK_SIZE)?;
        }

        Ok(Self {
            file,
            next_block,
            empty_blocks,
            _marker: PhantomData
        })
    }

    /// Returns number of blocks written to file (some may be empty)
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// // Opens file without pre-filling it, creates it since it's first run
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test10.file")?;
    /// let mut cbd: Cabide<u8> = Cabide::new("test10.file", Some(1000))?;
    /// assert_eq!(cbd.blocks()?, 1000);
    /// # std::fs::remove_file("test10.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocks(&self) -> Result<u64, Error> {
        Ok(((self.file.metadata()?.len() as f64) / (BLOCK_SIZE as f64)).ceil() as u64)
    }
}

impl<T: DeserializeOwned> Cabide<T> {
    fn read_update_metadata(
        &mut self,
        block: u64,
        empty_read_blocks: bool,
    ) -> Result<T, Error> {
        let mut content = vec![];
        let mut empty_block = None;
        self.file.seek(SeekFrom::Start(block * BLOCK_SIZE))?;

        let mut metadata = [0];
        let mut expected_metadata = Metadata::Start;
        loop {
            // Reads block metadata
            if Read::by_ref(&mut self.file).take(1).read(&mut metadata)? == 0 {
                // EOF
                break;
            }

            if content.is_empty() && metadata[0] != expected_metadata as u8 {
                // If its the first block and the metadata mismatch
                if metadata[0] == Metadata::Empty as u8 {
                    // If first block is empty we error
                    return Err(Error::EmptyBlock);
                } else {
                    // If first block is in the middle of an object (continuation) we error
                    debug_assert_eq!(metadata[0], Metadata::Continuation as u8);
                    return Err(Error::ContinuationBlock);
                }
            } else if metadata[0] != expected_metadata as u8 {
                // Stop reading if all of the object has been read
                break;
            }

            // Overwrite the metadata if needed (in case of removal)
            if empty_read_blocks {
                if let Some((_, blocks)) = &mut empty_block {
                    *blocks += 1;
                } else {
                    empty_block = Some((block, 1));
                }

                self.file.seek(SeekFrom::Current(-1))?;
                self.file.write_all(&[Metadata::Empty as u8])?;
            }

            Read::by_ref(&mut self.file).take(CONTENT_SIZE).read_to_end(&mut content)?;

            // We must seek the last byte, which may be a END_BLOCK or a padding byte
            self.file.seek(SeekFrom::Current(1))?;
            

            // Makes sure we stop reading if object changes
            expected_metadata = Metadata::Continuation;
        }

        if let Some((index, size)) = empty_block {
            self.empty_blocks
                .entry(size)
                .and_modify(|vec| vec.push(index as u64))
                .or_insert_with(|| vec![index as u64]);
        }

        // Objects may be padded with Metadata::Empty, so we must truncate it
        while content.last() == Some(&(Metadata::Empty as u8)) {
            content.truncate(content.len() - 1);
        }

        // All blocks have a END_BYTE before the (optional) padding, so we remove it if it was read
        // because of the padding
        if content.last() == Some(&END_BYTE) {
            content.truncate(content.len() - 1);
        }

        let cursor = Cursor::new(content);
        let obj = deserialize_from(cursor).map_err(|_| Error::CorruptedBlock)?;
        Ok(obj)
    }

    /// Mark object blocks as empty, cacheing them, returns removed content
    ///
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test3.file")?;
    /// // Opens database, creates it since it's first run
    /// let mut cbd: Cabide<u8> = Cabide::new("test3.file", None)?;
    ///
    /// for i in 0..100 {
    ///     cbd.write(&i)?;
    /// }
    ///
    /// assert_eq!(cbd.remove(30)?, 30);
    /// assert_eq!(cbd.remove(3)?, 3);
    ///
    /// // Writing re-uses freed blocks in the middle of the file (using the last ones first)
    /// assert_eq!(cbd.write(&100)?, 3);
    /// assert_eq!(cbd.remove(3)?, 100);
    /// # std::fs::remove_file("test3.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove(&mut self, block: u64) -> Result<T, Error> {
        self.read_update_metadata(block, true)
    }

    /// Returns object deserialized from specified starting block (and its continuations)
    ///
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// // Opens file pre-filling it, creates it since it's first run
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test4.file")?;
    /// let mut cbd: Cabide<u8> = Cabide::new("test4.file", None)?;
    ///
    /// for i in 0..100 {
    ///     cbd.write(&i)?;
    /// }
    ///
    /// assert_eq!(cbd.read(30)?, 30);
    /// assert_eq!(cbd.read(3)?, 3);
    /// # std::fs::remove_file("test4.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read(&mut self, block: u64) -> Result<T, Error> {
        self.read_update_metadata(block, false)
    }

    /// Returns first element to be selected by the `filter` function
    ///
    /// Works in O(n), testing each block until the first is found
    ///
    /// ```rust
    /// use serde::{Serialize, Deserialize};
    /// use cabide::Cabide;
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct Student {
    ///     name: String,
    ///     dre: u64,
    ///     classes: Vec<u16>
    /// }
    ///
    /// # use rand::{distributions::*, random, thread_rng};
    /// # fn random_student(classes: Vec<u16>) -> Student {
    /// #     // this is slow af since it doesn't cache thread_rng
    /// #     let mut dre = random();
    /// #     while dre == 10101010 {
    /// #         dre = random();
    /// #     }
    /// #     Student {
    /// #         name: (0..7)
    /// #             .map(|_| Alphanumeric.sample(&mut thread_rng()))
    /// #             .collect(),
    /// #         dre,
    /// #         classes: classes.clone(),
    /// #     }
    /// # }
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test5.file")?;
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<Student> = Cabide::new("test5.file", Some(1000))?;
    /// assert_eq!(cbd.blocks()?, 1000);
    ///
    /// for i in 0..20 {
    ///     cbd.write(&random_student(vec![1023, random(), random()]))?;
    /// }
    ///
    /// cbd.write(&Student {
    ///     name: "Mr Legit Student".to_owned(),
    ///     dre: 10101010,
    ///     classes: vec![3],
    /// })?;
    ///
    /// let student = cbd.first(|student| student.dre == 10101010).unwrap();
    /// assert_eq!(&student.name, "Mr Legit Student");
    /// # std::fs::remove_file("test5.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn first(&mut self, filter: impl Fn(&T) -> bool) -> Option<T> {
        for block in 0..self.blocks().unwrap_or(0) {
            match self.read(block) {
                Ok(data) => {
                    if filter(&data) {
                        return Some(data);
                    }
                }
                Err(Error::EmptyBlock) => continue,
                Err(Error::ContinuationBlock) => continue,
                _ => return None,
            }
        }
        None
    }

    /// Returns list of element selected by the `filter` function
    ///
    /// ```rust
    /// use serde::{Serialize, Deserialize};
    /// use cabide::Cabide;
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct Student {
    ///     name: String,
    ///     dre: u64,
    ///     classes: Vec<u16>
    /// }
    ///
    /// # use rand::{distributions::*, random, thread_rng};
    /// # fn random_student(classes: Vec<u16>) -> Student {
    /// #     // this is slow af since it doesn't cache thread_rng
    /// #     Student {
    /// #         name: (0..7)
    /// #             .map(|_| Alphanumeric.sample(&mut thread_rng()))
    /// #             .collect(),
    /// #         dre: random(),
    /// #         classes,
    /// #     }
    /// # }
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test6.file")?;
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<Student> = Cabide::new("test6.file", Some(1000))?;
    ///
    /// cbd.write(&Student {
    ///     name: "Mr Legit Student".to_owned(),
    ///     dre: 10101010,
    ///     classes: vec![3],
    /// })?;
    ///
    /// for i in 0..20 {
    ///     cbd.write(&random_student(vec![1023, random(), random()]))?;
    /// }
    ///
    /// let students = cbd.filter(|student| student.classes.contains(&1023));
    /// assert_eq!(students.len(), 20);
    /// # std::fs::remove_file("test6.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(&mut self, filter: impl Fn(&T) -> bool) -> Vec<T> {
        let mut vec = vec![];
        for block in 0..self.blocks().unwrap_or(0) {
            match self.read(block) {
                Ok(data) => {
                    if filter(&data) {
                        vec.push(data);
                    }
                }
                Err(Error::EmptyBlock) => continue,
                Err(Error::ContinuationBlock) => continue,
                // We ignore IO errors, this may be a mistake (or not, only future will know)
                _ => continue,
            }
        }
        vec
    }
}

impl<T: Serialize> Cabide<T> {
    /// Writes data to database, splitting data in multiple blocks if needed
    ///
    /// Re-uses removed blocks, doesn't fragment data
    ///
    /// ```
    /// use cabide::Cabide;
    ///
    /// # fn main() -> Result<(), cabide::Error> {
    /// # std::fs::File::create("test7.file")?;
    /// let mut cbd: Cabide<u8> = Cabide::new("test7.file", None)?;
    ///
    /// for i in 0..1000 {
    ///     assert_eq!(cbd.write(&rand::random())?, i);
    /// }
    ///
    /// cbd.remove(30)?;
    /// cbd.remove(58)?;
    ///
    /// // Since there are empty blocks in the middle of the file we re-use one of them
    /// // (the last one to be available that fits the data)
    /// assert_eq!(cbd.write(&rand::random())?, 58);
    /// # std::fs::remove_file("test7.file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write(&mut self, obj: &T) -> Result<u64, Error> {
        let raw = serialize(obj).map_err(|_| Error::CorruptedBlock)?;
        let blocks_needed = raw.len() / (CONTENT_SIZE as usize);

        let (mut starting_block, mut remaining_blocks, mut delete_block) = (None, None, None);
        // First we check if there are empty blocks with the needed size
        for (blocks, block_vec) in &mut self.empty_blocks {
            if *blocks * (CONTENT_SIZE as usize) >= raw.len() {
                starting_block = block_vec.pop();

                if let Some(starting_block) = starting_block {
                    let index = starting_block as usize;
                    remaining_blocks = Some((*blocks - blocks_needed, index + blocks_needed));
                    break;
                } else if delete_block.is_none() {
                    // We need to handle empty leafs, but we only handle one at a time
                    delete_block = Some(*blocks);
                }
            }
        }

        // If BTreeMap leaf's has no starting block we remove it
        if let Some(blocks) = delete_block {
            self.empty_blocks.remove(&blocks);
        }

        // Returns unused free blocks from the extracted chain to the empty_blocks list
        if let Some((blocks, index)) = remaining_blocks {
            self.empty_blocks
                .entry(blocks)
                .and_modify(|vec| vec.push(index as u64))
                .or_insert_with(|| vec![index as u64]);
        }

        let starting_block = if let Some(block) = starting_block {
            block
        } else {
            // If there wasn't any fragmented empty block we take the next available one
            // We need to update self.next_block taking into account how many bytes we are writing
            let block = self.next_block;
            self.next_block += ((raw.len() as f64) / (CONTENT_SIZE as f64)).ceil() as u64;
            block
        };

        self.file
            .seek(SeekFrom::Start(starting_block * BLOCK_SIZE))?;

        let (mut written, mut blocks, mut metadata) = (0, 0, Metadata::Start);
        // Split encoded data in chunks, appending the metadata to each block before writing the chunks
        for buff in raw.chunks(CONTENT_SIZE as usize) {
            written += self.file.write(&[metadata as u8])?;
            written += self.file.write(buff)?;
            written += self.file.write(&[END_BYTE])?;
            metadata = Metadata::Continuation;
            blocks += 1;
        }

        // Last chunk may need to be padded
        let null_byte = Metadata::Empty
            .as_char()
            .to_string()
            .repeat((blocks * BLOCK_SIZE) as usize - written);
        self.file.write_all(null_byte.as_bytes())?;
        Ok(starting_block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::*, random, thread_rng};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct InnerData {
        wow: Option<f32>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Data {
        this: u8,
        that: bool,
        there: String,
        those: u64,
        inner: InnerData,
    }

    fn random_data() -> Data {
        // this is slow af since it doesn't cache thread_rng
        Data {
            this: random(),
            that: random(),
            there: (0..40)
                .map(|_| Alphanumeric.sample(&mut thread_rng()))
                .collect(),
            those: random(),
            inner: InnerData {
                wow: Some(random()).filter(|_| random()),
            },
        }
    }

    #[test]
    fn persistance() {
        std::fs::File::create("cabide.test").unwrap();
        let mut cbd: Cabide<Data> = Cabide::new("cabide.test", None).unwrap();
        cbd.file.set_len(0).unwrap();

        let mut blocks = vec![];
        for _ in 0..50 {
            let data = random_data();
            let block = cbd.write(&data).unwrap();
            blocks.push((block, data));
        }

        // this drops the last cabide, therefore closes the file
        cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for (block, data) in &blocks {
            let read: Data = cbd.read(*block).unwrap();
            assert_eq!(&read, data);
        }

        assert_eq!(cbd.remove(blocks[8].0).unwrap(), blocks[8].1);

        assert_eq!(cbd.remove(blocks[13].0).unwrap(), blocks[13].1);

        // this drops the last cabide, therefore closes the file
        cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for i in &[8, 13] {
            let data = random_data();
            let block = cbd.write(&data).unwrap();
            blocks[*i as usize] = (block, data);
        }

        cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for (block, data) in blocks {
            assert_eq!(cbd.read(block).unwrap(), data);
        }
        std::fs::remove_file("cabide.test").unwrap();
    }
}
