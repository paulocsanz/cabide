mod error;

use crate::error::Error;

use serde_json::{from_reader, to_string};
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::{collections::BTreeMap, fs::File, fs::OpenOptions, marker::PhantomData, path::Path};

/// Each block has ending byte to identify when it ends (therefore zero bytes
const END_BLOCK: u8 = 1;

/// Size of blocks data is written/read on
///
/// Smaller blocks mean more metadata per object, since object needs more blocks to be stored
///
/// Bigger blocks mean more zero padding to fill the entire block
const BLOCK_SIZE: u64 = 20;

/// Available space in each block to hold content (currently there are 2 bytes of padding per block
const CONTENT_SIZE: u64 = BLOCK_SIZE - 2;

#[derive(PartialEq, Copy, Clone)]
enum Metadata {
    Empty = 0,
    Start,
    Continuation,
}

impl Metadata {
    fn as_char(self) -> char {
        (self as u8).into()
    }
}

/// Abstracts typed database binded to a specific file
///
/// Specified type (order is important) will be (de)serialized from/to the file
///
/// If the type changes, either with a different field definition order, different field types or if more fields are added (de)serialization will be broken, please keep the type unchanged or migrate the database
///
/// Free blocks in the middle of the file will be cached and prefered, but no data is fragmented over them
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use cabide::Cabide;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Data {
///     name: String,
///     maybe_number: Option<u8>,
/// }
///
/// # use rand::{distributions::*, random, thread_rng};
/// # fn random_data() -> Data {
/// #     // this is slow af since it doesn't cache thread_rng
/// #     Data {
/// #         name: (0..10)
/// #             .map(|_| Alphanumeric.sample(&mut thread_rng()))
/// #             .collect(),
/// #         maybe_number: Some(random()).filter(|_| random()),
/// #     }
/// # }
///
/// // Opens file pre-filling it, creates it since it's first run
/// # std::fs::File::create("test.file").unwrap();
/// let mut cbd: Cabide<Data> = Cabide::new("test.file", Some(1000)).unwrap();
/// assert_eq!(cbd.blocks().unwrap(), 1000);
///
/// // Writes continuously from last block
/// for i in 0..100 {
///     assert_eq!(cbd.write(&random_data()).unwrap(), i);
/// }
///
/// cbd.remove(30).unwrap();
/// cbd.remove(31).unwrap();
///
/// // Since there are empty blocks in the middle of the file we re-use one of them
/// assert_eq!(cbd.write(&random_data()).unwrap(), 31);
/// # std::fs::remove_file("test.file").unwrap();
/// ```
#[derive(Debug)]
pub struct Cabide<T> {
    // File which typed database is binded to
    file: File,
    // Caches number of next empty block
    next_block: Option<u64>,
    // (number of continuous empty blocks -> list of "starting block"s)
    empty_blocks: BTreeMap<usize, Vec<u64>>,
    // Marks that database must contain a single type
    _marker: PhantomData<T>,
}

impl<T> Cabide<T> {
    /// Binds database to specified file, creating it if non existant
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
    /// let mut cbd: Cabide<u8> = Cabide::new("test2.file", None).unwrap();
    /// assert_eq!(cbd.blocks().unwrap(), 0);
    ///
    /// // Re-opens file now pre-filling it
    /// cbd = Cabide::new("test2.file", Some(1000)).unwrap();
    /// assert_eq!(cbd.blocks().unwrap(), 1000);
    ///
    /// // Re-opens the file asking for less blocks than available, only to be ignored
    /// cbd = Cabide::new("test2.file", Some(30)).unwrap();
    /// assert_eq!(cbd.blocks().unwrap(), 1000);
    /// # std::fs::remove_file("test2.file").unwrap();
    /// ```
    pub fn new(filename: impl AsRef<Path>, mut blocks: Option<u64>) -> Result<Self, Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(filename)?;
        let (mut next_block, mut empty_blocks) = (None, BTreeMap::default());

        let current_length = file.metadata()?.len();
        // If file already has data we need to parse it to generate an up-to-date Cabide
        if current_length > 0 {
            next_block = Some(((current_length as f64) / (BLOCK_SIZE as f64)).ceil() as u64 + 1);

            // If less pre-filled blocks than currently exist are asked for we ignore them
            blocks = blocks.filter(|blocks| next_block.map_or(0, |b| b - 1) < *blocks);

            // Holds empty blocks chain
            let mut empty_block = None;

            // We need to find the empty blocks in the middle of the file
            for curr_block in 0..next_block.unwrap_or(0) {
                let mut metadata: [u8; 1] = [0];

                file.seek(SeekFrom::Start(curr_block * BLOCK_SIZE))?;
                match file.read_exact(&mut metadata[..1]).map_err(|e| e.kind()) {
                    Ok(()) => {},
                    Err(io::ErrorKind::UnexpectedEof) => break,
                    Err(err) => return Err(io::Error::from(err).into()),
                }

                if let Some((current, size)) = &mut empty_block {
                    if metadata[0] == 0 {
                        // Free blocks chain keeps going
                        *size += 1;
                    } else {
                        // Free blocks chain ended, we must store it
                        empty_blocks
                            .entry(*size)
                            .and_modify(|vec: &mut Vec<u64>| vec.push(*current))
                            .or_insert_with(|| vec![*current]);
                    }
                } else {
                    // First block of empty chain
                    empty_block = Some((curr_block, 0));
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
    /// let mut cbd: Cabide<u8> = Cabide::new("test10.file", Some(1000)).unwrap();
    /// assert_eq!(cbd.blocks().unwrap(), 1000);
    /// ```
    pub fn blocks(&self) -> Result<u64, Error> {
        Ok(((self.file.metadata()?.len() as f64) / (BLOCK_SIZE as f64)).ceil() as u64)
    }
}

impl<T: DeserializeOwned> Cabide<T> {
    fn read_update_metadata(
        &mut self,
        block: u64,
        update_metadata: Option<Metadata>,
    ) -> Result<T, Error> {
        let mut content = String::new();
        self.file.seek(SeekFrom::Start(block * BLOCK_SIZE))?;

        let mut metadata = [0];
        let mut expected_metadata = Metadata::Start;
        loop {
            // Reads block metadata
            if Read::by_ref(&mut self.file).take(1).read(&mut metadata)? == 0 {
                // EOF
                break;
            }

            // Overwrite the metadata if needed (in case of removal)
            if let Some(new_metadata) = update_metadata {
                self.file.seek(SeekFrom::Current(-1))?;
                self.file.write(&[new_metadata as u8])?;
            }

            if content.len() == 0 && metadata[0] != expected_metadata as u8 {
                // If its the first block and the metadata mismatch
                if metadata[0] == Metadata::Empty as u8 {
                    // If first block is empty we error
                    return Err(Error::EmptyBlock);
                } else {
                    // If first block is in the middle of an object (continuation) we error
                    return Err(Error::ContinuationBlock);
                }
            } else if metadata[0] != expected_metadata as u8 {
                // Stop reading if all of the object has been read
                break;
            }

            Read::by_ref(&mut self.file).take(CONTENT_SIZE).read_to_string(&mut content)?;

            // Every block ends with END_BLOCK, so we can skip it
            self.file.seek(SeekFrom::Current(1))?;

            // Makes sure we stop reading if object changes
            expected_metadata = Metadata::Continuation;
        }

        // Objects may be padded with Metadata::Empty, so we must truncate it
        while content.as_bytes().last() == Some(&(Metadata::Empty as u8)) {
            content.truncate(content.len() - 1);
        }

        // Skips END_BLOCK if it was read
        if content.as_bytes().last() == Some(&END_BLOCK) {
            content.truncate(content.len() - 1);
        }

        let cursor = Cursor::new(content);
        let obj = from_reader(cursor)?;
        Ok(obj)
    }

    /// Empties specified starting block (and its continuations) returning its content
    ///
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<u8> = Cabide::new("test3.file", None).unwrap();
    ///
    /// // Writes continuously from last block
    /// for i in 0..100 {
    ///     cbd.write(&i).unwrap();
    /// }
    ///
    /// assert_eq!(cbd.remove(30).unwrap(), 30);
    /// assert_eq!(cbd.remove(3).unwrap(), 3);
    ///
    /// // Writing re-uses freed blocks in the middle of the file (using the last ones first)
    /// cbd.write(&30).unwrap();
    /// assert_eq!(cbd.remove(30).unwrap(), 30);
    /// # std::fs::remove_file("test3.file").unwrap();
    /// ```
    pub fn remove(&mut self, block: u64) -> Result<T, Error> {
        self.read_update_metadata(block, Some(Metadata::Empty))
    }

    /// Reads type from specified starting block (and its continuations)
    ///
    /// ```rust
    /// use cabide::Cabide;
    ///
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<u8> = Cabide::new("test4.file", None).unwrap();
    ///
    /// for i in 0..100 {
    ///     cbd.write(&i).unwrap();
    /// }
    ///
    /// assert_eq!(cbd.read(30).unwrap(), 30);
    /// assert_eq!(cbd.read(3).unwrap(), 3);
    /// # std::fs::remove_file("test4.file").unwrap();
    /// ```
    pub fn read(&mut self, block: u64) -> Result<T, Error> {
        self.read_update_metadata(block, None)
    }

    /// Returns first element to be selected by the `filter` function
    ///
    /// Works in O(n), testing each block
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
    ///
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<Student> = Cabide::new("test5.file", Some(1000)).unwrap();
    /// assert_eq!(cbd.blocks().unwrap(), 1000);
    ///
    /// cbd.write(&Student {
    ///     name: "Mr Legit Student".to_owned(),
    ///     dre: 10101010,
    ///     classes: vec![3],
    /// }).unwrap();
    ///
    /// // Writes continuously from last block
    /// for i in 0..20 {
    ///     cbd.write(&random_student(vec![1023, 8, 13])).unwrap();
    /// }
    ///
    /// let student = cbd.first(|student| student.dre == 10101010).unwrap();
    /// assert_eq!(&student.name, "Mr Legit Student");
    /// # std::fs::remove_file("test5.file").unwrap();
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
    ///
    /// // Opens file pre-filling it, creates it since it's first run
    /// let mut cbd: Cabide<Student> = Cabide::new("test6.file", Some(1000)).unwrap();
    /// # std::fs::File::create("test.file").unwrap();
    /// cbd.write(&Student {
    ///     name: "Mr Legit Student".to_owned(),
    ///     dre: 10101010,
    ///     classes: vec![3],
    /// }).unwrap();
    ///
    /// // Writes continuously from last block
    /// for i in 0..20 {
    ///     cbd.write(&random_student(vec![1023, 8, 13])).unwrap();
    /// }
    ///
    /// let students = cbd.filter(|student| student.classes.contains(&1023));
    /// assert_eq!(students.len(), 20);
    /// # std::fs::remove_file("test6.file").unwrap();
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
    /// Writes encoded type to database, splitting data in multiple blocks
    ///
    /// Re-uses removed blocks, doesn't fragment data
    ///
    /// ```
    /// use cabide::Cabide;
    ///
    /// let mut cbd: Cabide<u8> = Cabide::new("test7.file", None).unwrap();
    ///
    /// // Writes continuously from last block
    /// for i in 0..1000 {
    ///     assert_eq!(cbd.write(&rand::random()).unwrap(), i);
    /// }
    ///
    /// cbd.remove(30);
    /// cbd.remove(58);
    ///
    /// // Since there are empty blocks in the middle of the file we re-use one of them
    /// assert_eq!(cbd.write(&rand::random()).unwrap(), 58);
    /// # std::fs::remove_file("test7.file").unwrap();
    /// ```
    pub fn write(&mut self, obj: &T) -> Result<u64, Error> {
        let raw = to_string(obj)?;
        let blocks_needed = raw.len() / (CONTENT_SIZE as usize);

        let (mut starting_block, mut remaining_blocks) = (None, None);
        // First we check if there are empty blocks with the needed size
        for (blocks, block_vec) in &mut self.empty_blocks {
            if *blocks * (CONTENT_SIZE as usize) >= raw.len() {
                starting_block = block_vec.pop();

                // We return the blocks we don't need to the empty_blocks list
                if blocks_needed < *blocks {
                    let index = starting_block.unwrap() as usize;
                    remaining_blocks = Some((blocks - blocks_needed, index + blocks_needed));
                }

                break;
            }
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
            let block = self.next_block.unwrap_or(0);
            let offset = ((raw.len() as f64) / (CONTENT_SIZE as f64)).ceil() as u64;
            if let Some(block) = &mut self.next_block {
                *block += offset;
            } else {
                self.next_block = Some(offset);
            }
            block
        };

        self.file
            .seek(SeekFrom::Start(starting_block * BLOCK_SIZE))?;

        let (mut written, mut blocks, mut metadata) = (0, 0, Metadata::Start);
        // Split encoded data in chunks, appending the metadata to each block before writing the chunks
        // (and each END_BLOCK)
        for buff in raw.as_bytes().chunks((BLOCK_SIZE as usize) - 2) {
            written += self.file.write(&[metadata as u8])?;
            written += self.file.write(buff)?;
            written += self.file.write(&[END_BLOCK])?;
            metadata = Metadata::Continuation;
            blocks += 1;
        }

        // Last chunk may need to be padded
        let null_byte = Metadata::Empty
            .as_char()
            .to_string()
            .repeat((blocks * BLOCK_SIZE) as usize - written);
        self.file.write(null_byte.as_bytes())?;
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
        std::fs::remove_file("cabide.test").unwrap();

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

        /*
        cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for (block, data) in blocks {
            println!("{} {:?}", block, data);
            assert_eq!(cbd.read(block).unwrap(), data);
        }
        */
    }
}
