use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_reader, to_string};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::{collections::BTreeMap, fs::File, fs::OpenOptions, path::Path};

const BLOCK_SIZE: u64 = 20;

#[derive(PartialEq, Copy, Clone)]
enum State {
    Empty = 0,
    Start,
    Continuation,
}

pub struct Cabide {
    file: File,
    last_block: Option<u64>,
    // (number of blocks -> first block)
    free_blocks: BTreeMap<usize, Vec<u64>>,
}

impl Cabide {
    /// Binds database to speficied file, creating it if non existant
    ///
    /// Pads file to have specified number of blocks, pre-filling it
    pub fn new(filename: impl AsRef<Path>, mut blocks: Option<u64>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(filename)?;
        let (mut last_block, mut free_blocks) = (None, BTreeMap::default());

        let current_length = file.metadata()?.len();
        // If file already has data we need to parse it to generate an updated Cabide
        if current_length > 0 {
            last_block = Some(current_length / BLOCK_SIZE);
            blocks = blocks.filter(|blocks| last_block.map_or(0, |b| b + 1) < *blocks);

            // We need to find free blocks in the middle of the file
            let mut block = [0; BLOCK_SIZE as usize];
            let mut free_block = None;
            let mut curr_block = 0;
            loop {
                if curr_block >= last_block.unwrap_or(0) {
                    break;
                }

                // Only one byte needs to be read, the rest can be skipped, but Im too tired for that
                let bytes_read = Read::by_ref(&mut file).take(BLOCK_SIZE).read(&mut block)?;
                if bytes_read == 0 {
                    break;
                }

                if block[0] == 0 && free_block.is_none() {
                    free_block = Some((curr_block, 0));
                } else if block[0] == 0 {
                    if let Some((_, size)) = &mut free_block {
                        *size += 1;
                    }
                } else if let Some((current, size)) = free_block {
                    free_blocks
                        .entry(size)
                        .and_modify(|vec: &mut Vec<u64>| vec.push(current))
                        .or_insert_with(|| vec![current]);
                }

                curr_block += 1;
            }
        }

        // Pre-fill the file if desired
        if let Some(blocks) = blocks {
            // `set_len` works assuming that `State::Empty` is 0
            // So we assert it at compile time
            const _STATE_EMPTY_MUST_BE_ZERO: u8 = 0 - (State::Empty as u8);

            file.set_len(blocks * BLOCK_SIZE)?;
        }

        Ok(Self {
            file,
            last_block,
            free_blocks,
        })
    }

    fn read_update_state<T: DeserializeOwned>(
        &mut self,
        block: u64,
        update_state: Option<State>,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let mut content = String::new();
        self.file.seek(SeekFrom::Start(block * BLOCK_SIZE))?;

        let state = State::Start;
        let mut expected_state = State::Start;
        loop {
            // Reads block metadata
            let bytes_read = Read::by_ref(&mut self.file)
                .take(1)
                .read(&mut [state as u8])?;
            if bytes_read == 0 {
                // Breaks if EOF
                break;
            }

            // Overwrite the state if needed (in case of removal)
            if let Some(new_state) = update_state {
                self.file.seek(SeekFrom::Current(-1))?;
                self.file.write(&[new_state as u8])?;
            }

            // Stop reading if all of the object has been read
            if state != expected_state {
                break;
            }

            // Keep reading object
            Read::by_ref(&mut self.file)
                .take(BLOCK_SIZE - 1)
                .read_to_string(&mut content)?;

            // Makes sure we keep reading the same object
            expected_state = State::Continuation;
        }

        // Objects may be padded with State::Empty, so we must truncate it
        while content.chars().last() == Some((State::Empty as u8) as char) {
            content.truncate(content.len() - 1);
        }

        let cursor = Cursor::new(content);
        let obj = from_reader(cursor)?;
        Ok(obj)
    }

    /// Empties speficied starting block (and its continuations) returning its content
    pub fn remove<T: DeserializeOwned>(
        &mut self,
        block: u64,
    ) -> Result<T, Box<dyn std::error::Error>> {
        self.read_update_state(block, Some(State::Empty))
    }

    /// Reads type from speficied starting block (and its continuations)
    pub fn read<T: DeserializeOwned>(
        &mut self,
        block: u64,
    ) -> Result<T, Box<dyn std::error::Error>> {
        self.read_update_state(block, None)
    }

    /// Writes encoded type to database, splitting data in multiple blocks
    ///
    /// Re-uses removed blocks, doesn't fragment data
    pub fn write(&mut self, obj: impl Serialize) -> Result<u64, Box<dyn std::error::Error>> {
        let raw = to_string(&obj)?;

        let mut first_block = None;
        // First we check if there are free blocks with the needed size
        for (blocks, block_vec) in &self.free_blocks {
            if *blocks * (BLOCK_SIZE as usize) >= raw.len() {
                first_block = Some((block_vec[block_vec.len() - 1], *blocks, block_vec.len() - 1));
                break;
            }
        }

        let first_block = if let Some((block, blocks, index)) = first_block {
            // If there was an available block we take it
            let block_vec = self.free_blocks.get_mut(&blocks).unwrap();
            block_vec.remove(index);

            let blocks_needed = raw.len() / (BLOCK_SIZE as usize);
            if blocks_needed < blocks {
                self.free_blocks
                    .entry(blocks - blocks_needed)
                    .and_modify(|vec| vec.push((index + blocks_needed) as u64))
                    .or_insert_with(|| vec![(index + blocks_needed) as u64]);
            }

            block
        } else {
            // If there wasn't any fragmented free block we take the last available one
            // We need to update self.last_block taking into account how many bytes we are writing
            let block = self.last_block.unwrap_or(0);
            let offset = ((raw.len() as f64) / (BLOCK_SIZE as f64)).ceil() as u64;
            if let Some(block) = &mut self.last_block {
                *block += offset;
            } else {
                self.last_block = Some(offset);
            }
            block
        };

        self.file.seek(SeekFrom::Start(first_block * BLOCK_SIZE))?;

        let mut state = State::Start;
        // Split encoded data in chunks, appending the state to each block before writing the chunks
        for buff in raw.as_bytes().chunks((BLOCK_SIZE as usize) - 1) {
            self.file.write(&[state as u8])?;
            self.file.write(buff)?;
            state = State::Continuation;
        }

        // Last chunk may need to be padded
        self.file.write(
            ((State::Empty as u8) as char)
                .to_string()
                .repeat(raw.len() % (BLOCK_SIZE as usize))
                .as_bytes(),
        )?;
        Ok(first_block)
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
        let mut cbd = Cabide::new("cabide.test", Some(0)).unwrap();
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

        assert_eq!(cbd.remove::<Data>(blocks[8].0).unwrap(), blocks[8].1);

        assert_eq!(cbd.remove::<Data>(blocks[13].0).unwrap(), blocks[13].1);

        // this drops the last cabide, therefore closes the file
        cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for i in &[8, 13] {
            let data = random_data();
            let block = cbd.write(&data).unwrap();
            blocks[*i as usize] = (block, data);
        }

        //cbd = Cabide::new("cabide.test", Some(10)).unwrap();

        for (block, data) in blocks {
            assert_eq!(cbd.read::<Data>(block).unwrap(), data);
        }
    }
}
