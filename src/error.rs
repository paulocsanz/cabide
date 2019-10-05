use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Serde(serde_json::Error),
    //Bincode(Box<bincode::ErrorKind>),
    ContinuationBlock,
    EmptyBlock
}

impl From<io::Error> for Error {
    fn from(io: io::Error) -> Self {
        Self::Io(io)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Serde(err)
    }
}

/*
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Self::Bincode(err)
    }
}
*/
