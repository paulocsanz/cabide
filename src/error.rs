use std::{io, fmt};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Bincode(Box<bincode::ErrorKind>),
    ContinuationBlock,
    EmptyBlock
}

impl From<io::Error> for Error {
    fn from(io: io::Error) -> Self {
        Self::Io(io)
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Self::Bincode(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(err) => write!(fmt, "{}", err.to_string()),
            Error::Bincode(err) => write!(fmt, "{}", err.to_string()),
            Error::ContinuationBlock => write!(fmt, "Continuation Block"),
            Error::EmptyBlock => write!(fmt, "Empty Block"),
        }
    }
}

impl std::error::Error for Error {}
