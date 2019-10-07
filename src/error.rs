use std::{io, fmt};

/// Enumerates all errors possible in this crate
#[derive(Debug)]
pub enum Error {
    /// IO errors, the basis of this crate since everything is binded to a file
    Io(io::Error),
    /// Means deserialization failed, file is either corrupted or the type is wrong
    CorruptedBlock,
    /// Happens if you try to read from a block that is in the middle of an object
    ContinuationBlock,
    /// Happens if you try to read from a empty block
    EmptyBlock
}

impl From<io::Error> for Error {
    #[inline(always)]
    fn from(io: io::Error) -> Self {
        Self::Io(io)
    }
}

impl fmt::Display for Error {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(err) => write!(fmt, "{}", err.to_string()),
            Error::CorruptedBlock => write!(fmt, "Unable to deserialize a block, file is corrupted or type is wrong"),
            Error::ContinuationBlock => write!(fmt, "Continuation Block"),
            Error::EmptyBlock => write!(fmt, "Empty Block"),
        }
    }
}

impl std::error::Error for Error {}
