use std::{io, fmt};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Serde(serde_json::Error),
    FromUtf8,
    ContinuationBlock,
    EmptyBlock
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_: std::string::FromUtf8Error) -> Self {
        Self::FromUtf8
    }
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

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(err) => write!(fmt, "{}", err.to_string()),
            Error::Serde(err) => write!(fmt, "{}", err.to_string()),
            Error::FromUtf8 => write!(fmt, "Convertion from byte array to utf8 string failed"),
            Error::ContinuationBlock => write!(fmt, "Continuation Block"),
            Error::EmptyBlock => write!(fmt, "Empty Block"),
        }
    }
}

impl std::error::Error for Error {}
