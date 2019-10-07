/// Each block has a END_BYTE to identify where the optional padding starts
pub const END_BYTE: u8 = 8;

/// Size of binary block that database deals with
///
/// Smaller blocks mean more metadata per object, since each block needs 2 bytes of metadata, making objects need more blocks
///
/// Bigger blocks mean more zero padding to fill the entire block (when it's the last block of the object)
pub const BLOCK_SIZE: u64 = 30;

/// Space available in each block to hold content (currently there are 2 bytes of metadata per block)
pub const CONTENT_SIZE: u64 = BLOCK_SIZE - 2;

/// Block's starting byte, determines how to interpret blcok
#[derive(PartialEq, Copy, Clone)]
pub enum Metadata {
    Empty = 0,
    Start,
    Continuation,
}

impl Metadata {
    #[inline(always)]
    pub fn as_char(self) -> char {
        (self as u8).into()
    }
}
