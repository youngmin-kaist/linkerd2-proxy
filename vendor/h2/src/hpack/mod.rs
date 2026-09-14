mod decoder;
mod encoder;
pub(crate) mod header;
pub(crate) mod huffman;
pub mod mirror;
pub mod selective;
mod table;
pub mod transcode;

#[cfg(test)]
mod test;

pub use self::decoder::{Decoder, DecoderError, NeedMore};
pub use self::encoder::Encoder;
pub use self::header::{BytesStr, Header};
// Only reachable from outside with the `unstable-hpack` feature.
#[allow(unused_imports)]
pub use self::mirror::{EncoderTable, Kind, MirrorTable};
#[allow(unused_imports)]
pub use self::selective::{NeededSet, RawHeaderReps};
#[allow(unused_imports)]
pub use self::transcode::{transcode, Error as TranscodeError, PolicyFields, TranscodeStats};
