use std::io::Result;

use crate::types::Block;

pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Block>> + Sync + Send>>;
