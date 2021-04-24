use std::io::Read;

use chrono_tz::Tz;

use crate::protocols::Packet;
use crate::protocols::{self, HelloRequest, QueryRequest};

use crate::{
    binary::ReadEx,
    errors::{DriverError, Error, Result},
};

/// The internal clickhouse client request parser.
pub(crate) struct Parser<T> {
    reader: T,

    tz: Tz,
    compress: Option<bool>,
    hello_request: Option<HelloRequest>,
}

/// The parser can be used to parse clickhouse client requests
impl<'a, T: Read> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub(crate) fn new(
        reader: T,
        tz: Tz,
        compress: Option<bool>,
        hello_request: Option<HelloRequest>,
    ) -> Parser<T> {
        Self {
            reader,
            tz,
            compress,
            hello_request,
        }
    }

    pub(crate) fn parse_packet(&mut self) -> Result<Packet> {
        let packet = self.reader.read_uvarint()?;
        match packet {
            protocols::CLIENT_PING => Ok(Packet::Ping),
            protocols::CLIENT_CANCEL => Ok(Packet::Cancel),
            protocols::CLIENT_QUERY => Ok(self.parse_query()?),
            protocols::CLIENT_HELLO => Ok(self.parse_hello()?),

            _ => Err(Error::Driver(DriverError::UnknownPacket { packet })),
        }
    }

    fn parse_hello(&mut self) -> Result<Packet> {
        Ok(Packet::Hello(HelloRequest::read_from(&mut self.reader)?))
    }

    fn parse_query(&mut self) -> Result<Packet> {
        match self.hello_request {
            Some(ref hello) => {
                let query = QueryRequest::read_from(&mut self.reader, hello)?;
                Ok(Packet::Query(query))
            }
            _ => Err(Error::Driver(DriverError::UnexpectedPacket)),
        }
    }
}
