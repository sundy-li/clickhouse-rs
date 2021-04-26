use crate::binary::{Encoder, ReadEx};
use crate::types::{Block, ResultWriter};

use crate::io::Stream;
use chrono_tz::Tz;
use errors::{DriverError, Error, Result};
use io::ClickhouseTransport;
use log::debug;
use log::error;
use protocols::{
    ExceptionResponse, HelloRequest, HelloResponse, Packet, QueryRequest, SERVER_END_OF_STREAM,
};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;

mod binary;
pub mod error_codes;
pub mod errors;
pub mod io;
pub mod protocols;
pub mod types;

#[macro_use]
extern crate bitflags;

pub trait ClickHouseSession {
    fn execute_query(&self, query: &str, stage: u64, writer: &mut ResultWriter) -> Result<()>;

    fn with_stack_trace(&self) -> bool {
        false
    }

    fn dbms_name(&self) -> &str {
        "clickhouse-server"
    }

    // None is by default, which will use same version as client send
    fn dbms_version_major(&self) -> u64 {
        19
    }

    fn dbms_version_minor(&self) -> u64 {
        17
    }

    fn dbms_tcp_protocol_version(&self) -> u64 {
        54428
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    fn server_display_name(&self) -> &str {
        "clickhouse-server"
    }

    fn dbms_version_patch(&self) -> u64 {
        1
    }
}

#[derive(Default)]
pub struct QueryState {
    pub query_id: String,
    pub stage: u64,
    pub compression: u64,
    pub query: String,
    pub is_cancelled: bool,
    pub is_connection_closed: bool,
    /// empty or not
    pub is_empty: bool,
    /// Data was sent.
    pub sent_all_data: bool,
}

impl QueryState {
    fn reset(&mut self) {
        self.stage = 0;
        self.is_cancelled = false;
        self.is_connection_closed = false;
        self.is_empty = false;
        self.sent_all_data = false;
    }
}


pub struct CHContext<S> {
    pub state: QueryState,
    pub session: S,
}

impl<S: ClickHouseSession> CHContext<S> {
    fn new(state: QueryState, session: S) -> Self {
        Self {
            state,
            session,
        }
    }
}

/// A server that speaks the ClickHouseprotocol, and can delegate client commands to a backend
/// that implements [`ClickHouseSession`]
pub struct ClickHouseServer {}

impl ClickHouseServer {
    pub async fn run_on_stream<S>(session: S, stream: TcpStream) -> Result<()>
        where
            S: ClickHouseSession {
        ClickHouseServer::run_on(session, stream.into()).await
    }
}

impl ClickHouseServer {
    async fn run_on<S>(session: S, stream: Stream) -> Result<()>
        where
            S: ClickHouseSession {
        let mut srv = ClickHouseServer {};

        srv.run(session, stream).await?;
        Ok(())
    }

    async fn run<S>(&mut self, session: S, stream: Stream) -> Result<()>
        where
            S: ClickHouseSession {
        debug!("Handle New session");
        let tz: Tz = session.timezone().parse()?;
        let ctx = CHContext::new(QueryState::default(), session);
        let mut transport = ClickhouseTransport::new(ctx, stream, tz);

        while let Some(ret) = transport.next().await {
            match ret {
                Ok(()) => {
                    debug!("Ready one");
                },
                Err(e) => {
                    error!("Error in {:?}", e);
                    break;
                }
            }
        }
        debug!("Exited one session");
        Ok(())
    }
}

#[macro_export]
macro_rules! row {
    () => { $crate::types::RNil };
    ( $i:ident, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($i).into(), $i.into())
    };
    ( $i:ident ) => { row!($i: $i) };

    ( $k:ident: $v:expr ) => {
        $crate::types::RNil.put(stringify!($k).into(), $v.into())
    };

    ( $k:ident: $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($k).into(), $v.into())
    };

    ( $k:expr => $v:expr ) => {
        $crate::types::RNil.put($k.into(), $v.into())
    };

    ( $k:expr => $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put($k.into(), $v.into())
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
