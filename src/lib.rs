use std::sync::Arc;

use chrono_tz::Tz;
use errors::Result;
use io::ClickhouseTransport;
use log::debug;
use log::error;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;

use crate::io::block_stream::SendableDataBlockStream;
use crate::io::Stream;
use crate::types::Block;
use crate::types::Progress;

mod binary;
pub mod error_codes;
pub mod errors;
pub mod io;
pub mod protocols;
pub mod types;

#[macro_use]
extern crate bitflags;

#[async_trait::async_trait]
pub trait ClickHouseSession: Send + Sync {
    async fn execute_query(&self, _: &QueryState) -> Result<QueryResponse>;

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

    fn get_progress(&self) -> Progress {
        Progress::default()
    }
}

#[derive(Default, Clone)]
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
    pub sent_all_data: bool
}

pub struct QueryResponse {
    pub input_stream: SendableDataBlockStream,
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

#[derive(Clone)]
pub struct CHContext {
    pub state: QueryState,
    pub session: Arc<dyn ClickHouseSession>
}

impl CHContext {
    fn new(state: QueryState, session: Arc<dyn ClickHouseSession>) -> Self {
        Self { state, session }
    }
}

/// A server that speaks the ClickHouseprotocol, and can delegate client commands to a backend
/// that implements [`ClickHouseSession`]
pub struct ClickHouseServer {}

impl ClickHouseServer {
    pub async fn run_on_stream(
        session: Arc<dyn ClickHouseSession>,
        stream: TcpStream
    ) -> Result<()> {
        ClickHouseServer::run_on(session, stream.into()).await
    }
}

impl ClickHouseServer {
    async fn run_on(session: Arc<dyn ClickHouseSession>, stream: Stream) -> Result<()> {
        let mut srv = ClickHouseServer {};
        srv.run(session, stream).await?;
        Ok(())
    }

    async fn run(&mut self, session: Arc<dyn ClickHouseSession>, stream: Stream) -> Result<()> {
        debug!("Handle New session");
        let tz: Tz = session.timezone().parse()?;
        let ctx = CHContext::new(QueryState::default(), session);
        let mut transport = ClickhouseTransport::new(ctx, stream, tz);

        while let Some(ret) = transport.next().await {
            match ret {
                Err(e) => {
                    error!("Error in {:?}", e);
                    break;
                }
                _ => {}
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
