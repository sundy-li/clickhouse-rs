use std::env;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clickhouse_srv::errors::Result;
use clickhouse_srv::types::Block;
use clickhouse_srv::types::Progress;
use clickhouse_srv::ClickHouseServer;
use clickhouse_srv::QueryResponse;
use clickhouse_srv::QueryState;
use futures::task::Context;
use futures::task::Poll;
use futures::Stream;
use log::info;
use tokio::net::TcpListener;

extern crate clickhouse_srv;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "clickhouse_srv=debug");
    env_logger::init();
    let host_port = "127.0.0.1:9000";

    // Note that this is the Tokio TcpListener, which is fully async.
    let listener = TcpListener::bind(host_port).await?;

    info!("Server start at {}", host_port);

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Spawn our handler to be run asynchronously.
        tokio::spawn(async move {
            if let Err(e) = ClickHouseServer::run_on_stream(Arc::new(Session {}), stream).await {
                info!("Error: {:?}", e);
            }
        });
    }
}

struct Session {}

impl clickhouse_srv::ClickHouseSession for Session {
    fn execute_query(&self, state: &mut QueryState) -> Result<QueryResponse> {
        let query = state.query.clone();
        info!("Receive query {}", query);
        if query.starts_with("insert") {
            return Err("INSERT is not supported currently".into());
        }

        Ok(QueryResponse {
            input_stream: Some(Box::pin(SimpleBlockStream {
                idx: 0,
                start: 10,
                end: 24,
                blocks: 10,
            })),
        })
    }

    fn dbms_name(&self) -> &str {
        "ClickHouse-X"
    }

    fn dbms_version_major(&self) -> u64 {
        2021
    }

    fn dbms_version_minor(&self) -> u64 {
        5
    }

    // the MIN_SERVER_REVISION for suggestions is 54406
    fn dbms_tcp_protocol_version(&self) -> u64 {
        54405
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    fn server_display_name(&self) -> &str {
        "ClickHouse-X"
    }

    fn dbms_version_patch(&self) -> u64 {
        0
    }

    fn get_progress(&self) -> Progress {
        Progress {
            rows: 100 ,
            bytes: 1000,
            total_rows: 1000,
        }
    }
}

struct SimpleBlockStream {
    idx: u32,
    start: u32,
    end: u32,
    blocks: u32,
}

impl Stream for SimpleBlockStream {
    type Item = anyhow::Result<Block>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        self.idx += 1;
        if self.idx > self.blocks {
            return Poll::Ready(None);
        }
        let block = Some(Block::new().column("abc", (self.start..self.end).collect::<Vec<u32>>()));

        thread::sleep(Duration::from_millis(100));
        Poll::Ready(block.map(Ok))
    }
}
