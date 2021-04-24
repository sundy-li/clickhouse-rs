use std::{env, error::Error, net, thread};
use tokio::net::{TcpListener, TcpStream};

use clickhouse_srv::types::ResultWriter;
use clickhouse_srv::{errors::Result, types::Block, ClickHouseServer};
use log::info;

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
            if let Err(e) = ClickHouseServer::run_on_stream(Session {}, stream).await {
                info!("Error: {}", e);
            }
        });
    }
}

struct Session {}

impl clickhouse_srv::ClickHouseSession for Session {
    fn execute_query(&self, query: &str, _stage: u64, writer: &mut ResultWriter) -> Result<()> {
        info!("Receive query {}", query);
        if query.starts_with("insert") {
            return Err("INSERT is not supported currently".into());
        }
        let block = Block::new().column("abc", (1i32..1000).collect::<Vec<i32>>());
        writer.write_block(block)?;
        Ok(())
    }

    fn dbms_name(&self) -> &str {
        "ClickHouse-X"
    }

    fn server_display_name(&self) -> &str {
        "ClickHouse-X"
    }
}
