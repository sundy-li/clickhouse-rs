use std::{env, io::Write, net, thread};

use clickhouse_srv::types::ResultWriter;
use clickhouse_srv::{errors::Result, types::Block, ClickHouseServer};
use log::info;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

extern crate clickhouse_srv;

fn main() {
    env::set_var("RUST_LOG", "clickhouse_srv=debug");
    env_logger::init();

    let mut threads = Vec::new();
    let host_port = "127.0.0.1:9000";
    let listener = net::TcpListener::bind(host_port).unwrap();

    info!("Server start at {}", host_port);
    while let Ok((s, _)) = listener.accept() {
        threads.push(thread::spawn(move || {
            let s = ClickHouseServer::run_on_tcp(Session {}, s);
            match s {
                Err(e) => println!("{}", e.to_string()),
                _ => {}
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }
}

struct Session {}

impl<W: Write> clickhouse_srv::ClickHouseSession<W> for Session {
    fn execute_query(&self, query: &str, stage: u64, writer: &mut ResultWriter) -> Result<()> {
        info!("Receive query {}", query);
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
