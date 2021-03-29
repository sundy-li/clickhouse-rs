use std::{io::Write, net, thread};

use clickhouse_srv::{errors::Result, types::Block, ClickHouseServer, SendableBlockStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use clickhouse_srv::types::ResultWriter;

extern crate clickhouse_srv;

fn main() {
    let mut threads = Vec::new();
    let host_port = "127.0.0.1:9000";
    let listener = net::TcpListener::bind(host_port).unwrap();

    println!("Server start at {}", host_port);
    while let Ok((s, _)) = listener.accept() {
        threads.push(thread::spawn(move || {
            ClickHouseServer::run_on_tcp(Session {}, s).unwrap();
        }));
    }

    for t in threads {
        t.join().unwrap();
    }
}

struct Session {}

impl<W: Write> clickhouse_srv::ClickHouseSession<W> for Session {
    fn execute_query(&self, query: &str, stage: u64, writer: &mut ResultWriter) -> Result<()> {
        let block = Block::new().column("abc", vec![1u32, 2, 3, 4]);
        writer.write_block(block)?;
        writer.finalize()?;
        Ok(())
    }

    fn dbms_name(&self) -> &str {
        "Datafuse"
    }

    fn server_display_name(&self) -> &str {
        "Datafuse"
    }
}
