use log::debug;

use crate::binary::Encoder;
use crate::connection::Connection;
use crate::errors::Result;
use crate::protocols::HelloResponse;
use crate::protocols::Packet;
use crate::protocols::Stage;
use crate::protocols::SERVER_PONG;
use crate::CHContext;

pub struct Cmd {
    packet: Packet
}

impl Cmd {
    pub fn create(packet: Packet) -> Self {
        Self { packet }
    }

    pub async fn apply(self, connection: &mut Connection, ctx: &mut CHContext) -> Result<()> {
        let mut encoder = Encoder::new();
        match self.packet {
            Packet::Ping => {
                encoder.uvarint(SERVER_PONG);
            }
            // todo cancel
            Packet::Cancel => {}
            Packet::Hello(hello) => {
                let response = HelloResponse {
                    dbms_name: connection.session.dbms_name().to_string(),
                    dbms_version_major: connection.session.dbms_version_major(),
                    dbms_version_minor: connection.session.dbms_version_minor(),
                    dbms_tcp_protocol_version: connection.session.dbms_tcp_protocol_version(),
                    timezone: connection.session.timezone().to_string(),
                    server_display_name: connection.session.server_display_name().to_string(),
                    dbms_version_patch: connection.session.dbms_version_patch()
                };

                ctx.client_revision = connection
                    .session
                    .dbms_tcp_protocol_version()
                    .min(hello.client_revision);
                ctx.hello = Some(hello.clone());

                response.encode(&mut encoder, ctx.client_revision)?;
            }
            Packet::Query(query) => {
                ctx.state.query = query.query.clone();
                ctx.state.compression = query.compression;

                let session = connection.session.clone();
                session.execute_query(ctx, connection).await?;

                if let Some(_) = &ctx.state.out {
                    ctx.state.stage = Stage::InsertPrepare;
                } else {
                    connection.write_end_of_stream().await?;
                }
            }
            Packet::Data(block) => {
                if block.is_empty() {
                    match ctx.state.stage {
                        Stage::InsertPrepare => {
                            ctx.state.stage = Stage::InsertStart;
                        }
                        Stage::InsertStart => {
                            ctx.state.stage = Stage::Default;
                            // that mean all blocks has been sent
                            ctx.state.reset();
                            debug!("finish insert");
                            connection.write_end_of_stream().await?;
                        }
                        _ => {}
                    }
                } else {
                    if let Some(out) = &ctx.state.out {
                        // out.block_stream.
                        out.send(block).await.unwrap();
                    }
                }
            }
        };

        let bytes = encoder.get_buffer();
        if !bytes.is_empty() {
            connection.write_bytes(bytes).await?;
        }
        Ok(())
    }
}
