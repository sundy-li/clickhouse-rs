use log::debug;

use crate::binary::Encoder;
use crate::connection::Connection;
use crate::errors::Result;
use crate::protocols::HelloResponse;
use crate::protocols::Packet;
use crate::protocols::SERVER_PONG;
use crate::CHContext;

pub struct Cmd {
    packet: Packet
}

impl Cmd {
    pub fn create(packet: Packet) -> Self {
        Self { packet }
    }

    pub async fn apply(&mut self, connection: &mut Connection, ctx: &mut CHContext) -> Result<()> {
        ctx.state.reset();
        debug!("Got packet {:?}", self.packet);

        let mut encoder = Encoder::new();
        match &mut self.packet {
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

                hello.client_revision = connection
                    .session
                    .dbms_tcp_protocol_version()
                    .min(hello.client_revision);

                ctx.client_revision = hello.client_revision;
                ctx.hello = Some(hello.clone());

                response.encode(&mut encoder, ctx.client_revision)?;
            }
            Packet::Query(query) => {
                ctx.state.query = query.query.clone();
                ctx.state.stage = query.stage;
                ctx.state.compression = query.compression;

                let session = connection.session.clone();
                // connection.buffer.clear();

                session.execute_query(ctx, connection).await?;
                connection.write_end_of_stream().await?;
            }
            Packet::Data(_) => {
                //TODO inserts
            }
        };

        let bytes = encoder.get_buffer();
        if !bytes.is_empty() {
            connection.write_bytes(bytes).await?;
        }
        Ok(())
    }
}
