use std::{
    io::{self, Cursor},
    pin::Pin,
    ptr,
    task::{self, Poll},
};

use chrono_tz::Tz;
use futures::stream::Stream;
use futures::StreamExt;

use log::trace;
use log::debug;

use pin_project::pin_project;

use crate::{binary::Parser, errors::{DriverError, Error, Result}, io::{read_to_end::read_to_end, Stream as InnerStream}, protocols::Packet, protocols, ClickHouseSession, CHContext};
use crate::binary::Encoder;
use std::io::Write;
use crate::protocols::{SERVER_PONG, HelloResponse, HelloRequest, ExceptionResponse, SERVER_END_OF_STREAM};
use crate::types::ResultWriter;

/// Line transport
#[pin_project(project = ClickhouseTransportProj)]
pub struct ClickhouseTransport<S> {
    // Inner socket
    #[pin]
    inner: InnerStream,
    ctx: CHContext<S>,
    // Set to true when inner.read returns Ok(0);
    done: bool,
    // Buffered read data
    rd: Vec<u8>,
    // Whether the buffer is known to be incomplete
    buf_is_incomplete: bool,
    // Current buffer to write to the socket
    wr: io::Cursor<Vec<u8>>,
    // Server time zone
    timezone: Tz,
    // Whether there are unread packets
    pub(crate) inconsistent: bool,

    hello: Option<HelloRequest>,
}

impl<S: ClickHouseSession> ClickhouseTransport<S> {
    pub fn new(ctx: CHContext<S>, inner: InnerStream, timezone: Tz) -> Self {
        ClickhouseTransport {
            inner,
            ctx,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr: io::Cursor::new(vec![]),
            timezone,
            inconsistent: false,
            hello: None,
        }
    }
}

impl<'p, S: ClickHouseSession> ClickhouseTransportProj<'p, S> {
    fn wr_is_empty(&self) -> bool {
        self.wr_remaining() == 0
    }

    fn wr_remaining(&self) -> usize {
        self.wr.get_ref().len() - self.wr_pos()
    }

    fn wr_pos(&self) -> usize {
        self.wr.position() as usize
    }

    fn wr_flush(&mut self, cx: &mut task::Context) -> io::Result<bool> {
        // Making the borrow checker happy
        let res = {
            let buf = {
                let pos = self.wr.position() as usize;
                let buf = &self.wr.get_ref()[pos..];

                debug!("writing; remaining size: {}  = {:?}", buf.len(), buf);
                buf
            };

            self.inner.as_mut().poll_write(cx, buf)
        };

        match res {
            Poll::Ready(Ok(mut n)) => {
                debug!("transport flush {:?}", n);

                n += self.wr.position() as usize;
                self.wr.set_position(n as u64);
                Ok(true)
            }
            Poll::Ready(Err(e)) => {
                debug!("transport flush error; err={:?}", e);
                Err(e)
            }
            Poll::Pending => Ok(false),
        }
    }

    fn try_parse_msg(&mut self) -> Result<Packet> {
        let pos;

        let mut cursor = Cursor::new(&self.rd);
        let ret = {
            let mut parser = Parser::new(&mut cursor, self.timezone.clone());
            parser.parse_packet(self.hello, self.ctx.state.compression > 0)
        };
        pos = cursor.position() as usize;

        match ret {
            Err(ref e) if e.is_would_block() => {}
            _ => {
                // Data is consumed
                let new_len = self.rd.len() - pos;
                unsafe {
                    ptr::copy(self.rd.as_ptr().add(pos), self.rd.as_mut_ptr(), new_len);
                    self.rd.set_len(new_len);
                }
            }
        }
        ret
    }

    fn response(&mut self, packet: Packet) -> Result<()> {
        let mut encoder = Encoder::new();
        debug!("Got packet {:?}", packet);
        match packet {
            Packet::Ping => {
                encoder.uvarint(protocols::SERVER_PONG);
            }
            // todo cancel
            Packet::Cancel => {
            }
            Packet::Hello(hello) => {
                let response = HelloResponse {
                    dbms_name: self.ctx.session.dbms_name().to_string(),
                    dbms_version_major: self.ctx.session.dbms_version_major(),
                    dbms_version_minor: self.ctx.session.dbms_version_minor(),
                    dbms_tcp_protocol_version: self.ctx.session.dbms_tcp_protocol_version(),
                    timezone: self.ctx.session.timezone().to_string(),
                    server_display_name: self.ctx.session.server_display_name().to_string(),
                    dbms_version_patch: self.ctx.session.dbms_version_patch(),
                };

                response.encode(&mut encoder, hello.client_revision.clone())?;

                *self.hello = Some(hello);
            }
            Packet::Query(query) => {
                self.ctx.state.query = query.query;
                self.ctx.state.stage = query.stage;
                self.ctx.state.compression = query.compression;

                let mut result_writer = ResultWriter::new(&mut encoder, self.ctx.state.compression > 0);

                if let Err(e) = self.ctx.session.execute_query(
                    &self.ctx.state.query,
                    self.ctx.state.stage,
                    &mut result_writer,
                ) {
                    ExceptionResponse::encode(&mut encoder, &e, self.ctx.session.with_stack_trace())?
                }

                encoder.uvarint(SERVER_END_OF_STREAM);
            }
            Packet::Data(_) => {
                //TODO inserts
            }
        }
        let bytes = encoder.get_buffer();
        debug!("write packets: {}", bytes.len());
        *self.wr = Cursor::new(bytes);
        Ok(())
    }

    fn response_error(&mut self, err: Error) -> Result<()>{
        let mut encoder = Encoder::new();
        match err {
            Error::Driver(DriverError::UnknownPacket { packet }) => {
                if packet == 'G' as u64 || packet == 'P' as u64 {
                    encoder.string("HTTP/1.0 400 Bad Request\r\n\r\n");
                    let bytes = encoder.get_buffer();
                    *self.wr = Cursor::new(bytes);
                    return Ok(());
                }
                debug!("Error in packets {:?}", err);
            },
            _ => {},
        }

        Ok(())
    }
}

impl<S: ClickHouseSession> Stream for ClickhouseTransport<S> {
    type Item = Result<()>;

    /// Read a message from the `Transport`
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if !this.wr_is_empty() {
            this.wr_flush(cx);
        }

        loop {
            match read_to_end(this.inner.as_mut(), cx, &mut this.rd) {
                Poll::Pending => {
                    let packet = this.try_parse_msg();
                    match packet {
                        Err(e) => {
                            if e.is_would_block() {
                                return Poll::Pending;
                            } else {
                                match this.response_error(e) {
                                    Err(f) => return Poll::Ready(Some(Err(f.into()))),
                                    _ => {
                                        break;
                                    },
                                }
                            }
                        },
                        Ok(packet) => {
                            match this.response(packet) {
                                Err(f) => return Poll::Ready(Some(Err(f.into()))),
                                _ => {
                                },
                            }
                        },
                    }
                    break;
                }
                Poll::Ready(Ok(n)) => {
                    debug!("Read {} bytes", n);

                    if n == 0 {
                        break;
                        // return Poll::Ready(Some(Ok(())));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
            }
        }

        if !this.wr_is_empty() {
            this.wr_flush(cx);
        }
        Poll::Pending
    }
}
