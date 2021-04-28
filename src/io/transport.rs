use std::io::Cursor;
use std::pin::Pin;
use std::ptr;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use std::task::{self};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use chrono_tz::Tz;
use futures::executor::block_on;
use futures::stream::Stream;
use futures::StreamExt;
use log::debug;
use pin_project::pin_project;

use crate::binary::Encoder;
use crate::binary::Parser;
use crate::errors::DriverError;
use crate::errors::Error;
use crate::errors::Result;
use crate::io::read_to_end::read_buf;
use crate::io::Stream as InnerStream;
use crate::protocols::ExceptionResponse;
use crate::protocols::HelloRequest;
use crate::protocols::HelloResponse;
use crate::protocols::Packet;
use crate::protocols::SERVER_END_OF_STREAM;
use crate::protocols::SERVER_PONG;
use crate::CHContext;

const INTERACTIVE_DALAY: Duration = Duration::from_millis(10);

/// Line transport
#[pin_project(project = ClickhouseTransportProj)]
pub struct ClickhouseTransport {
    // Inner socket
    #[pin]
    inner: InnerStream,
    ctx: CHContext,
    // Set to true when inner.read returns Ok(0);
    done: bool,
    // Buffered read data
    rd: Vec<u8>,
    // Whether the buffer is known to be incomplete
    buf_is_incomplete: bool,
    // Current buffer to write to the socket
    wr_stream: Option<Receiver<Result<Vec<u8>>>>,
    // Server time zone
    timezone: Tz,
    // Whether there are unread packets
    pub(crate) inconsistent: bool,

    hello: Option<HelloRequest>,
    client_revision: u64,

    send_progress_time: Arc<Mutex<Instant>>,
}

impl ClickhouseTransport {
    pub fn new(ctx: CHContext, inner: InnerStream, timezone: Tz) -> Self {
        ClickhouseTransport {
            inner,
            ctx,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr_stream: None,
            timezone,
            inconsistent: false,
            hello: None,
            client_revision: 0,

            send_progress_time: Arc::new(Mutex::new(Instant::now())),
        }
    }
}

impl<'p> ClickhouseTransportProj<'p> {
    fn wr_flush(&mut self, cx: &mut task::Context) -> Poll<Option<Result<()>>> {
        if let Some(stream) = &*self.wr_stream {
            for item in stream {
                match item {
                    Ok(v) => {
                        let size = v.len();
                        loop {
                            let res = self.inner.as_mut().poll_write(cx, &v);
                            match res {
                                Poll::Ready(Ok(n)) if n == size => {
                                    break;
                                }
                                Poll::Ready(Err(e)) => {
                                    return Poll::Ready(Some(Err(e.into())));
                                }
                                _ => continue
                            }
                        }
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e.into())));
                    }
                }
            }
        }
        Poll::Ready(Some(Ok(())))
    }

    fn try_parse_msg(&mut self) -> Result<Packet> {
        let pos;

        let mut cursor = Cursor::new(&self.rd);
        let ret = {
            let mut parser = Parser::new(&mut cursor, *self.timezone);
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

    // return Ok(true) if it's totally consumed
    fn response(&mut self, packet: Packet) -> Result<bool> {
        self.ctx.state.reset();
        let (sender, receiver) = channel();
        *self.wr_stream = Some(receiver);

        let mut encoder = Encoder::new();
        debug!("Got packet {:?}", packet);
        let ret = match packet {
            Packet::Ping => {
                encoder.uvarint(SERVER_PONG);
                true
            }
            // todo cancel
            Packet::Cancel => true,
            Packet::Hello(mut hello) => {
                let response = HelloResponse {
                    dbms_name: self.ctx.session.dbms_name().to_string(),
                    dbms_version_major: self.ctx.session.dbms_version_major(),
                    dbms_version_minor: self.ctx.session.dbms_version_minor(),
                    dbms_tcp_protocol_version: self.ctx.session.dbms_tcp_protocol_version(),
                    timezone: self.ctx.session.timezone().to_string(),
                    server_display_name: self.ctx.session.server_display_name().to_string(),
                    dbms_version_patch: self.ctx.session.dbms_version_patch(),
                };

                hello.client_revision = self
                    .ctx
                    .session
                    .dbms_tcp_protocol_version()
                    .min(hello.client_revision);
                *self.client_revision = hello.client_revision;
                *self.hello = Some(hello);

                response.encode(&mut encoder, *self.client_revision)?;
                true
            }
            Packet::Query(query) => {
                self.ctx.state.query = query.query;
                self.ctx.state.stage = query.stage;
                self.ctx.state.compression = query.compression;

                // TODO, if it's not insert query, we should discard the remaining rd
                self.rd.clear();

                let compress = self.ctx.state.compression > 0;
                let client_revision = *self.client_revision;
                let with_stack_trace = self.ctx.session.with_stack_trace();
                let sender = sender.clone();
                let ctx = self.ctx.clone();
                let send_progress_time = self.send_progress_time.clone();

                thread::spawn(move || {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_io()
                        .worker_threads(4)
                        .build().unwrap()
                        .block_on(async move {
                        let mut encoder = Encoder::new();
                        match ctx.session.execute_query(&ctx.state).await {
                            Err(err) => {
                                ExceptionResponse::write(
                                    &mut encoder,
                                    &err,
                                    with_stack_trace,
                                );
                                encoder.uvarint(SERVER_END_OF_STREAM);
                            }
                            Ok(mut response) => {
                                // async process blocks and progress
                                while let Some(block) = response.input_stream.next().await {
                                    let mut encoder = Encoder::new();
                                    match block {
                                        Ok(block) => {
                                            if send_progress_time.lock().unwrap().elapsed()
                                                >= INTERACTIVE_DALAY
                                            {
                                                ctx.session
                                                    .get_progress()
                                                    .write(&mut encoder, client_revision);
                                                *send_progress_time.lock().unwrap() = Instant::now();
                                            }
                                            block.send_server_data(&mut encoder, compress);
                                        }
                                        Err(err) => {
                                            ExceptionResponse::write(
                                                &mut encoder,
                                                &Error::from(err),
                                                with_stack_trace,
                                            );
                                            encoder.uvarint(SERVER_END_OF_STREAM);
                                        }
                                    }
                                    sender.send(Ok(encoder.get_buffer())).ok();
                                }
                                let mut encoder = Encoder::new();
                                ctx.session
                                    .get_progress()
                                    .write(&mut encoder, client_revision);
                                encoder.uvarint(SERVER_END_OF_STREAM);
                                sender.send(Ok(encoder.get_buffer())).ok();
                            }
                        }
                    })
                });
                true
            }
            Packet::Data(_) => {
                //TODO inserts
                true
            }
        };

        let bytes = encoder.get_buffer();
        thread::spawn(move || {
            sender.send(Ok(bytes)).ok();
        });

        Ok(ret)
    }

    fn response_error_packet(&mut self, err: Error) -> Result<()> {
        let (sender, receiver) = channel();
        *self.wr_stream = Some(receiver);

        let mut encoder = Encoder::new();
        match err {
            Error::Driver(DriverError::UnknownPacket { packet }) => {
                if packet == 'G' as u64 || packet == 'P' as u64 {
                    encoder
                        .string("HTTP/1.0 400 Bad Request, maybe you are using http port\r\n\r\n");
                    let bytes = encoder.get_buffer();
                    thread::spawn(move || {
                        sender.send(Ok(bytes)).ok();
                    });
                    return Err("HTTP/1.0 400 Bad Request got".into());
                }
                debug!("Error in packets {:?}", err);
            }
            _ => {}
        }

        Ok(())
    }

    fn send_error(&mut self, e: Error) -> Result<()> {
        let mut encoder = Encoder::new();
        ExceptionResponse::write(&mut encoder, &e, self.ctx.session.with_stack_trace());
        encoder.uvarint(SERVER_END_OF_STREAM);
        Ok(())
    }
}

impl Stream for ClickhouseTransport {
    type Item = Result<()>;

    /// Read a message from the `Transport`
    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match read_buf(this.inner.as_mut(), cx, &mut this.rd) {
                Poll::Pending => {
                    if !this.rd.is_empty() {
                        let packet = this.try_parse_msg();
                        match packet {
                            Err(e) => {
                                if e.is_would_block() {
                                    continue;
                                } else {
                                    match this.response_error_packet(e) {
                                        Err(e) => {
                                            let _ = this.wr_flush(cx);
                                            return Poll::Ready(Some(Err(e)));
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Ok(packet) => {
                                match this.response(packet) {
                                    Err(e) => {
                                        //send exceptions
                                        this.ctx.state.stage = 0;
                                        this.rd.clear();
                                        let _ = this.send_error(e);
                                    }
                                    Ok(false) => continue,
                                    _ => {}
                                }
                            }
                        }
                        break;
                    }
                }
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        break;
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
            }
        }

        this.wr_flush(cx)
    }
}
