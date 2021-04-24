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

use pin_project::pin_project;

use crate::{
    binary::Parser,
    errors::{DriverError, Error, Result},
    io::{read_to_end::read_to_end, Stream as InnerStream},
    protocols::Packet,
};

/// Line transport
#[pin_project(project = ClickhouseTransportProj)]
pub struct ClickhouseTransport {
    // Inner socket
    #[pin]
    inner: InnerStream,
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
    state: TransportState,
}

impl ClickhouseTransport {
    pub fn new(inner: InnerStream, timezone: Tz) -> Self {
        ClickhouseTransport {
            inner,
            done: false,
            rd: vec![],
            buf_is_incomplete: false,
            wr: io::Cursor::new(vec![]),
            timezone: timezone,
            inconsistent: false,
            state: TransportState::Receive,
        }
    }
}

enum TransportState {
    Ask,
    Receive,
    Yield(Box<Option<Packet>>),
    Done,
}

impl<'p> ClickhouseTransportProj<'p> {
    fn try_parse_msg(&mut self) -> Poll<Option<Result<Packet>>> {
        let pos;
        let ret = {
            let mut cursor = Cursor::new(&self.rd);
            let res = {
                let mut parser = Parser::new(&mut cursor, *self.timezone, None, None);
                parser.parse_packet()
            };
            pos = cursor.position() as usize;

            if let Ok(Packet::Hello(ref packet)) = res {}

            match res {
                Ok(val) => Poll::Ready(Some(Ok(val))),
                Err(e) => {
                    if e.is_would_block() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(Err(e.into())))
                    }
                }
            }
        };

        match ret {
            Poll::Pending => (),
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
}

impl ClickhouseTransport {
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

                trace!("writing; remaining={:?}", buf);
                buf
            };

            Pin::new(&mut self.inner).poll_write(cx, buf)
        };

        match res {
            Poll::Ready(Ok(mut n)) => {
                n += self.wr.position() as usize;
                self.wr.set_position(n as u64);
                Ok(true)
            }
            Poll::Ready(Err(e)) => {
                trace!("transport flush error; err={:?}", e);
                Err(e)
            }
            Poll::Pending => Ok(false),
        }
    }

    async fn flush(&mut self, bytes: Vec<u8>) -> Result<()> {
        Ok(())
    }
}

impl Stream for ClickhouseTransport {
    type Item = Result<Packet>;

    /// Read a message from the `Transport`
    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // Check whether our currently buffered data is enough for a packet
        // before reading any more data. This prevents the buffer from growing
        // indefinitely when the sender is faster than we can consume the data
        if !*this.buf_is_incomplete && !this.rd.is_empty() {
            if let Poll::Ready(ret) = this.try_parse_msg()? {
                return Poll::Ready(ret.map(Ok));
            }
        }

        *this.done = false;
        // Fill the buffer!
        while !*this.done {
            match read_to_end(this.inner.as_mut(), cx, &mut this.rd) {
                Poll::Ready(Ok(0)) => {
                    *this.done = true;
                    break;
                }
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => break,
            }
        }

        // Try to parse the new data!
        let ret = this.try_parse_msg();

        *this.buf_is_incomplete = matches!(ret, Poll::Pending);

        ret
    }
}
