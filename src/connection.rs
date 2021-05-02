use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::protocols::{Packet, SERVER_END_OF_STREAM, ExceptionResponse};
use crate::binary::{Parser, Encoder};
use chrono_tz::Tz;
use crate::types::{Marshal, StatBuffer, Block, Progress};
use crate::binary;
use crate::errors::{Result, Error};

/// Send and receive `Packet` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    pub buffer: BytesMut,
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    tz: Tz,
    with_stack_trace: bool,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(stream: TcpStream, tz: Tz) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
            tz,
            with_stack_trace: false,
        }
    }

    /// Read a single `Packet` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_packet`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_packet(&mut self) -> crate::Result<Option<Packet>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_packet()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_packet(&mut self) -> crate::Result<Option<Packet>> {
        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);
        let mut parser = Parser::new(&mut buf, self.tz);

        let hello = None;
        let packet = parser.parse_packet(&hello, true);

        match packet {
            Ok(packet) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Packet::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;
                buf.set_position(0);
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(packet))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(err) if err.is_would_block() => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }


    pub async fn write_block(&mut self, block: Block) -> Result<()> {
        let mut encoder = Encoder::new();
        block.send_server_data(&mut encoder, true);
        self.stream.write_all(&encoder.get_buffer()).await?;
        Ok(())
    }

    pub async fn write_progress(&mut self, progress: Progress, client_revision: u64) -> Result<()> {
        let mut encoder = Encoder::new();
        progress.write(&mut encoder, client_revision);
        self.stream.write_all(&encoder.get_buffer()).await?;
        Ok(())
    }

    pub async fn write_end_of_stream(&mut self) -> Result<()> {
        let mut encoder = Encoder::new();
        encoder.uvarint(SERVER_END_OF_STREAM);
        self.stream.write_all(&encoder.get_buffer()).await?;
        Ok(())
    }

    pub async fn write_error(&mut self, err: Error) -> Result<()> {
        let mut encoder = Encoder::new();
        ExceptionResponse::write(
            &mut encoder,
            &err,
            self.with_stack_trace,
        );

        self.stream.write_all(&encoder.get_buffer()).await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, bytes: Vec<u8> ) -> Result<()> {
        println!("Write: {:?}", bytes);
        self.stream.write_all(&bytes).await?;
        Ok(())
    }
}