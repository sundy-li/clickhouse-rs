use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use pin_project::pin_project;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_native_tls::TlsStream;

#[cfg(all(feature = "tls"))]
type SecureTcpStream = TlsStream<TcpStream>;

#[pin_project(project = StreamProj)]
pub enum Stream {
    Plain(#[pin] TcpStream),
    #[cfg(feature = "tls")]
    Secure(#[pin] SecureTcpStream)
}

impl From<TcpStream> for Stream {
    fn from(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }
}

#[cfg(feature = "tls")]
impl From<SecureTcpStream> for Stream {
    fn from(stream: SecureTcpStream) -> Stream {
        Self::Secure(stream)
    }
}

impl Stream {
    pub(crate) fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8]
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);

        let result = match self.project() {
            StreamProj::Plain(stream) => stream.poll_read(cx, &mut read_buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_read(cx, &mut read_buf)
        };

        match result {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(x)) => Poll::Ready(Err(x)),
            Poll::Pending => Poll::Pending
        }
    }

    pub(crate) fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8]
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Plain(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            StreamProj::Secure(stream) => stream.poll_write(cx, buf)
        }
    }
}
