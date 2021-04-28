use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures_util::ready;

use crate::io::Stream as InnerStream;

struct Guard<'a> {
    buf: &'a mut Vec<u8>,
    len: usize
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        unsafe {
            self.buf.set_len(self.len);
        }
    }
}

pub fn read_buf(
    mut rd: Pin<&mut InnerStream>,
    cx: &mut Context<'_>,
    buf: &mut Vec<u8>
) -> Poll<io::Result<usize>> {
    let start_len = buf.len();
    let mut g = Guard {
        len: buf.len(),
        buf
    };

    if g.len == g.buf.len() {
        unsafe {
            g.buf.reserve(32);
            let capacity = g.buf.capacity();
            g.buf.set_len(capacity);
        }
    }

    match ready!(rd.as_mut().poll_read(cx, &mut g.buf[g.len..])) {
        Ok(n) => {
            g.len += n;
            Poll::Ready(Ok(g.len - start_len))
        }
        Err(e) => Poll::Ready(Err(e))
    }
}

#[allow(dead_code)]
pub(crate) fn read_to_end(
    mut rd: Pin<&mut InnerStream>,
    cx: &mut Context<'_>,
    buf: &mut Vec<u8>
) -> Poll<io::Result<usize>> {
    let start_len = buf.len();
    let mut g = Guard {
        len: buf.len(),
        buf
    };
    let ret;
    loop {
        if g.len == g.buf.len() {
            unsafe {
                g.buf.reserve(32);
                let capacity = g.buf.capacity();
                g.buf.set_len(capacity);
            }
        }

        match ready!(rd.as_mut().poll_read(cx, &mut g.buf[g.len..])) {
            Ok(0) => {
                ret = Poll::Ready(Ok(g.len - start_len));
                break;
            }
            Ok(n) => {
                g.len += n;
            }
            Err(e) => {
                ret = Poll::Ready(Err(e));
                break;
            }
        }
    }

    ret
}
