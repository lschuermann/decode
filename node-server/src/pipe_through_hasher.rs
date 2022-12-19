use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use sha3::digest::generic_array::GenericArray;
use sha3::{Digest, Sha3_256};

pub struct PipeThroughHasher<W: AsyncWrite + std::marker::Unpin> {
    hasher: Sha3_256,
    writer: W,
    write_error: Option<IOErrorKind>,
    shutdown: bool,
}

impl<W: AsyncWrite + std::marker::Unpin> PipeThroughHasher<W> {
    pub fn new(writer: W) -> Self {
        PipeThroughHasher {
            hasher: Sha3_256::new(),
            writer,
            write_error: None,
            shutdown: false,
        }
    }

    pub fn get_digest(self) -> Result<[u8; 32], IOErrorKind> {
        if let Some(err) = self.write_error {
            Err(err)
        } else {
            let mut digest = [0; 32];
            self.hasher
                .finalize_into(GenericArray::from_mut_slice(&mut digest[..]));
            Ok(digest)
        }
    }
}

impl<W: AsyncWrite + std::marker::Unpin> AsyncWrite for PipeThroughHasher<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, IOError>> {
        if self.shutdown {
            return Poll::Ready(Err(IOErrorKind::BrokenPipe.into()));
        }

        // Perform the poll
        let poll_res = Pin::new(&mut self.writer).poll_write(cx, buf);

        // Depending on the result, either mark the operation as
        // failed or consume the returned number of bytes and hash
        // them:
        if let Poll::Ready(res) = &poll_res {
            // We did something! Check whether we've written some
            // bytes or encountered an error.
            match res {
                Ok(bytes) => {
                    // We've written some bytes, also pass them into
                    // the hasher:
                    self.hasher.update(&buf[..*bytes]);
                }
                Err(err) => {
                    self.write_error = Some(err.kind());
                }
            }
        }

        poll_res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IOError>> {
        if self.shutdown {
            return Poll::Ready(Err(IOErrorKind::BrokenPipe.into()));
        }

        let poll_res = Pin::new(&mut self.writer).poll_flush(cx);

        if let Poll::Ready(res) = &poll_res {
            if let Err(err) = res {
                self.write_error = Some(err.kind());
            }
        }

        poll_res
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), IOError>> {
        if self.shutdown {
            return Poll::Ready(Err(IOErrorKind::BrokenPipe.into()));
        }

        let poll_res = Pin::new(&mut self.writer).poll_shutdown(cx);

        if let Poll::Ready(res) = &poll_res {
            match res {
                Ok(()) => self.shutdown = true,
                Err(err) => {
                    self.write_error = Some(err.kind());
                }
            }
        }

        poll_res
    }
}
