use std::future::Future;
use std::io;

use monoio::io::AsyncReadRent;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

const BUF_SIZE: usize = 8192;

pub trait AsyncReadRentSkip: AsyncReadRent {
    /// Skip n bytes in the stream.
    fn skip(&mut self, nbytes: usize) -> impl Future<Output = VortexResult<()>>;
}

impl<R: AsyncReadRent> AsyncReadRentSkip for R {
    async fn skip(&mut self, nbytes: usize) -> VortexResult<()> {
        if nbytes < BUF_SIZE {
            let buf = Vec::with_capacity(nbytes);
            let (res_len, _) = self.read(buf).await;
            return res_len.map(|_| ()).map_err(|e| vortex_err!(IOError: e))
        }

        let mut buf: Vec<u8> = Vec::with_capacity(BUF_SIZE);
        let mut remaining: usize = nbytes;

        while remaining >= BUF_SIZE {
            buf.clear();
            let (read_res, buf_read) = self.read(buf).await;
            match read_res {
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                    // retry
                    buf = buf_read;
                    continue;
                }
                Err(e) => {
                    // should return error
                    vortex_bail!(IOError: e);
                }
                Ok(n) => {
                    remaining -= n;
                    buf = buf_read;
                }
            }
        }

        if remaining > 0 {
            let buf = Vec::with_capacity(remaining);
            let (res_len, _) = self.read(buf).await;
            return res_len.map(|_| ()).map_err(|e| vortex_err!(IOError: e))
        }

        Ok(())
    }
}
