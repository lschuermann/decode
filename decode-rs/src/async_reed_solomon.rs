use std::borrow::BorrowMut;
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use reed_solomon_erasure::{galois_8::Field as Galois8Field, Error as RSError, ReedSolomon};

use crate::transposed_slices;

#[derive(Clone, Debug)]
pub enum AsyncReedSolomonError {
    /// Mismatch between the number of readers, writers and/or
    /// data+parity shards:
    ReadersWritersShardsMismatch,

    /// We don't have all readers and/or writers required for the current
    /// operation:
    MissingReadersWriters,

    /// We've hit an EOF before all required data could be read.
    UnexpectedEof,

    // Miscellaenous IO error occurred
    IOError(io::ErrorKind),

    /// Miscellaneous reed-solomon-erasure crate error occured:
    ReedSolomonError(RSError),
}

pub struct AsyncReedSolomon {
    rs: ReedSolomon<Galois8Field>,
    burst_len: usize,
    // TODO: users might want to pass this in externally. Maybe take
    // something which is AsRefMut<Option<AsRefMut<[u8]>>>?
    input_buffers: Vec<Vec<u8>>,
    output_buffers: Vec<Vec<u8>>,
    interspersed_buffer: Vec<u8>,
}

impl AsyncReedSolomon {
    pub fn new(
        data_shards: usize,
        parity_shards: usize,
        burst_len: usize,
    ) -> Result<Self, AsyncReedSolomonError> {
        let input_buffers = vec![vec![]; data_shards + parity_shards];
        let output_buffers = vec![vec![]; data_shards + parity_shards];

        Ok(AsyncReedSolomon {
            rs: ReedSolomon::new(data_shards, parity_shards)
                .map_err(AsyncReedSolomonError::ReedSolomonError)?,
            burst_len,
            input_buffers,
            output_buffers,
            interspersed_buffer: Vec::new(),
        })
    }

    pub fn shrink(&mut self) {
        self.input_buffers
            .iter_mut()
            .chain(self.output_buffers.iter_mut())
            .for_each(|buf| {
                buf.clear();
                buf.shrink_to_fit();
            });
        self.interspersed_buffer.clear();
        self.interspersed_buffer.shrink_to_fit();
    }

    pub async fn read_interspersed_data<R: AsyncRead + AsyncReadExt + Unpin, BR: BorrowMut<R>>(
        &mut self,
        mut reader: BR,
        limit: usize,
    ) -> Result<(usize, usize), AsyncReedSolomonError> {
        log::trace!("Reading data into interspersed buffers, limit: {}", limit);
        let data_shards = self.rs.data_shard_count();

        // First, read as much data as we can into the interspersed reader
        // buffer. To avoid frequent reallocations we use [`AsyncReadExt::read`]
        // and passing a mutable slice, retrying until we've hit an Eof or read
        // the maximum length. For this, extend the buffer to the desired length
        // (we don't care about any preexisting contents):
        self.interspersed_buffer.resize(limit, 0);

        // Now, read the data:
        let mut offset = 0;
        while offset < limit {
            let read_bytes: usize = reader
                .borrow_mut()
                .read(&mut self.interspersed_buffer[offset..limit])
                .await
                .map_err(
                    // Eof is not reported as an error to us here.
                    |ioerr| AsyncReedSolomonError::IOError(ioerr.kind()),
                )?;

            offset += read_bytes;

            if read_bytes == 0 {
                break;
            }
        }

        // Offet represents how much data was actually read:
        let data_len = offset;
        log::trace!("Linear read yielded {} bytes.", data_len);

        // We've copied the non-interspersed data into memory, now distribute it
        // over shards, making sure to fill up all remaining shards of the
        // current column with zero. First, ensure that the input data buffers
        // are empty and have enough capacity such that we don't need to
        // reallocate on the fly:
        self.input_buffers.iter_mut().for_each(|vec| {
            // Ceiling division, to ensure we have enough space for the last
            // partial column:
            vec.resize((data_len + data_shards - 1) / data_shards, 0);
        });

        // Now actually copy the data:
        let mut column = 0;
        let mut shard = 0;
        for byte in self.interspersed_buffer[..data_len].iter() {
            // Insert the byte into the appropriate shard:
            self.input_buffers[shard][column] = *byte;

            // Increment the shard, switch to the next column once all shards of
            // the current have been filled:
            shard += 1;
            if shard >= data_shards {
                shard = 0;
                column += 1;
            }
        }

        // If we've started writing a partial column (shard != 0), pad it with
        // zeroes:
        if shard != 0 {
            for s in shard..data_shards {
                self.input_buffers[s][column] = 0;
            }
            column += 1;
        }

        log::trace!(
            "Interspersing data has resulted in {} columns and {} rows of \
	     which {} have been padded with a zero element.",
            column,
            data_shards,
            (data_shards - shard) % data_shards
        );

        Ok((data_len, column))
    }

    pub async fn write_interspersed_data<
        W: AsyncWrite + AsyncWriteExt + Unpin,
        BW: BorrowMut<W>,
    >(
        &mut self,
        mut writer: BW,
        limit: usize,
    ) -> Result<(), AsyncReedSolomonError> {
        // Reading the transposed data stream byte-by-byte into the
        // [`AsyncWrite`] will be much too expensive (even with a buffered
        // writer). Instead, first write as much data as we can into the
        // transposed buffer. For this, reserve the required capacity and clear
        // the buffer. This should not reallocate if the underlying [`Vec`] is
        // already of sufficient capacity:
        self.interspersed_buffer.clear();
        self.interspersed_buffer.reserve(limit);

        // Build a transposed view of the data slices, creating an iterator over
        // them and collecting this iterator into the transposed buffer:
        self.interspersed_buffer.extend(
            transposed_slices::TransposedSlices::new(
                &self.input_buffers[..self.rs.data_shard_count()],
            )
            .iter()
            .take(limit),
        );

        // Stream the entire written Vec into the passed writer:
        writer
            .borrow_mut()
            .write_all(&self.interspersed_buffer)
            .await
            .map_err(|ioerr| AsyncReedSolomonError::IOError(ioerr.kind()))?;

        Ok(())
    }

    pub async fn encode<
        R: AsyncRead + AsyncReadExt + Unpin,
        BR: BorrowMut<R>,
        W: AsyncWrite + AsyncWriteExt + Unpin,
        BW: BorrowMut<W>,
    >(
        &mut self,
        mut reader: BR,
        writers: &mut [BW],
        len: usize,
    ) -> Result<(), AsyncReedSolomonError> {
        log::trace!("Encoding chunk with len {}", len);

        let data_shard_start_idx = 0;
        let data_shard_end_idx = data_shard_start_idx + self.rs.data_shard_count();
        let data_shard_idx = data_shard_start_idx..data_shard_end_idx;
        let parity_shard_start_idx = data_shard_end_idx;
        let parity_shard_end_idx = parity_shard_start_idx + self.rs.parity_shard_count();
        let parity_shard_idx = parity_shard_start_idx..parity_shard_end_idx;

        // Encoding data into parity chunks requires one reader (which is used
        // to read interspered data shards out of), along with data_shards +
        // parity_shards.
        if writers.len() != self.rs.data_shard_count() + self.rs.parity_shard_count() {
            return Err(AsyncReedSolomonError::MissingReadersWriters);
        }

        // In a loop, compute reed solomon shards until there is nothing left to
        // compute. We might want to have an A/B set of buffers which we can
        // swap such that we can read from the source & do calcuations + output
        // data simultaneously.
        let mut processed: usize = 0;
        while processed < len {
            // Read source data from the source into our buffers. The burst
            // length must be divisible by the number of data shards to avoid
            // padding in the middle of shards of a single chunk. However, we
            // must also limit ourselves to not overshoot len:
            let read_limit = std::cmp::min(
                // A single full burst:
                self.rs.data_shard_count() * self.burst_len,
                // Limit to length:
                len - processed,
            );
            let (read_bytes, written_columns) = self
                .read_interspersed_data::<R, &mut R>(reader.borrow_mut(), read_limit)
                .await?;

            // Sanity check to ensure that we haven't padded (except when we are
            // approaching the file limit, were it's fine for us to pad:
            let max_read_bytes = written_columns * self.rs.data_shard_count();
            if read_bytes != max_read_bytes && processed + read_bytes != len {
                return Err(AsyncReedSolomonError::UnexpectedEof);
            }

            // Now that we've read all required input data, encode it. For this,
            // first initialize the parity vectors to hold exactly
            // `written_columns` elements:
            self.output_buffers[parity_shard_idx.clone()]
                .iter_mut()
                .for_each(|out_buf| {
                    // TODO: make sure this does not change the underlying
                    // reservation (capacity remains constant)
                    out_buf.resize(written_columns, 0)
                });

            // Perform the actual encoding based on borrowed buffers:
            self.rs
                .encode_sep(
                    &self.input_buffers[data_shard_idx.clone()],
                    &mut self.output_buffers[parity_shard_idx.clone()],
                )
                .map_err(AsyncReedSolomonError::ReedSolomonError)?;

            // Write the output results to the respective writers:
            let (data_writers, parity_writers) = writers.split_at_mut(parity_shard_start_idx);
            let write_results: Vec<Result<(), std::io::Error>> = futures::future::join_all(
                data_writers
                    .iter_mut()
                    .enumerate()
                    .map(|(i, writer)| writer.borrow_mut().write_all(&self.input_buffers[i]))
                    .chain(parity_writers.iter_mut().enumerate().map(|(i, writer)| {
                        writer
                            .borrow_mut()
                            .write_all(&self.output_buffers[parity_shard_start_idx + i])
                    })),
            )
            .await;

            // Iterate over the write results, reporting the first
            // error we can find:
            write_results
                .iter()
                .find(|res| res.is_err())
                .unwrap_or(&Ok(()))
                .as_ref()
                .map_err(|e| AsyncReedSolomonError::IOError(e.kind()))?;

            // Add the read and encoded bytes to the processed byte count.
            processed += read_bytes;
        }

        // Shutdown all the writers (also flushes them)
        let shutdown_results: Vec<Result<(), std::io::Error>> =
            futures::future::join_all(writers.iter_mut().map(|w| w.borrow_mut().shutdown())).await;

        // Iterate over the write results, reporting the first error
        // we can find:
        shutdown_results
            .iter()
            .find(|res| res.is_err())
            .unwrap_or(&Ok(()))
            .as_ref()
            .map_err(|e| AsyncReedSolomonError::IOError(e.kind()))?;

        // All data processed, writers closed, we are done:
        Ok(())
    }

    pub async fn reconstruct_data<
        R: AsyncRead + AsyncReadExt + Unpin,
        BR: BorrowMut<R>,
        W: AsyncWrite + AsyncWriteExt + Unpin,
        BW: BorrowMut<W>,
    >(
        &mut self,
        readers: &mut [Option<BR>],
        mut writer: BW,
        len: usize,
    ) -> Result<(), AsyncReedSolomonError> {
        // Basic sanity check: we can't recunstruct any data if we don't have at
        // least as many readers as data shards. Also, the length of the readers
        // list should be equal to the number of data and parity shards, as we
        // use the index of readers for the shard index:
        let reader_count = readers.iter().filter(|r| r.is_some()).count();
        if readers.len() != self.rs.data_shard_count() + self.rs.parity_shard_count()
            || reader_count != self.rs.data_shard_count()
        {
            return Err(AsyncReedSolomonError::MissingReadersWriters);
        }

        // Ensure that the input_buffer length (for all present readers) is
        // sufficient to capture enough data from the writers up to burst
        // length. For this, clear and then reserve the required space (we're
        // clearing the contents on writing them once per iteration anyways). We
        // do this for the first `data_shard_count` buffers, as well as for all
        // other writer's buffers provided.
        self.input_buffers
            .iter_mut()
            .zip(readers.iter())
            .enumerate()
            .filter(|(i, (_b, r))| *i < self.rs.data_shard_count() || r.is_some())
            .for_each(|(_i, (b, _r))| {
                b.clear();
                b.resize(self.burst_len, 0);
            });

        // Reconstruct from bursts of reads until we've reconstructed `len`
        // bytes of data:
        let mut reconstructed: usize = 0;
        while reconstructed < len {
            // Determine the remaining read length on a per-reader granularity
            // and then issue the reads simutaneously:
            let read_columns = (std::cmp::min(len - reconstructed, self.burst_len)
                + self.rs.data_shard_count()
                - 1)
                / self.rs.data_shard_count();

            // Subslice the buffers once, returning a data structure whose
            // format is compatible with what [`ReedSolomon::reconstruct_data`]
            // expects:
            let mut limited_buffers: Vec<(&mut [u8], bool)> = self
                .input_buffers
                .iter_mut()
                .zip(readers.iter())
                .enumerate()
                .map(|(i, (b, r))| {
                    if r.is_some() {
                        (&mut b[..read_columns], true)
                    } else if i < self.rs.data_shard_count() {
                        (&mut b[..read_columns], false)
                    } else {
                        // Don't actually slice any memory, this shard must not
                        // be relevant to the Reed Solomon reconstruction
                        // because we don't have it, and it is not one of the
                        // data shards to be reconstructed.
                        (&mut b[..0], false)
                    }
                })
                .collect();

            let read_results: Vec<Result<usize, std::io::Error>> = futures::future::join_all(
                limited_buffers
                    .iter_mut()
                    .zip(readers.iter_mut())
                    .filter_map(|((buffer, valid), opt_reader)| {
                        if *valid {
                            assert!(buffer.len() == read_columns);
                            Some((buffer, opt_reader.as_mut().unwrap()))
                        } else {
                            None
                        }
                    })
                    .map(|(buffer, reader)| reader.borrow_mut().read_exact(buffer)),
            )
            .await;

            // Iterate over the write results, reporting the first error we can
            // find. [`AsyncReadExt::read_exact`] is specified to return an
            // error of [`io::ErrorKind::UnexpectedEof`] if it can't fill the
            // entire buffer, hence we can ignore the returned usize length.
            read_results
                .iter()
                .find(|res| res.is_err())
                .unwrap_or(&Ok(0))
                .as_ref()
                .map_err(|e| AsyncReedSolomonError::IOError(e.kind()))?;

            // Run the routine for reconstructing shards.
            self.rs
                .reconstruct_data(&mut limited_buffers)
                .map_err(AsyncReedSolomonError::ReedSolomonError)?;

            // Make sure that the reconstruction algorithm has marked all data
            // shards as valid and their length hasn't been changed.
            assert!(limited_buffers[..self.rs.data_shard_count()]
                .iter()
                .find(|(buffer, valid)| !valid || buffer.len() != read_columns)
                .is_none());

            // Okay, now dump the buffer contents into the writer:
            let write_len = std::cmp::min(
                len - reconstructed,
                read_columns * self.rs.data_shard_count(),
            );
            self.write_interspersed_data::<W, &mut W>(writer.borrow_mut(), write_len)
                .await?;

            reconstructed += write_len;
        }

        Ok(())
    }
}
