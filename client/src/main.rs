use std::borrow::BorrowMut;
use std::io;
use std::path::PathBuf;

use exitcode;
use log;
use simple_logger;

use clap::{Parser, Subcommand};

use tokio::fs as async_fs;
use tokio::io::{self as async_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use decode_rs::api::coord as coord_api;
use decode_rs::api_client::coord::CoordAPIClient;
use decode_rs::api_client::node::NodeAPIClient;
use decode_rs::api_client::reqwest::Url;

use reed_solomon_erasure::{galois_8::Field as Galois8Field, Error as RSError, ReedSolomon};

const MISC_ERR: exitcode::ExitCode = 1;

// ceiling division, requires a + b to not overflow
fn div_ceil(a: u64, b: u64) -> u64 {
    (a + b - 1) / b
}

fn parse_url<S: AsRef<str>>(url: S, target_type: &'static str) -> Result<Url, exitcode::ExitCode> {
    Url::parse(url.as_ref()).map_err(|parseerr| {
        log::error!(
            "Failed to parse the provided {} URL (\"{}\"): {:?}",
            target_type,
            url.as_ref(),
            parseerr
        );
        MISC_ERR
    })
}

#[derive(Parser)]
struct UploadCommand {
    // Base URL of coordiator:
    coord_url: String,

    // Path of object to upload:
    object_path: String,
}

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
    interspersed_read_buffer: Vec<u8>,
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
            interspersed_read_buffer: Vec::new(),
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
        self.interspersed_read_buffer.clear();
        self.interspersed_read_buffer.shrink_to_fit();
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
        self.interspersed_read_buffer.resize(limit, 0);

        // Now, read the data:
        let mut offset = 0;
        while offset < limit {
            let read_bytes: usize = reader
                .borrow_mut()
                .read(&mut self.interspersed_read_buffer[offset..limit])
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
        for byte in self.interspersed_read_buffer[..data_len].iter() {
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
}

async fn upload_chunk(
    _cli: &Cli,
    node_api_client: &NodeAPIClient,
    async_reed_solomon: &mut AsyncReedSolomon,
    object_file: &mut async_fs::File,
    upload_map: &coord_api::ObjectUploadMap,
    chunk_idx: u64,
) -> Result<(), exitcode::ExitCode> {
    let chunk_offset = upload_map.chunk_size * chunk_idx;
    let chunk_size = std::cmp::min(
        upload_map.chunk_size as usize,
        (upload_map.object_size - (upload_map.chunk_size * chunk_idx)) as usize,
    );
    log::info!(
        "Upload chunk #{}, starting at offset {}, length {}",
        chunk_idx,
        chunk_offset,
        chunk_size,
    );

    // We shouldn't need to seek in the file assuming that AsyncReedSolomon is
    // correctly implemented. However, when enabling debug output or running on
    // a debug build, recover the current file offset and print it nonetheless:
    if cfg!(debug_assertions) {
        let current_offset =
            async_io::AsyncSeekExt::seek(object_file, async_io::SeekFrom::Current(0))
                .await
                .map_err(|ioerr| {
                    log::error!(
                        "Error while seeking in file to recover current offset: {:?}",
                        ioerr
                    );
                    MISC_ERR
                })?;

        if current_offset != upload_map.chunk_size * chunk_idx {
            panic!(
                "Unexpected offset in file, expected: {}, actual: {}",
                upload_map.chunk_size * chunk_idx,
                current_offset
            );
        }
    }

    async fn upload_shards(
        node_api_client: &NodeAPIClient,
        chunk_idx: u64,
        encoded_readers: Vec<async_io::DuplexStream>,
    ) -> Result<(), exitcode::ExitCode> {
        // TODO!
        let parsed_node_url = parse_url("http://localhost:8000", "node")?;

        // Spawn the clients, one for every reader:
        let upload_requests: Vec<_> = encoded_readers
            .into_iter()
            .map(|reader| node_api_client.upload_shard(&parsed_node_url, "", reader))
            .collect();

        // Finally, collectively await the requests:
        let upload_results: Vec<Result<(), decode_rs::api_client::node::NodeAPIUploadError>> =
            futures::future::join_all(upload_requests.into_iter()).await;

        // Iterate over the write results, reporting the first error we can find:
        if let (shard_idx, Err(e)) = upload_results
            .iter()
            .enumerate()
            .find(|(_, res)| res.is_err())
            .unwrap_or((0, &Ok(())))
        {
            log::error!(
                "Error while uploading shard {} of chunk {}: {:?}",
                shard_idx,
                chunk_idx,
                e
            );
            return Err(MISC_ERR);
        }

        Ok(())
    }

    async fn encode_shards(
        async_reed_solomon: &mut AsyncReedSolomon,
        object_file: &mut async_fs::File,
        mut encoded_writers: Vec<async_io::DuplexStream>,
        chunk_idx: u64,
        chunk_size: usize,
    ) -> Result<(), exitcode::ExitCode> {
        async_reed_solomon
            .encode::<async_fs::File, _, tokio::io::DuplexStream, _>(
                object_file,
                encoded_writers.as_mut_slice(),
                chunk_size,
            )
            .await
            .map_err(|e| {
                log::error!(
                    "Error while encoding chunk {} into shards: {:?}",
                    chunk_idx,
                    e
                );
                MISC_ERR
            })
    }

    // First, get a pair of [`DuplexStream`] for every encoded shard, which we
    // can then create to spawn clients.
    let (encoded_writers, encoded_readers): (
        Vec<async_io::DuplexStream>,
        Vec<async_io::DuplexStream>,
    ) = (0..(upload_map.code_ratio_data + upload_map.code_ratio_parity))
        .map(|_| async_io::duplex(64 * 1024))
        .unzip();

    // Encode the data into the writers...
    let encode_fut = encode_shards(
        async_reed_solomon,
        object_file,
        encoded_writers,
        chunk_idx,
        chunk_size,
    );

    // ...while in parallel streaming the resulting encoded shards:
    let upload_fut = upload_shards(node_api_client, chunk_idx, encoded_readers);

    // Now, execute both:
    futures::try_join!(encode_fut, upload_fut)?;

    Ok(())
}

async fn upload_command(cli: &Cli, cmd: &UploadCommand) -> Result<(), exitcode::ExitCode> {
    // Try to parse the passed coordinator base URL:
    let parsed_url = parse_url(&cmd.coord_url, "coordinator")?;

    // Create the coordinator API client:
    let coord_api_client = CoordAPIClient::new(parsed_url);

    // In order to initiate an upload at the coordinator, we need to stat the
    // object size we are going to upload. However, as the documentation of
    // [std::io::Metadata::is_file] outlines, the most reliable way to detect
    // whether we can access a file is to open it. Then we can still obtain the
    // metadata and read the file's length:
    let mut object_file =
        async_fs::File::open(&cmd.object_path)
            .await
            .map_err(|ioerr| match ioerr.kind() {
                io::ErrorKind::NotFound => {
                    log::error!("Cannot find object at path \"{}\".", &cmd.object_path);
                    exitcode::NOINPUT
                }
                io::ErrorKind::PermissionDenied => {
                    log::error!(
                        "Permission denied while trying to access \"{}\".",
                        &cmd.object_path
                    );
                    exitcode::NOPERM
                }
                _ => {
                    log::error!(
                        "Error while trying to access \"{}\": {:?}.",
                        &cmd.object_path,
                        ioerr
                    );
                    MISC_ERR
                }
            })?;

    // Now, try to obtain the file's length:
    let object_file_meta = object_file.metadata().await.map_err(|ioerr| {
        // We don't special case here, as we expect that opening the file
        // will uncover most plausible error cases.
        log::error!(
            "Error while obtaining the object file's metadata: {:?}",
            ioerr
        );
        MISC_ERR
    })?;

    let object_size = object_file_meta.len();

    // With the object length determined, make a request to the coordinator:
    let upload_map = (match coord_api_client.upload_object(object_size).await {
        Err(e) => {
            log::error!("An error occured while issuing the initial upload request to the coordinator: {:?}", e);
            Err(MISC_ERR)
        }
        Ok(map) => Ok(map),
    })?;

    log::info!(
        "Upload map:\n\
         \t- Object ID: {:?}\n\
         \t- Object size: {} bytes\n\
         \t- Chunk size: {} bytes\n\
         \t- Shard size: {} bytes\n\
         \t- Code ratio: {} data, {} parity\n",
        upload_map.object_id,
        upload_map.object_size,
        upload_map.chunk_size,
        upload_map.shard_size,
        upload_map.code_ratio_data,
        upload_map.code_ratio_parity
    );

    // TODO: error handling!
    let mut async_reed_solomon = AsyncReedSolomon::new(
        upload_map.code_ratio_data as usize,
        upload_map.code_ratio_parity as usize,
        1024 * 1024,
    )
    .unwrap();

    let node_api_client = NodeAPIClient::new();

    for chunk_idx in 0..div_ceil(upload_map.object_size, upload_map.chunk_size) {
        upload_chunk(
            cli,
            &node_api_client,
            &mut async_reed_solomon,
            &mut object_file,
            &upload_map,
            chunk_idx,
        )
        .await?;
    }
    Ok(())
}

#[derive(Subcommand)]
enum Commands {
    /// Upload an object
    Upload(UploadCommand),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Suppress any non-error output, overrides `--verbose`
    #[arg(short, long)]
    quiet: bool,

    /// Increase the output verbosity
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging based on the verbosity level provided:
    simple_logger::init_with_level(match (cli.quiet, cli.verbose) {
        (true, _) => log::Level::Error,
        (_, 0) => log::Level::Info,
        (_, 1) => log::Level::Debug,
        (_, _) => log::Level::Trace,
    })
    .unwrap();

    // Handle each subcommand seperately:
    let ec: exitcode::ExitCode = match cli.command {
        Commands::Upload(ref cmd) => upload_command(&cli, cmd),
    }
    .await
    .map_or_else(|ec| ec, |()| exitcode::OK);

    // Exit with the appropriate exit code:
    std::process::exit(ec);
}
