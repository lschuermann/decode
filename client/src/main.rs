use std::io;
use std::path::PathBuf;

use exitcode;
use log;
use simple_logger;

use clap::{Parser, Subcommand};

use tokio::fs as async_fs;
use tokio::io as async_io;

use decode_rs::api::coord as coord_api;
use decode_rs::api::node as node_api;
use decode_rs::api_client::coord::CoordAPIClient;
use decode_rs::api_client::node::{NodeAPIClient, NodeAPIUploadError};
use decode_rs::api_client::reqwest::Url;
use decode_rs::async_reed_solomon::AsyncReedSolomon;

const MISC_ERR: exitcode::ExitCode = 1;

// ceiling division, requires a + b to not overflow
fn div_ceil(a: u64, b: u64) -> u64 {
    (a + b - 1) / b
}

fn parse_url<S: AsRef<str>>(
    url: S,
    target_type: &'static str,
) -> Result<(Url, bool), exitcode::ExitCode> {
    // For now, the parsing logic is actually pretty simple. If the scheme is
    // `decode`, assume `https` (the default). If the scheme is
    // `decode+$SCHEME`, use `$SCHEME`.

    // TODO: Can we get around allocating memory here? Fixing a Url's scheme
    // after parsing using `Url::set_scheme` is utterly useless given that
    // method's restrictions.
    let (stripped_url, proto_override) =
        if let Some(prefix_less) = url.as_ref().strip_prefix("decode://") {
            (format!("https://{}", prefix_less), false)
        } else if let Some(stripped) = url.as_ref().strip_prefix("decode+") {
            (stripped.to_string(), true)
        } else {
            log::error!(
                "Unable to parse provided {} URL (\"{}\"): invalid scheme, \
		 should match ^decode(\\+.*)://",
                target_type,
                url.as_ref(),
            );
            return Err(MISC_ERR);
        };

    Url::parse(&stripped_url)
        .map(|url| (url, proto_override))
        .map_err(|parseerr| {
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
    /// Base URL of coordiator (decode://<hostname>)
    coord_url: String,

    /// Object file to upload
    object_path: String,
}

#[derive(Parser)]
struct DownloadCommand {
    /// Object URL (decode://<hostname>/<object-id>)
    object_url: String,
}

async fn assert_offset(
    target_offset: u64,
    file: &mut (impl async_io::AsyncSeekExt + Unpin),
) -> Result<(), exitcode::ExitCode> {
    let current_offset = async_io::AsyncSeekExt::seek(file, async_io::SeekFrom::Current(0))
        .await
        .map_err(|ioerr| {
            log::error!(
                "Error while seeking in file to recover current offset: {:?}",
                ioerr
            );
            MISC_ERR
        })?;

    if current_offset != target_offset {
        panic!(
            "Unexpected offset in file, expected: {}, actual: {}",
            target_offset, current_offset
        );
    }

    Ok(())
}

async fn upload_chunk(
    _cli: &Cli,
    node_api_client: &NodeAPIClient,
    async_reed_solomon: &mut AsyncReedSolomon,
    object_file: &mut async_fs::File,
    upload_map: &coord_api::ObjectUploadMap,
    chunk_idx: usize,
) -> Result<Vec<node_api::ShardUploadReceipt<'static, 'static>>, exitcode::ExitCode> {
    let chunk_offset = upload_map.chunk_size * chunk_idx as u64;
    let chunk_size = std::cmp::min(
        upload_map.chunk_size as usize,
        (upload_map.object_size - (upload_map.chunk_size * chunk_idx as u64)) as usize,
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
        assert_offset(upload_map.chunk_size * chunk_idx as u64, object_file).await?;
    }

    async fn upload_shards(
        node_api_client: &NodeAPIClient,
        chunk_idx: usize,
        encoded_readers: Vec<async_io::DuplexStream>,
        shard_specs: &[coord_api::ObjectUploadShardSpec],
    ) -> Result<Vec<node_api::ShardUploadReceipt<'static, 'static>>, exitcode::ExitCode> {
        assert!(encoded_readers.len() == shard_specs.len());

        // Spawn the clients, one for every reader:
        let upload_requests: Vec<_> = encoded_readers
            .into_iter()
            .zip(shard_specs.iter())
            .enumerate()
            .map(|(shard_idx, (reader, shard_spec))| {
                log::info!(
                    "Uploading shard #{} of chunk #{} to node \"{}\"",
                    shard_idx,
                    chunk_idx,
                    &shard_spec.node,
                );
                node_api_client.upload_shard(
                    Url::parse(&shard_spec.node).unwrap(),
                    &shard_spec.ticket,
                    reader,
                )
            })
            .collect();

        // Finally, collectively await the requests:
        let upload_results: Vec<
            Result<node_api::ShardUploadReceipt<'static, 'static>, NodeAPIUploadError>,
        > = futures::future::join_all(upload_requests.into_iter()).await;

        // Iterate over the write results, reporting the first error we can find:
        if let Some((shard_idx, Err(e))) = upload_results
            .iter()
            .enumerate()
            .find(|(_, res)| res.is_err())
        {
            log::error!(
                "Error while uploading shard {} of chunk {}: {:?}",
                shard_idx,
                chunk_idx,
                e
            );
            return Err(MISC_ERR);
        }

        Ok(upload_results.into_iter().map(|res| res.unwrap()).collect())
    }

    async fn encode_shards(
        async_reed_solomon: &mut AsyncReedSolomon,
        object_file: &mut async_fs::File,
        mut encoded_writers: Vec<async_io::DuplexStream>,
        chunk_idx: usize,
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
    let upload_fut = upload_shards(
        node_api_client,
        chunk_idx,
        encoded_readers,
        upload_map.shard_map[chunk_idx as usize].as_slice(),
    );

    // Now, execute both:
    let ((), receipts) = futures::try_join!(encode_fut, upload_fut)?;

    Ok(receipts)
}

async fn upload_command(cli: &Cli, cmd: &UploadCommand) -> Result<(), exitcode::ExitCode> {
    // Try to parse the passed coordinator base URL:
    let (parsed_url, proto_override) = parse_url(&cmd.coord_url, "coordinator")?;

    // Create the coordinator API client:
    let coord_api_client = CoordAPIClient::new(parsed_url.clone());

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
         \t- Code ratio: {} data, {} parity",
        upload_map.object_id,
        upload_map.object_size,
        upload_map.chunk_size,
        upload_map.shard_size,
        upload_map.code_ratio_data,
        upload_map.code_ratio_parity
    );

    // Validate that the shard_map corresponds to the calculated number of
    // chunks in an object and encoded shards in a chunk. Also, we need to be
    // able to parse all provided URLs before attempting to upload anything.
    if upload_map.object_size != object_size {
        log::error!(
            "The object size of the upload map ({}) does not match the actual \
	     object size ({})",
            upload_map.object_size,
            object_size,
        );
        return Err(MISC_ERR);
    };

    let chunk_count = div_ceil(upload_map.object_size, upload_map.chunk_size);
    if upload_map.shard_map.len() as u64 != chunk_count {
        log::error!(
            "The provided shard map does not contain the expected number of \
	     chunks (expected: {}, actual: {})",
            chunk_count,
            upload_map.shard_map.len(),
        );
        return Err(MISC_ERR);
    }

    let shard_count = div_ceil(upload_map.chunk_size, upload_map.shard_size);
    if shard_count != upload_map.code_ratio_data as u64 {
        log::error!(
            "The provided chunk_size and shard_size result in {} shards per \
	     chunk, however the code parameters provided expect {} data shards",
            shard_count,
            upload_map.code_ratio_data,
        );
    }

    for (chunk_idx, chunk_shards) in upload_map.shard_map.iter().enumerate() {
        if chunk_shards.len()
            != upload_map.code_ratio_data as usize + upload_map.code_ratio_parity as usize
        {
            log::error!(
                "The provided upload map defines {} data and parity shards for \
		 chunk {}, however the code parameters dictate {} data and {} \
		 parity shards per chunk.",
                chunk_shards.len(),
                chunk_idx,
                upload_map.code_ratio_data,
                upload_map.code_ratio_parity,
            );

            return Err(MISC_ERR);
        }

        for (shard_idx, shard_spec) in chunk_shards.iter().enumerate() {
            if let Err(e) = reqwest::Url::parse(&shard_spec.node) {
                log::error!(
                    "Unable to parse node URL provided for shard {} of chunk \
		     {}: \"{}\", encountered error {:?}",
                    shard_idx,
                    chunk_idx,
                    shard_spec.node,
                    e,
                );

                return Err(MISC_ERR);
            }
        }
    }

    // TODO: error handling!
    let mut async_reed_solomon = AsyncReedSolomon::new(
        upload_map.code_ratio_data as usize,
        upload_map.code_ratio_parity as usize,
        1024 * 1024,
    )
    .unwrap();

    let node_api_client = NodeAPIClient::new();

    let mut upload_receipts: Vec<Vec<node_api::ShardUploadReceipt>> =
        Vec::with_capacity(upload_map.shard_map.len());
    for chunk_idx in 0..upload_map.shard_map.len() {
        upload_receipts.push(
            upload_chunk(
                cli,
                &node_api_client,
                &mut async_reed_solomon,
                &mut object_file,
                &upload_map,
                chunk_idx,
            )
            .await?,
        );
    }

    // Compute the object URL just before moving the upload_map in the finalize
    // API call:
    let mut res_url = parsed_url;
    // TODO: how to deal with subdirectories?
    res_url.set_path(&upload_map.object_id.to_string());

    coord_api_client
        .finalize_object(upload_map, upload_receipts)
        .await
        .map_err(|e| {
            log::error!(
                "An error occurred while finalizing the object upload: {:?}",
                e
            );
            MISC_ERR
        })?;

    if proto_override {
        println!("decode+{}", res_url.to_string());
    } else {
        println!(
            "decode{}",
            res_url.to_string().strip_prefix(res_url.scheme()).unwrap()
        );
    }

    Ok(())
}

async fn download_command(_cli: &Cli, cmd: &DownloadCommand) -> Result<(), exitcode::ExitCode> {
    // Try to parse the passed coordinator base URL:
    let (parsed_url, _) = parse_url(&cmd.object_url, "coordinator")?;

    // Parse the specified path as a UUID:
    let object_id_str = parsed_url.path().strip_prefix("/").unwrap();
    let object_id = uuid::Uuid::parse_str(object_id_str).map_err(|e| {
        log::error!(
            "Failed to parse provided object URL, unable to interpret \"{}\" \
	     as UUID: {:?}",
            object_id_str,
            e
        );
        MISC_ERR
    })?;

    // Create the coordinator API client:
    let coord_api_client = CoordAPIClient::new(parsed_url.clone());

    // Fetch the object retrieval map, guiding us to shards:
    let retrieval_map = coord_api_client.get_object(object_id).await.map_err(|e| {
        log::error!(
            "An error occured while querying the object retrieval map from the \
	     coordinator: {:?}",
            e,
        );

        MISC_ERR
    })?;

    log::info!(
        "Retrieval map:\n\
         \t- Object ID: {:?}\n\
         \t- Object size: {} bytes\n\
         \t- Chunk size: {} bytes\n\
         \t- Shard size: {} bytes\n\
         \t- Code ratio: {} data, {} parity",
        object_id,
        retrieval_map.object_size,
        retrieval_map.chunk_size,
        retrieval_map.shard_size,
        retrieval_map.code_ratio_data,
        retrieval_map.code_ratio_parity
    );

    let chunk_count = div_ceil(retrieval_map.object_size, retrieval_map.chunk_size);
    if retrieval_map.shard_map.len() as u64 != chunk_count {
        log::error!(
            "The provided shard map does not contain the expected number of \
	     chunks (expected: {}, actual: {})",
            chunk_count,
            retrieval_map.shard_map.len(),
        );
        return Err(MISC_ERR);
    }

    let shard_count = div_ceil(retrieval_map.chunk_size, retrieval_map.shard_size);
    if shard_count != retrieval_map.code_ratio_data as u64 {
        log::error!(
            "The provided chunk_size and shard_size result in {} shards per \
	     chunk, however the code parameters provided expect {} data shards",
            shard_count,
            retrieval_map.code_ratio_data,
        );
    }

    for (chunk_idx, chunk_shards) in retrieval_map.shard_map.iter().enumerate() {
        if chunk_shards.len()
            != retrieval_map.code_ratio_data as usize + retrieval_map.code_ratio_parity as usize
        {
            log::error!(
                "The provided upload map defines {} data and parity shards for \
		 chunk {}, however the code parameters dictate {} data and {} \
		 parity shards per chunk.",
                chunk_shards.len(),
                chunk_idx,
                retrieval_map.code_ratio_data,
                retrieval_map.code_ratio_parity,
            );

            return Err(MISC_ERR);
        }

        for (shard_idx, shard_spec) in chunk_shards.iter().enumerate() {
            for (node_idx, node_ref) in shard_spec.nodes.iter().enumerate() {
                if *node_ref >= retrieval_map.node_map.len() {
                    log::error!(
                        "Node #{} of shard #{} of chunk #{} is out of bounds \
			 of the node_map list.",
                        node_idx,
                        shard_idx,
                        chunk_idx,
                    );

                    return Err(MISC_ERR);
                }
            }
        }
    }

    let parsed_node_map = retrieval_map
        .node_map
        .iter()
        .map(|node_url_str| {
            reqwest::Url::parse(&node_url_str).map_err(|parse_err| {
                log::error!(
                    "Unable to parse node URL \"{}\", encountered error {:?}",
                    node_url_str,
                    parse_err,
                );

                MISC_ERR
            })
        })
        .collect::<Result<Vec<reqwest::Url>, exitcode::ExitCode>>()?;

    // TODO: implement mechanism to parse file name from URL and to allow
    // overriding it from the CLI
    let object_path = PathBuf::from(object_id.to_string());

    // TODO: implement mechanism to automatically rename the file if one already
    // exists with a colliding name
    let mut object_file = async_fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&object_path)
        .await
        .map_err(|ioerr| match ioerr.kind() {
            io::ErrorKind::AlreadyExists => {
                log::error!(
                    "Unable to create output file at \"{}\", file exists!",
                    object_path.display(),
                );

                exitcode::CANTCREAT
            }
            _ => {
                log::error!("Error while creating output file: {:?}", ioerr);
                MISC_ERR
            }
        })?;

    // TODO: error handling!
    let mut async_reed_solomon = AsyncReedSolomon::new(
        retrieval_map.code_ratio_data as usize,
        retrieval_map.code_ratio_parity as usize,
        1024 * 1024,
    )
    .unwrap();

    let node_api_client = NodeAPIClient::new();

    let mut rng: rand::rngs::SmallRng =
        rand::SeedableRng::from_rng(&mut rand::thread_rng()).unwrap();

    for (chunk_idx, chunk_shards) in retrieval_map.shard_map.iter().enumerate() {
        use rand::prelude::IteratorRandom;

        let chunk_offset = retrieval_map.chunk_size * chunk_idx as u64;
        let chunk_size = std::cmp::min(
            retrieval_map.chunk_size as usize,
            (retrieval_map.object_size - (retrieval_map.chunk_size * chunk_idx as u64)) as usize,
        );
        log::info!(
            "Downloading chunk #{}, starting at offset {}, length {}",
            chunk_idx,
            chunk_offset,
            chunk_size,
        );

        // We shouldn't need to seek in the file assuming that AsyncReedSolomon is
        // correctly implemented. However, when enabling debug output or running on
        // a debug build, recover the current file offset and print it nonetheless:
        if cfg!(debug_assertions) {
            assert_offset(
                retrieval_map.chunk_size * chunk_idx as u64,
                &mut object_file,
            )
            .await?;
        }

        // Collect all shard-node combinations into one array which we
        // can use to try and find shards to download:
        let parsed_node_map_ref = &parsed_node_map;
        let mut shard_nodes: Vec<(usize, String, String, reqwest::Url)> = chunk_shards
            .iter()
            .enumerate()
            .flat_map(|(idx, shard_spec)| {
                shard_spec.nodes.iter().map(move |shard_node_idx| {
                    (
                        idx,
                        shard_spec.digest.clone(),
                        "dummyticket".to_string(), // TODO!
                        parsed_node_map_ref[*shard_node_idx].clone(),
                    )
                })
            })
            .collect();

        // Now, try to initate downloads one-by-one. Potentially we could
        // optimize by making the first requests be a bulk operation.
        let shards: usize =
            retrieval_map.code_ratio_data as usize + retrieval_map.code_ratio_parity as usize;
        let mut initiated_shard_download_count = 0;
        let mut initiated_shard_downloads: Vec<_> = (0..shards).map(|_| None).collect();
        let mut input_shards: Vec<_> = (0..shards).map(|_| None).collect();
        while shard_nodes.len() > 0
            && initiated_shard_download_count < retrieval_map.code_ratio_data
        {
            log::trace!(
                "Trying to find a shard+node to fetch. Remaining shard/node \
		 combinations: {}, required shards: {}, accepted reqs: {}",
                shard_nodes.len(),
                retrieval_map.code_ratio_data,
                initiated_shard_download_count,
            );
            // Remove a random entry from the shard_nodes vector and
            // check whether we've already started a download for this
            // shard:
            let i = (0..shard_nodes.len()).choose(&mut rng).unwrap();
            let (idx, digest_str, ticket, node_url) = shard_nodes.swap_remove(i);
            if initiated_shard_downloads[idx].is_some() {
                continue;
            }

            let mut digest = [0_u8; 32];
            hex::decode_to_slice(digest_str.as_bytes(), &mut digest[..]).unwrap();
            #[derive(Debug, Clone)]
            struct DigestWrapper(pub [u8; 32]);
            impl AsRef<[u8; 32]> for DigestWrapper {
                fn as_ref(&self) -> &[u8; 32] {
                    &self.0
                }
            }
            let digest = DigestWrapper(digest);

            log::debug!("Trying to fetch shard {:?} from node {}", digest, node_url);

            // Set off the request, see if it works and returns a stream:
            let shard_stream = match node_api_client
                .download_shard_req(node_url.clone(), digest.clone(), ticket)
                .await
            {
                Err(err) => {
                    log::debug!(
                        "Error while trying to download shard {:?} from \
			     node {}: {:?}, trying another node/shard.",
                        digest,
                        node_url,
                        err,
                    );
                    continue;
                }
                Ok(stream) => {
                    log::trace!(
                        "Node accepted request and provides shard data stream, \
			 continuing."
                    );
                    stream
                }
            };

            // Okay, we got a success response with a stream! Store this along
            // with an `DuplexStream`, where the other end of it is supplied as
            // an input_shard to the async reed solomon reconstruct routine:
            let (reader, writer) = async_io::duplex(64 * 1024);
            initiated_shard_downloads[idx] = Some((shard_stream, writer));
            initiated_shard_download_count += 1;
            input_shards[idx] = Some(reader);
        }

        // If we don't have a sufficient number shards, retry!
        if initiated_shard_download_count < retrieval_map.code_ratio_data {
            log::error!(
                "Could not fetch a sufficient number of shards to reconstruct \
		 chunk {}",
                chunk_idx
            );
            return Err(MISC_ERR);
        }

        // Join the download futures into a single one:
        let download_fut = futures::future::try_join_all(
            initiated_shard_downloads.into_iter().filter_map(|e| e).map(
                |(mut stream, mut writer)| async move {
                    use futures::StreamExt;
                    use tokio::io::AsyncWriteExt;

                    while let Some(res) = stream.next().await {
                        let mut buffer = res.map_err(|e| {
                            log::error!("Error while reading from reqwest bytes stream: {:?}", e);
                            MISC_ERR
                        })?;
                        writer.write_all_buf(&mut buffer).await.map_err(|e| {
                            log::error!("Error while writing to DuplexStream buffer: {:?}", e);
                            MISC_ERR
                        })?;
                    }

                    Ok(())
                },
            ),
        );

        async fn decode_shards(
            async_reed_solomon: &mut AsyncReedSolomon,
            mut readers: Vec<Option<async_io::DuplexStream>>,
            object_file: &mut async_fs::File,
            chunk_idx: usize,
            chunk_len: usize,
        ) -> Result<(), exitcode::ExitCode> {
            async_reed_solomon
                .reconstruct_data::<tokio::io::DuplexStream, _, async_fs::File, _>(
                    &mut readers,
                    object_file,
                    chunk_len,
                )
                .await
                .map_err(|e| {
                    log::error!(
                        "Error while decoding chunk {} from shards: {:?}",
                        chunk_idx,
                        e
                    );
                    MISC_ERR
                })
        }

        // ...while simultaneously decoding them.
        let decode_fut = decode_shards(
            &mut async_reed_solomon,
            input_shards,
            &mut object_file,
            chunk_idx,
            chunk_size,
        );

        // Now, execute both:
        let (_, ()) = futures::try_join!(download_fut, decode_fut)?;
    }

    // object_file.borrow_mut().shutdown().await.map_err(|e| AsyncReedSolomonError::IOError(e.kind()))?;

    Ok(())
}

#[derive(Subcommand)]
enum Commands {
    /// Upload an object
    Upload(UploadCommand),

    /// Download an object
    Download(DownloadCommand),
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
        Commands::Upload(ref cmd) => upload_command(&cli, cmd).await,
        Commands::Download(ref cmd) => download_command(&cli, cmd).await,
    }
    .map_or_else(|ec| ec, |()| exitcode::OK);

    // Exit with the appropriate exit code:
    std::process::exit(ec);
}
