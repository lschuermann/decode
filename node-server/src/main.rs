#[macro_use]
extern crate rocket;

use std::borrow::Cow;
use std::io::ErrorKind as IOErrorKind;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use rocket::data::{ByteUnit, ToByteUnit};
use rocket::fairing::AdHoc;
use rocket::http::ContentType;
use rocket::request::FromParam;
use rocket::response::{self, Responder};
use rocket::serde::json::Json;
use rocket::{Data, State};
use rocket::{Request, Response};

use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};

use reqwest::Url;

use uuid::Uuid;

mod config;
mod error;
mod pipe_through_hasher;
mod shard_store;

use decode_rs::api::node as node_api;
use decode_rs::api_client::node as node_api_client;
use decode_rs::async_reed_solomon::{AsyncReedSolomon, AsyncReedSolomonError};

use tokio::io as async_io;

/// Parsed and validated node server configuration, along with other
/// shared state and instances:
struct NodeServerState {
    node_id: Uuid,
    public_url: Url,
    coordinator_url: Url,
    max_shard_size: ByteUnit,
    reconstruct_retries: usize,
    shard_store: Arc<shard_store::ShardStore<32>>,
}

#[derive(Clone, Debug)]
struct HexDigest<const N: usize> {
    digest: [u8; N],
}

impl<const N: usize> FromStr for HexDigest<N> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != N * 2 {
            Err(())?
        }

        let mut digest = [0; N];
        hex::decode_to_slice(s, &mut digest).map_err(|_| ())?;

        Ok(HexDigest { digest })
    }
}

impl<'r, const N: usize> FromParam<'r> for HexDigest<N> {
    type Error = ();

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        Self::from_str(param)
    }
}

impl<const N: usize> AsRef<[u8; N]> for HexDigest<N> {
    fn as_ref(&self) -> &[u8; N] {
        &self.digest
    }
}

impl<const N: usize> ToString for HexDigest<N> {
    fn to_string(&self) -> String {
        hex::encode(&self.digest)
    }
}

struct ShardResponse<'a, const N: usize>(shard_store::Shard<'a, N>);
impl<'r, 'a: 'r, const N: usize> Responder<'r, 'a> for ShardResponse<'a, N> {
    fn respond_to(self, _req: &'r Request<'_>) -> response::Result<'a> {
        Response::build()
            .header(ContentType::Binary)
            .streamed_body(self.0)
            .ok()
    }
}

#[post("/shard/<shard_digest>/reconstruct", data = "<reconstruct_map>")]
async fn reconstruct_shard<'a>(
    shard_digest: HexDigest<32>,
    reconstruct_map: Json<node_api::ShardReconstructRequest>,
    state: &State<NodeServerState>,
) -> Result<(), error::APIError> {
    use rand::seq::IteratorRandom;
    use tokio::io::AsyncWriteExt;

    // Perform some basic sanity checks on the request:
    let shard_count = ((reconstruct_map.chunk_size + reconstruct_map.shard_size - 1)
        / reconstruct_map.shard_size)
        + reconstruct_map.code_ratio_parity as u64;
    if shard_count != reconstruct_map.shard_map.len() as u64 {
        log::error!(
            "Passed reconstruct map parameters (chunk_size {}, shard_size {}, \
	     calculated shard count {} data + parity) don't match length of \
	     shard map ({}).",
            reconstruct_map.chunk_size,
            reconstruct_map.shard_size,
            shard_count,
            reconstruct_map.shard_map.len()
        );
        // TODO: proper error handling
        return Err(error::APIError::InternalServerError);
    }

    // Validate the individual shards: ensure that all node references
    // refer to an in-bounds entry of the node_map, and make sure that
    // we have at least `code_ratio_data` shards with nodes present:
    let mut shards_with_nodes = 0;
    for (shard_idx, shard_spec) in reconstruct_map.shard_map.iter().enumerate() {
        if shard_spec
            .nodes
            .iter()
            .find(|n| **n > reconstruct_map.node_map.len() as usize)
            .is_some()
        {
            log::error!(
                "Passed reconstruct map's shard {} references a non-existent node",
                shard_idx,
            );
            // TODO: proper error handling
            return Err(error::APIError::InternalServerError);
        }

        if shard_spec.nodes.len() > 0 {
            shards_with_nodes += 1;
        }
    }

    if shards_with_nodes < reconstruct_map.code_ratio_data {
        log::error!(
            "Received an insufficient number of shards with nodes to \
	     reconstruct the target shard."
        );
        // TODO: proper error handling
        return Err(error::APIError::InternalServerError);
    }

    let parsed_node_map = reconstruct_map
        .node_map
        .iter()
        .map(|node_url_str| {
            reqwest::Url::parse(&node_url_str).map_err(|parse_err| {
                log::error!(
                    "Unable to parse node URL \"{}\", encountered error {:?}",
                    node_url_str,
                    parse_err,
                );

                // TODO: proper error handling
                error::APIError::InternalServerError
            })
        })
        .collect::<Result<Vec<reqwest::Url>, error::APIError>>()?;

    // Our target shard must be part of the set of shards, search for it and
    // determine its index:
    let shard_digest_str = shard_digest.to_string();
    let target_shard_idx = reconstruct_map
        .shard_map
        .iter()
        .enumerate()
        .find(|(_idx, shard_spec)| shard_spec.digest == shard_digest_str)
        .map(|(idx, _shard_spec)| idx)
        .ok_or_else(|| {
            log::error!(
                "Received reconstruct request, but target shard ({:?}) is not in \
	     the set of shards of the to-be reconstructed object.",
                shard_digest,
            );
            error::APIError::InternalServerError
        })?;

    // TODO: error handling!
    let mut async_reed_solomon = AsyncReedSolomon::new(
        reconstruct_map.code_ratio_data as usize,
        reconstruct_map.code_ratio_parity as usize,
        1024 * 1024,
    )
    .unwrap();

    let node_api_client = node_api_client::NodeAPIClient::new();

    let mut rng: rand::rngs::SmallRng =
        rand::SeedableRng::from_rng(&mut rand::thread_rng()).unwrap();

    // Reconstructing the shards could fail for any number of reasons
    // (especially given that this method is going to be triggered in
    // times of nodes failing). So regardless of which errors may
    // occur, try to retry the operation until hitting the retry
    // limit:
    let mut resp = Ok(());
    for retry_count in 0..=state.reconstruct_retries {
        // It's fine for us to return here on error, as we haven't yet allocated
        // any resources we need to free.
        let mut insertion_shard =
            state
                .shard_store
                .insert_shard_by_writer()
                .await
                .map_err(|err| {
                    log::error!("Failed to acquire insertion shard: {:?}", err);
                    error::APIError::InternalServerError
                })?;

        // Instantiate a PipeThroughHasher to calculate a checksum for the
        // streamed data.
        let mut pipethroughhasher =
            pipe_through_hasher::PipeThroughHasher::new(insertion_shard.as_async_writer());

        // Collect all shard-node combinations into one array which we
        // can use to try and find shards to download:
        let parsed_node_map_ref = &parsed_node_map;
        let mut shard_nodes: Vec<(usize, String, String, reqwest::Url)> = reconstruct_map
            .shard_map
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
            reconstruct_map.code_ratio_data as usize + reconstruct_map.code_ratio_parity as usize;
        let mut initiated_shard_download_count = 0;
        let mut initiated_shard_downloads: Vec<_> = (0..shards).map(|_| None).collect();
        let mut input_shards: Vec<_> = (0..shards).map(|_| None).collect();
        while shard_nodes.len() > 0
            && initiated_shard_download_count < reconstruct_map.code_ratio_data
        {
            log::trace!(
                "Trying to find a shard+node to fetch. Remaining shard/node \
		 combinations: {}, required shards: {}, accepted reqs: {}",
                shard_nodes.len(),
                reconstruct_map.code_ratio_data,
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

            // Try to parse the passed digest as a hex-string. While it not
            // being a valid hex-encoded digest would be sufficient reason to
            // outright reject the request, this is more tricky for us to do at
            // this stage (without converting twice), hence we just skip the
            // shard on error:
            let digest = match HexDigest::from_str(&digest_str) {
                Err(()) => {
                    log::error!(
                        "Instrusted to reconstruct shard {:?} from another \
			     which is not a valid hex-encoded digest: \"{}\"",
                        shard_digest,
                        digest_str
                    );
                    continue;
                }
                Ok(digest) => digest,
            };

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
        if initiated_shard_download_count < reconstruct_map.code_ratio_data {
            log::warn!(
                "Could not fetch a sufficient number of shards to reconstruct \
		 shard {:?}, {}",
                shard_digest,
                if retry_count < state.reconstruct_retries {
                    "retring..."
                } else {
                    "giving up!"
                },
            );
            continue;
        }

        // Join the download futures into a single one:
        let download_future = futures::future::try_join_all(
            initiated_shard_downloads.into_iter().filter_map(|e| e).map(
                |(mut stream, mut writer)| async move {
                    use futures::StreamExt;
                    use tokio::io::AsyncWriteExt;

                    while let Some(res) = stream.next().await {
                        let mut buffer = res.map_err(|e| Err(Ok(e)))?;
                        writer
                            .write_all_buf(&mut buffer)
                            .await
                            .map_err(|e| Err(Err(e)))?;
                    }

                    Ok::<(), Result<AsyncReedSolomonError, Result<reqwest::Error, std::io::Error>>>(
                        (),
                    )
                },
            ),
        );

        // We further need to have an array of writers for the desired shards to
        // output. Create it over a `DuplexStream`, as Rust does not like
        // capturing the implicit lifetime bounds associated with the pipe
        // through hasher when we place it in a vector:
        let (output_writer, mut output_reader) = async_io::duplex(64 * 1024);
        let mut output_shards: Vec<Option<async_io::DuplexStream>> =
            (0..shard_count).map(|_| None).collect();
        output_shards[target_shard_idx] = Some(output_writer);

        let reconstruct_future = async {
            use async_io::AsyncWriteExt;

            async_reed_solomon
                .reconstruct_shards(
                    &mut input_shards,
                    &mut output_shards,
                    Some(reconstruct_map.chunk_size as usize),
                )
                .await
                .map_err(|e| Ok(e))?;

            // Finally, shutdown the writers:
            for w in output_shards {
                if let Some(mut writer) = w {
                    writer.shutdown().await.unwrap();
                }
            }

            Ok(())
        };

        // Finally, pipe the output_reader into the pipethroughhasher:
        let output_future = async {
            async_io::copy(&mut output_reader, &mut pipethroughhasher)
                .await
                .map_err(|e| {
                    Err::<AsyncReedSolomonError, Result<reqwest::Error, std::io::Error>>(Err(e))
                })
        };

        // Await all futures:
        let res = futures::try_join!(download_future, reconstruct_future, output_future);

        // Shutdown the stream, which will also flush to the file:
        pipethroughhasher.shutdown().await.map_err(|err| {
            log::error!(
                "Failed to shutdown the pipe-through hasher writer: {:?}",
                err
            );
            error::APIError::InternalServerError
        })?;

        if let Err(e) = res {
            log::error!("Error while reconstructing shard: {:?}", e);
            insertion_shard.abort().await.map_err(|err| {
                log::error!("Error occured while aborting an InsertionShard: {:?}", err);
                error::APIError::InternalServerError
            })?;
        } else {
            // Retrieve the digest from the hasher, which will consume it.
            let calculated_digest = pipethroughhasher.get_digest().map_err(|err| {
                log::error!(
                    "Failed to retrieve the shard digest from the pipe-through hasher: {:?}",
                    err
                );
                error::APIError::InternalServerError
            })?;

            if calculated_digest != *shard_digest.as_ref() {
                // Mismatch between the requested digest and reconstructed data,
                // this is bad! Reject the shard and respond with an error:
                log::error!("Reconstructed shard digest mismatch!");
                insertion_shard.abort().await.map_err(|err| {
                    log::error!("Error occured while aborting an InsertionShard: {:?}", err);
                    error::APIError::InternalServerError
                })?;
            } else {
                // Finalize the inserted shard:
                insertion_shard
                    .finalize(&calculated_digest)
                    .await
                    .map_err(|err| {
                        log::error!("Failed finalizing insertion of chunk: {:?}", err);
                        error::APIError::InternalServerError
                    })?;

                // Everything worked, respond with 200 OK
                resp = Ok(());
                break;
            }
        }
    }

    resp
}

#[post("/shard/<shard_digest>/fetch", data = "<fetch_info>")]
async fn fetch_shard<'a>(
    shard_digest: HexDigest<32>,
    state: &State<NodeServerState>,
    fetch_info: Json<node_api::ShardFetchRequest<'a>>,
) -> Result<(), error::APIError> {
    use tokio::io::AsyncWriteExt;

    let source_node_url = reqwest::Url::parse(&fetch_info.source_node).map_err(|err| {
        // TODO: proper error handling!
        log::error!(
            "Failed to parse the provided source node URL (\"{}\"): {:?}",
            fetch_info.source_node,
            err
        );
        error::APIError::InternalServerError
    })?;

    let node_client = node_api_client::NodeAPIClient::new();

    let mut insertion_shard = state
        .shard_store
        .insert_shard_by_writer()
        .await
        .map_err(|err| {
            log::error!("Failed to acquire insertion shard: {:?}", err);
            error::APIError::InternalServerError
        })?;

    // Instantiate a PipeThroughHasher to calculate a checksum for the
    // streamed data.
    let mut pipethroughhasher =
        pipe_through_hasher::PipeThroughHasher::new(insertion_shard.as_async_writer());

    // Request the shard from the passed source node, piping it into
    // the insertion shard and through the pipe through hasher:
    node_client
        .download_shard(
            &source_node_url,
            &shard_digest,
            &fetch_info.ticket,
            &mut pipethroughhasher,
        )
        .await
        .map_err(|err| {
            // TODO: proper error handling!
            log::error!(
                "Error while fetching shard {:x?} from remote node at \"{}\": {:?}",
                AsRef::<[u8; 32]>::as_ref(&shard_digest),
                &fetch_info.source_node,
                err
            );
            error::APIError::InternalServerError
        })?;

    // Shutdown the stream, which will also flush to the file:
    pipethroughhasher.shutdown().await.map_err(|err| {
        log::error!(
            "Failed to shutdown the pipe-through hasher writer: {:?}",
            err
        );
        error::APIError::InternalServerError
    })?;

    // Retrieve the digest from the hasher, which will consume it.
    let calculated_digest = pipethroughhasher.get_digest().map_err(|err| {
        log::error!(
            "Failed to retrieve the shard digest from the pipe-through hasher: {:?}",
            err
        );
        error::APIError::InternalServerError
    })?;

    if calculated_digest != *shard_digest.as_ref() {
        // Mismatch between the requested digest and received data,
        // this is bad! Reject the shard and respond with an error:
        insertion_shard.abort().await.map_err(|err| {
            log::error!("Error occured while aborting an InsertionShard: {:?}", err);
            error::APIError::InternalServerError
        })?;

        // TODO: repond with a proper error
        Err(error::APIError::InternalServerError)
    } else {
        // Finalize the inserted shard:
        insertion_shard
            .finalize(&calculated_digest)
            .await
            .map_err(|err| {
                log::error!("Failed finalizing insertion of chunk: {:?}", err);
                error::APIError::InternalServerError
            })?;

        // Everything worked, respond with 200 OK
        Ok(())
    }
}

#[get("/shard/<shard_digest>")]
async fn get_shard<'a>(
    shard_digest: HexDigest<32>,
    state: &'a State<NodeServerState>,
) -> Result<ShardResponse<'a, 32>, error::APIError> {
    // Try to retrieve the requested shard from the shard store:
    let shard = state
        .shard_store
        .get_shard(shard_digest.as_ref())
        .await
        .map_err(|err| match err.kind() {
            IOErrorKind::NotFound => error::APIError::ResourceNotFound,
            _ => {
                log::error!("Failed to open shard from shard store: {:?}", err);
                error::APIError::InternalServerError
            }
        })?;

    Ok(ShardResponse(shard))
}

// TODO: Handle error when upload is aborted!
//
// [2022-11-11][20:20:21][node_server][ERROR] Failed to write uploaded data to the pipe-through hasher writer: Custom { kind: Other, error: hyper::Error(Body, Custom { kind: UnexpectedEof, error: "unexpected EOF during chunk size line" }) }
// [2022-11-11][20:20:21][node_server::shard_store][ERROR] An instance of InsertionShard instance must not be dropped.
// [2022-11-11][20:20:21][_][WARN] Responding with registered (internal_server_error) /v0 500 catcher.
#[post("/shard", data = "<data>")]
async fn post_shard(
    data: Data<'_>,
    state: &State<NodeServerState>,
) -> Result<Json<node_api::ShardUploadReceipt<'static, 'static>>, error::APIError> {
    use tokio::io::AsyncWriteExt;

    let mut insertion_shard = state
        .shard_store
        .insert_shard_by_writer()
        .await
        .map_err(|err| {
            log::error!("Failed to acquire insertion shard: {:?}", err);
            error::APIError::InternalServerError
        })?;

    // Instantiate a PipeThroughHasher to calculate a checksum for the
    // streamed data.
    let mut pipethroughhasher =
        pipe_through_hasher::PipeThroughHasher::new(insertion_shard.as_async_writer());

    // 3. Open the provided data payload and stream it to the hasher,
    // which will in turn forward it to the temporary file. We await
    // it, meaning that after this invocation the digest should be
    // complete (seen all data).
    let data_written = data
        .open(state.max_shard_size)
        .stream_to(&mut pipethroughhasher)
        .await
        .map_err(|err| {
            log::error!(
                "Failed to write uploaded data to the pipe-through hasher writer: {:?}",
                err
            );
            error::APIError::InternalServerError
        })?;

    // 4. Shutdown the stream, which will also flush to the file:
    pipethroughhasher.shutdown().await.map_err(|err| {
        log::error!(
            "Failed to shutdown the pipe-through hasher writer: {:?}",
            err
        );
        error::APIError::InternalServerError
    })?;

    if !data_written.complete {
        // Could not read all data provided by the client (ran into max shard
        // size limit). Consume the hasher, ...
        std::mem::drop(pipethroughhasher);

        // ... delete the temporary file ...
        insertion_shard.abort().await.map_err(|err| {
            log::error!(
                "Failed to abort inserting a chunk into the chunk store: {:?}",
                err
            );
            error::APIError::InternalServerError
        })?;

        // ... and report to the client accordingly:
        Err(error::APIError::ShardTooLarge {
            max_bytes: state.max_shard_size.as_u64(),
        })
    } else {
        // 5. Retrieve the digest from the hasher, which will consume it.
        let digest = pipethroughhasher.get_digest().map_err(|err| {
            log::error!(
                "Failed to retrieve the shard digest from the pipe-through hasher: {:?}",
                err
            );
            error::APIError::InternalServerError
        })?;

        // Finalize the inserted shard:
        insertion_shard.finalize(&digest).await.map_err(|err| {
            log::error!("Failed finalizing insertion of chunk: {:?}", err);
            error::APIError::InternalServerError
        })?;

        Ok(Json(node_api::ShardUploadReceipt {
            digest: Cow::Owned(hex::encode(&digest)),
            // TODO: generate an actual receipt
            receipt: Cow::Borrowed("dummy_receipt"),
        }))
    }
}

#[get("/stats")]
async fn get_statistics() -> Json<node_api::NodeStatistics> {
    Json(node_api::NodeStatistics {
        bandwidth: 42,
        cpu_usage: 0,
        disk_usage: 9001, // over 9000
        load_avg: 0.5,
        disk_capacity: 1099511627776,
        disk_free: 549755813888,
    })
}

async fn initial_server_state(
    parsed_config: config::NodeServerConfigInterface,
) -> Result<NodeServerState, String> {
    Ok(NodeServerState {
        node_id: parsed_config
            .node_id
            .ok_or("Node needs to be assigned a unique, valid UUID.".to_string())?,

        public_url: Url::parse(
            &parsed_config
                .public_url
                .ok_or("Node must be supplied its public base URL.".to_string())?,
        )
        .map_err(|err| format!("Failed to parse public base URL configuration: {:?}", err))?,

        coordinator_url: Url::parse(
            &parsed_config
                .coordinator_url
                .ok_or("Node must be passed the coordinator base URL.".to_string())?,
        )
        .map_err(|err| {
            format!(
                "Failed to parse coordinator base URL configuration: {:?}",
                err
            )
        })?,

        shard_store: Arc::new(
            shard_store::ShardStore::new(PathBuf::from(&parsed_config.shards_path), false, 3)
                .await
                .map_err(|err| match err {
                    Ok(lock_id) => format!(
                        "Cannot acquire lock to create ShardStore instance, locked by {}",
                        lock_id
                    ),
                    Err(ioerr) => format!("Cannot create ShardStore instance: {:?}", ioerr),
                })?,
        ),

        max_shard_size: 10.gibibytes(),

        reconstruct_retries: parsed_config.reconstruct_retries,
    })
}

#[launch]
async fn rocket() -> _ {
    // TODO: change log level at runtime based on the configuration file
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Trace)
        .chain(std::io::stdout())
        .apply()
        .unwrap();

    let figment = Figment::from(rocket::Config::default())
        .merge(Serialized::defaults(
            config::NodeServerConfigInterface::default(),
        ))
        .merge(Toml::file("DecodeNode.toml"));

    let parsed_config: config::NodeServerConfigInterface = figment
        .extract()
        .expect("Failed to parse the node server configuration.");

    let node_server_state = match initial_server_state(parsed_config).await {
        Ok(state) => state,
        Err(errmsg) => {
            log::error!("{}", errmsg);
            std::process::exit(1);
        }
    };

    {
        let shard_store = node_server_state.shard_store.clone();
        let node_id = node_server_state.node_id.clone();
        let node_public_url = node_server_state.public_url.clone();
        let coordinator_url = node_server_state.coordinator_url.clone();

        tokio::spawn(async move {
            use futures::stream::StreamExt;

            let coord_api_client =
                decode_rs::api_client::coord::CoordAPIClient::new(coordinator_url.clone());
            let shards = shard_store
                .iter_shards()
                .map(|digest| hex::encode(&digest))
                .collect()
                .await;
            if let Err(e) = coord_api_client
                .register_node(&node_id, &node_public_url, shards)
                .await
            {
                log::error!("Node registration failed: {:?}", e);

                // TODO: kindly request the server to shutdown
                std::process::exit(1);
            }
        });
    }

    rocket::custom(figment)
        .register("/v0/", error::get_catchers())
        // Mount the error catchers also under the top-level, such
        // that we return JSON errors here (although this is not
        // specified under any contract and entirely arbitrary)
        .register("/", error::get_catchers())
        .mount(
            "/v0/",
            routes![
                get_shard,
                post_shard,
                fetch_shard,
                reconstruct_shard,
                get_statistics,
            ],
        )
        .manage(node_server_state)
        .attach(AdHoc::config::<config::NodeServerConfigInterface>())
}
