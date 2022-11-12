#[macro_use]
extern crate rocket;

use std::borrow::Cow;
use std::io::ErrorKind as IOErrorKind;
use std::path::PathBuf;
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

/// Parsed and validated node server configuration, along with other
/// shared state and instances:
struct NodeServerState {
    node_id: Uuid,
    public_url: Url,
    coordinator_url: Url,
    max_shard_size: ByteUnit,
    shard_store: Arc<shard_store::ShardStore<32>>,
}

struct HexDigest<const N: usize> {
    digest: [u8; N],
}

impl<const N: usize> HexDigest<N> {
    fn get_digest(&self) -> &[u8; N] {
        &self.digest
    }
}

impl<'r, const N: usize> FromParam<'r> for HexDigest<N> {
    type Error = ();

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        if param.len() != N * 2 {
            Err(())?
        }

        let mut digest = [0; N];
        hex::decode_to_slice(param, &mut digest).map_err(|_| ())?;

        Ok(HexDigest { digest })
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

#[get("/shard/<shard_digest>")]
async fn get_shard<'a>(
    shard_digest: HexDigest<32>,
    state: &'a State<NodeServerState>,
) -> Result<ShardResponse<'a, 32>, error::APIError> {
    // Try to retrieve the requested shard from the shard store:
    let shard = state
        .shard_store
        .get_shard(shard_digest.get_digest())
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
            // TODO: return proper size!
            size: 0,
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
        .level(log::LevelFilter::Warn)
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
        .mount("/v0/", routes![get_shard, post_shard, get_statistics])
        .manage(node_server_state)
        .attach(AdHoc::config::<config::NodeServerConfigInterface>())
}
