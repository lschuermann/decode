#[macro_use]
extern crate rocket;

use std::path::PathBuf;

use rocket::data::{ByteUnit, ToByteUnit};
use rocket::fairing::AdHoc;
use rocket::fs::NamedFile;
use rocket::request::FromParam;
use rocket::{Data, State};

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

/// Parsed and validated node server configuration, along with other
/// shared state and instances:
struct NodeServerState {
    node_id: Uuid,
    public_url: Url,
    coordinator_url: Url,
    shards_path: PathBuf,
    digest_path_gen: shard_store::DigestPathGenerator<32>,
    max_shard_size: ByteUnit,
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

#[get("/shard/<shard_digest>")]
async fn get_shard(
    shard_digest: HexDigest<32>,
    state: &State<NodeServerState>,
) -> Option<NamedFile> {
    let shard_path = state
        .digest_path_gen
        .get_path(&state.shards_path, shard_digest.get_digest());

    NamedFile::open(&shard_path).await.ok()
}

#[post("/shard", data = "<data>")]
async fn post_shard(data: Data<'_>, state: &State<NodeServerState>) -> Result<(), error::APIError> {
    use tokio::fs::{
        create_dir_all as async_create_dir_all, rename as async_rename_file, File as AsyncFile,
    };
    use tokio::io::AsyncWriteExt;

    // 1. Create a temporary file path:
    let tmppath = PathBuf::from("foo.txt"); // TODO: make sure that this is unique

    // 1. Create and open a temporary file to stream the intermediate
    // data to:
    let tmpfile = AsyncFile::create(&tmppath).await.map_err(|err| {
        log::error!(
            "Failed to write client contents to temporary file: {:?}",
            err
        );
        error::APIError::InternalServerError
    })?;

    // 2. Instantiate a PipeThroughHasher to calculate a checksum for
    // the streamed data.
    let mut pipethroughhasher = pipe_through_hasher::PipeThroughHasher::new(tmpfile);

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
        // TODO: delete temporary file!

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

        // 6. Move the temporary file to its final destination path:
        let shard_path = state.digest_path_gen.get_path(&state.shards_path, &digest);
        async_create_dir_all(&shard_path.parent().unwrap())
            .await
            .unwrap();
        async_rename_file(&tmppath, &shard_path).await.unwrap();

        Ok(())
    }
}

fn initial_server_state(
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

        shards_path: PathBuf::from(parsed_config.shards_path),

        digest_path_gen: shard_store::DigestPathGenerator::new(3),

        max_shard_size: 10.gibibytes(),
    })
}

#[launch]
fn rocket() -> _ {
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
        .level(log::LevelFilter::Debug)
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

    let node_server_state = match initial_server_state(parsed_config) {
        Ok(state) => state,
        Err(errmsg) => {
            log::error!("{}", errmsg);
            std::process::exit(1);
        }
    };

    rocket::custom(figment)
        .register("/v0/", error::get_catchers())
        // Mount the error catchers also under the top-level, such
        // that we return JSON errors here (although this is not
        // specified under any contract and entirely arbitrary)
        .register("/", error::get_catchers())
        .mount("/v0/", routes![get_shard, post_shard])
        .attach(AdHoc::config::<config::NodeServerConfigInterface>())
        .manage(node_server_state)
}
