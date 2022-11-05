use std::path::PathBuf;
use std::io;

use log;
use simple_logger;
use exitcode;

use clap::{Parser, Subcommand};

use tokio::fs as async_fs;

use decode_rs::api_client::coord::CoordAPIClient;
use decode_rs::api_client::reqwest::Url;


const MISC_ERR: exitcode::ExitCode = 1;

fn parse_coord_url<S: AsRef<str>>(url: S) -> Result<Url, exitcode::ExitCode> {
    Url::parse(url.as_ref())
	.map_err(|parseerr| {
	    log::error!("Failed to parse the provided coordinator URL (\"{}\"): {:?}", url.as_ref(), parseerr);
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

async fn upload_command(cmd: UploadCommand) -> Result<(), exitcode::ExitCode> {
    // Try to parse the passed coordinator base URL:
    let parsed_url = parse_coord_url(&cmd.coord_url)?;

    // Create the coordinator API client:
    let coord_api_client = CoordAPIClient::new(parsed_url);

    // In order to initiate an upload at the coordinator, we need to stat the
    // object size we are going to upload. However, as the documentation of
    // [std::io::Metadata::is_file] outlines, the most reliable way to detect
    // whether we can access a file is to open it. Then we can still obtain the
    // metadata and read the file's length:
    let object_file = async_fs::File::open(&cmd.object_path).await
	.map_err(|ioerr| match ioerr.kind() {
	    io::ErrorKind::NotFound => {
		log::error!("Cannot find object at path \"{}\".", &cmd.object_path);
		exitcode::NOINPUT
	    },
	    io::ErrorKind::PermissionDenied => {
		log::error!("Permission denied while trying to access \"{}\".", &cmd.object_path);
		exitcode::NOPERM
	    },
	    _ => {
		log::error!("Error while trying to access \"{}\": {:?}.", &cmd.object_path, ioerr);
		MISC_ERR
	    },
	})?;

    // Now, try to obtain the file's length:
    let object_file_meta = object_file.metadata().await
	.map_err(|ioerr| {
	    // We don't special case here, as we expect that opening the file
	    // will uncover most plausible error cases.
	    log::error!("Error while obtaining the object file's metadata: {:?}", ioerr);
	    MISC_ERR
	})?;

    let object_size = object_file_meta.len();

    // With the object length determined, make a request to the coordinator:
    let upload_map =
	(match coord_api_client.upload_object(object_size).await {
	    Err(e) => {
		log::error!("An error occured while issuing the initial upload request to the coordinator: {:?}", e);
		Err(MISC_ERR)
	    },
	    Ok(map) => Ok(map),
	})?;

    println!("Got upload map: {:?}", upload_map);

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
    }).unwrap();

    // Handle each subcommand seperately:
    let ec: exitcode::ExitCode = match cli.command {
	Commands::Upload(cmd) => upload_command(cmd),
    }.await.map_or_else(|ec| ec, |()| exitcode::OK);

    // Exit with the appropriate exit code:
    std::process::exit(ec);


    // // You can check the value provided by positional arguments, or option
    // // arguments:
    // if let Some(name) = cli.name.as_deref() {
    //     println!("Value for name: {}", name);
    // }

    // if let Some(config_path) = cli.config.as_deref() {
    //     println!("Value for config: {}", config_path.display());
    // }

    // // You can see how many times a particular flag or argument occurred
    // // Note, only flags can have multiple occurrences
    // match cli.debug {
    //     0 => println!("Debug mode is off"),
    //     1 => println!("Debug mode is kind of on"),
    //     2 => println!("Debug mode is on"),
    //     _ => println!("Don't be crazy"),
    // }

    // // You can check for the existence of subcommands, and if found use their
    // // matches just as you would the top level cmd
    // match &cli.command {
    //     Some(Commands::Upload(UploadCommand { list })) => {
    //         if *list {
    //             println!("Printing testing lists...");
    //         } else {
    //             println!("Not printing testing lists...");
    //         }
    //     }
    //     None => {}
    // }

    // Continued program logic goes here...
}
