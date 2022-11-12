use std::borrow::Cow;
use std::io::Cursor;

use rocket::http::{ContentType, Status};
use rocket::response::{Responder, Result as ResponseResult};
use rocket::Catcher;
use rocket::{Request, Response};

use decode_rs::api::node as node_api;

pub enum APIError {
    ShardTooLarge { max_bytes: u64 },

    ResourceNotFound,

    InternalServerError,
}

fn json_error_response<'r>(err: &node_api::APIError) -> Response<'r> {
    let serialized = err.serialize_json();

    Response::build()
        .status(Status {
            code: err.http_status_code().map(|s| s.get()).unwrap_or(500),
        })
        .header(ContentType::JSON)
        .sized_body(serialized.len(), Cursor::new(serialized))
        .finalize()
}

impl<'r, 'o: 'r> Responder<'r, 'o> for APIError {
    fn respond_to(self, req: &'r Request<'_>) -> ResponseResult<'o> {
        match self {
            APIError::ShardTooLarge { max_bytes } => {
                Ok(json_error_response(&node_api::APIError::ShardTooLarge {
                    max_bytes,
                    description: Cow::Borrowed(
                        "Uploaded shard exceeds this node's maximum shard size limitation.",
                    ),
                }))
            }

            APIError::ResourceNotFound => {
                // Use the generic 404 responder to avoid leaking
                // further information about the requested object.
                Status::NotFound.respond_to(req)
            }

            APIError::InternalServerError => {
                // Use the generic 500 responder, which also logs the
                // error.
                Status::InternalServerError.respond_to(req)
            }
        }
    }
}

// ---------- Generic Rocket Error Catchers ----------

#[catch(404)]
fn not_found(_req: &Request) -> String {
    let err = node_api::APIError::ResourceNotFound {
        description: Cow::Borrowed("The requested resource cound not be found."),
    };

    debug_assert!(err.http_status_code().map(|s| s.get()) == Some(404));

    err.serialize_json()
}

#[catch(500)]
fn internal_server_error(_req: &Request) -> String {
    let err = node_api::APIError::InternalServerError {
        description: Cow::Borrowed("An internal server error occured."),
    };

    debug_assert!(err.http_status_code().map(|s| s.get()) == Some(500));

    err.serialize_json()
}

pub fn get_catchers() -> Vec<Catcher> {
    catchers![not_found, internal_server_error,]
}
