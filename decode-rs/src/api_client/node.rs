use std::num::NonZeroU16;

use reqwest;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::api::node as node_api;

#[derive(Debug, Clone)]
pub enum NodeAPIClientError {
    /// A timeout occured while performing the request
    Timeout,

    /// Unable to connect to the remote host
    Connect,

    // TODO: Don't expose this generic error type. All API errors
    // should be converted into specific variants of this generic
    // client error enum type, or specific error enums of the various
    // request we support.
    APIError(node_api::APIError<'static, 'static>),

    /// Unknown error
    Unknown,
}

impl NodeAPIClientError {
    fn from_reqwest_generic_err(err: reqwest::Error) -> Result<Self, reqwest::Error> {
        if err.is_timeout() {
            Ok(NodeAPIClientError::Timeout)
        } else if err.is_connect() {
            Ok(NodeAPIClientError::Connect)
        } else {
            Err(err)
        }
    }
}

#[derive(Debug, Clone)]
pub enum NodeAPIUploadError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(NodeAPIClientError),
}

impl NodeAPIUploadError {
    fn from_reqwest_error(err: reqwest::Error) -> NodeAPIUploadError {
        // Handle the generic reqwest error cases first:
        match NodeAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => NodeAPIUploadError::MiscError(generic_err),
            Err(_err) => {
                // There should be no upload-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                NodeAPIUploadError::MiscError(NodeAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: node_api::APIError<'desc, 'resp_body>,
    ) -> NodeAPIUploadError {
        NodeAPIUploadError::MiscError(NodeAPIClientError::APIError(apierr.into_owned()))
    }
}

pub struct NodeAPIClient {
    http_client: reqwest::Client,
}

impl NodeAPIClient {
    pub fn new() -> Self {
        NodeAPIClient {
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn upload_shard(
        &self,
        base_url: &reqwest::Url,
        ticket: impl AsRef<str>,
        reader: impl AsyncRead + AsyncReadExt + Unpin + Send + Sync + 'static,
    ) -> Result<(), NodeAPIUploadError> {
        // Create a [`Body`] object which wraps a [`Stream`], which in
        // turn is generated from the [`AsyncRead`] passed in:
        let body = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
            reader,
            tokio_util::codec::BytesCodec::new(),
        ));

        let resp = self
            .http_client
            .post(
                base_url
                    .join("/v0/shard")
                    .expect("Failed to construct shard upload URL"),
            )
            .header(
                reqwest::header::AUTHORIZATION,
                &format!("Bearer {}", ticket.as_ref()),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .map_err(NodeAPIUploadError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        let bytes = resp
            .bytes()
            .await
            .map_err(NodeAPIUploadError::from_reqwest_error)?;
        let parsed = node_api::ShardUploadResponse::from_http_resp(status, bytes.as_ref());

        match parsed {
            node_api::ShardUploadResponse::Success(_) => Ok(()),
            node_api::ShardUploadResponse::APIError(api_err) => {
                Err(NodeAPIUploadError::from_api_error(api_err))
            }
        }
    }
}
