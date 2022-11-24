use std::borrow::Borrow;
use std::io;
use std::num::NonZeroU16;

use reqwest;

use futures_util::stream::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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

#[derive(Debug, Clone)]
pub enum NodeAPIDownloadError {
    /// Miscellaneous client error not specific to this request type:
    MiscError(NodeAPIClientError),

    /// IO Error while writing to the provided writer
    WriteIOError(io::ErrorKind),
}

impl NodeAPIDownloadError {
    fn from_reqwest_error(err: reqwest::Error) -> NodeAPIDownloadError {
        // Handle the generic reqwest error cases first:
        match NodeAPIClientError::from_reqwest_generic_err(err) {
            Ok(generic_err) => NodeAPIDownloadError::MiscError(generic_err),
            Err(_err) => {
                // There should be no download-specific cases which can
                // cause a [reqwest::Error] to occur. Hence return a
                // miscellaneous Unknown error:
                NodeAPIDownloadError::MiscError(NodeAPIClientError::Unknown)
            }
        }
    }

    fn from_api_error<'desc, 'resp_body>(
        apierr: node_api::APIError<'desc, 'resp_body>,
    ) -> NodeAPIDownloadError {
        NodeAPIDownloadError::MiscError(NodeAPIClientError::APIError(apierr.into_owned()))
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
        base_url: impl Borrow<reqwest::Url>,
        ticket: impl AsRef<str>,
        reader: impl AsyncRead + AsyncReadExt + Unpin + Send + Sync + 'static,
    ) -> Result<node_api::ShardUploadReceipt<'static, 'static>, NodeAPIUploadError> {
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
                    .borrow()
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
            node_api::ShardUploadResponse::Success(receipt) => Ok(receipt.into_owned()),
            node_api::ShardUploadResponse::APIError(api_err) => {
                Err(NodeAPIUploadError::from_api_error(api_err))
            }
        }
    }

    pub async fn download_shard_req<'a>(
        &self,
        base_url: impl Borrow<reqwest::Url>,
        shard_digest: impl AsRef<[u8; 32]>,
        ticket: impl AsRef<str>,
    ) -> Result<
        impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>>,
        NodeAPIDownloadError,
    > {
        let resp = self
            .http_client
            .get(
                base_url
                    .borrow()
                    .join("/v0/shard/")
                    .and_then(|url| url.join(&hex::encode(shard_digest.as_ref())))
                    .expect("Failed to construct shard retrieval URL"),
            )
            .header(
                reqwest::header::AUTHORIZATION,
                &format!("Bearer {}", ticket.as_ref()),
            )
            .send()
            .await
            .map_err(NodeAPIDownloadError::from_reqwest_error)?;

        let status = NonZeroU16::new(resp.status().as_u16()).unwrap();
        if status != NonZeroU16::new(200).unwrap() {
            // Received some non-200 OK response, parse as error and return:
            let bytes = resp
                .bytes()
                .await
                .map_err(NodeAPIDownloadError::from_reqwest_error)?;
            let parsed = node_api::ShardRetrievalError::from_http_resp(status, bytes.as_ref());
            return Err(NodeAPIDownloadError::from_api_error(parsed.0));
        } else {
            // Status looks good, return the response body as a byte-stream:
            Ok(resp.bytes_stream())
        }
    }

    // TODO: document. This does not shut down the writer!
    pub async fn download_shard(
        &self,
        base_url: impl Borrow<reqwest::Url>,
        shard_digest: impl AsRef<[u8; 32]>,
        ticket: impl AsRef<str>,
        mut writer: impl AsyncWrite + AsyncWriteExt + Unpin,
    ) -> Result<(), NodeAPIDownloadError> {
        let mut resp_stream = self
            .download_shard_req(base_url, shard_digest, ticket)
            .await?;

        while let Some(item) = resp_stream.next().await {
            match item {
                Err(reqwest_error) => {
                    return Err(NodeAPIDownloadError::from_reqwest_error(reqwest_error))
                }
                Ok(mut buffer) => writer
                    .write_all_buf(&mut buffer)
                    .await
                    .map_err(|ioerror| NodeAPIDownloadError::WriteIOError(ioerror.kind()))?,
            }
        }

        Ok(())
    }
}
