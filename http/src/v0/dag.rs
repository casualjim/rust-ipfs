use crate::v0::support::{
    try_only_named_multipart, with_ipfs, MaybeTimeoutExt, NotImplemented, StringError,
    StringSerialized,
};
use cid::Cid;
use futures::stream::Stream;
use ipfs::{
    ipld::{DAG_CBOR, DAG_JSON, DAG_PB, DAG_RAW},
    Ipfs, IpfsTypes,
};
use mime::Mime;

use serde::Deserialize;
use serde_json::json;
use warp::{query, reply, Buf, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    format: Option<String>,
    hash: Option<String>,
    #[serde(rename = "input-enc", default)]
    encoding: InputEncoding,
}

#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum InputEncoding {
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "json")]
    Json,
}

impl Default for InputEncoding {
    fn default() -> Self {
        InputEncoding::Json
    }
}

pub fn put<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<PutQuery>())
        .and(warp::header::<Mime>("content-type")) // TODO: rejects if missing
        .and(warp::body::stream())
        .and_then(put_query)
}

async fn put_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: PutQuery,
    mime: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    use multihash::{Code, Multihash, MultihashDigest};

    if query.encoding != InputEncoding::Raw {
        return Err(NotImplemented.into());
    }

    let (format, v0_fmt) = match query.format.as_deref().unwrap_or("dag-cbor") {
        "dag-cbor" => (DAG_CBOR, false),
        "dag-pb" => (DAG_PB, true),
        "dag-json" => (DAG_JSON, false),
        "raw" => (DAG_RAW, false),
        _ => return Err(StringError::from("unknown codec").into()),
    };

    let (hasher, v0_hash): (fn(&[u8]) -> Multihash, bool) =
        match query.hash.as_deref().unwrap_or("sha2-256") {
            "sha2-256" => (|i: &[u8]| Code::Sha2_256.digest(i), true),
            "sha2-512" => (|i: &[u8]| Code::Sha2_512.digest(i), false),
            "sha3-512" => (|i: &[u8]| Code::Sha3_512.digest(i), false),
            _ => return Err(StringError::from("unknown hash").into()),
        };

    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let data = try_only_named_multipart(&["data", "file"], 1024 * 1024, boundary, body)
        .await
        .map_err(StringError::from)?;

    let digest = hasher(&data);

    let cid = if v0_fmt && v0_hash {
        // this is quite ugly way but apparently js-ipfs generates a v0 cid for this combination
        // which is also created by go-ipfs
        Cid::new_v0(digest).expect("cidv0 creation cannot fail for dag-pb and sha2-256")
    } else {
        Cid::new_v1(format, digest)
    };

    let reply = json!({
        "Cid": { "/": cid.to_string() }
    });

    // delay reallocation until cid has been generated
    let data = data.into_boxed_slice();
    let block = ipfs::Block { cid, data };
    ipfs.put_block(block).await.map_err(StringError::from)?;
    Ok(reply::json(&reply))
}

/// Per https://docs-beta.ipfs.io/reference/http/api/#api-v0-block-resolve this endpoint takes in a
/// path and resolves it to the last block (the cid), and to the path inside the final block
/// (rempath).
pub fn resolve<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<ResolveOptions>())
        .and_then(inner_resolve)
}

#[derive(Debug, Deserialize)]
struct ResolveOptions {
    arg: String,
    timeout: Option<StringSerialized<humantime::Duration>>,
    #[serde(rename = "local-resolve", default)]
    local_resolve: bool,
}

async fn inner_resolve<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    opts: ResolveOptions,
) -> Result<impl Reply, Rejection> {
    use ipfs::IpfsPath;
    use std::convert::TryFrom;

    let path = IpfsPath::try_from(opts.arg.as_str()).map_err(StringError::from)?;

    // I think the naming of local_resolve is quite confusing. when following links we "resolve
    // globally" and when not following links we are "resolving locally", or in single document.
    let follow_links = !opts.local_resolve;

    let (resolved, remaining) = ipfs
        .dag()
        .resolve(path, follow_links)
        .maybe_timeout(opts.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?;

    let current = resolved.source();

    Ok(reply::json(&json!({
        "Cid": { "/": current.to_string() },
        "RemPath": StringSerialized(remaining),
    })))
}
