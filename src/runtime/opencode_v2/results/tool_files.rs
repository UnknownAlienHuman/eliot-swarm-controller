//! Exact binary/file content retrieval from a completed native tool result.
//! The selector never supplies a path or URL: it can only name a file item that
//! is already present in the exact projected assistant message.
use super::{complete_assistant, unavailable};
use crate::{
    error::{Error, Result},
    model,
    runtime::opencode_v2::{Options, Service},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::Url;
use serde_json::{Value, json};
use std::{
    path::{Component, Path, PathBuf},
    str,
};

const MAX_TOOL_FILE_BYTES: usize = 64 * 1024 * 1024;
const MAX_TOOL_CALL_ID_BYTES: usize = 512;
const MAX_TOOL_NAME_BYTES: usize = 512;
const MAX_NATIVE_NAME_BYTES: usize = 4096;
const MAX_MIME_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Descriptor {
    uri: String,
    mime: String,
    name: Option<String>,
    tool_name: String,
    tool_status: String,
}

pub(super) struct ToolFile {
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub source: Value,
}

pub(super) struct ToolFileLocator<'a> {
    pub root: &'a str,
    pub session: &'a str,
    pub message_id: &'a str,
    pub tool_call_id: &'a str,
    pub content_index: usize,
}

fn bounded_text(value: &Value, field: &str, limit: usize) -> Result<String> {
    let text = model::text(value, field)?;
    if text.len() > limit || text.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(unavailable("NATIVE_TOOL_FILE_SCHEMA"));
    }
    Ok(text.to_owned())
}

fn valid_mime(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_MIME_BYTES
        || value
            .bytes()
            .any(|byte| !byte.is_ascii() || byte.is_ascii_control())
    {
        return false;
    }
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    let token = |part: &str| {
        !part.is_empty()
            && part.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            })
    };
    token(kind) && token(subtype)
}

fn extract_descriptor(
    message: &Value,
    tool_call_id: &str,
    content_index: usize,
) -> Result<Descriptor> {
    if tool_call_id.is_empty()
        || tool_call_id.len() > MAX_TOOL_CALL_ID_BYTES
        || tool_call_id.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(
            "tool_call_id is invalid or exceeds the boundary",
        ));
    }
    let content = message["content"]
        .as_array()
        .ok_or_else(|| unavailable("NATIVE_MESSAGE_SCHEMA"))?;
    let mut found = None;
    for part in content {
        if part["type"] != "tool" || part["id"] != tool_call_id {
            continue;
        }
        if found.is_some() {
            return Err(unavailable("NATIVE_TOOL_CALL_DUPLICATE"));
        }
        let status = part["state"]["status"]
            .as_str()
            .filter(|status| matches!(*status, "completed" | "error"))
            .ok_or_else(|| unavailable("RESULT_TOOL_NOT_COMPLETE"))?;
        let items = part["state"]["content"]
            .as_array()
            .ok_or_else(|| unavailable("RESULT_TOOL_FILE_NOT_AVAILABLE"))?;
        let item = items
            .get(content_index)
            .ok_or_else(|| unavailable("RESULT_TOOL_FILE_NOT_AVAILABLE"))?;
        model::fields(item, &["type", "uri", "mime", "name"])
            .map_err(|_| unavailable("NATIVE_TOOL_FILE_SCHEMA"))?;
        if item["type"] != "file" {
            return Err(unavailable("RESULT_TOOL_FILE_NOT_AVAILABLE"));
        }
        let mime = bounded_text(item, "mime", MAX_MIME_BYTES)?;
        if !valid_mime(&mime) {
            return Err(unavailable("NATIVE_TOOL_FILE_SCHEMA"));
        }
        let uri = bounded_text(item, "uri", MAX_TOOL_FILE_BYTES * 2)?;
        let name = match item.get("name") {
            None | Some(Value::Null) => None,
            Some(Value::String(name))
                if name.len() <= MAX_NATIVE_NAME_BYTES
                    && !name.bytes().any(|byte| byte.is_ascii_control()) =>
            {
                Some(name.clone())
            }
            Some(_) => return Err(unavailable("NATIVE_TOOL_FILE_SCHEMA")),
        };
        let tool_name = bounded_text(part, "name", MAX_TOOL_NAME_BYTES)?;
        found = Some(Descriptor {
            uri,
            mime,
            name,
            tool_name,
            tool_status: status.to_owned(),
        });
    }
    found.ok_or_else(|| unavailable("RESULT_TOOL_FILE_NOT_AVAILABLE"))
}

fn decode_data_uri(uri: &str, expected_mime: &str) -> Result<Vec<u8>> {
    let body = uri
        .strip_prefix("data:")
        .ok_or_else(|| unavailable("RESULT_TOOL_FILE_URI_UNSUPPORTED"))?;
    let (metadata, payload) = body
        .split_once(',')
        .ok_or_else(|| unavailable("NATIVE_TOOL_FILE_SCHEMA"))?;
    let mime = metadata
        .strip_suffix(";base64")
        .filter(|mime| !mime.contains(';') && valid_mime(mime))
        .ok_or_else(|| unavailable("RESULT_TOOL_FILE_URI_UNSUPPORTED"))?;
    if !mime.eq_ignore_ascii_case(expected_mime) {
        return Err(unavailable("RESULT_TOOL_FILE_MIME_CHANGED"));
    }
    if payload.len() > MAX_TOOL_FILE_BYTES.div_ceil(3) * 4 {
        return Err(unavailable("RESULT_TOOL_FILE_LIMIT"));
    }
    let bytes = STANDARD
        .decode(payload)
        .map_err(|_| unavailable("NATIVE_TOOL_FILE_SCHEMA"))?;
    if bytes.len() > MAX_TOOL_FILE_BYTES || STANDARD.encode(&bytes) != payload {
        return Err(unavailable("NATIVE_TOOL_FILE_SCHEMA"));
    }
    Ok(bytes)
}

fn file_path(uri: &str, root: &Path) -> Result<PathBuf> {
    let url = Url::parse(uri).map_err(|_| unavailable("NATIVE_TOOL_FILE_SCHEMA"))?;
    if url.scheme() != "file"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url
            .host_str()
            .is_some_and(|host| !host.eq_ignore_ascii_case("localhost"))
    {
        return Err(unavailable("RESULT_TOOL_FILE_URI_UNSUPPORTED"));
    }
    let absolute = url
        .to_file_path()
        .map_err(|_| unavailable("NATIVE_TOOL_FILE_SCHEMA"))?;
    let relative = absolute
        .strip_prefix(root)
        .map_err(|_| unavailable("RESULT_TOOL_FILE_OUTSIDE_SCOPE"))?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(unavailable("RESULT_TOOL_FILE_OUTSIDE_SCOPE"));
    }
    Ok(relative.to_owned())
}

impl Service {
    async fn tool_file_bytes(
        &self,
        descriptor: &Descriptor,
        options: &Options,
        scope: &Value,
    ) -> Result<(Vec<u8>, &'static str)> {
        if descriptor.uri.starts_with("data:") {
            return Ok((
                decode_data_uri(&descriptor.uri, &descriptor.mime)?,
                "inline_data_uri",
            ));
        }
        if descriptor.uri.starts_with("file:") {
            let location = model::text(&scope["location"], "directory")
                .map_err(|_| unavailable("NATIVE_LOCATION_MISMATCH"))?;
            if Path::new(location) != options.directory {
                return Err(unavailable("NATIVE_LOCATION_MISMATCH"));
            }
            let relative = file_path(&descriptor.uri, &options.directory)?;
            let body = self
                .get_location_file(&relative, &options.directory, MAX_TOOL_FILE_BYTES)
                .await?;
            if !body.media_type.eq_ignore_ascii_case(&descriptor.mime) {
                return Err(unavailable("RESULT_TOOL_FILE_MIME_CHANGED"));
            }
            return Ok((body.bytes, "native_location_fs_read"));
        }
        Err(unavailable("RESULT_TOOL_FILE_URI_UNSUPPORTED"))
    }

    pub(super) async fn tool_file(
        &self,
        locator: ToolFileLocator<'_>,
        options: &Options,
        scope: &Value,
    ) -> Result<ToolFile> {
        let message = self.message(locator.session, locator.message_id).await?;
        complete_assistant(&message)?;
        if locator.session == locator.root && message["model"] != json!(options.model) {
            return Err(unavailable("RESULT_MODEL_CHANGED"));
        }
        let descriptor = extract_descriptor(&message, locator.tool_call_id, locator.content_index)?;
        let message_digest = format!(
            "sha256:{}",
            model::digest(model::canonical(&message)?.as_bytes())
        );
        let descriptor_document = json!({
            "uri":&descriptor.uri,
            "mime":&descriptor.mime,
            "name":&descriptor.name,
            "tool_name":&descriptor.tool_name,
            "tool_status":&descriptor.tool_status
        });
        let descriptor_digest = format!(
            "sha256:{}",
            model::digest(model::canonical(&descriptor_document)?.as_bytes())
        );
        let uri_digest = format!("sha256:{}", model::digest(descriptor.uri.as_bytes()));
        let (bytes, retrieval) = self.tool_file_bytes(&descriptor, options, scope).await?;
        let second_message = self.message(locator.session, locator.message_id).await?;
        complete_assistant(&second_message)?;
        let second_descriptor =
            extract_descriptor(&second_message, locator.tool_call_id, locator.content_index)?;
        if second_message != message || second_descriptor != descriptor {
            return Err(unavailable("RESULT_SOURCE_CHANGED"));
        }
        let (second_bytes, second_retrieval) = self
            .tool_file_bytes(&second_descriptor, options, scope)
            .await?;
        if second_retrieval != retrieval
            || second_bytes.len() != bytes.len()
            || model::digest(&second_bytes) != model::digest(&bytes)
        {
            return Err(unavailable("RESULT_SOURCE_CHANGED"));
        }
        let name = descriptor.name.clone();
        Ok(ToolFile {
            bytes,
            media_type: descriptor.mime.clone(),
            source: json!({
                "kind":"tool_file",
                "native_session_id":locator.session,
                "message_id":locator.message_id,
                "tool_call_id":locator.tool_call_id,
                "content_index":locator.content_index,
                "tool_name":&descriptor.tool_name,
                "tool_status":&descriptor.tool_status,
                "native_name":name,
                "mime":&descriptor.mime,
                "uri_scheme":if descriptor.uri.starts_with("data:") {"data"} else {"file"},
                "uri_digest":uri_digest,
                "descriptor_digest":descriptor_digest,
                "message_digest":message_digest,
                "retrieval":retrieval,
                "read_method":if retrieval=="inline_data_uri" {"session.message.get"} else {"session.message.get+fs.read"},
                "source_size_limit":MAX_TOOL_FILE_BYTES,
                "raw_uri_persisted":false,
                "native_path_persisted":false
            }),
        })
    }
}
