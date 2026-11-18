//! Read-only OpenCode model catalogue from the frozen native child.
//!
//! The plugin runs its frozen snapshot `cmd` with `models --standalone`
//! (observed `opencode models --standalone`, opencode 2.2.7): no session, no turn and no
//! inference. donor: `plugins/antigravity/src/native/models.rs`
//! (`run_models`) for the bounded child pattern (exact argv, stdin null,
//! piped stdout with an output cap, stderr discarded, kill-and-reap under a
//! deadline, static error strings). The daemon owns one reader Job per
//! invocation and confirms family closure before returning its result. The
//! installed `--help` documents `models --standalone` as a read-only surface;
//! `GET /openapi.json` of `opencode serve` describes the per-session
//! `GET /api/model` inventory instead, which needs a live session and is
//! therefore not the catalogue reader.

use std::{ffi::OsStr, path::Path, process::Stdio, time::Duration};

use eliot_core::api::harness::native_model_display_name;
use eliot_core::{Error, Result};
use serde_json::Value;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout, timeout_at},
};

/// Keep the private server under the reader Job, never the background service.
pub(crate) fn models_argv() -> [&'static str; 2] {
    ["models", "--standalone"]
}

/// Output cap for the catalogue read: the observed catalogue is tens of KiB
/// (408 ids), so 512 KiB leaves headroom while bounding a misbehaving child.
pub(crate) const MAX_MODELS_BYTES: usize = 512 * 1024;

/// Maximum catalogue ids, mirroring `MAX_HARNESS_MODELS` in Core.
pub(crate) const MAX_MODELS: usize = 1024;

/// Maximum model id bytes, mirroring `MAX_HARNESS_MODEL_ID_BYTES` in Core.
pub(crate) const MAX_MODEL_ID_BYTES: usize = 256;

/// Exact argv for the model metadata read: `api model.list --standalone`. The
/// same read-only native surface as `models --standalone` (no shared server, no
/// session, no turn, no inference): it answers the installed `model.list`
/// operation (`GET /api/model`) against a private server, so
/// it sees the same authenticated providers as `models --standalone`.
pub(crate) fn model_list_argv() -> [&'static str; 3] {
    ["api", "model.list", "--standalone"]
}

/// Output cap for the metadata read: the observed inventory is ~320 KiB
/// UTF-8 (425 entries), so 2 MiB leaves headroom while bounding a
/// misbehaving child.
pub(crate) const MAX_MODEL_LIST_BYTES: usize = 2 * 1024 * 1024;

/// Maximum variant ids kept per model; the installed inventory peaks at
/// seven, so anything larger is a corrupt read, never a wider wardrobe.
pub(crate) const MAX_VARIANTS_PER_MODEL: usize = 64;

/// One catalogue model's presentation label and selectable variant ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeModelMetadata {
    /// Native `provider/model` id, verbatim.
    pub id: String,
    /// Verbatim `Model.Info.name` when present and valid.
    pub display_name: Option<String>,
    /// Selectable `Model.Variant` ids, when the model reports a variants array.
    pub variants: Option<Vec<String>>,
}

/// Run the frozen native exe with exact argv `api model.list --standalone` and return
/// one entry per reported model with metadata, in wire
/// order. Bounded by `deadline` and [`MAX_MODEL_LIST_BYTES`]; like
/// [`run_models`] this is read-only (no session, no inference) and any
/// failure is a static typed string naming the `model.list` surface. The
/// daemon closes this reader's whole family before deciding whether an
/// optional metadata failure permits the ids-only catalogue.
pub(crate) async fn run_model_metadata(
    program: &OsStr,
    deadline: Instant,
    xdg: Option<(&Path, &Path)>,
) -> Result<Vec<NativeModelMetadata>> {
    let mut command = Command::new(program);
    if let Some((config_home, data_home)) = xdg {
        command
            .env("XDG_CONFIG_HOME", config_home)
            .env("XDG_DATA_HOME", data_home);
    }
    command
        .args(model_list_argv())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode model.list could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode model.list could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_MODEL_LIST_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode model.list did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode model.list metadata was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_MODEL_LIST_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode model.list metadata was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode model.list did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode model.list metadata was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode model.list did not return a catalogue",
        ));
    }
    let text = String::from_utf8(buffer).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode model.list metadata was unreadable",
        )
    })?;
    parse_model_list_output(&text)
}

/// Parse `opencode api model.list --standalone` stdout: the installed `{"data":
/// [Model.Info]}` envelope (a bare array decodes too). Entries join
/// `providerID`/`id` verbatim, keep `name` as the display name, and map
/// `variants` to selectable ids in wire order. Entries with an unusable
/// identity or no usable metadata are skipped.
pub(crate) fn parse_model_list_output(text: &str) -> Result<Vec<NativeModelMetadata>> {
    let value: Value = serde_json::from_str(text).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode model.list metadata was unreadable",
        )
    })?;
    let items = value
        .get("data")
        .unwrap_or(&value)
        .as_array()
        .ok_or_else(|| {
            Error::new(
                "native_readback",
                "OpenCode model.list metadata was unreadable",
            )
        })?;
    if items.len() > MAX_MODELS {
        return Err(Error::new(
            "source_integrity",
            "OpenCode model.list catalogue exceeds 1024 entries",
        ));
    }
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let provider = item
            .get("providerID")
            .and_then(Value::as_str)
            .filter(|provider| !provider.is_empty());
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let (Some(provider), Some(id)) = (provider, id) else {
            continue;
        };
        if provider.len() + id.len() + 1 > MAX_MODEL_ID_BYTES
            || provider.chars().any(char::is_control)
            || id.chars().any(char::is_control)
            || provider.chars().any(char::is_whitespace)
            || id.chars().any(char::is_whitespace)
        {
            continue;
        }
        let display_name = item
            .get("name")
            .and_then(Value::as_str)
            .and_then(native_model_display_name)
            .map(str::to_owned);
        let variants = item.get("variants").and_then(Value::as_array);
        if variants.is_some_and(|variants| variants.len() > MAX_VARIANTS_PER_MODEL) {
            return Err(Error::new(
                "source_integrity",
                "OpenCode model.list reports too many variants",
            ));
        }
        let variants = variants.map(|variants| {
            let mut ids = Vec::with_capacity(variants.len());
            for variant in variants {
                let Some(variant) = variant
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|variant| {
                        !variant.is_empty()
                            && variant.len() <= 128
                            && !variant.chars().any(char::is_control)
                            && !variant.chars().any(char::is_whitespace)
                    })
                else {
                    continue;
                };
                if !ids.iter().any(|known: &String| known == variant) {
                    ids.push(variant.to_owned());
                }
            }
            ids
        });
        if display_name.is_none() && variants.is_none() {
            continue;
        }
        entries.push(NativeModelMetadata {
            id: format!("{provider}/{id}"),
            display_name,
            variants,
        });
    }
    Ok(entries)
}

/// Grace for reaping a catalogue child after the kill signal.
const KILL_GRACE: Duration = Duration::from_secs(2);

/// Run the frozen native exe with exact argv `models --standalone` and return its
/// `provider/model` ids verbatim, in wire order. Bounded by `deadline` and
/// [`MAX_MODELS_BYTES`]; stderr is discarded like every other helper, so
/// failures are static typed strings naming the `models` surface.
pub(crate) async fn run_models(
    program: &OsStr,
    deadline: Instant,
    xdg: Option<(&Path, &Path)>,
) -> Result<Vec<String>> {
    let mut command = Command::new(program);
    if let Some((config_home, data_home)) = xdg {
        command
            .env("XDG_CONFIG_HOME", config_home)
            .env("XDG_DATA_HOME", data_home);
    }
    command
        .args(models_argv())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode models could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode models could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_MODELS_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode models metadata was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_MODELS_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode models metadata was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode models metadata was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode models did not return a catalogue",
        ));
    }
    let text = String::from_utf8(buffer)
        .map_err(|_| Error::new("native_readback", "OpenCode models metadata was unreadable"))?;
    parse_models_output(&text)
}

/// Parse `opencode models --standalone` stdout verbatim: one `provider/model` id per
/// line (nested segments such as `modal/deepseek-ai/DeepSeek-V4.1-Flash`
/// stay intact). Ids keep their exact bytes (a trailing `\r` is a line
/// ending, not part of the id); blank lines are skipped, never
/// interpreted. Anything else malformed (controls, whitespace inside an id)
/// fails the read instead of guessed.
pub(crate) fn parse_models_output(text: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > MAX_MODEL_ID_BYTES
            || line.chars().any(char::is_control)
            || line.chars().any(char::is_whitespace)
        {
            return Err(Error::new(
                "source_integrity",
                "OpenCode models catalogue is malformed",
            ));
        }
        ids.push(line.to_owned());
        if ids.len() > MAX_MODELS {
            return Err(Error::new(
                "source_integrity",
                "OpenCode models catalogue exceeds 1024 entries",
            ));
        }
    }
    Ok(ids)
}

async fn kill_and_reap(child: &mut tokio::process::Child) -> bool {
    // `start_kill` signals only; `kill` would already wait unboundedly for
    // exit (tokio `Child::kill` awaits `wait`), so the single bounded wait
    // below is the only blocking call in cleanup.
    let _ = child.start_kill();
    timeout(KILL_GRACE, child.wait())
        .await
        .is_ok_and(|wait| wait.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Recorded `opencode models` shape (opencode 2.2.7): one provider/model
    // id per line, nested segments intact, no display names.
    const RECORDED: &str = "modal/deepseek-ai/DeepSeek-V4.1-Flash\n\
        modal/meta-models/Muse-Spark-Open-816B-A42B\n\
        opencode-go/deepseek-v4.1-flash\n\
        opencode-go/gpt-6-luna\n";

    #[test]
    fn recorded_catalogue_parses_ids_verbatim() {
        let ids = parse_models_output(RECORDED).expect("recorded output parses");
        assert_eq!(
            ids,
            vec![
                "modal/deepseek-ai/DeepSeek-V4.1-Flash",
                "modal/meta-models/Muse-Spark-Open-816B-A42B",
                "opencode-go/deepseek-v4.1-flash",
                "opencode-go/gpt-6-luna",
            ]
        );
    }

    #[test]
    fn blank_and_crlf_lines_are_skipped() {
        let ids = parse_models_output("opencode-go/beam-free\r\n\n  \nopencode-go/kimi-k3\r\n")
            .expect("blanks are skipped");
        assert_eq!(ids, vec!["opencode-go/beam-free", "opencode-go/kimi-k3"]);
    }

    #[test]
    fn whitespace_inside_id_is_malformed() {
        let error = parse_models_output("opencode go/model\n").unwrap_err();
        assert_eq!(error.code, "source_integrity");
        let error = parse_models_output(" provider/model\n").unwrap_err();
        assert_eq!(error.code, "source_integrity");
    }

    #[test]
    fn catalogue_caps_at_1024_entries() {
        let mut text = String::new();
        for index in 0..=MAX_MODELS {
            text.push_str(&format!("provider/model-{index}\n"));
        }
        let error = parse_models_output(&text).unwrap_err();
        assert_eq!(error.code, "source_integrity");
        assert!(error.message.contains("1024"));
    }

    #[test]
    fn empty_catalogue_reads_empty_without_inference() {
        assert_eq!(
            parse_models_output("").expect("empty parses"),
            Vec::<String>::new()
        );
    }

    // Recorded `opencode api model.list` entries (installed 2.2.7, three
    // of 425 verbatim, minified): the installed `{"data": [...]}` envelope
    // wraps the same array. Provider-specific variant settings are
    // ignored; only the selectable ids map.
    const RECORDED_MODEL_LIST: &str = "[{\"id\": \"longcat-2.5-free\", \"modelID\": \"longcat-2.5-free\", \"providerID\": \"opencode\", \"family\": \"longcat\", \"name\": \"LongCat 2.5 Free\", \"compatibility\": {\"reasoningField\": \"reasoning_content\"}, \"package\": \"@opencode/ai/providers/openai-compatible\", \"settings\": {\"apiKey\": \"public\", \"baseURL\": \"https://opencode.ai/inference/openai/v1\", \"provider\": \"opencode\"}, \"capabilities\": {\"tools\": true, \"input\": [\"text\", \"image\"], \"output\": [\"text\"], \"temperature\": false}, \"variants\": [], \"time\": {\"released\": 1793577600000}, \"cost\": [{\"input\": 0, \"output\": 0, \"cache\": {\"read\": 0, \"write\": 0}}], \"status\": \"active\", \"enabled\": true, \"limit\": {\"context\": 1000000, \"output\": 262144}}, {\"id\": \"deepseek-v4.1-flash-free\", \"modelID\": \"deepseek-v4.1-flash-free\", \"providerID\": \"opencode\", \"family\": \"deepseek-flash\", \"name\": \"DeepSeek V4.1 Flash Free\", \"compatibility\": {\"reasoningField\": \"reasoning_content\"}, \"package\": \"@opencode/ai/providers/openai-compatible\", \"settings\": {\"apiKey\": \"public\", \"baseURL\": \"https://opencode.ai/inference/openai/v1\", \"provider\": \"opencode\"}, \"capabilities\": {\"tools\": true, \"input\": [\"text\", \"image\"], \"output\": [\"text\"], \"temperature\": true}, \"variants\": [{\"id\": \"low\", \"settings\": {\"reasoningEffort\": \"low\"}}, {\"id\": \"high\", \"settings\": {\"reasoningEffort\": \"high\"}}, {\"id\": \"max\", \"settings\": {\"reasoningEffort\": \"max\"}}], \"time\": {\"released\": 1792195200000}, \"cost\": [{\"input\": 0, \"output\": 0, \"cache\": {\"read\": 0, \"write\": 0}}], \"status\": \"active\", \"enabled\": true, \"limit\": {\"context\": 200000, \"output\": 32000}}, {\"id\": \"meta/muse-spark-open\", \"modelID\": \"meta/muse-spark-open\", \"providerID\": \"openrouter\", \"family\": \"muse\", \"name\": \"Muse Spark Open\", \"package\": \"@opencode/ai/providers/openrouter\", \"settings\": {\"baseURL\": \"https://openrouter.ai/api/v1\"}, \"headers\": {\"HTTP-Referer\": \"https://opencode.ai/\", \"X-Title\": \"opencode\"}, \"capabilities\": {\"tools\": true, \"input\": [\"text\", \"image\", \"video\", \"pdf\", \"audio\"], \"output\": [\"text\"], \"temperature\": true}, \"variants\": [{\"id\": \"low\", \"settings\": {\"reasoning\": {\"effort\": \"low\"}}}, {\"id\": \"medium\", \"settings\": {\"reasoning\": {\"effort\": \"medium\"}}}, {\"id\": \"high\", \"settings\": {\"reasoning\": {\"effort\": \"high\"}}}, {\"id\": \"xhigh\", \"settings\": {\"reasoning\": {\"effort\": \"xhigh\"}}}], \"time\": {\"released\": 1792022400000}, \"cost\": [{\"input\": 1.25, \"output\": 4.25, \"cache\": {\"read\": 0.15, \"write\": 0}}], \"status\": \"active\", \"enabled\": true, \"limit\": {\"context\": 1048576, \"output\": 943718}}]";

    fn recorded_envelope() -> String {
        format!("{{\"data\": {RECORDED_MODEL_LIST}}}")
    }

    #[test]
    fn recorded_model_list_parses_variant_ids() {
        let entries = parse_model_list_output(&recorded_envelope()).expect("recorded parses");
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            NativeModelMetadata {
                id: "opencode/longcat-2.5-free".to_owned(),
                display_name: Some("LongCat 2.5 Free".to_owned()),
                variants: Some(Vec::new()),
            }
        );
        assert_eq!(
            entries[1],
            NativeModelMetadata {
                id: "opencode/deepseek-v4.1-flash-free".to_owned(),
                display_name: Some("DeepSeek V4.1 Flash Free".to_owned()),
                variants: Some(vec!["low".to_owned(), "high".to_owned(), "max".to_owned()]),
            }
        );
        assert_eq!(
            entries[2],
            NativeModelMetadata {
                id: "openrouter/meta/muse-spark-open".to_owned(),
                display_name: Some("Muse Spark Open".to_owned()),
                variants: Some(vec![
                    "low".to_owned(),
                    "medium".to_owned(),
                    "high".to_owned(),
                    "xhigh".to_owned(),
                ]),
            }
        );
    }

    #[test]
    fn bare_array_decodes_like_the_envelope() {
        let entries = parse_model_list_output(RECORDED_MODEL_LIST).expect("bare array parses");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].id, "opencode/deepseek-v4.1-flash-free");
    }

    #[test]
    fn keeps_valid_names_independently_of_variants() {
        let entries = parse_model_list_output(
            r#"{"data": [{"providerID": "p", "id": "novar", "name": " Model Name "},
                          {"providerID": "p", "id": "nometa", "name": "bad\nname"},
                          {"providerID": "p", "id": "empty", "variants": []},
                          {"providerID": " p", "id": "m", "name": "Invalid Identity", "variants": []},
                          {"providerID": "p", "id": "ok",
                           "variants": [{"id": "low"}]}]}"#,
        )
        .expect("skips parse");
        assert_eq!(
            entries,
            vec![
                NativeModelMetadata {
                    id: "p/novar".to_owned(),
                    display_name: Some(" Model Name ".to_owned()),
                    variants: None,
                },
                NativeModelMetadata {
                    id: "p/empty".to_owned(),
                    display_name: None,
                    variants: Some(Vec::new()),
                },
                NativeModelMetadata {
                    id: "p/ok".to_owned(),
                    display_name: None,
                    variants: Some(vec!["low".to_owned()]),
                },
            ]
        );
    }

    #[test]
    fn malformed_variant_ids_are_dropped_not_guessed() {
        let entries = parse_model_list_output(
            r#"{"data": [{"providerID": "p", "id": "m",
                           "variants": [{"id": ""}, {"id": "has space"},
                                        {"id": "low"}, {"id": "low"},
                                        {"id": null}, {}]}]}"#,
        )
        .expect("drops parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].variants, Some(vec!["low".to_owned()]));
    }

    #[test]
    fn model_list_caps_entries_and_variants() {
        let mut text = String::from(r#"{"data": ["#);
        for index in 0..=MAX_MODELS {
            if index > 0 {
                text.push(',');
            }
            text.push_str(&format!(
                r#"{{"providerID": "p", "id": "m{index}", "variants": []}}"#
            ));
        }
        text.push_str("]}");
        let error = parse_model_list_output(&text).unwrap_err();
        assert_eq!(error.code, "source_integrity");
        assert!(error.message.contains("1024"));
        let mut crowded = String::from(r#"{"data": [{"providerID": "p", "id": "m", "variants": ["#);
        for index in 0..=MAX_VARIANTS_PER_MODEL {
            if index > 0 {
                crowded.push(',');
            }
            crowded.push_str(&format!(r#"{{"id": "v{index}"}}"#));
        }
        crowded.push_str("]}]}");
        let error = parse_model_list_output(&crowded).unwrap_err();
        assert_eq!(error.code, "source_integrity");
        assert!(error.message.contains("variants"));
    }

    #[test]
    fn non_json_fails_the_overlay_read() {
        let error = parse_model_list_output("not json").unwrap_err();
        assert_eq!(error.code, "native_readback");
        let error = parse_model_list_output(r#"{"data": {}}"#).unwrap_err();
        assert_eq!(error.code, "native_readback");
    }
}
