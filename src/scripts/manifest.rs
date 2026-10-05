use crate::{
    error::{Error, Result},
    model,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
};

pub const MAX_BUNDLE_FILES: usize = 64;
// The complete request is transported in one ordinary IPC/MCP frame. Keep the
// decoded source small enough that canonical base64 plus the closed manifest
// remains below the default 1 MiB frame without raising a global transport cap.
pub const MAX_BUNDLE_REQUEST_BYTES: usize = 900 * 1024;
pub const MAX_BUNDLE_FILE_BYTES: usize = 256 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 512 * 1024;
pub const MAX_INPUT_BYTES: usize = 256 * 1024;
pub const MAX_INVOCATION_BYTES: usize = MAX_INPUT_BYTES + 4096;
pub const MAX_RESULT_BYTES: usize = 256 * 1024;
pub const MAX_STDERR_BYTES: usize = 1024 * 1024;
pub const MAX_SCRIPT_DURATION_MS: u64 = 5 * 60 * 1000;
pub const MAX_ARGUMENTS: usize = 32;
pub const MAX_ARGUMENT_BYTES: usize = 4096;
pub const MAX_INHERITED_ENVIRONMENT: usize = 32;
pub const MAX_ENVIRONMENT_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_ENVIRONMENT_BYTES: usize = 256 * 1024;
pub const MAX_SCHEMA_DEPTH: usize = 16;
pub const MAX_SCHEMA_PROPERTIES: usize = 64;
pub const MAX_SCHEMA_BYTES: usize = 32 * 1024;
pub const MAX_PATH_BYTES: usize = 240;
pub const MAX_INTERPRETER_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_CONTROLLER_EFFECTS: usize = 1;
pub const MAX_CONTROLLER_EFFECT_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterpreterKind {
    Python,
    Powershell,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScriptTrust {
    TrustedLocal,
    Isolated,
}

/// Closed invocation capability set. New variants are new authority and must
/// stay deliberately narrow instead of naming arbitrary Store methods.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ScriptControllerEffect {
    TaskOwnerMessage,
    /// Notify the current Manager who owns an enabled event-triggered
    /// ScriptRun. Store derives the recipient and only grants this on a
    /// taskless system-event invocation.
    ManagerNotification,
    /// Create one Task in the current Manager-owned system-event project.
    /// The Store derives both the project and the child Operation identity.
    TaskCreate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptBundleRequest {
    pub script_id: String,
    pub interpreter_kind: InterpreterKind,
    pub interpreter_path: PathBuf,
    pub entrypoint: String,
    #[serde(default)]
    pub argv: Vec<String>,
    pub trust: ScriptTrust,
    #[serde(default)]
    pub inherit_environment: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controller_effects: Vec<ScriptControllerEffect>,
    pub input_schema: ScriptValueSchema,
    pub result_schema: ScriptValueSchema,
    pub files: Vec<BundleFileRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BundleFileRequest {
    pub path: String,
    pub content_base64: String,
}

/// A deliberately small, closed schema language for script JSON. This is not
/// JSON Schema and rejects unsupported fields rather than ignoring them.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptValueSchema {
    Null,
    Boolean,
    Integer,
    Number,
    String {
        max_bytes: usize,
    },
    Array {
        items: Box<ScriptValueSchema>,
        max_items: usize,
    },
    Object {
        #[serde(default)]
        properties: BTreeMap<String, ScriptValueSchema>,
        #[serde(default)]
        required: Vec<String>,
        #[serde(default)]
        additional_properties: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterpreterIdentity {
    pub kind: InterpreterKind,
    pub canonical_path: PathBuf,
    pub sha256: String,
    pub byte_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleFile {
    pub path: String,
    pub sha256: String,
    pub byte_length: u64,
    pub content_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptBundle {
    pub bundle_version: u32,
    pub script_id: String,
    pub interpreter: InterpreterIdentity,
    pub entrypoint: String,
    pub argv: Vec<String>,
    pub trust: ScriptTrust,
    pub inherit_environment: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controller_effects: Vec<ScriptControllerEffect>,
    pub input_schema: ScriptValueSchema,
    pub result_schema: ScriptValueSchema,
    pub files: Vec<BundleFile>,
}

impl ScriptBundleRequest {
    pub fn validate(&self) -> Result<()> {
        validate_script_id(&self.script_id)?;
        if self.trust == ScriptTrust::Isolated {
            return Err(Error::new(
                "SCRIPT_ISOLATION_UNSUPPORTED",
                "only trusted_local scripts are supported; process groups are not a sandbox",
            ));
        }
        if !self.interpreter_path.is_absolute() {
            return Err(Error::invalid("interpreter_path must be absolute"));
        }
        if self.argv.len() > MAX_ARGUMENTS
            || self
                .argv
                .iter()
                .any(|arg| arg.len() > MAX_ARGUMENT_BYTES || arg.contains('\0'))
        {
            return Err(Error::invalid(
                "script argv exceeds its count or size limit",
            ));
        }
        if self.files.is_empty() || self.files.len() > MAX_BUNDLE_FILES {
            return Err(Error::invalid("script bundle must contain 1..=64 files"));
        }
        if model::canonical(&json!(self))?.len() > MAX_BUNDLE_REQUEST_BYTES {
            return Err(Error::invalid(
                "canonical script bundle request exceeds 900 KiB; split it into smaller support files",
            ));
        }
        validate_bundle_path(&self.entrypoint)?;
        let mut names = BTreeSet::new();
        let mut total = 0usize;
        let mut found_entrypoint = false;
        for file in &self.files {
            validate_bundle_path(&file.path)?;
            if !names.insert(file.path.to_ascii_lowercase()) {
                return Err(Error::invalid(
                    "script bundle paths must be unique ignoring case",
                ));
            }
            found_entrypoint |= file.path == self.entrypoint;
            let decoded = decode_file(&file.content_base64)?;
            if decoded.len() > MAX_BUNDLE_FILE_BYTES {
                return Err(Error::invalid("script bundle file exceeds 256 KiB"));
            }
            total = total
                .checked_add(decoded.len())
                .ok_or_else(|| Error::invalid("script bundle size overflow"))?;
            if total > MAX_BUNDLE_BYTES {
                return Err(Error::invalid(
                    "script bundle exceeds 512 KiB decoded source",
                ));
            }
        }
        if !found_entrypoint {
            return Err(Error::invalid(
                "script entrypoint is absent from the bundle",
            ));
        }
        if self.inherit_environment.len() > MAX_INHERITED_ENVIRONMENT {
            return Err(Error::invalid(
                "script may inherit at most 32 named environment variables",
            ));
        }
        validate_controller_effects(&self.controller_effects)?;
        let mut environment_names = BTreeSet::new();
        for name in &self.inherit_environment {
            if !valid_environment_name(name) || !environment_names.insert(name.to_ascii_uppercase())
            {
                return Err(Error::invalid(
                    "script environment names must be valid and unique",
                ));
            }
        }
        self.input_schema.validate_definition(0)?;
        self.result_schema.validate_definition(0)?;
        let schema_bytes =
            model::canonical(&json!({"input":self.input_schema,"result":self.result_schema}))?
                .len();
        if schema_bytes > MAX_SCHEMA_BYTES {
            return Err(Error::invalid("script input/result schemas exceed 32 KiB"));
        }
        Ok(())
    }

    pub fn capture(&self) -> Result<ScriptBundle> {
        self.validate()?;
        let interpreter = capture_interpreter(&self.interpreter_path, self.interpreter_kind)?;
        let mut files = Vec::with_capacity(self.files.len());
        for file in &self.files {
            let bytes = decode_file(&file.content_base64)?;
            files.push(BundleFile {
                path: file.path.clone(),
                sha256: model::digest(&bytes),
                byte_length: bytes.len() as u64,
                content_base64: STANDARD.encode(bytes),
            });
        }
        Ok(ScriptBundle {
            bundle_version: 1,
            script_id: self.script_id.clone(),
            interpreter,
            entrypoint: self.entrypoint.clone(),
            argv: self.argv.clone(),
            trust: self.trust.clone(),
            inherit_environment: self.inherit_environment.clone(),
            controller_effects: self.controller_effects.clone(),
            input_schema: self.input_schema.clone(),
            result_schema: self.result_schema.clone(),
            files,
        })
    }
}

impl ScriptValueSchema {
    pub fn validate_definition(&self, depth: usize) -> Result<()> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(Error::invalid("script schema nesting exceeds 16"));
        }
        match self {
            Self::Null | Self::Boolean | Self::Integer | Self::Number => {}
            Self::String { max_bytes } if *max_bytes <= MAX_INPUT_BYTES => {}
            Self::String { .. } => {
                return Err(Error::invalid("schema string max_bytes exceeds 256 KiB"));
            }
            Self::Array { items, max_items } => {
                if *max_items > 4096 {
                    return Err(Error::invalid("schema array max_items exceeds 4096"));
                }
                items.validate_definition(depth + 1)?;
            }
            Self::Object {
                properties,
                required,
                ..
            } => {
                if properties.len() > MAX_SCHEMA_PROPERTIES {
                    return Err(Error::invalid("schema has more than 64 properties"));
                }
                let mut names = BTreeSet::new();
                for name in required {
                    if !properties.contains_key(name) || !names.insert(name) {
                        return Err(Error::invalid(
                            "schema required fields must uniquely name properties",
                        ));
                    }
                }
                for (name, schema) in properties {
                    if name.is_empty() || name.len() > 128 || name.contains('\0') {
                        return Err(Error::invalid("invalid schema property name"));
                    }
                    schema.validate_definition(depth + 1)?;
                }
            }
        }
        Ok(())
    }

    pub fn validate_value(&self, value: &Value) -> Result<()> {
        self.validate_value_at(value, 0)
    }

    fn validate_value_at(&self, value: &Value, depth: usize) -> Result<()> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(Error::invalid(
                "script JSON value exceeds schema nesting limit",
            ));
        }
        let valid = match self {
            Self::Null => value.is_null(),
            Self::Boolean => value.is_boolean(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Number => value.is_number(),
            Self::String { max_bytes } => {
                value.as_str().is_some_and(|text| text.len() <= *max_bytes)
            }
            Self::Array { items, max_items } => {
                let Some(values) = value.as_array() else {
                    return schema_mismatch();
                };
                if values.len() > *max_items {
                    return schema_mismatch();
                }
                for item in values {
                    items.validate_value_at(item, depth + 1)?;
                }
                true
            }
            Self::Object {
                properties,
                required,
                additional_properties,
            } => {
                let Some(values) = value.as_object() else {
                    return schema_mismatch();
                };
                if required.iter().any(|field| !values.contains_key(field))
                    || (!additional_properties
                        && values.keys().any(|key| !properties.contains_key(key)))
                {
                    return schema_mismatch();
                }
                for (key, item) in values {
                    if let Some(schema) = properties.get(key) {
                        schema.validate_value_at(item, depth + 1)?;
                    }
                }
                true
            }
        };
        if valid { Ok(()) } else { schema_mismatch() }
    }
}

impl ScriptBundle {
    /// Revalidate an immutable retained bundle at every consumer boundary.
    /// Captured digests are data, so they are recomputed from the bundled bytes
    /// rather than trusted because they were stored alongside them.
    pub fn validate(&self) -> Result<()> {
        if self.bundle_version != 1 || self.trust != ScriptTrust::TrustedLocal {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained bundle version or trust mode is unsupported",
            ));
        }
        validate_script_id(&self.script_id)?;
        validate_bundle_path(&self.entrypoint)?;
        if self.argv.len() > MAX_ARGUMENTS
            || self
                .argv
                .iter()
                .any(|arg| arg.len() > MAX_ARGUMENT_BYTES || arg.contains('\0'))
            || self.files.is_empty()
            || self.files.len() > MAX_BUNDLE_FILES
            || !self.interpreter.canonical_path.is_absolute()
            || self.interpreter.sha256.len() != 64
            || !self
                .interpreter
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.interpreter.byte_length == 0
            || self.interpreter.byte_length > MAX_INTERPRETER_BYTES
        {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained bundle metadata is invalid",
            ));
        }
        let mut paths = BTreeSet::new();
        let mut total = 0usize;
        let mut entrypoint_present = false;
        for file in &self.files {
            validate_bundle_path(&file.path)?;
            if !paths.insert(file.path.to_ascii_lowercase()) {
                return Err(Error::new(
                    "SCRIPT_BUNDLE_DAMAGED",
                    "retained bundle paths are not unique",
                ));
            }
            entrypoint_present |= file.path == self.entrypoint;
            let bytes = decode_file(&file.content_base64)?;
            if bytes.len() > MAX_BUNDLE_FILE_BYTES
                || bytes.len() as u64 != file.byte_length
                || model::digest(&bytes) != file.sha256
            {
                return Err(Error::new(
                    "SCRIPT_BUNDLE_DAMAGED",
                    "retained bundle file digest or size differs",
                ));
            }
            total = total.checked_add(bytes.len()).ok_or_else(|| {
                Error::new("SCRIPT_BUNDLE_DAMAGED", "retained bundle size overflow")
            })?;
            if total > MAX_BUNDLE_BYTES {
                return Err(Error::new(
                    "SCRIPT_BUNDLE_DAMAGED",
                    "retained bundle exceeds its size limit",
                ));
            }
        }
        if !entrypoint_present {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained entrypoint is absent",
            ));
        }
        if self.inherit_environment.len() > MAX_INHERITED_ENVIRONMENT {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained environment list is too large",
            ));
        }
        validate_controller_effects(&self.controller_effects).map_err(|_| {
            Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained controller effect grant is invalid",
            )
        })?;
        let mut environment_names = BTreeSet::new();
        for name in &self.inherit_environment {
            if !valid_environment_name(name) || !environment_names.insert(name.to_ascii_uppercase())
            {
                return Err(Error::new(
                    "SCRIPT_BUNDLE_DAMAGED",
                    "retained environment names are invalid",
                ));
            }
        }
        self.input_schema.validate_definition(0)?;
        self.result_schema.validate_definition(0)?;
        let schema_bytes =
            model::canonical(&json!({"input":self.input_schema,"result":self.result_schema}))?
                .len();
        if schema_bytes > MAX_SCHEMA_BYTES {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "retained schemas exceed their size limit",
            ));
        }
        Ok(())
    }
}

fn validate_controller_effects(effects: &[ScriptControllerEffect]) -> Result<()> {
    if effects.len() > MAX_CONTROLLER_EFFECTS {
        return Err(Error::invalid(
            "a script bundle may declare at most one controller effect",
        ));
    }
    let mut unique = BTreeSet::new();
    if effects.iter().any(|effect| !unique.insert(*effect)) {
        return Err(Error::invalid(
            "script controller effect grants must be unique",
        ));
    }
    Ok(())
}

pub fn validate_script_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
    {
        return Err(Error::invalid(
            "script_id must be 1..=64 lowercase ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

pub fn validate_bundle_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.contains(['\\', ':', '\0'])
        || path.starts_with('/')
        || path.ends_with('/')
    {
        return Err(Error::invalid(
            "bundle path is not a portable relative path",
        ));
    }
    if !path
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
    {
        return Err(Error::invalid(
            "bundle path contains unsupported characters",
        ));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.ends_with('.')
        {
            return Err(Error::invalid("bundle path contains an unsafe component"));
        }
        let stem = component
            .split('.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || ["COM", "LPT"].iter().any(|prefix| {
                stem.strip_prefix(prefix)
                    .is_some_and(|n| n.len() == 1 && matches!(n.as_bytes()[0], b'1'..=b'9'))
            })
        {
            return Err(Error::invalid(
                "bundle path uses a reserved portable filename",
            ));
        }
    }
    let parsed = Path::new(path);
    if parsed.is_absolute()
        || parsed
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(Error::invalid("bundle path is not normalized"));
    }
    Ok(())
}

pub fn decode_file(encoded: &str) -> Result<Vec<u8>> {
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| Error::invalid("bundle file content is not base64"))?;
    if STANDARD.encode(&bytes) != encoded {
        return Err(Error::invalid(
            "bundle file content must use canonical base64",
        ));
    }
    Ok(bytes)
}

pub fn capture_interpreter(path: &Path, kind: InterpreterKind) -> Result<InterpreterIdentity> {
    if !path.is_absolute() {
        return Err(Error::invalid("interpreter_path must be absolute"));
    }
    let source_metadata = std::fs::symlink_metadata(path)?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        return Err(Error::invalid(
            "configured interpreter path must be a regular file, not a link",
        ));
    }
    let canonical_path = std::fs::canonicalize(path).map_err(|_| {
        Error::new(
            "SCRIPT_INTERPRETER_MISSING",
            "configured interpreter path is not available",
        )
    })?;
    let metadata = std::fs::symlink_metadata(&canonical_path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_INTERPRETER_BYTES
    {
        return Err(Error::invalid(
            "configured interpreter must be a regular executable file",
        ));
    }
    let basename = canonical_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match kind {
        InterpreterKind::Python if !basename.starts_with("python") => {
            return Err(Error::invalid(
                "Python interpreter path must name a Python executable",
            ));
        }
        InterpreterKind::Powershell if !basename.starts_with("pwsh") => {
            return Err(Error::invalid("PowerShell interpreter path must name pwsh"));
        }
        _ => {}
    }
    #[cfg(windows)]
    if !canonical_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("exe"))
    {
        return Err(Error::invalid(
            "Windows scripts require a native .exe interpreter path",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(Error::invalid("configured interpreter is not executable"));
        }
    }
    let mut file = File::open(&canonical_path)?;
    let mut digest = sha2::Sha256::new();
    use sha2::Digest;
    let mut length = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| Error::invalid("interpreter size overflow"))?;
        if length > MAX_INTERPRETER_BYTES {
            return Err(Error::invalid("configured interpreter exceeds 512 MiB"));
        }
    }
    Ok(InterpreterIdentity {
        kind,
        canonical_path,
        sha256: format!("{:x}", digest.finalize()),
        byte_length: length,
    })
}

fn valid_environment_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && (name.as_bytes()[0].is_ascii_alphabetic() || name.starts_with('_'))
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.to_ascii_uppercase().starts_with("SWARM_")
}

fn schema_mismatch<T>() -> Result<T> {
    Err(Error::new(
        "SCRIPT_SCHEMA_MISMATCH",
        "script JSON does not match the registered schema",
    ))
}

use serde_json::json;
