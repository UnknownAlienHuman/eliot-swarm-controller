//! OpenCode-specific human-readable provenance for the native model catalogue.

pub(crate) fn model_catalogue_presentation(
    _cmd: &str,
    provenance: &str,
    _empty: bool,
    _default: Option<&str>,
) -> (&'static str, Option<String>) {
    let note = match provenance {
        "session" => "Advisory OpenCode catalogue from the live session GET /api/model",
        "preview" => {
            "OpenCode launch preview using the profile's provider settings and auth; workspace and live provider state can change availability"
        }
        _ => "OpenCode catalogue from the installed CLI using native configuration and auth",
    };
    ("native", Some(note.to_owned()))
}
