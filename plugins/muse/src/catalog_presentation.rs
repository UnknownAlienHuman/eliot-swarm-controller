//! Muse-specific presentation of native model catalogue provenance.

pub(crate) fn model_catalogue_presentation(
    cmd: &str,
    _provenance: &str,
    empty: bool,
    default: Option<&str>,
) -> (&'static str, Option<String>) {
    if empty {
        return (
            "empty",
            Some(format!(
                "Muse model/list from {cmd} returned an empty catalogue after provider discovery"
            )),
        );
    }

    let note = match default {
        Some(marked) if !marked.trim().is_empty() => format!(
            "Muse model/list marks a row with the native isDefault marker ({marked}); an omitted --model still defers to the server default at launch"
        ),
        _ => "Muse model/list names no default marker; the server default applies at launch"
            .to_owned(),
    };
    ("native", Some(note))
}
