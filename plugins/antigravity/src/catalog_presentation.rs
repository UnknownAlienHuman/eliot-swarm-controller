//! Antigravity-specific human-readable provenance for its native catalogue.

pub(crate) fn model_catalogue_presentation(
    _cmd: &str,
    _provenance: &str,
    _empty: bool,
    _default: Option<&str>,
) -> (&'static str, Option<String>) {
    (
        "native",
        Some(
            "Antigravity catalogue from `agy models`; effort-suffixed ids map to thought_level at launch"
                .to_owned(),
        ),
    )
}
