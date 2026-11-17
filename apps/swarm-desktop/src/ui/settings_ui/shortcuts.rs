use super::*;

/// Keyboard shortcuts: read-only (the window's keyboard hook owns them).
pub(super) fn shortcuts_page(palette: Palette) -> View {
    let rows: Vec<KeyedView> = [
        ("New task", "Ctrl+N"),
        ("Search tasks", "Ctrl+K"),
        ("Choose the model", "Ctrl+M"),
        ("Settings", "Ctrl+,"),
        ("Model picker: move", "↑ ↓"),
        ("Model picker: effort", "← →"),
        ("Model picker: favorite", "Ctrl+F"),
        ("Choose / close a menu", "Enter / Esc"),
    ]
    .into_iter()
    .map(|(name, keys)| KeyedView::new(name, m3::setting_row(name, "", m3::badge(palette, keys))))
    .collect();
    m3::settings_card(palette, rows)
}
