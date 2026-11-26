use super::*;

/// Keyboard shortcuts: read-only (the window's keyboard hook owns them).
pub(super) fn shortcuts_page() -> View {
    let rows: Vec<KeyedView> = [
        ("New task", "Ctrl+N"),
        ("Search tasks", "Ctrl+K"),
        ("Choose the model", "Ctrl+M"),
        ("Settings", "Ctrl+,"),
        ("Back / Forward", "Alt+← / Alt+→"),
        ("Hide or show the sidebar", "Ctrl+B"),
        ("Open or close Changes", "Ctrl+Shift+G"),
        ("Open or close Details", "Ctrl+Shift+D"),
        ("Open or close Terminal", "Ctrl+J"),
        ("Model picker: move", "↑ ↓"),
        ("Model picker: effort", "← →"),
        ("Model picker: favorite", "Ctrl+F"),
        ("Choose / close a menu", "Enter / Esc"),
    ]
    .into_iter()
    .map(|(name, keys)| {
        KeyedView::new(name, controls::setting_row(name, "", controls::badge(keys)))
    })
    .collect();
    controls::settings_card(rows)
}
