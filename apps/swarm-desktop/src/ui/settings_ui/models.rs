use super::*;

impl DesktopApp {
    /// Models & presets: form changes save at once; the TOML source,
    /// while open or holding typed changes, keeps Save, Validate and
    /// Discard for the whole draft.
    pub(super) fn models_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let raw = !self.autosaves(Page::Models);
        let dirty = self.editor.draft != self.editor.base;
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.profiles_view(context, palette),
            if raw {
                self.save_bar(Page::Models, dirty, context, palette)
            } else {
                View::empty()
            },
        ))
    }
}
