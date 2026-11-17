use super::*;

impl DesktopApp {
    pub(super) fn models_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let dirty = self.editor.draft != self.editor.base;
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.profiles_view(context, palette),
            self.save_bar(Page::Models, dirty, context, palette),
        ))
    }
}
