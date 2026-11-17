use super::*;

impl DesktopApp {
    /// Default access: the level a new task starts with (the composer's
    /// access chip and `[desktop] last_access`), saved with this page.
    pub(super) fn access_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let shown = self
            .settings
            .access_draft
            .or_else(|| self.settings.saved_access())
            .unwrap_or(Access::Ask);
        let index = Access::ALL.iter().position(|access| *access == shown);
        let labels: Vec<String> = Access::ALL
            .iter()
            .map(|access| access.label().to_owned())
            .collect();
        let busy = self.settings.busy();
        let picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("access:{}", shown.wire()),
            m3::enum_width(ComboBox::new())
                .items_source(labels)
                .selected_index(index)
                .is_enabled(!busy)
                .automation_name("Default access for new tasks")
                .on_selection_changed(
                    context.callback(|index| Message::Settings(SettingsMsg::AccessDraft(index))),
                ),
        )]);
        let levels: Vec<KeyedView> = Access::ALL
            .iter()
            .map(|access| {
                let title = m3::body(access.label());
                let title = if *access == Access::Full {
                    title.foreground(palette.warning())
                } else {
                    title
                };
                KeyedView::new(
                    access.wire(),
                    Grid::new()
                        .min_height(tokens::height::FORM_ROW)
                        .margin(Thickness::xy(0.0, tokens::spacing::SM))
                        .columns([
                            GridLength::Pixel(tokens::layout::LABEL_WIDTH),
                            GridLength::STAR,
                        ])
                        .column_spacing(tokens::spacing::MD)
                        .children((
                            title.vertical_alignment(VerticalAlignment::Center),
                            m3::secondary(access.description())
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap),
                        )),
                )
            })
            .collect();
        let dirty = self.settings.dirty(Page::Access);
        StackPanel::new().spacing(tokens::spacing::MD).children((
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "default",
                    m3::setting_row(
                        "Default access for new tasks",
                        "The composer starts with this level; choosing another there also changes it.",
                        picker,
                    ),
                )],
            ),
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::card_title("What each level allows"),
                m3::settings_card(palette, levels),
                m3::secondary(
                    "Presets and Auto keep their own permissions. OpenCode has no Full access mode.",
                )
                .text_wrapping(TextWrapping::Wrap),
            )),
            self.save_bar(Page::Access, dirty, context, palette),
        ))
    }
}
