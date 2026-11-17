//! The new-task composer's chips and the popovers they open: the project
//! list, the model picker and the access menu (see [`super::launch_ui`]
//! for their state and keys).

use super::launch_ui::{CHAT_PROJECT, OTHER_FOLDER, Pop};
use super::*;
use crate::picker::{Choice, Item, ManageGroup, Row};
use crate::settings::Access;
use crate::theme::winui::Palette;

/// The picker's key hints (OpenCode's footer).
const PICKER_HINTS: &str = "↑↓ move   ←→ effort   Enter choose   Ctrl+F favorite   Esc close";
/// Models a Manage models group lists before it asks for a search.
const MANAGE_GROUP_ROWS: usize = 150;

impl DesktopApp {
    /// The composer's top row for a new task: the project chip (and the
    /// folder field for a folder outside the projects).
    pub(super) fn composer_top_row(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let label = if self.launch.chat {
            "No project (chat)".to_owned()
        } else {
            self.selected_project()
                .map(|project| self.project_label(project))
                .or_else(|| {
                    (!self.launch.workspace.is_empty())
                        .then(|| nav::project_name(&self.launch.workspace, &[]))
                })
                .unwrap_or_else(|| "No project".to_owned())
        };
        let open = self.launch.open == Some(Pop::Project);
        let glyph = if self.launch.chat {
            m3::glyph::CHAT
        } else {
            m3::glyph::FOLDER
        };
        let chip = m3::chip(palette)
            .automation_name(format!(
                "Project: {label} ({})",
                if open { "list open" } else { "choose" }
            ))
            .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Project))))
            .content(chip_content(glyph, &label, None));
        let folder: View = if !self.launch.chat
            && (self.launch.custom_workspace || self.nav.projects.is_empty())
        {
            m3::field()
                .text(self.launch.workspace.clone())
                .placeholder_text("Folder path")
                .automation_name("Folder")
                .width(tokens::layout::FOLDER_WIDTH)
                .vertical_alignment(VerticalAlignment::Center)
                .on_text_changed(
                    context.callback(|value| Message::Launch(LaunchMsg::WorkspaceChanged(value))),
                )
                .into()
        } else {
            View::empty()
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .children((chip, folder))
    }

    /// The composer's bottom-left chips for a new task: the model chip
    /// (`Harness · Model · effort`) and the access chip.
    pub(super) fn composer_chips(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let label = self.launch.choice.chip_label(self.launch.auto_profile());
        let glyph = match self.launch.choice {
            Choice::Auto => m3::glyph::AUTO,
            _ => m3::glyph::MODEL,
        };
        let model = m3::chip(palette)
            .automation_name(format!("Model: {label} (Ctrl+M)"))
            .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Model))))
            .content(chip_content(glyph, &label, None));
        let access: View = match &self.launch.choice {
            Choice::Model(_) => {
                let access = self.launch.access();
                let full = access == Access::Full;
                // Keyed by level: a chip's colors apply when it is created.
                Grid::new().keyed_children(vec![KeyedView::new(
                    format!("access:{}", access.wire()),
                    if full {
                        m3::chip(palette).resource_overrides(palette.warning_chip())
                    } else {
                        m3::chip(palette)
                    }
                    .automation_name(format!("Access: {}", access.label()))
                    .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Access))))
                    .content(chip_content(
                        access_glyph(access),
                        access.label(),
                        None,
                    )),
                )])
            }
            // A preset (or Auto's pick) carries its own permissions.
            _ => m3::chip_static(
                palette,
                m3::glyph::SHIELD,
                &match self.launch.profile() {
                    Some(profile) => format!(
                        "{} (preset)",
                        launch_ui::access_label(profile.permissions.as_deref())
                    ),
                    None => "Access from the preset".to_owned(),
                },
            ),
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .children((model, access))
    }

    /// The open popover, left-aligned in the reading column right above the
    /// composer (empty when none is open).
    pub(super) fn popover_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let Some(pop) = self.launch.open else {
            return View::empty();
        };
        if self.launch.preset_target().is_some() {
            // A preset's picker opens in its Settings form.
            return View::empty();
        }
        let content = match pop {
            Pop::Model => self.model_popover(context, palette),
            Pop::Manage => self.manage_popover(context, palette),
            Pop::Access => self.access_popover(context, palette),
            Pop::Project => self.project_popover(context, palette),
        };
        Border::new()
            .grid_row(5)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
            .content(content)
    }

    pub(super) fn model_popover(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let rows = self.picker_rows();
        let highlight = self.launch.highlight.as_deref();
        let mut items: Vec<KeyedView> = Vec::new();
        for row in &rows {
            match row {
                Row::Heading {
                    key,
                    title,
                    note,
                    group: Some(group),
                    collapsed,
                } => items.push(KeyedView::new(
                    format!("{key}:{collapsed}"),
                    group_heading(group, title, note, *collapsed, 0.0, context, palette),
                )),
                Row::Heading {
                    key, title, note, ..
                } => {
                    items.push(KeyedView::new(key.clone(), heading(title, note)));
                }
                Row::Provider {
                    key,
                    title,
                    group,
                    collapsed,
                } => items.push(KeyedView::new(
                    format!("{key}:{collapsed}"),
                    group_heading(
                        group,
                        title,
                        "",
                        *collapsed,
                        tokens::layout::CHECK_COLUMN,
                        context,
                        palette,
                    ),
                )),
                Row::Note { key, text } => items.push(KeyedView::new(
                    key.clone(),
                    m3::secondary(text.clone())
                        .text_wrapping(TextWrapping::Wrap)
                        .margin(Thickness::new(
                            tokens::spacing::INSET + tokens::layout::CHECK_COLUMN,
                            tokens::spacing::XS,
                            tokens::spacing::SM,
                            tokens::spacing::XS,
                        )),
                )),
                Row::Failure { key, text } => items.push(KeyedView::new(
                    format!("{key}:{}", self.launch.catalog_loading),
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .margin(Thickness::new(
                            tokens::spacing::INSET + tokens::layout::CHECK_COLUMN,
                            tokens::spacing::XS,
                            tokens::spacing::SM,
                            tokens::spacing::XS,
                        ))
                        .children((
                            m3::secondary(text.clone())
                                .foreground(palette.error())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap),
                            m3::text_button(palette)
                                .grid_column(1)
                                .is_enabled(!self.launch.catalog_loading)
                                .automation_name(format!("Retry: {text}"))
                                .on_click(context.message(Message::Launch(LaunchMsg::CatalogRetry)))
                                .content(m3::label(if self.launch.catalog_loading {
                                    "Reading…"
                                } else {
                                    "Retry"
                                })),
                        )),
                )),
                Row::Choice(item) => {
                    let highlighted = highlight == Some(item.key.as_str());
                    // Not keyed by the highlight: the highlight is the row's
                    // fill, and re-creating the row under the pointer as it
                    // enters would drop the click that follows.
                    items.push(KeyedView::new(
                        format!("{}:{}", item.key, item.current),
                        self.picker_row(item, highlighted, context, palette),
                    ));
                }
            }
        }
        let field = m3::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.query.clone())
            .placeholder_text("Search models")
            .automation_name("Search models")
            .on_text_changed(
                context.callback(|text| Message::Launch(LaunchMsg::PickerQuery(text))),
            );
        // OpenCode's sliders button at the right end of the search row.
        let manage = m3::icon_button(palette)
            .grid_column(1)
            .width(tokens::height::CONTROL)
            .height(tokens::height::CONTROL)
            .automation_name("Manage models")
            .on_click(context.message(Message::Launch(LaunchMsg::ManageOpened)))
            .content(m3::icon(m3::glyph::SLIDERS))
            .tooltip("Manage models");
        let search = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::XS)
            .children((field, manage));
        let list = ScrollViewer::new()
            .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Models")
            .content(StackPanel::new().keyed_children(items));
        m3::popover(palette)
            .width(tokens::layout::PICKER_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Left)
            .content(StackPanel::new().spacing(tokens::spacing::XS).children((
                search,
                list,
                m3::divider(palette),
                m3::secondary(PICKER_HINTS).margin(Thickness::new(
                    tokens::spacing::SM,
                    0.0,
                    tokens::spacing::SM,
                    tokens::spacing::XS,
                )),
            )))
    }

    /// One picker row: a check on the current choice, the model and its
    /// provider, badges, and its effort; the highlighted row shows the
    /// efforts to choose from and the favorite star.
    fn picker_row(
        &self,
        item: &Item,
        highlighted: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let enabled = item.disabled.is_none();
        let check: View = if item.current {
            m3::icon(m3::glyph::CHECK)
        } else {
            View::empty()
        };
        let mut marks: Vec<KeyedView> = Vec::new();
        if item.default {
            marks.push(KeyedView::new("default", m3::badge(palette, "default")));
        }
        if item.hidden && !highlighted {
            marks.push(KeyedView::new("hidden", m3::badge(palette, "hidden")));
        }
        if item.favorite && !highlighted {
            marks.push(KeyedView::new("star", m3::icon(m3::glyph::STAR_FILLED)));
        }
        match (&item.disabled, &item.effort) {
            (Some(reason), _) => {
                marks.push(KeyedView::new("reason", m3::secondary(reason.clone())));
            }
            (None, Some(effort)) if !highlighted || item.efforts.is_empty() => {
                marks.push(KeyedView::new("effort", m3::secondary(effort.clone())));
            }
            _ => {}
        }
        let trailing: View = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .vertical_alignment(VerticalAlignment::Center)
            .keyed_children(marks);
        let mut name = format!("{}, {}", item.title, item.detail);
        if let Some(effort) = &item.effort {
            name.push_str(&format!(", effort {effort}"));
        }
        if item.current {
            name.push_str(", current");
        }
        if item.hidden {
            name.push_str(", hidden in Manage models");
        }
        if let Some(reason) = &item.disabled {
            name.push_str(&format!(", {reason}"));
        }
        let mut button = m3::row(palette, false)
            .is_enabled(enabled)
            .automation_name(name)
            .on_click(context.message(Message::Launch(LaunchMsg::PickerChosen(item.key.clone()))));
        if highlighted {
            button = button.element_ref(&self.launch.row_ref);
        }
        let button = button.content(
            Grid::new()
                .columns([
                    GridLength::Pixel(tokens::layout::CHECK_COLUMN),
                    GridLength::STAR,
                ])
                .column_spacing(tokens::spacing::SM)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(check),
                    Border::new().grid_column(1).content(m3::row_content(
                        None,
                        &item.title,
                        &item.detail,
                        trailing,
                    )),
                )),
        );
        let extras: View =
            if highlighted && enabled {
                let segments: Vec<KeyedView> = item
                    .efforts
                    .iter()
                    .map(|effort| {
                        let selected = item.effort.as_deref() == Some(effort.as_str());
                        KeyedView::new(
                            format!("effort:{effort}:{selected}"),
                            m3::segment(palette, selected)
                                .automation_name(format!("{} with effort {effort}", item.title))
                                .on_click(context.message(Message::Launch(
                                    LaunchMsg::PickerEffort(item.key.clone(), effort.clone()),
                                )))
                                .content(m3::segment_label(effort)),
                        )
                    })
                    .collect();
                // The efforts on their own track, so the chosen one stands
                // out from the row's highlight.
                let mut cells: Vec<KeyedView> = Vec::new();
                if item.hidden {
                    cells.push(KeyedView::new(
                        "hidden",
                        Border::new()
                            .margin(Thickness::new(0.0, 0.0, tokens::spacing::SM, 0.0))
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(m3::badge(palette, "hidden")),
                    ));
                }
                if !segments.is_empty() {
                    cells.push(KeyedView::new("efforts", m3::segmented(palette, segments)));
                }
                if item.model_ref().is_some() {
                    let glyph = if item.favorite {
                        m3::glyph::STAR_FILLED
                    } else {
                        m3::glyph::STAR
                    };
                    cells.push(KeyedView::new(
                        format!("star:{}", item.favorite),
                        m3::icon_button(palette)
                            .automation_name(if item.favorite {
                                format!("Remove {} from favorites (Ctrl+F)", item.title)
                            } else {
                                format!("Add {} to favorites (Ctrl+F)", item.title)
                            })
                            .on_click(context.message(Message::Launch(LaunchMsg::PickerFavorite(
                                item.key.clone(),
                            ))))
                            .content(m3::icon(glyph)),
                    ));
                }
                StackPanel::new()
                    .grid_column(1)
                    .orientation(Orientation::Horizontal)
                    .vertical_alignment(VerticalAlignment::Center)
                    .keyed_children(cells)
            } else {
                View::empty()
            };
        Border::new()
            .corner_radius(tokens::radius::CONTROL)
            .background(if highlighted {
                palette.highlight()
            } else {
                Brush::Solid(Color::transparent())
            })
            .on_pointer_entered({
                let key = item.key.clone();
                context.callback(move |_: PointerEventInfo| {
                    Message::Launch(LaunchMsg::PickerHover(key.clone()))
                })
            })
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((button, extras)),
            )
    }

    /// The Codex access menu: three levels, each with an icon, a title and
    /// one line of description; Full access in the warning color.
    fn access_popover(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let current = self.launch.access();
        let mut rows: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            m3::secondary("How should the Agent's actions be approved?").margin(Thickness::new(
                tokens::spacing::INSET,
                tokens::spacing::XS,
                tokens::spacing::INSET,
                tokens::spacing::XS,
            )),
        )];
        for (access, refused) in self.access_levels() {
            let highlighted = self.launch.access_highlight == Some(access);
            let selected = access == current;
            let warn = access == Access::Full;
            let title = m3::body(access.label());
            let description = m3::secondary(refused.unwrap_or(access.description()))
                .text_wrapping(TextWrapping::Wrap);
            let (title, description) = if warn {
                (
                    title.foreground(palette.warning()),
                    description.foreground(palette.warning()),
                )
            } else {
                (title, description)
            };
            // The icon takes the row's foreground (warning for Full).
            let icon = m3::icon(access_glyph(access));
            let row = Button::new()
                .resource_overrides(if warn {
                    palette.warning_row()
                } else {
                    palette.row(false)
                })
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .horizontal_content_alignment(HorizontalAlignment::Stretch)
                .is_enabled(refused.is_none())
                .automation_name(format!(
                    "{}: {}{}",
                    access.label(),
                    refused.unwrap_or(access.description()),
                    if selected { " (current)" } else { "" }
                ))
                .on_click(context.message(Message::Launch(LaunchMsg::AccessChosen(access))))
                .content(
                    Grid::new()
                        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::INSET)
                        .margin(Thickness::xy(0.0, tokens::spacing::XS))
                        .children((
                            Border::new()
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(icon),
                            StackPanel::new()
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .children((title, description)),
                            Border::new()
                                .grid_column(2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(if selected {
                                    m3::icon(m3::glyph::CHECK)
                                } else {
                                    View::empty()
                                }),
                        )),
                );
            rows.push(KeyedView::new(
                format!(
                    "{}:{highlighted}:{selected}:{}",
                    access.wire(),
                    refused.is_some()
                ),
                Border::new()
                    .corner_radius(tokens::radius::CONTROL)
                    .background(if highlighted {
                        palette.highlight()
                    } else {
                        Brush::Solid(Color::transparent())
                    })
                    .on_pointer_entered(context.callback(move |_: PointerEventInfo| {
                        Message::Launch(LaunchMsg::AccessHover(access))
                    }))
                    .content(row),
            ));
        }
        m3::popover(palette)
            .width(tokens::layout::MENU_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Left)
            .content(StackPanel::new().keyed_children(rows))
    }

    /// The project list: a search field, the registered projects (name and
    /// folder) and "Other folder…".
    fn project_popover(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let current = self
            .selected_project()
            .map(|project| project.workspace.clone());
        let highlight = self.launch.highlight.as_deref();
        let rows: Vec<KeyedView> = self
            .project_rows()
            .into_iter()
            .map(|(checkout, name, path)| {
                let highlighted = highlight == Some(checkout.as_str());
                let selected = if self.launch.chat {
                    checkout == CHAT_PROJECT
                } else {
                    current.as_deref() == Some(checkout.as_str())
                        || (checkout == OTHER_FOLDER && self.launch.custom_workspace)
                };
                let glyph = match checkout.as_str() {
                    OTHER_FOLDER => m3::glyph::OPEN_FOLDER,
                    CHAT_PROJECT => m3::glyph::CHAT,
                    _ => m3::glyph::FOLDER,
                };
                KeyedView::new(
                    format!("{checkout}:{highlighted}:{selected}"),
                    Border::new()
                        .corner_radius(tokens::radius::CONTROL)
                        .background(if highlighted {
                            palette.highlight()
                        } else {
                            Brush::Solid(Color::transparent())
                        })
                        .content(
                            m3::row(palette, false)
                                .automation_name(format!("{name}, {path}"))
                                .on_click(context.message(Message::Launch(
                                    LaunchMsg::ProjectChosen(checkout.clone()),
                                )))
                                .content(m3::row_content(
                                    Some(glyph),
                                    &name,
                                    &path,
                                    if selected {
                                        m3::icon(m3::glyph::CHECK)
                                    } else {
                                        View::empty()
                                    },
                                )),
                        ),
                )
            })
            .collect();
        let search = m3::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.project_query.clone())
            .placeholder_text("Search projects")
            .automation_name("Search projects")
            .on_text_changed(
                context.callback(|text| Message::Launch(LaunchMsg::ProjectQuery(text))),
            );
        m3::popover(palette)
            .width(tokens::layout::MENU_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Left)
            .content(
                StackPanel::new().spacing(tokens::spacing::XS).children((
                    search,
                    ScrollViewer::new()
                        .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
                        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                        .content(StackPanel::new().keyed_children(rows)),
                )),
            )
    }
}

impl DesktopApp {
    /// OpenCode's Manage models panel: the title and subtitle, a search
    /// field, then each provider group (collapsible, with its group toggle)
    /// and one toggle per model.
    pub(super) fn manage_popover(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let groups = match self.launch.catalog.as_ref() {
            Some(Ok(reply)) => {
                crate::picker::manage_groups(reply, &self.prefs, &self.launch.manage_query)
            }
            _ => Vec::new(),
        };
        let mut rows: Vec<KeyedView> = Vec::new();
        match self.launch.catalog.as_ref() {
            None => rows.push(KeyedView::new(
                "loading",
                m3::secondary("Reading the models…"),
            )),
            Some(Err(error)) => rows.push(KeyedView::new(
                "error",
                m3::secondary(format!("Models not read: {error}"))
                    .text_wrapping(TextWrapping::Wrap),
            )),
            Some(Ok(_)) if groups.is_empty() => rows.push(KeyedView::new(
                "none",
                m3::secondary("No models match. Clear the search to see every model.")
                    .margin(Thickness::xy(tokens::spacing::INSET, tokens::spacing::XS)),
            )),
            Some(Ok(_)) => {}
        }
        for group in &groups {
            rows.push(KeyedView::new(
                format!(
                    "group:{}:{}:{}:{}",
                    group.key, group.collapsed, group.visible, group.shown
                ),
                manage_group_row(group, context, palette),
            ));
            if group.collapsed {
                continue;
            }
            for model in group.models.iter().take(MANAGE_GROUP_ROWS) {
                let (harness, provider, id) = (
                    group.harness.clone(),
                    group.provider.clone(),
                    model.id.clone(),
                );
                rows.push(KeyedView::new(
                    format!("model:{}:{}:{}", group.key, model.id, model.visible),
                    Grid::new()
                        .columns([
                            GridLength::Pixel(tokens::layout::CHECK_COLUMN),
                            GridLength::STAR,
                            GridLength::Auto,
                        ])
                        .min_height(tokens::height::SUB_ROW)
                        .margin(Thickness::new(tokens::spacing::INSET, 0.0, 0.0, 0.0))
                        .children((
                            View::empty(),
                            m3::body(model.title.clone())
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            ToggleSwitch::new()
                                .grid_column(2)
                                .width(tokens::layout::SWITCH_WIDTH)
                                .is_on(model.visible)
                                .vertical_alignment(VerticalAlignment::Center)
                                .automation_name(format!(
                                    "Show {} in the model selector",
                                    model.title
                                ))
                                .on_toggled(context.callback(move |on: bool| {
                                    Message::Launch(LaunchMsg::ManageModel {
                                        harness: harness.clone(),
                                        provider: provider.clone(),
                                        model: id.clone(),
                                        visible: on,
                                    })
                                })),
                        )),
                ));
            }
            if group.models.len() > MANAGE_GROUP_ROWS {
                rows.push(KeyedView::new(
                    format!("more:{}", group.key),
                    m3::secondary(format!(
                        "{} more: search to find one.",
                        group.models.len() - MANAGE_GROUP_ROWS
                    ))
                    .margin(Thickness::new(
                        tokens::spacing::INSET + tokens::layout::CHECK_COLUMN,
                        tokens::spacing::XS,
                        0.0,
                        tokens::spacing::XS,
                    )),
                ));
            }
        }
        let header = Grid::new()
            .columns([GridLength::Auto, GridLength::STAR])
            .column_spacing(tokens::spacing::SM)
            .children((
                m3::icon_button(palette)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .vertical_alignment(VerticalAlignment::Top)
                    .automation_name("Back to the model selector")
                    .on_click(context.message(Message::Launch(LaunchMsg::ManageClosed)))
                    .content(m3::icon(m3::glyph::BACK)),
                StackPanel::new().grid_column(1).children((
                    m3::section_title("Manage models"),
                    m3::secondary("Customize which models appear in the model selector"),
                )),
            ));
        let search = m3::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.manage_query.clone())
            .placeholder_text("Search models")
            .automation_name("Search models to manage")
            .on_text_changed(
                context.callback(|text| Message::Launch(LaunchMsg::ManageQuery(text))),
            );
        m3::popover(palette)
            .width(tokens::layout::PICKER_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Left)
            .content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    header,
                    search,
                    ScrollViewer::new()
                        .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
                        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                        .automation_name("Model groups")
                        .content(StackPanel::new().keyed_children(rows)),
                )),
            )
    }
}

/// A Manage models group row: its chevron and title collapse it; the
/// switch shows or hides the whole group (OpenCode's Manage models).
fn manage_group_row(
    group: &ManageGroup,
    context: &mut ViewContext<DesktopApp>,
    palette: Palette,
) -> View {
    let (glyph, state) = if group.collapsed {
        (m3::glyph::CHEVRON_RIGHT, "collapsed")
    } else {
        (m3::glyph::CHEVRON_DOWN, "expanded")
    };
    let count = format!("{} of {} shown", group.shown, group.all.len());
    let key = group.key.clone();
    let toggle = m3::row(palette, false)
        .automation_name(format!("{}, {count}, {state}", group.title))
        .on_click(context.message(Message::Launch(LaunchMsg::GroupToggled(key.clone()))))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(tokens::spacing::SM)
                .children((
                    m3::icon(glyph),
                    Border::new().grid_column(1).content(m3::row_content(
                        None,
                        &group.title,
                        &count,
                        View::empty(),
                    )),
                )),
        );
    let group_key = group.key.clone();
    let check = ToggleSwitch::new()
        .grid_column(1)
        .width(tokens::layout::SWITCH_WIDTH)
        .is_on(group.visible)
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(format!("Show every {} model", group.title))
        .on_toggled(context.callback(move |on: bool| {
            Message::Launch(LaunchMsg::ManageGroup(group_key.clone(), on))
        }));
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((toggle, check))
}

/// A collapsible picker group: the chevron and title collapse or expand it
/// (remembered in `[desktop] collapsed_model_groups`).
fn group_heading(
    group: &str,
    title: &str,
    note: &str,
    collapsed: bool,
    indent: f64,
    context: &mut ViewContext<DesktopApp>,
    palette: Palette,
) -> View {
    let (glyph, state) = if collapsed {
        (m3::glyph::CHEVRON_RIGHT, "collapsed")
    } else {
        (m3::glyph::CHEVRON_DOWN, "expanded")
    };
    let label: View = if indent > 0.0 {
        m3::secondary(title.to_owned()).into()
    } else {
        m3::section_label(title.to_owned())
            .font_weight(m3::weight(tokens::text::SECTION_TITLE))
            .into()
    };
    m3::sub_row(palette, false)
        .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
        .automation_name(format!("{title} models, {state}"))
        .on_click(context.message(Message::Launch(LaunchMsg::GroupToggled(group.to_owned()))))
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    m3::icon(glyph),
                    label,
                    if note.is_empty() {
                        View::empty()
                    } else {
                        m3::secondary(note.to_owned()).into()
                    },
                )),
        )
}

/// A chip's content: its icon, label and the ▾ that says it opens.
fn chip_content(glyph: &str, label: &str, _note: Option<&str>) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .children((
            m3::icon(glyph),
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            m3::icon(m3::glyph::CHEVRON_DOWN),
        ))
}

fn access_glyph(access: Access) -> &'static str {
    match access {
        Access::Ask => m3::glyph::LOCK,
        Access::Auto => m3::glyph::SHIELD,
        Access::Full => m3::glyph::WARNING,
    }
}

/// A picker section title and its note ("Codex  codex-cli 0.167.1",
/// "Muse  not installed").
fn heading(title: &str, note: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .margin(Thickness::new(
            tokens::spacing::INSET,
            tokens::spacing::SM,
            tokens::spacing::INSET,
            tokens::spacing::XS,
        ))
        .children((
            m3::section_label(title.to_owned())
                .font_weight(m3::weight(tokens::text::SECTION_TITLE)),
            if note.is_empty() {
                View::empty()
            } else {
                m3::secondary(note.to_owned()).into()
            },
        ))
}
