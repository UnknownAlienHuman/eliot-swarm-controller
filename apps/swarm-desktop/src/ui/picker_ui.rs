//! The new-task composer's chips and the popovers they open: the project
//! list, the model picker and the access menu (see [`super::launch_ui`]
//! for their state and keys). The open Agent's effort and usage popovers
//! are built by the conversation's composer. Every popover is a real
//! WinUI `Flyout` on its chip ([`DesktopApp::popover_chip`]): its presenter
//! gives the menu its surface, edge, corner and inset, so the content here
//! is the rows alone. A flyout's
//! content takes no tooltip (Reactor 0.100.0 refuses a tooltip under a
//! flyout).

use eliot_core::api::native_profile::SandboxPermissions;
use std::collections::HashMap;

use super::launch_ui::{CHAT_PROJECT, OTHER_FOLDER, Pop};
use super::*;
use crate::picker::{self, Choice, Item, ItemKind, ManageGroup, Row};
use crate::settings::Access;

/// The picker's key hints (OpenCode's footer).
const PICKER_HINTS: &str = "↑↓ move   ←→ effort   Enter choose   Ctrl+F favorite   Esc close";
/// What a model row with no explicit effort launches: the harness's own
/// default (its accessible name; the chip reads "Default").
const DEFAULT_EFFORT: &str = "Default effort";
/// Models a Manage models group lists before it asks for a search.
const MANAGE_GROUP_ROWS: usize = 150;

impl DesktopApp {
    /// The new task's project, as its chip and the Home line under the
    /// title name it.
    pub(super) fn launch_project_label(&self) -> String {
        if self.launch.chat {
            "No project (chat)".to_owned()
        } else if self.launch.workspace.is_empty() {
            "No project".to_owned()
        } else {
            self.place_title(&self.launch.workspace)
        }
    }

    /// The new task's project is registered but its folder is provably gone
    /// (§8.2): Home says so and Start stays closed, so a task never starts
    /// in some other folder.
    pub(super) fn launch_folder_missing(&self) -> bool {
        !self.launch.chat
            && self.nav.projects.iter().any(|project| {
                project.missing
                    && nav::path_key(&project.workspace) == nav::path_key(&self.launch.workspace)
            })
    }

    /// The new task's folder field, above the composer's field, for a
    /// folder outside the projects (none otherwise).
    pub(super) fn composer_folder_row(&self, context: &mut ViewContext<Self>) -> Option<View> {
        (!self.launch.chat && (self.launch.custom_workspace || self.nav.projects.is_empty())).then(
            || {
                controls::field()
                    .text(self.launch.workspace.clone())
                    .placeholder_text("Folder path")
                    .automation_name("Folder")
                    .width(tokens::layout::FOLDER_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .vertical_alignment(VerticalAlignment::Center)
                    .on_text_changed(crate::views::text_edits(
                        context
                            .callback(|value| Message::Launch(LaunchMsg::WorkspaceChanged(value))),
                    ))
                    .into()
            },
        )
    }

    /// The new task's project chip (it opens the project list).
    fn project_chip(&self, context: &mut ViewContext<Self>) -> View {
        let label = self.launch_project_label();
        let open = self.launch.open == Some(Pop::Project);
        let glyph = if self.launch.chat {
            controls::glyph::CHAT
        } else {
            controls::glyph::OPEN_FOLDER
        };
        self.popover_chip(
            controls::chip()
                .automation_name(format!(
                    "Project: {label} ({})",
                    if open { "list open" } else { "choose" }
                ))
                .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Project)))),
            controls::chip_content(Some(glyph), &label, false),
            &[Pop::Project],
            FlyoutPlacement::TopEdgeAlignedLeft,
            context,
        )
    }

    /// Where the new task works (the design's place chip): "New worktree"
    /// or "Work locally"; a click switches between them where the folder
    /// lets it choose, and a project that starts each task in its own
    /// worktree shows that fixed fact.
    fn place_chip(&self, context: &mut ViewContext<Self>) -> Option<View> {
        match self.worktree_chip() {
            launch_ui::WorktreeChip::Hidden => None,
            launch_ui::WorktreeChip::Fixed => Some(
                controls::fixed_chip(
                    false,
                    Some(controls::glyph::WORKTREE),
                    "New worktree",
                    "New worktree: this project starts each task in its own worktree",
                )
                .tooltip("This project starts each task in its own worktree."),
            ),
            launch_ui::WorktreeChip::Toggle(on) => Some(
                controls::chip()
                    .automation_name(if on {
                        "New worktree: the task works in a new worktree (switch to work locally)"
                    } else {
                        "Work locally: the task works in this folder (switch to a new worktree)"
                    })
                    .on_click(context.callback(|()| Message::Launch(LaunchMsg::WorktreeToggled)))
                    .content(controls::chip_content(
                        Some(controls::glyph::WORKTREE),
                        if on { "New worktree" } else { "Work locally" },
                        false,
                    ))
                    .tooltip(if on {
                        "The task works in a new worktree. Click to work in this folder."
                    } else {
                        "The task works in this folder. Click to start it in a new worktree."
                    }),
            ),
        }
    }

    /// The new task's chips (the design's bar, after +): the project,
    /// where it works, the model (Auto by default, Swarm's pick in its
    /// tooltip) and the access level (a preset's is read-only). The
    /// project's label trims to the composer's room.
    pub(super) fn composer_chips(&self, context: &mut ViewContext<Self>) -> View {
        let auto_profile = self.launch.auto_profile();
        let label = match self.launch.choice {
            Choice::Auto => "Auto".to_owned(),
            _ => self.launch.choice.chip_label(auto_profile),
        };
        let model_open = matches!(self.launch.open, Some(Pop::Model | Pop::Manage));
        let model = self.popover_chip(
            controls::chip()
                .automation_name(format!(
                    "Model: {} (Ctrl+M)",
                    self.launch.choice.chip_name(auto_profile)
                ))
                .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Model)))),
            controls::chip_content(Some(controls::glyph::MODEL), &label, false),
            &[Pop::Model, Pop::Manage],
            FlyoutPlacement::TopEdgeAlignedLeft,
            context,
        );
        // Swarm's pick for Auto in the tooltip (a chip carrying its open
        // flyout takes none).
        let model = match (&self.launch.choice, model_open) {
            (Choice::Auto, false) => model.tooltip(match auto_profile {
                Some(profile) => format!("Routing picks {profile}"),
                None => self.launch.auto_detail(),
            }),
            _ => model,
        };
        let access: View = match &self.launch.choice {
            Choice::Model(_) => {
                let access = self.launch.access();
                // Keyed by level.
                Grid::new()
                    .keyed_children(vec![KeyedView::new(
                        format!("access:{}", access.wire()),
                        self.popover_chip(
                            controls::chip()
                                .automation_name(format!("Access: {}", access.label()))
                                .on_click(context.callback(|()| {
                                    Message::Launch(LaunchMsg::Toggle(Pop::Access))
                                })),
                            controls::chip_content(
                                Some(access_glyph(access)),
                                access.label(),
                                false,
                            ),
                            &[Pop::Access],
                            FlyoutPlacement::TopEdgeAlignedLeft,
                            context,
                        ),
                    )])
                    .into()
            }
            // A preset (or Auto's pick) carries its own permissions: an
            // outlined chip that is not a control (Full access in the
            // attention tone, as the design shows it).
            _ => {
                let permissions = self
                    .launch
                    .profile()
                    .and_then(|profile| profile.permissions.as_deref());
                let label = match self.launch.profile() {
                    Some(_) => launch_ui::access_label(permissions),
                    None => "Preset access".to_owned(),
                };
                let full = permissions.and_then(SandboxPermissions::parse)
                    == Some(SandboxPermissions::DangerFullAccess);
                controls::fixed_chip(
                    full,
                    Some(controls::glyph::SHIELD),
                    &label,
                    &format!("Access: {label}, set by the preset"),
                )
                .tooltip("Set by the preset.")
            }
        };
        // The design: project, place, model, access, 6 DIPs apart.
        controls::chip_row(
            [
                Some(self.project_chip(context)),
                self.place_chip(context),
                Some(model),
                Some(access),
            ]
            .into_iter()
            .flatten()
            .collect(),
        )
    }

    /// `chip`, which opens `pops`: while one of them is open (and not for a
    /// preset, whose Model field carries its own), the chip carries it as
    /// its WinUI `Flyout`, at `placement` from the chip, with the anchor id
    /// the window opens it by (`crate::flyout`). The content is one native
    /// element (a flyout takes exactly one), switched in place between the
    /// popovers of one chip.
    pub(super) fn popover_chip(
        &self,
        chip: Button,
        label: View,
        pops: &[Pop],
        placement: FlyoutPlacement,
        context: &mut ViewContext<Self>,
    ) -> View {
        let open = self
            .launch
            .open
            .filter(|pop| pops.contains(pop) && self.launch.preset.is_none());
        self.anchored_popover(chip, label, open, placement, context)
    }

    /// `chip` with its `label` content, carrying the flyout of `open`, if
    /// any (see [`Self::popover_chip`]).
    pub(super) fn anchored_popover(
        &self,
        chip: Button,
        label: View,
        open: Option<Pop>,
        placement: FlyoutPlacement,
        context: &mut ViewContext<Self>,
    ) -> View {
        let Some(pop) = open else {
            return chip.content(label).into();
        };
        let content: Option<View> = match pop {
            Pop::Model => Some(self.model_popover(context)),
            Pop::Manage => Some(self.manage_popover(context)),
            Pop::Access => Some(self.access_popover(context)),
            Pop::Project => Some(self.project_popover(context)),
            Pop::Quotas => Some(self.quotas_popover(context)),
            Pop::Effort => self.effort_popover(context),
            Pop::Usage => self.usage_popover(context),
        };
        chip.automation_id(crate::flyout::POPOVER_ANCHOR)
            .content(label)
            .flyout_with(Flyout::rich(Border::new().maybe_content(content)).placement(placement))
    }

    pub(super) fn model_popover(&self, context: &mut ViewContext<Self>) -> View {
        let rows = self.picker_rows();
        // Titles are presentation only. Conversation rows and their actions
        // still use the Agent's scoped catalog and exact model ids.
        let titles: HashMap<_, _> = self
            .launch
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.as_ref().ok())
            .into_iter()
            .flat_map(|catalog| &catalog.models)
            .filter(|model| !model.display_name.trim().is_empty())
            .map(|model| {
                (
                    (model.harness.as_str(), model.id.as_str()),
                    model.display_name.as_str(),
                )
            })
            .collect();
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
                    group_heading(group, title, note, *collapsed, 0.0, context),
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
                    ),
                )),
                Row::Note { key, text } => items.push(KeyedView::new(
                    key.clone(),
                    controls::secondary(text.clone())
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
                            controls::secondary(text.clone())
                                .foreground(ThemeBrush::SystemCritical)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap),
                            controls::text_button()
                                .grid_column(1)
                                .is_enabled(!self.launch.catalog_loading)
                                .automation_name(format!("Retry: {text}"))
                                .on_click(context.message(Message::Launch(LaunchMsg::CatalogRetry)))
                                .content(controls::label(if self.launch.catalog_loading {
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
                    let title = match &item.kind {
                        ItemKind::Model {
                            harness,
                            model: Some(id),
                            ..
                        } => titles
                            .get(&(harness.as_str(), id.as_str()))
                            .copied()
                            .unwrap_or(id),
                        _ => &item.title,
                    };
                    let choice = self.picker_row(item, title, highlighted, context);
                    items.push(KeyedView::new(
                        format!("{}:{}:{title}", item.key, item.current),
                        choice,
                    ));
                }
            }
        }
        let field = controls::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.query.clone())
            .placeholder_text("Search models")
            .automation_name("Search models")
            .on_text_changed(crate::views::text_edits(
                context.callback(|text| Message::Launch(LaunchMsg::PickerQuery(text))),
            ));
        // OpenCode's sliders button at the right end of the search row: the
        // picker's one filter control (Manage models, and Auto's order).
        let manage = controls::icon_button()
            .grid_column(1)
            .width(tokens::height::CONTROL)
            .height(tokens::height::CONTROL)
            .automation_name("Manage models")
            .on_click(context.message(Message::Launch(LaunchMsg::ManageOpened)))
            .content(controls::icon(controls::glyph::SLIDERS));
        let search = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::XS)
            .children((field, manage));
        let list = ScrollViewer::new()
            .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Models")
            .content(StackPanel::new().keyed_children(items));
        StackPanel::new()
            .width(tokens::layout::PICKER_WIDTH)
            .spacing(tokens::spacing::XS)
            .children((
                search,
                list,
                controls::divider(),
                controls::secondary(PICKER_HINTS).margin(Thickness::new(
                    tokens::spacing::SM,
                    0.0,
                    tokens::spacing::SM,
                    tokens::spacing::XS,
                )),
            ))
            .into()
    }

    /// One choosable row (OpenCode's): the check, the full model name and an
    /// optional note, its badges, then the effort it launches as one short
    /// chip. The highlighted row (hover or keyboard) swaps the chip for a
    /// compact effort segment and shows the favorite star. Choosing the row
    /// launches exactly the model and effort it shows (`Item::choice`); the
    /// segments are siblings of the row's button, so each can be clicked.
    fn picker_row(
        &self,
        item: &Item,
        title: &str,
        highlighted: bool,
        context: &mut ViewContext<Self>,
    ) -> View {
        let enabled = item.disabled.is_none();
        let note = item.disabled.as_deref().unwrap_or(&item.detail);
        let mut name = title.to_owned();
        for (show, label) in [
            (item.current, "current"),
            (item.default, "default model"),
            (item.favorite, "favorite"),
            (item.hidden, "hidden in Manage models"),
        ] {
            if show {
                name.push_str(", ");
                name.push_str(label);
            }
        }
        // Screen readers hear the effort in full words ("medium"); a model
        // row without one launches the harness's default effort.
        let model_row = matches!(item.kind, ItemKind::Model { .. });
        let effort_words = match item.effort.as_deref() {
            Some(effort) => Some(picker::effort_name(effort)),
            None if model_row => Some(DEFAULT_EFFORT.to_lowercase()),
            None => None,
        };
        for value in [
            &item.detail[..],
            effort_words.as_deref().unwrap_or(""),
            item.disabled.as_deref().unwrap_or(""),
        ] {
            if !value.is_empty() {
                name.push_str(", ");
                name.push_str(value);
            }
        }
        let mut button = controls::row(false)
            .is_enabled(enabled)
            .automation_name(name)
            .on_click(context.message(Message::Launch(LaunchMsg::PickerChosen(item.key.clone()))));
        if highlighted {
            button = button.element_ref(&self.launch.row_ref);
        }
        let check: Option<View> = item.current.then(|| controls::icon(controls::glyph::CHECK));
        let labels: Vec<_> = [
            (item.default, "Default"),
            (item.favorite, "Favorite"),
            (item.hidden, "Hidden"),
        ]
        .into_iter()
        .filter(|(show, _)| *show)
        .map(|(_, label)| KeyedView::new(label, controls::badge(label)))
        .collect();
        // The name takes the width it needs (the picker is sized for the
        // widest catalogue name); only the note gives way.
        let button = button.content(
            Grid::new()
                .columns([
                    GridLength::Pixel(tokens::layout::CHECK_COLUMN),
                    GridLength::Auto,
                    GridLength::STAR,
                    GridLength::Auto,
                ])
                .column_spacing(tokens::spacing::SM)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .maybe_content(check),
                    TextBlock::new()
                        .grid_column(1)
                        .text(title)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    controls::secondary(note.to_owned())
                        .grid_column(2)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    StackPanel::new()
                        .grid_column(3)
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::XS)
                        .vertical_alignment(VerticalAlignment::Center)
                        .keyed_children(labels),
                )),
        );
        let segment = highlighted && enabled && !item.efforts.is_empty();
        let effort: Option<View> = if segment {
            let segments: Vec<KeyedView> =
                item.efforts
                    .iter()
                    .map(|effort| {
                        let selected = item.effort.as_deref() == Some(effort);
                        KeyedView::new(
                            format!("effort:{effort}:{selected}"),
                            controls::segment(selected)
                                .style(ButtonStyle::Subtle)
                                .height(tokens::height::CHIP)
                                .min_height(tokens::height::CHIP)
                                .min_width(0.0)
                                .automation_name(format!(
                                    "{title} with effort {}",
                                    picker::effort_name(effort)
                                ))
                                .on_click(context.message(Message::Launch(
                                    LaunchMsg::PickerEffort(item.key.clone(), effort.clone()),
                                )))
                                .content(chip_text(
                                    picker::effort_label(effort),
                                    &picker::effort_name(effort),
                                )),
                        )
                    })
                    .collect();
            Some(effort_chip(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .keyed_children(segments),
            ))
        } else if let Some((label, name)) = match item.effort.as_deref() {
            Some(effort) => Some((picker::effort_label(effort), picker::effort_name(effort))),
            // Display only: the row's choice still carries no effort.
            None if model_row => Some(("Default", DEFAULT_EFFORT.to_owned())),
            None => None,
        } {
            Some(effort_chip(chip_text(label, &name)))
        } else {
            None
        };
        let star: Option<View> = if highlighted && enabled && item.model_ref().is_some() {
            let (glyph, action) = if item.favorite {
                (
                    controls::glyph::STAR_FILLED,
                    format!("Remove {title} from favorites"),
                )
            } else {
                (controls::glyph::STAR, format!("Add {title} to favorites"))
            };
            Some(
                controls::icon_button()
                    .automation_name(action)
                    .on_click(
                        context
                            .message(Message::Launch(LaunchMsg::PickerFavorite(item.key.clone()))),
                    )
                    .content(controls::icon(glyph))
                    .into(),
            )
        } else {
            None
        };
        // Every row keeps the star's column, so the effort chips line up.
        let trailing = Grid::new()
            .grid_column(1)
            .columns([GridLength::Auto, GridLength::Pixel(tokens::height::CONTROL)])
            .column_spacing(tokens::spacing::XS)
            .vertical_alignment(VerticalAlignment::Center)
            .keyed_children(
                [
                    effort.map(|effort| KeyedView::new(format!("effort:{segment}"), effort)),
                    Some(KeyedView::new(
                        format!("star:{highlighted}:{}", item.favorite),
                        Border::new().grid_column(1).maybe_content(star),
                    )),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            );
        Border::new()
            .corner_radius(tokens::radius::CONTROL)
            .background(if highlighted {
                Brush::Theme(ThemeBrush::CardBackground)
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
                    .children((button, trailing)),
            )
            .into()
    }

    /// The Codex access menu: three levels, each with an icon, a title and
    /// one line of description; Full access in the warning color.
    fn access_popover(&self, context: &mut ViewContext<Self>) -> View {
        let current = self.launch.access();
        let mut rows: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            controls::secondary("How should the Agent's actions be approved?").margin(
                Thickness::new(
                    tokens::spacing::INSET,
                    tokens::spacing::XS,
                    tokens::spacing::INSET,
                    tokens::spacing::XS,
                ),
            ),
        )];
        for (access, refused) in self.access_levels() {
            let highlighted = self.launch.access_highlight == Some(access);
            let selected = access == current;
            let warn = access == Access::Full;
            let title = controls::body(access.label());
            let description = controls::secondary(refused.unwrap_or(access.description()))
                .text_wrapping(TextWrapping::Wrap);
            let (title, description) = if warn {
                (
                    title.foreground(ThemeBrush::SystemCritical),
                    description.foreground(ThemeBrush::SystemCritical),
                )
            } else {
                (title, description)
            };
            // The icon takes the row's foreground (warning for Full).
            let icon = controls::icon(access_glyph(access));
            let row = Button::new()
                .style(ButtonStyle::Subtle)
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
                                .maybe_content(
                                    selected.then(|| controls::icon(controls::glyph::CHECK)),
                                ),
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
                        Brush::Theme(ThemeBrush::CardBackground)
                    } else {
                        Brush::Solid(Color::transparent())
                    })
                    .on_pointer_entered(context.callback(move |_: PointerEventInfo| {
                        Message::Launch(LaunchMsg::AccessHover(access))
                    }))
                    .content(row),
            ));
        }
        StackPanel::new()
            .width(tokens::layout::MENU_WIDTH)
            .keyed_children(rows)
            .into()
    }

    /// The project list: a search field, the registered projects (name and
    /// folder) and "Other folder…".
    fn project_popover(&self, context: &mut ViewContext<Self>) -> View {
        let current = self
            .selected_project()
            .map(|project| project.workspace.clone());
        let highlight = self.launch.highlight.as_deref();
        let rows: Vec<KeyedView> = self
            .project_rows()
            .into_iter()
            .map(|(checkout, name, path, paused)| {
                let highlighted = highlight == Some(checkout.as_str());
                let selected = if self.launch.chat {
                    checkout == CHAT_PROJECT
                } else {
                    current.as_deref() == Some(checkout.as_str())
                        || (checkout == OTHER_FOLDER && self.launch.custom_workspace)
                };
                let glyph = match checkout.as_str() {
                    OTHER_FOLDER => controls::glyph::OPEN_FOLDER,
                    CHAT_PROJECT => controls::glyph::CHAT,
                    _ => controls::glyph::FOLDER,
                };
                KeyedView::new(
                    format!("{checkout}:{highlighted}:{selected}:{paused}"),
                    Border::new()
                        .corner_radius(tokens::radius::CONTROL)
                        .background(if highlighted {
                            Brush::Theme(ThemeBrush::CardBackground)
                        } else {
                            Brush::Solid(Color::transparent())
                        })
                        .content(
                            controls::row(false)
                                .is_enabled(!paused)
                                .automation_name(if paused {
                                    format!("{name}, {GIT_PAUSED}")
                                } else {
                                    format!("{name}, {path}")
                                })
                                .on_click(context.message(Message::Launch(
                                    LaunchMsg::ProjectChosen(checkout.clone()),
                                )))
                                .content(controls::row_content(
                                    Some(glyph),
                                    &name,
                                    &path,
                                    selected.then(|| controls::icon(controls::glyph::CHECK)),
                                )),
                        ),
                )
            })
            .collect();
        let search = controls::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.project_query.clone())
            .placeholder_text("Search projects")
            .automation_name("Search projects")
            .on_text_changed(crate::views::text_edits(
                context.callback(|text| Message::Launch(LaunchMsg::ProjectQuery(text))),
            ));
        StackPanel::new()
            .width(tokens::layout::MENU_WIDTH)
            .spacing(tokens::spacing::XS)
            .children((
                search,
                ScrollViewer::new()
                    .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .content(StackPanel::new().keyed_children(rows)),
            ))
            .into()
    }
}

impl DesktopApp {
    /// OpenCode's Manage models panel: the title and subtitle, a search
    /// field, then each provider group (collapsible, with its group toggle)
    /// and one toggle per model.
    pub(super) fn manage_popover(&self, context: &mut ViewContext<Self>) -> View {
        let groups = match self.launch.catalog.as_ref() {
            Some(Ok(reply)) => {
                crate::picker::manage_groups(reply, &self.prefs, &self.launch.manage_query)
            }
            _ => Vec::new(),
        };
        let mut rows: Vec<KeyedView> = Vec::new();
        // Auto's order lives with the other picker settings, so the picker
        // keeps one filter control (only a new task's picker offers Auto).
        if self.popover_agent().is_none() && self.launch.preset_target().is_none() {
            rows.push(KeyedView::new(
                "auto-order",
                controls::row(false)
                    .automation_name("Edit Auto order")
                    .on_click(context.message(Message::Shell(ShellMsg::AutoSettings)))
                    .content(controls::row_content(
                        Some(controls::glyph::ROUTING),
                        "Edit Auto order",
                        "Settings › Routing",
                        controls::icon(controls::glyph::CHEVRON_RIGHT),
                    )),
            ));
        }
        match self.launch.catalog.as_ref() {
            None => rows.push(KeyedView::new(
                "loading",
                controls::secondary("Reading the models…"),
            )),
            Some(Err(error)) => rows.push(KeyedView::new(
                "error",
                controls::secondary(format!("Models not read: {error}"))
                    .text_wrapping(TextWrapping::Wrap),
            )),
            Some(Ok(_)) if groups.is_empty() => rows.push(KeyedView::new(
                "none",
                controls::secondary("No models match. Clear the search to see every model.")
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
                manage_group_row(group, context),
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
                        .keyed_children(crate::views::slots([
                            None,
                            Some(
                                controls::body(model.title.clone())
                                    .grid_column(1)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .into(),
                            ),
                            Some(
                                ToggleSwitch::new()
                                    .grid_column(2)
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
                                    }))
                                    .into(),
                            ),
                        ])),
                ));
            }
            if group.models.len() > MANAGE_GROUP_ROWS {
                rows.push(KeyedView::new(
                    format!("more:{}", group.key),
                    controls::secondary(format!(
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
                controls::icon_button()
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .vertical_alignment(VerticalAlignment::Top)
                    .automation_name("Back to the model selector")
                    .on_click(context.message(Message::Launch(LaunchMsg::ManageClosed)))
                    .content(controls::icon(controls::glyph::BACK)),
                StackPanel::new().grid_column(1).children((
                    controls::section_title("Manage models"),
                    controls::secondary("Customize which models appear in the model selector"),
                )),
            ));
        let search = controls::field()
            .element_ref(&self.launch.field_ref)
            .text(self.launch.manage_query.clone())
            .placeholder_text("Search models")
            .automation_name("Search models to manage")
            .on_text_changed(crate::views::text_edits(
                context.callback(|text| Message::Launch(LaunchMsg::ManageQuery(text))),
            ));
        StackPanel::new()
            .width(tokens::layout::PICKER_WIDTH)
            .spacing(tokens::spacing::SM)
            .children((
                header,
                search,
                ScrollViewer::new()
                    .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Model groups")
                    .content(StackPanel::new().keyed_children(rows)),
            ))
            .into()
    }
}

/// A Manage models group row: its chevron and title collapse it; the
/// switch shows or hides the whole group (OpenCode's Manage models).
fn manage_group_row(group: &ManageGroup, context: &mut ViewContext<DesktopApp>) -> View {
    let (glyph, state) = if group.collapsed {
        (controls::glyph::CHEVRON_RIGHT, "collapsed")
    } else {
        (controls::glyph::CHEVRON_DOWN, "expanded")
    };
    let count = format!("{} of {} shown", group.shown, group.all.len());
    let key = group.key.clone();
    let toggle = controls::row(false)
        .automation_name(format!("{}, {count}, {state}", group.title))
        .on_click(context.message(Message::Launch(LaunchMsg::GroupToggled(key.clone()))))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(tokens::spacing::SM)
                .children((
                    controls::icon(glyph),
                    Border::new().grid_column(1).content(controls::row_content(
                        None,
                        &group.title,
                        &count,
                        None,
                    )),
                )),
        );
    let group_key = group.key.clone();
    let check = ToggleSwitch::new()
        .grid_column(1)
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
        .into()
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
) -> View {
    let (glyph, state) = if collapsed {
        (controls::glyph::CHEVRON_RIGHT, "collapsed")
    } else {
        (controls::glyph::CHEVRON_DOWN, "expanded")
    };
    let label = controls::section_label(title.to_owned())
        .font_weight(controls::weight(tokens::text::HEADER));
    controls::sub_row(false)
        .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
        .automation_name(format!("{title} models, {state}"))
        .on_click(context.message(Message::Launch(LaunchMsg::GroupToggled(group.to_owned()))))
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .keyed_children(crate::views::slots([
                    Some(controls::icon(glyph)),
                    Some(label.into()),
                    (!note.is_empty()).then(|| controls::secondary(note.to_owned()).into()),
                ])),
        )
        .into()
}

/// A filled effort chip container, shared by its static tag and segments.
fn effort_chip(content: impl Into<View>) -> View {
    Border::new()
        .height(tokens::height::CHIP)
        .corner_radius(tokens::radius::CONTROL)
        .background(ThemeBrush::CardBackground)
        .vertical_alignment(VerticalAlignment::Center)
        .content(content)
        .into()
}

/// The short effort label, named in full words for accessibility.
fn chip_text(label: &str, name: &str) -> TextBlock {
    controls::text(label.to_owned(), tokens::text::CHIP)
        .automation_name(name)
        .margin(Thickness::xy(tokens::spacing::SM, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
}

fn access_glyph(access: Access) -> &'static str {
    match access {
        Access::Ask => controls::glyph::LOCK,
        Access::Auto => controls::glyph::SHIELD,
        Access::Full => controls::glyph::WARNING,
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
        .keyed_children(crate::views::slots([
            Some(
                controls::section_label(title.to_owned())
                    .font_weight(controls::weight(tokens::text::HEADER))
                    .into(),
            ),
            (!note.is_empty()).then(|| controls::secondary(note.to_owned()).into()),
        ]))
        .into()
}
