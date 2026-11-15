//! Settings: one page with sections, the one place for everything that
//! needs adjusting (theme and startup, models and profiles, routing,
//! projects, automation and scripts, Cloud MCP, advanced). Every read and
//! write goes through [`crate::config_client`], which the unified
//! `settings.*` API (k55) replaces.

use super::*;
use crate::settings::{Startup, ThemeChoice};
use crate::theme::winui::Palette;

/// The sections, in page order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SettingsSection {
    General,
    Models,
    Routing,
    Projects,
    Automation,
    CloudMcp,
    Advanced,
}

impl SettingsSection {
    const ALL: [Self; 7] = [
        Self::General,
        Self::Models,
        Self::Routing,
        Self::Projects,
        Self::Automation,
        Self::CloudMcp,
        Self::Advanced,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Models => "Models and profiles",
            Self::Routing => "Routing",
            Self::Projects => "Projects",
            Self::Automation => "Automation and scripts",
            Self::CloudMcp => "Cloud MCP",
            Self::Advanced => "Advanced",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum SettingsMsg {
    SectionSelected(SettingsSection),
    /// Index into the Routing picker (0: the first allowed profile).
    AutoProfilePicked(Option<usize>),
}

/// The Routing picker's first entry.
const FIRST_ALLOWED: &str = "The first launchable profile the project allows";

impl DesktopApp {
    pub(super) fn settings_update(&mut self, msg: SettingsMsg, context: &ComponentContext<Self>) {
        match msg {
            SettingsMsg::SectionSelected(section) => {
                if self.settings_section != section {
                    self.settings_section = section;
                    self.reprime_fields(context);
                    self.settings_entered(context);
                }
            }
            SettingsMsg::AutoProfilePicked(index) => {
                // A ComboBox reports None when its items are replaced.
                let Some(index) = index else {
                    return;
                };
                let profile = index
                    .checked_sub(1)
                    .and_then(|index| self.launch.profile_summaries.get(index))
                    .map(|profile| profile.name.clone());
                if profile != self.prefs.default_profile {
                    self.shell_update(ShellMsg::DefaultProfileChosen(profile), context);
                }
            }
        }
    }

    /// Read what the shown section needs, once.
    pub(super) fn settings_entered(&mut self, context: &ComponentContext<Self>) {
        match self.settings_section {
            SettingsSection::Models => {
                if !self.editor.loaded && !self.editor.busy {
                    self.load_profile_source(context);
                }
                self.load_profile_list(context);
            }
            SettingsSection::Routing => self.load_profile_list(context),
            SettingsSection::Automation => {
                if !self.scripts.listed {
                    self.load_scripts(context);
                }
                self.load_rules(context);
            }
            _ => {}
        }
    }

    /// The Settings page: the section list, then the shown section.
    pub(super) fn settings_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let rows: Vec<KeyedView> = SettingsSection::ALL
            .iter()
            .map(|section| {
                let selected = *section == self.settings_section;
                KeyedView::new(
                    section.label(),
                    m3::row(palette, selected)
                        .automation_name(if selected {
                            format!("{} (shown)", section.label())
                        } else {
                            section.label().to_owned()
                        })
                        .on_click(
                            context
                                .message(Message::Settings(SettingsMsg::SectionSelected(*section))),
                        )
                        .content(TextBlock::new().text(section.label())),
                )
            })
            .collect();
        let nav = StackPanel::new()
            .spacing(2.0)
            .margin(Thickness::new(
                theme::spacing::MD,
                theme::spacing::LG,
                theme::spacing::SM,
                theme::spacing::MD,
            ))
            .children((
                m3::title("Settings", theme::type_scale::TITLE_LARGE).margin(Thickness::new(
                    theme::spacing::SM,
                    0.0,
                    0.0,
                    theme::spacing::MD,
                )),
                StackPanel::new().spacing(2.0).keyed_children(rows),
            ));
        let section = self.settings_section;
        let body: View = match section {
            SettingsSection::General => scroll(self.general_section(context, palette)),
            SettingsSection::Models => self.profiles_view(context, palette),
            SettingsSection::Routing => scroll(self.routing_section(context, palette)),
            SettingsSection::Projects => scroll(self.projects_section(palette)),
            SettingsSection::Automation => self.automation_section(context, palette),
            SettingsSection::CloudMcp => scroll(note_card(
                palette,
                "Cloud MCP",
                &[
                    "Cloud MCP is configured in config.toml under [cloud_mcp].",
                    "The daemon has no read or write call for it yet; editing it here comes with the unified settings API.",
                ],
            )),
            SettingsSection::Advanced => scroll(self.advanced_section(context, palette)),
        };
        // The section's tree is keyed by section, so its editors are new
        // controls (see `reprime_fields`).
        let content = Grid::new()
            .grid_column(1)
            .rows([GridLength::Auto, GridLength::STAR])
            .row_spacing(theme::spacing::MD)
            .max_width(theme::layout::READING_WIDTH + 2.0 * theme::spacing::LG)
            .horizontal_alignment(HorizontalAlignment::Left)
            .margin(Thickness::new(
                theme::spacing::LG,
                theme::spacing::LG,
                theme::spacing::LG,
                theme::spacing::LG,
            ))
            .keyed_children(vec![
                KeyedView::new(
                    format!("title:{}", section.label()),
                    m3::title(section.label(), theme::type_scale::TITLE_MEDIUM),
                ),
                KeyedView::new(
                    format!("body:{}", section.label()),
                    Border::new().grid_row(1).content(body),
                ),
            ]);
        Grid::new()
            .columns([GridLength::Pixel(240.0), GridLength::STAR])
            .children((nav, content))
    }

    fn general_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let theme_choices: Vec<KeyedView> =
            [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark]
                .into_iter()
                .map(|choice| {
                    let selected = self.prefs.theme == choice;
                    KeyedView::new(
                        choice.label(),
                        m3::segment(palette, selected)
                            .automation_name(format!(
                                "Theme {}{}",
                                choice.label(),
                                if selected { " (current)" } else { "" }
                            ))
                            .on_click(
                                context.message(Message::Shell(ShellMsg::ThemeChosen(choice))),
                            )
                            .content(TextBlock::new().text(choice.label())),
                    )
                })
                .collect();
        let startup_choices: Vec<KeyedView> = [
            (Startup::NewTask, "New task"),
            (Startup::LastConversation, "Last conversation"),
        ]
        .into_iter()
        .map(|(choice, label)| {
            let selected = self.prefs.startup == choice;
            KeyedView::new(
                label,
                m3::segment(palette, selected)
                    .automation_name(format!(
                        "Open at start: {label}{}",
                        if selected { " (current)" } else { "" }
                    ))
                    .on_click(context.message(Message::Shell(ShellMsg::StartupChosen(choice))))
                    .content(TextBlock::new().text(label)),
            )
        })
        .collect();
        StackPanel::new().spacing(theme::spacing::SM).children((
            setting_card(
                palette,
                "Theme",
                "System follows the Windows setting.",
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::XS)
                    .keyed_children(theme_choices),
            ),
            setting_card(
                palette,
                "Open at start",
                "What the window shows when it opens.",
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::XS)
                    .keyed_children(startup_choices),
            ),
        ))
    }

    fn routing_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let mut names = vec![FIRST_ALLOWED.to_owned()];
        names.extend(
            self.launch
                .profile_summaries
                .iter()
                .map(|profile| profile.name.clone()),
        );
        let selected = match &self.prefs.default_profile {
            None => Some(0),
            Some(name) => self
                .launch
                .profile_summaries
                .iter()
                .position(|profile| &profile.name == name)
                .map(|index| index + 1),
        };
        // WinUI drops a ComboBox's selection when its items are replaced, so
        // a new profile list gets a new control.
        let key = format!("auto:{}", names.join("\u{1f}"));
        let picker = Grid::new().keyed_children(vec![KeyedView::new(
            key,
            ComboBox::new()
                .items_source(names)
                .selected_index(selected)
                .automation_name("Profile Auto launches")
                .min_width(320.0)
                .on_selection_changed(
                    context
                        .callback(|index| Message::Settings(SettingsMsg::AutoProfilePicked(index))),
                ),
        )]);
        StackPanel::new()
            .spacing(theme::spacing::SM)
            .children((
                note_card(
                    palette,
                    "Automatic routing",
                    &[
                        "Coming: the daemon will choose the profile for each task by role and availability, and record why.",
                    ],
                ),
                setting_card(
                    palette,
                    "Until then, Auto launches",
                    "Used when the composer's profile chip says Auto. A profile the project does not allow is skipped.",
                    picker,
                ),
            ))
    }

    fn projects_section(&self, palette: Palette) -> View {
        let mut cards: Vec<KeyedView> = self
            .nav
            .projects
            .iter()
            .map(|project| {
                let profiles = if project.profiles.is_empty() {
                    "any profile".to_owned()
                } else {
                    project.profiles.join(", ")
                };
                KeyedView::new(
                    project.id.clone(),
                    note_card_owned(
                        palette,
                        &project.id,
                        vec![
                            project.workspace.clone(),
                            format!("Allowed profiles: {profiles}"),
                        ],
                    ),
                )
            })
            .collect();
        if cards.is_empty() {
            cards.push(KeyedView::new(
                "none",
                m3::muted("No registered projects.", theme::type_scale::BODY_MEDIUM),
            ));
        }
        cards.push(KeyedView::new(
            "how",
            m3::muted(
                "Projects are registered in config.toml ([projects.ID]); automatic registration comes with the unified settings API.",
                theme::type_scale::BODY_SMALL,
            )
            .text_wrapping(TextWrapping::Wrap),
        ));
        StackPanel::new()
            .spacing(theme::spacing::SM)
            .keyed_children(cards)
    }

    fn automation_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        Grid::new()
            .rows([GridLength::Pixel(260.0), GridLength::STAR])
            .row_spacing(theme::spacing::LG)
            .children((
                config_view::automation_screen(
                    &self.automation.rules,
                    &self.automation.status,
                    context.message(Message::Automation(
                        AutomationMsg::AutomationRefreshRequested,
                    )),
                    palette,
                ),
                Border::new()
                    .grid_row(1)
                    .content(self.scripts_view(context, palette)),
            ))
    }

    fn advanced_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data_dir = match &self.data_dir {
            Ok(path) => path.display().to_string(),
            Err(error) => format!("unavailable: {error}"),
        };
        let prefs = config_client::preferences_path().map_or_else(
            || "no per-user settings folder".to_owned(),
            |path| path.display().to_string(),
        );
        StackPanel::new().spacing(theme::spacing::SM).children((
            note_card_owned(
                palette,
                "Daemon data",
                vec![
                    data_dir,
                    "Every read and change goes through the daemon's authenticated local client."
                        .to_owned(),
                ],
            ),
            note_card_owned(palette, "Desktop preferences file", vec![prefs]),
            setting_card(
                palette,
                "Reload",
                "Read projects, Agents, profiles and the open conversation again.",
                m3::tonal(palette)
                    .on_click(context.message(Message::Shell(ShellMsg::RefreshRequested)))
                    .content(TextBlock::new().text("Reload")),
            ),
        ))
    }
}

/// A section body that scrolls.
fn scroll(content: View) -> View {
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .content(content)
}

/// One setting: its name and what it does on the left, the control right.
fn setting_card(palette: Palette, title: &str, hint: &str, control: View) -> View {
    m3::card(palette).content(
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(theme::spacing::MD)
            .children((
                StackPanel::new()
                    .spacing(2.0)
                    .vertical_alignment(VerticalAlignment::Center)
                    .children((
                        m3::text(title, theme::type_scale::TITLE_SMALL),
                        m3::muted(hint, theme::type_scale::BODY_SMALL)
                            .text_wrapping(TextWrapping::Wrap),
                    )),
                Border::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(control),
            )),
    )
}

/// A card of a title and plain lines.
fn note_card(palette: Palette, title: &str, lines: &[&str]) -> View {
    note_card_owned(
        palette,
        title,
        lines.iter().map(|line| (*line).to_owned()).collect(),
    )
}

fn note_card_owned(palette: Palette, title: &str, lines: Vec<String>) -> View {
    m3::card(palette).content(
        StackPanel::new().spacing(2.0).children((
            m3::text(title, theme::type_scale::TITLE_SMALL),
            m3::muted(lines.join("\n"), theme::type_scale::BODY_SMALL)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        )),
    )
}
