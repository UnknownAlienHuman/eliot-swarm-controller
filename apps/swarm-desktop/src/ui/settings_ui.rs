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

/// The section list beside the shown section.
const SECTION_LIST_WIDTH: f64 = 280.0;

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

    /// The Settings page: the section list (navigation items), then the
    /// shown section under its title-large title.
    pub(super) fn settings_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let rows: Vec<KeyedView> = SettingsSection::ALL
            .iter()
            .map(|section| {
                let selected = *section == self.settings_section;
                KeyedView::new(
                    format!("{}:{selected}", section.label()),
                    m3::nav_item(palette, selected)
                        .automation_name(if selected {
                            format!("{} (shown)", section.label())
                        } else {
                            section.label().to_owned()
                        })
                        .on_click(
                            context
                                .message(Message::Settings(SettingsMsg::SectionSelected(*section))),
                        )
                        .content(m3::nav_content(
                            TextBlock::new()
                                .text(section.label())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                )
            })
            .collect();
        let nav = StackPanel::new()
            .margin(Thickness::new(
                12.0,
                theme::spacing::LG,
                theme::spacing::SM,
                theme::spacing::MD,
            ))
            .children((
                m3::title("Settings", theme::type_scale::TITLE_LARGE).margin(Thickness::new(
                    theme::spacing::MD,
                    0.0,
                    0.0,
                    theme::spacing::MD,
                )),
                StackPanel::new().keyed_children(rows),
            ));
        let section = self.settings_section;
        let body: View = match section {
            SettingsSection::General => scroll(self.general_section(context, palette)),
            SettingsSection::Models => self.profiles_view(context, palette),
            SettingsSection::Routing => scroll(self.routing_section(context, palette)),
            SettingsSection::Projects => scroll(self.projects_section(palette)),
            SettingsSection::Automation => self.automation_section(context, palette),
            SettingsSection::CloudMcp => scroll(m3::list_group(
                palette,
                vec![(
                    "cloud".to_owned(),
                    m3::list_item(
                        "Configured in config.toml",
                        "Cloud MCP lives under [cloud_mcp]. The daemon has no read or write call for it yet; editing it here comes with the unified settings API.",
                        View::empty(),
                    ),
                )],
            )),
            SettingsSection::Advanced => scroll(self.advanced_section(context, palette)),
        };
        // The section's tree is keyed by section, so its editors are new
        // controls (see `reprime_fields`).
        // One explicit column width, so every section's groups line up
        // (content-sized until the window size is observed).
        let width = self.main_width().map(|width| {
            (width - SECTION_LIST_WIDTH - 2.0 * theme::spacing::LG)
                .clamp(320.0, theme::layout::READING_WIDTH)
        });
        let content = Grid::new()
            .grid_column(1)
            .rows([GridLength::Auto, GridLength::STAR])
            .row_spacing(theme::spacing::LG)
            .width(width)
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
                    m3::title(section.label(), theme::type_scale::TITLE_LARGE),
                ),
                KeyedView::new(
                    format!("body:{}", section.label()),
                    Border::new().grid_row(1).content(body),
                ),
            ]);
        Grid::new()
            .columns([GridLength::Pixel(SECTION_LIST_WIDTH), GridLength::STAR])
            .children((nav, content))
    }

    fn general_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let segments: Vec<KeyedView> = [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark]
            .into_iter()
            .map(|choice| {
                let selected = self.prefs.theme == choice;
                KeyedView::new(
                    format!("{}:{selected}", choice.label()),
                    m3::segment(palette, selected)
                        .automation_name(format!(
                            "Theme {}{}",
                            choice.label(),
                            if selected { " (current)" } else { "" }
                        ))
                        .on_click(context.message(Message::Shell(ShellMsg::ThemeChosen(choice))))
                        .content(m3::segment_label(choice.label(), selected)),
                )
            })
            .collect();
        let reopen = self.prefs.startup == Startup::LastConversation;
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            ToggleSwitch::new()
                .is_on(reopen)
                .automation_name("Reopen the last conversation at start")
                .on_toggled(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                })),
        )]);
        m3::list_group(
            palette,
            vec![
                (
                    "theme".to_owned(),
                    m3::list_item(
                        "Theme",
                        "System follows the Windows setting.",
                        m3::segmented(palette, segments),
                    ),
                ),
                (
                    "reopen".to_owned(),
                    m3::list_item(
                        "Reopen the last conversation",
                        "At start, open the conversation you left instead of a new task.",
                        switch,
                    ),
                ),
            ],
        )
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
                .min_width(280.0)
                .on_selection_changed(
                    context
                        .callback(|index| Message::Settings(SettingsMsg::AutoProfilePicked(index))),
                ),
        )]);
        m3::list_group(
            palette,
            vec![
                (
                    "auto".to_owned(),
                    m3::list_item(
                        "Auto launches",
                        "Used when the composer's profile chip says Auto. A profile the project does not allow is skipped.",
                        picker,
                    ),
                ),
                (
                    "coming".to_owned(),
                    m3::list_item(
                        "Automatic routing",
                        "Coming: the daemon will choose the profile for each task by role and availability, and record why.",
                        View::empty(),
                    ),
                ),
            ],
        )
    }

    fn projects_section(&self, palette: Palette) -> View {
        let items: Vec<(String, View)> = self
            .nav
            .projects
            .iter()
            .map(|project| {
                let profiles = if project.profiles.is_empty() {
                    "any profile".to_owned()
                } else {
                    project.profiles.join(", ")
                };
                (
                    project.id.clone(),
                    m3::list_item(
                        &project.id,
                        &format!(
                            "{}\nAllowed profiles: {profiles}",
                            nav::display_path(&project.workspace)
                        ),
                        View::empty(),
                    ),
                )
            })
            .collect();
        let list = if items.is_empty() {
            m3::muted("No registered projects.", theme::type_scale::BODY_MEDIUM).into()
        } else {
            m3::list_group(palette, items)
        };
        StackPanel::new()
            .spacing(theme::spacing::MD)
            .children((
                list,
                m3::muted(
                    "Projects are registered in config.toml ([projects.ID]); automatic registration comes with the unified settings API.",
                    theme::type_scale::BODY_SMALL,
                )
                .margin(Thickness::xy(theme::spacing::MD, 0.0))
                .text_wrapping(TextWrapping::Wrap),
            ))
    }

    fn automation_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        scroll(StackPanel::new().spacing(theme::spacing::LG).children((
            config_view::automation_screen(
                &self.automation.rules,
                &self.automation.status,
                context.message(Message::Automation(
                    AutomationMsg::AutomationRefreshRequested,
                )),
                palette,
            ),
            self.scripts_view(context, palette),
        )))
    }

    fn advanced_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let data_dir = match &self.data_dir {
            Ok(path) => nav::display_path(&path.display().to_string()).to_owned(),
            Err(error) => format!("unavailable: {error}"),
        };
        let prefs = config_client::preferences_path().map_or_else(
            || "no per-user settings folder".to_owned(),
            |path| path.display().to_string(),
        );
        m3::list_group(
            palette,
            vec![
                (
                    "data".to_owned(),
                    m3::list_item(
                        "Daemon data",
                        &format!(
                            "{data_dir}\nEvery read and change goes through the daemon's authenticated local client."
                        ),
                        View::empty(),
                    ),
                ),
                (
                    "prefs".to_owned(),
                    m3::list_item("Desktop preferences file", &prefs, View::empty()),
                ),
                (
                    "reload".to_owned(),
                    m3::list_item(
                        "Reload",
                        "Read projects, Agents, profiles and the open conversation again.",
                        m3::outlined(palette)
                            .on_click(context.message(Message::Shell(ShellMsg::RefreshRequested)))
                            .content(TextBlock::new().text("Reload")),
                    ),
                ),
            ],
        )
    }
}

/// A section body that scrolls.
fn scroll(content: View) -> View {
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .content(content)
}
