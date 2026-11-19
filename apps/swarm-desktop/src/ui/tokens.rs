//! The desktop's one shape, height, type, spacing and icon scale (owner
//! review 2026-11-16 of the #530 build: "rounded in one place and square in
//! another, huge in one place and tiny in another, the space is used
//! inefficiently").
//!
//! Every corner radius, control height, font size and weight, icon size and
//! spacing value of the views comes from this module; `app_resources`
//! writes the same values into the WinUI control styles, so a TextBox,
//! ComboBox, menu or dialog takes them without a per-control property.
//! Density is compact, as in the Codex app (32-DIP controls and rows, which
//! is also WinUI's own default control height); color stays the Material 3
//! roles of [`crate::theme`]. M3 shape tokens are themeable, so M3's pill
//! buttons become the 8-DIP control shape here.
//!
//! The M3 type-scale names map onto the four styles: headline and
//! title-large are [`text::PAGE_TITLE`], title-medium and title-small
//! [`text::SECTION_TITLE`], body-large, body-medium and the labels
//! [`text::BODY`], body-small [`text::SECONDARY`].

/// Corner radii: three values, the one circle and the M3 check box.
pub mod radius {
    /// Every control and row highlight: buttons, chips, text fields,
    /// dropdowns, list and navigation selection.
    pub const CONTROL: f64 = 8.0;
    /// Containers: cards, menus, the Details sections, dialogs, message
    /// bubbles and code blocks.
    pub const CONTAINER: f64 = 12.0;
    /// The composer.
    pub const COMPOSER: f64 = 16.0;
    /// The only circle: the composer's trailing button (half its side).
    pub const CIRCLE: f64 = super::height::FAB / 2.0;
    /// The Stop square inside it (Material's stop icon).
    pub const STOP: f64 = 3.0;
    /// Check boxes keep the M3 default corner.
    pub const CHECK_BOX: f64 = 2.0;
}

/// Material 3 elevation levels as `ThemeShadow` depth in DIPs (levels 3
/// and 4: the FAB at rest and under the pointer).
pub mod elevation {
    pub const LEVEL3: f64 = 6.0;
    pub const LEVEL4: f64 = 8.0;
}

/// Heights in DIPs (compact density).
pub mod height {
    /// Every control (button, chip, text field, dropdown, number box) and
    /// every list or navigation row.
    pub const CONTROL: f64 = 32.0;
    /// A form row: its label and its 32-DIP control.
    pub const FORM_ROW: f64 = 36.0;
    /// A sidebar sub-row: a task under its project, Recents and "No tasks".
    pub const SUB_ROW: f64 = 28.0;
    /// A picker row with a supporting line under its title (the move
    /// picker's tasks).
    pub const TWO_LINE_ROW: f64 = 48.0;
    /// A compact chip inside a row: the model picker's effort chip and the
    /// highlighted row's effort segment.
    pub const CHIP: f64 = 24.0;
    /// The composer's trailing button (Send, Stop, Steer): Material's small
    /// FAB, a touch-sized circle.
    pub const FAB: f64 = 40.0;
}

/// Native aliases of the shared Material 3 type scale.
pub mod text {
    pub use crate::theme::typography::{
        BODY, LABEL as SECONDARY, TITLE_LARGE as PAGE_TITLE, TITLE_MEDIUM as SECTION_TITLE,
        TypeStyle,
    };
}

/// The 4/8 spacing grid in DIPs.
pub mod spacing {
    /// The half step: stacked text lines.
    pub const XS: f64 = 4.0;
    /// Inside a group: between rows, a glyph and its label, buttons.
    pub const SM: f64 = 8.0;
    /// The content inset of a control or row (a button label's sides, a
    /// row's icon or text from its highlight edge).
    pub const INSET: f64 = 12.0;
    /// Between groups.
    pub const MD: f64 = 16.0;
    /// Page padding.
    pub const LG: f64 = 24.0;
}

/// Icon sizes in DIPs, all Segoe Fluent Icons (one stroke family).
pub mod icon {
    /// In rows, chips and buttons with a label.
    pub const ROW: f64 = 16.0;
    /// In toolbars: the conversation and panel headers.
    pub const TOOLBAR: f64 = 20.0;
    /// A dropdown's chevron (WinUI's ComboBox draws it at 12).
    pub const CHEVRON: f64 = 12.0;
    /// The Stop square in the composer's trailing button.
    pub const STOP: f64 = 14.0;
}

/// Widths and the few fixed sizes of the shell.
pub mod layout {
    /// The sidebar, and the Settings navigation that replaces it.
    pub const SIDEBAR_WIDTH: f64 = 280.0;
    /// The Details and monitor panel.
    pub const PANEL_WIDTH: f64 = 360.0;
    /// The conversation's reading column and the composer.
    pub const READING_WIDTH: f64 = 760.0;
    /// The Settings content, centred.
    pub const SETTINGS_WIDTH: f64 = 840.0;
    /// The label column of every form.
    pub const LABEL_WIDTH: f64 = 200.0;
    /// An enum dropdown ("low", "never", a role).
    pub const ENUM_WIDTH: f64 = 200.0;
    /// A number box.
    pub const NUMBER_WIDTH: f64 = 120.0;
    /// The composer's folder field (a new task outside the projects).
    pub const FOLDER_WIDTH: f64 = 280.0;
    /// The indent of a task under its project (and per worker level).
    pub const TREE_INDENT: f64 = 24.0;
    /// What runs an Agent (`Codex · qwen4-max`) at most in a Monitor row or
    /// the conversation header; a longer label trims.
    pub const RUNS_LABEL_WIDTH: f64 = 160.0;
    /// A sidebar row's short model (`qwen4-max`) at most; it trims past it.
    pub const SIDEBAR_MODEL_WIDTH: f64 = 80.0;
    /// The composer's field grows with its text up to this height.
    pub const COMPOSER_MAX_HEIGHT: f64 = 200.0;
    /// A multiline source editor (TOML, JSON, a script).
    pub const SOURCE_MIN_HEIGHT: f64 = 240.0;
    pub const SOURCE_MAX_HEIGHT: f64 = 420.0;
    /// The hand-off dialog's scrolling body.
    pub const DIALOG_MAX_HEIGHT: f64 = 520.0;
    /// A dropdown inside the hand-off dialog.
    pub const DIALOG_PICKER_WIDTH: f64 = 136.0;
    /// A status dot.
    pub const DOT: f64 = 8.0;
    /// Dividers.
    pub const HAIRLINE: f64 = 1.0;
    /// The indeterminate progress bar of a running turn.
    pub const PROGRESS_WIDTH: f64 = 160.0;
    /// A monitor chart's plot, and the stub of an unknown sample.
    pub const PLOT_HEIGHT: f64 = 64.0;
    pub const PLOT_STUB: f64 = 2.0;
    /// The gap between two chart bars.
    pub const PLOT_GAP: f64 = 1.0;
    /// The Monitor's state bar, a project's bar, and the surface gap
    /// between two segments of a stacked bar.
    pub const STATE_BAR: f64 = 12.0;
    /// A High Contrast state filter button has room for its glyph.
    pub const STATE_FILTER_HEIGHT: f64 = 20.0;
    pub const STATE_FILTER_MIN_WIDTH: f64 = 24.0;
    pub const PROJECT_BAR: f64 = 8.0;
    pub const BAR_GAP: f64 = 2.0;
    /// A Monitor project row: its name and count over its bar.
    pub const PROJECT_ROW: f64 = 44.0;
    /// The longest label of a Monitor filter chip.
    pub const FILTER_CHIP_TEXT: f64 = 200.0;
    /// The user's message bubble takes at most this share of the column.
    pub const BUBBLE_SHARE: f64 = 0.8;
    /// The narrowest content column.
    pub const MIN_COLUMN: f64 = 320.0;
    /// First window client size; the user resizes freely after.
    pub const WINDOW_WIDTH: f64 = 1440.0;
    pub const WINDOW_HEIGHT: f64 = 900.0;
    /// The model picker above the composer: the widest catalogue name
    /// (about 230 DIPs, "Gemini 3.1 Pro Preview Custom Tools") still fits
    /// beside the highlighted row's six-step effort segment and star.
    pub const PICKER_WIDTH: f64 = 600.0;
    /// The composer's project chip trims a long project name past this
    /// width.
    pub const PROJECT_CHIP_WIDTH: f64 = 280.0;
    /// The access and project menus above the composer.
    pub const MENU_WIDTH: f64 = 420.0;
    /// The scrolling list of a popover.
    pub const POPOVER_LIST_HEIGHT: f64 = 360.0;
    /// The check column of a popover row.
    pub const CHECK_COLUMN: f64 = 16.0;
    /// A switch's pill: WinUI's ToggleSwitch template keeps a 154-DIP
    /// content area after it (a StaticResource minimum), so a bare switch
    /// is sized to the pill and ends flush with its row.
    pub const SWITCH_WIDTH: f64 = 40.0;
    /// A quota ring (Models & Usage).
    pub const RING: f64 = 16.0;
    /// The composer's usage ring (Material's determinate circular
    /// indicator at the icon size of a toolbar).
    pub const USAGE_RING: f64 = 20.0;
}
