//! The desktop's one size scale: the approved design (owner, 2026-11-19),
//! `System.dc.html` of the eliot-design board. Codex layout and logic,
//! Material 3 tactile style, compact desktop density, one 4-DIP grid.
//!
//! Every corner radius, height, type role, spacing, icon size, width and
//! motion duration of the views comes from this module; `app_resources`
//! writes the same values into the WinUI control styles. Touch comfort
//! comes from hit areas, never from bigger visuals; tactility is the
//! state layers, ripple, elevation and motion of `crate::theme`.

/// Shape: full for everything pressed (half the height), 12 for cards and
/// tool rows, 20 for the composer, 28 for dialogs.
pub mod radius {
    /// A theme swatch's corner.
    pub const SWATCH: f64 = 6.0;
    /// A 32-DIP control, row or selection: full.
    pub const PILL: f64 = super::height::CONTROL / 2.0;
    /// A 28-DIP chip or segment: full.
    pub const CHIP: f64 = super::height::CHIP / 2.0;
    /// The 36-DIP FAB: a circle.
    pub const FAB: f64 = super::height::FAB / 2.0;
    /// The header's 24-DIP status pill: full.
    pub const BADGE: f64 = super::height::BADGE / 2.0;
    /// Cards, tool rows, suggestion cards, menus and popovers, the
    /// Details sections, code blocks.
    pub const CONTAINER: f64 = 12.0;
    /// Text fields that are not pills (forms, source editors).
    pub const FIELD: f64 = 12.0;
    /// The composer.
    pub const COMPOSER: f64 = 20.0;
    /// Dialogs.
    pub const DIALOG: f64 = 28.0;
    /// A message bubble's corners, and the corner by its sender.
    pub const BUBBLE: f64 = 18.0;
    pub const BUBBLE_TAIL: f64 = 4.0;
    /// The Stop square inside the FAB.
    pub const STOP: f64 = 3.0;
    pub const STATUS_MARK: f64 = 2.0;
    /// Check boxes keep the M3 corner.
    pub const CHECK_BOX: f64 = 2.0;
}

/// Heights in DIPs: Material 3's desktop density, as compact as Codex.
pub mod height {
    /// Buttons, fields, every list and nav row, nested ones included.
    pub const CONTROL: f64 = 32.0;
    /// A sidebar row under its project (same height as every row).
    pub const SUB_ROW: f64 = CONTROL;
    /// Chips and segments.
    pub const CHIP: f64 = 28.0;
    /// The composer's Send, Stop and Steer.
    pub const FAB: f64 = 36.0;
    /// The conversation header.
    pub const HEADER: f64 = 52.0;
    /// The sidebar's title row.
    pub const TITLE_ROW: f64 = 40.0;
    /// The header's status pill.
    pub const BADGE: f64 = 24.0;
    /// A form row: its label and its 32-DIP control.
    pub const FORM_ROW: f64 = 40.0;
    /// A picker row with a supporting line under its title.
    pub const TWO_LINE_ROW: f64 = 48.0;
    pub const SUGGESTION: f64 = 44.0;
}

/// The design's type roles. Segoe UI Variable is the Windows fallback
/// specified for Roboto Flex in the design.
pub mod text {
    pub const FAMILY: &str = "Segoe UI Variable";

    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct TypeStyle {
        pub size: f64,
        pub weight: u16,
    }

    const fn style(size: f64, weight: u16) -> TypeStyle {
        TypeStyle { size, weight }
    }

    pub const HERO: TypeStyle = style(28.0, 500);
    pub const TITLE: TypeStyle = style(16.0, 600);
    pub const HEADER: TypeStyle = style(15.0, 500);
    pub const BODY: TypeStyle = style(14.0, 400);
    pub const BUTTON: TypeStyle = style(14.0, 500);
    pub const CHIP: TypeStyle = style(13.0, 400);
    pub const LABEL: TypeStyle = style(12.0, 500);
    pub const CAPTION: TypeStyle = style(12.0, 400);
    pub const SECONDARY: TypeStyle = CAPTION;
    pub const SECTION_TITLE: TypeStyle = HEADER;
}

/// The 4-DIP grid in DIPs.
pub mod spacing {
    /// Between rows in the sidebar.
    pub const ROW_GAP: f64 = 2.0;
    /// The half step: stacked text lines.
    pub const XS: f64 = 4.0;
    /// Inside a group: between chips, a glyph and its label.
    pub const SM: f64 = 8.0;
    /// Icon-to-label gap inside a chip.
    pub const CHIP_GAP: f64 = 6.0;
    /// Leading padding of WinUI's text field template.
    pub const FIELD_LEADING_INSET: f64 = 10.0;
    /// A row's inset from its pill and the gap from its icon to its label
    /// ("every sidebar row starts its icon at 10 DIP inside an 8-DIP
    /// gutter").
    pub const ROW: f64 = 10.0;
    /// The content inset of a control, card or menu.
    pub const INSET: f64 = 12.0;
    /// The text inset of tabs, suggestion cards and message bubbles.
    pub const TEXT_INSET: f64 = 14.0;
    /// Between groups.
    pub const MD: f64 = 16.0;
    /// The conversation's side padding, page padding.
    pub const LG: f64 = 24.0;
    /// Between the Home title, suggestions and composer.
    pub const HOME_GROUP_GAP: f64 = 28.0;
    /// The main area's side padding.
    pub const XL: f64 = 32.0;
}

/// Icon sizes in DIPs, all Segoe Fluent Icons (one stroke family).
pub mod icon {
    /// In rows and icon buttons.
    pub const ROW: f64 = 18.0;
    /// In chips, tool rows and fields.
    pub const SMALL: f64 = 16.0;
    /// A chip's or row's chevron.
    pub const CHEVRON: f64 = 14.0;
    /// The Stop square in the FAB.
    pub const STOP: f64 = 12.0;
}

/// The design's motion: emphasized easing, 150 ms for states, 250 ms for a
/// popover's entrance (a fade and a 4-DIP rise).
pub mod motion {
    pub const STATE_MS: u64 = 150;
    pub const PRESS_MS: u64 = 100;
    pub const RIPPLE_MS: u64 = 300;
    pub const POPOVER_MS: u64 = 250;
    pub const PRESS_SCALE: f64 = 0.97;
    pub const FAB_PRESS_SCALE: f64 = 0.94;
    pub const POPOVER_START_SCALE: f64 = 0.97;
    pub const EMPHASIZED: &str = "0.2,0 0,1";
    pub const DECELERATE: &str = "0.05,0.7 0.1,1";
}

/// State layers of the content color over a container.
pub mod state {
    pub const HOVER: f64 = 0.08;
    pub const PRESSED: f64 = 0.12;
    /// The ripple above the pressed layer, fading on release.
    pub const RIPPLE: f64 = 0.03;
    pub const DISABLED_CONTAINER: f64 = 0.12;
    /// Disabled selection marks and other non-text affordances.
    pub const DISABLED_CONTENT: f64 = 0.38;
    pub const SCRIM: f64 = 0.32;
    pub const PLOT_STUB_OPACITY: f64 = 0.72;
}

/// Widths and the few fixed sizes of the shell.
pub mod layout {
    /// The sidebar, and the Settings navigation that replaces it.
    pub const SIDEBAR_WIDTH: f64 = 264.0;
    /// The sidebar's side gutter.
    pub const SIDEBAR_GUTTER: f64 = 8.0;
    /// The sidebar's top and bottom padding.
    pub const SIDEBAR_PADDING: f64 = 12.0;
    /// The Monitor and Details pane.
    pub const PANEL_WIDTH: f64 = 320.0;
    /// The narrowest the task area gets beside the full sidebar; a smaller
    /// window folds the sidebar to its rail.
    pub const MAIN_MIN_WIDTH: f64 = 480.0;
    /// The conversation's column and its composer.
    pub const READING_WIDTH: f64 = 720.0;
    /// The home screen's composer and suggestions.
    pub const HOME_WIDTH: f64 = 760.0;
    /// The Settings content, centred.
    pub const SETTINGS_WIDTH: f64 = 720.0;
    /// The label column of every form.
    pub const LABEL_WIDTH: f64 = 200.0;
    /// Where a form's controls start: past the label column and its gap
    /// (a note or check under a field lines up with the field).
    pub const FIELD_INDENT: f64 = LABEL_WIDTH + super::spacing::SM;
    /// The narrow label column of a path line (an automation's paths).
    pub const PATH_LABEL_WIDTH: f64 = 96.0;
    /// A confirmation dialog's width.
    pub const DIALOG_WIDTH: f64 = 420.0;
    /// A list of items inside a dialog before it scrolls.
    pub const DIALOG_LIST_MAX_HEIGHT: f64 = 240.0;
    pub const MOVE_DIALOG_WIDTH: f64 = 480.0;
    pub const MOVE_LIST_MAX_HEIGHT: f64 = 320.0;
    /// A theme's swatch in Settings › Appearance.
    pub const SWATCH_WIDTH: f64 = 48.0;
    pub const SWATCH_HEIGHT: f64 = 24.0;
    /// An enum dropdown ("low", "never", a role).
    pub const ENUM_WIDTH: f64 = 200.0;
    /// A number box.
    pub const NUMBER_WIDTH: f64 = 120.0;
    /// The composer's folder field (a new task outside the projects).
    pub const FOLDER_WIDTH: f64 = 280.0;
    /// A worktree project's name at most in its sidebar row, so the
    /// subproject beside it keeps the rest of the row.
    pub const PROJECT_NAME_WIDTH: f64 = 104.0;
    /// One nesting step of a Markdown list in an answer.
    pub const LIST_INDENT: f64 = 20.0;
    /// A Markdown list's marker column ("•", "12."): the item's text starts
    /// after it and wraps under itself.
    pub const LIST_MARKER_WIDTH: f64 = 22.0;
    /// A nested row's indent (a task under its project, a worker under its
    /// manager): its label starts under its parent's label.
    pub const TREE_INDENT: f64 = 28.0;
    /// What runs an Agent (`Codex · qwen4-max`) at most in a Monitor row or
    /// the conversation header; a longer label trims.
    pub const RUNS_LABEL_WIDTH: f64 = 200.0;
    pub const HEADER_TITLE_MIN_WIDTH: f64 = 96.0;
    pub const HEADER_STATUS_MIN_WIDTH: f64 = 64.0;
    /// The widest trailing word of a Monitor row (model, need or age).
    pub const MONITOR_TRAILING_WIDTH: f64 = 112.0;
    /// The composer's field grows with its text up to this height.
    pub const COMPOSER_MAX_HEIGHT: f64 = 200.0;
    /// The composer's field below its first line (the design's 44 DIP).
    pub const COMPOSER_MIN_HEIGHT: f64 = 44.0;
    /// Hide the keyboard hint in a composer narrower than this.
    pub const COMPOSER_HINT_MIN_WIDTH: f64 = 560.0;
    /// Bottom row space reserved for fixed controls, padding and gaps.
    pub const COMPOSER_ACTIONS_WIDTH: f64 = 190.0;
    pub const COMPOSER_ACCESS_WIDTH: f64 = 112.0;
    pub const COMPOSER_LABEL_MIN_WIDTH: f64 = 80.0;
    /// A multiline source editor (TOML, JSON, a script).
    pub const SOURCE_MIN_HEIGHT: f64 = 240.0;
    pub const SOURCE_MAX_HEIGHT: f64 = 420.0;
    /// The hand-off dialog's scrolling body.
    pub const DIALOG_MAX_HEIGHT: f64 = 520.0;
    /// A dropdown inside the hand-off dialog.
    pub const DIALOG_PICKER_WIDTH: f64 = 136.0;
    /// A status dot in a row.
    pub const DOT: f64 = 8.0;
    /// The dot of the header's status pill.
    pub const BADGE_DOT: f64 = 6.0;
    /// Dividers.
    pub const HAIRLINE: f64 = 1.0;
    pub const FOCUS_RING: f64 = 2.0;
    pub const STATUS_STROKE: f64 = 1.5;
    /// The indeterminate progress bar of a running turn.
    pub const PROGRESS_WIDTH: f64 = 160.0;
    /// A monitor chart's plot, and the stub of an unknown sample.
    pub const PLOT_HEIGHT: f64 = 64.0;
    pub const PLOT_STUB: f64 = 2.0;
    /// The gap between two chart bars.
    pub const PLOT_GAP: f64 = 1.0;
    /// The user's message bubble takes at most this share of the column.
    pub const BUBBLE_SHARE: f64 = 0.8;
    /// The narrowest content column.
    pub const MIN_COLUMN: f64 = 320.0;
    /// First window client size; the user resizes freely after.
    pub const WINDOW_WIDTH: f64 = 1440.0;
    pub const WINDOW_HEIGHT: f64 = 900.0;
    /// The model picker above the composer.
    pub const PICKER_WIDTH: f64 = 600.0;
    /// The composer's project chip trims a long project name past this
    /// width.
    pub const PROJECT_CHIP_WIDTH: f64 = 280.0;
    /// The access and project menus above the composer.
    pub const MENU_WIDTH: f64 = 420.0;
    pub const CONTEXT_MENU_WIDTH: f64 = 248.0;
    pub const USAGE_MENU_WIDTH: f64 = 320.0;
    pub const EFFORT_MENU_MIN_WIDTH: f64 = 300.0;
    /// The scrolling list of a popover.
    pub const POPOVER_LIST_HEIGHT: f64 = 360.0;
    /// The check column of a popover row.
    pub const CHECK_COLUMN: f64 = 16.0;
    /// A switch's pill: WinUI's ToggleSwitch template keeps a 154-DIP
    /// content area after it, so a bare switch is sized to the pill.
    pub const SWITCH_WIDTH: f64 = 40.0;
    /// A quota ring (Models & Usage).
    pub const RING: f64 = 16.0;
    /// The composer's usage ring.
    pub const USAGE_RING: f64 = 22.0;
    /// The neutral usage ring's stroke (the progress ring's track).
    pub const USAGE_RING_STROKE: f64 = 2.0;
}
