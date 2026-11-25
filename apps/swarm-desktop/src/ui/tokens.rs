//! The desktop's one size scale: the brand book "Eliot Swarm Material"
//! (Material 3 Expressive, owner-approved 2026-11-24) on the Codex
//! skeleton, compact desktop density, one 4-DIP grid. The values are the
//! brand book's tokens (`crate::brand`, generated from
//! `design/tokens.json`); this module names them for the views.
//!
//! Every corner radius, height, type role, spacing, icon size, width and
//! motion duration of the views comes from this module; `app_resources`
//! writes the same values into the WinUI control styles. Touch comfort
//! comes from hit areas, never from bigger visuals; tactility is the
//! state layers, ripple, elevation and motion of `crate::theme`.

use crate::brand;

/// Shape, in Google's names: full for buttons, rows and status pills,
/// small for chips, medium for cards, menus, fields and the composer's
/// action, extra-large for the main card, the composer and dialogs.
pub mod radius {
    use super::brand;

    /// A theme swatch's corner.
    pub const SWATCH: f64 = brand::RADIUS_SMALL;
    /// A 32-DIP control, row or selection: full.
    pub const PILL: f64 = super::height::CONTROL / 2.0;
    /// A chip or segment.
    pub const CHIP: f64 = brand::RADIUS_SMALL;
    /// Every button and icon button while pressed.
    pub const PRESSED: f64 = brand::RADIUS_SMALL;
    /// The composer's 40-DIP main action and New task.
    pub const FAB: f64 = brand::RADIUS_MEDIUM;
    /// The header's 24-DIP status pill: full.
    pub const BADGE: f64 = super::height::BADGE / 2.0;
    /// Cards, tool rows, menus and popovers, the Details sections, code
    /// blocks.
    pub const CONTAINER: f64 = brand::RADIUS_MEDIUM;
    /// Text fields that are not pills (forms, source editors).
    pub const FIELD: f64 = brand::RADIUS_MEDIUM;
    /// The composer and the main card.
    pub const COMPOSER: f64 = brand::RADIUS_EXTRA_LARGE;
    /// Dialogs.
    pub const DIALOG: f64 = brand::RADIUS_EXTRA_LARGE;
    /// A message bubble's corners, and the corner by its sender.
    pub const BUBBLE: f64 = brand::RADIUS_LARGE_INCREASED;
    pub const BUBBLE_TAIL: f64 = brand::RADIUS_EXTRA_SMALL;
    /// The Stop square inside the FAB.
    pub const STOP: f64 = brand::RADIUS_STOP_MARK;
    pub const STATUS_MARK: f64 = brand::RADIUS_STATUS_MARK;
    /// Check boxes keep the M3 corner.
    pub const CHECK_BOX: f64 = brand::RADIUS_CHECKBOX;
}

/// Heights in DIPs: Material 3's desktop density, as compact as Codex.
pub mod height {
    use super::brand;

    /// Buttons, fields, every list and nav row, nested ones included.
    pub const CONTROL: f64 = brand::SIZE_ROW;
    /// A sidebar row under its project (same height as every row).
    pub const SUB_ROW: f64 = CONTROL;
    /// Chips and segments.
    pub const CHIP: f64 = brand::SIZE_CHIP;
    /// The composer's Send, Stop and Steer.
    pub const FAB: f64 = brand::SIZE_COMPOSER_ACTION;
    /// The New task button.
    pub const NEW_TASK: f64 = brand::SIZE_NEW_TASK;
    /// The ▾ beside the composer's main action.
    pub const SEND_MENU: f64 = brand::SIZE_SEND_MENU;
    /// The conversation header.
    pub const HEADER: f64 = brand::SIZE_HEADER;
    /// Standalone fields; the Home question's project button.
    pub const FIELD: f64 = brand::SIZE_FIELD;
    /// The sidebar's title row.
    pub const TITLE_ROW: f64 = brand::SIZE_TITLE_ROW;
    /// The header's status pill.
    pub const BADGE: f64 = brand::SIZE_STATUS_PILL;
    /// A form row: its label and its 32-DIP control.
    pub const FORM_ROW: f64 = brand::SIZE_FORM_ROW;
    /// A picker row with a supporting line under its title.
    pub const TWO_LINE_ROW: f64 = brand::SIZE_TWO_LINE_ROW;
    pub const SUGGESTION: f64 = brand::SIZE_NEW_TASK;
}

/// The type roles, from Google's Material 3 type scale in the brand book,
/// set in Roboto (`crate::fonts`).
pub mod text {
    use super::brand;

    pub use brand::TypeStyle;

    /// The Home title.
    pub const HERO: TypeStyle = brand::TITLE_LARGE;
    /// Page and dialog titles, the conversation title.
    pub const TITLE: TypeStyle = brand::TITLE_MEDIUM;
    pub const HEADER: TypeStyle = brand::TITLE_MEDIUM;
    pub const BODY: TypeStyle = brand::BODY_MEDIUM;
    pub const BUTTON: TypeStyle = brand::LABEL_LARGE;
    pub const CHIP: TypeStyle = brand::LABEL_LARGE;
    pub const LABEL: TypeStyle = brand::LABEL_MEDIUM;
    pub const CAPTION: TypeStyle = brand::BODY_SMALL;
    /// Filled buttons, New task, the selected sidebar row.
    pub const EMPHASIZED: TypeStyle = brand::LABEL_LARGE_EMPHASIZED;
    /// A sidebar row's trailing word, age or +/− counts.
    pub const TRAILING: TypeStyle = brand::LABEL_SMALL;
    /// Badges and counters (the Inbox count).
    pub const BADGE: TypeStyle = brand::LABEL_SMALL_EMPHASIZED;
    /// A small card's or popover's title ("Usage").
    pub const SMALL_TITLE: TypeStyle = brand::TITLE_SMALL;
    /// The daemon screen's title.
    pub const PAGE: TypeStyle = brand::HEADLINE_MEDIUM;
    /// A worker row's label under its manager (the canvas's nested row).
    pub const KID_SIZE: f64 = brand::SIZE_KID_TEXT;
}

/// The 4-DIP grid in DIPs.
pub mod spacing {
    use super::brand;

    /// Between rows in the sidebar.
    pub const ROW_GAP: f64 = brand::SPACE_ROW_GAP;
    /// The half step: stacked text lines.
    pub const XS: f64 = brand::SPACE_XS;
    /// Inside a group: between chips, a glyph and its label.
    pub const SM: f64 = brand::SPACE_SM;
    /// Icon-to-label gap inside a chip.
    pub const CHIP_GAP: f64 = brand::SPACE_CHIP_GAP;
    /// Leading padding of WinUI's text field template.
    pub const FIELD_LEADING_INSET: f64 = brand::SPACE_FIELD_LEADING;
    /// A row's inset from its pill and the gap from its icon to its label
    /// ("every sidebar row starts its icon at 10 DIP inside an 8-DIP
    /// gutter").
    pub const ROW: f64 = brand::SPACE_ROW;
    /// The content inset of a control, card or menu.
    pub const INSET: f64 = brand::SPACE_INSET;
    /// The text inset of tabs, suggestion cards and message bubbles.
    pub const TEXT_INSET: f64 = brand::SPACE_TEXT_INSET;
    /// Between groups.
    pub const MD: f64 = brand::SPACE_MD;
    /// The conversation's side padding, page padding.
    pub const LG: f64 = brand::SPACE_LG;
    /// Between the Home title, suggestions and composer.
    pub const HOME_GROUP_GAP: f64 = brand::SPACE_HOME_GAP;
    /// The main area's side padding.
    pub const XL: f64 = brand::SPACE_XL;
}

/// Icon sizes in DIPs.
pub mod icon {
    use super::brand;

    /// In rows, buttons and icon buttons.
    pub const ROW: f64 = brand::SIZE_ICON;
    /// In chips, tool rows and fields.
    pub const SMALL: f64 = brand::SIZE_ICON_SM;
    /// A chip's or row's chevron, inline status icons.
    pub const CHEVRON: f64 = brand::SIZE_ICON_XS;
    /// The Stop square in the FAB.
    pub const STOP: f64 = brand::SIZE_STOP_MARK;
}

/// The brand book's motion (Material 3 Expressive springs), which the
/// control templates sample into key frames: the press scale, the pressed
/// corner and the switch's knob on spatial-fast, state layers on
/// effects-fast; the ripple grows on effects-slow's settle time with the
/// emphasized easing. A popover's entrance is its WinUI flyout's own.
pub mod motion {
    use super::brand;

    /// A Material spring: damping ratio, stiffness (unit mass, per second
    /// squared) and its settle time, the brand book's `-ms` token.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Spring {
        pub damping: f64,
        pub stiffness: f64,
        pub settle_ms: u64,
    }

    /// Press changes of buttons, switches and segments.
    pub const SPATIAL_FAST: Spring = Spring {
        damping: brand::SPATIAL_FAST_DAMPING,
        stiffness: brand::SPATIAL_FAST_STIFFNESS,
        settle_ms: brand::SPATIAL_FAST_MS,
    };
    /// State layers, hover and press colors.
    pub const EFFECTS_FAST: Spring = Spring {
        damping: brand::EFFECTS_FAST_DAMPING,
        stiffness: brand::EFFECTS_FAST_STIFFNESS,
        settle_ms: brand::EFFECTS_FAST_MS,
    };
    pub const RIPPLE_MS: u64 = brand::EFFECTS_SLOW_MS;
    pub const PRESS_SCALE: f64 = brand::SCALE_PRESS;
    pub const FAB_PRESS_SCALE: f64 = brand::SCALE_PRESS_ACTION;
    pub const EMPHASIZED: &str = crate::brand::EASING_EMPHASIZED;
    pub const DECELERATE: &str = crate::brand::EASING_EMPHASIZED_DECELERATE;
    /// The wave indicator's travel of one period.
    pub const WAVE_MS: u64 = brand::WAVE_MS;
}

/// The switch: Google's 52 × 32 track, a 16-DIP knob off, 24 on and 28
/// while pressed, a 2-DIP outline off, and the knob's state layer at the
/// pointer target's size.
pub mod switch {
    use super::brand;

    pub const WIDTH: f64 = brand::SIZE_SWITCH_WIDTH;
    pub const HEIGHT: f64 = brand::SIZE_SWITCH_HEIGHT;
    pub const KNOB: f64 = brand::SIZE_SWITCH_KNOB;
    pub const KNOB_ON: f64 = brand::SIZE_SWITCH_KNOB_ON;
    pub const KNOB_PRESSED: f64 = brand::SIZE_SWITCH_KNOB_PRESSED;
    pub const OUTLINE: f64 = brand::SIZE_SWITCH_OUTLINE;
    pub const STATE_LAYER: f64 = brand::SIZE_TARGET;
}

/// The wave indicator of a running turn and the straight track under it
/// (and under a determinate bar).
pub mod wave {
    use super::brand;

    pub const HEIGHT: f64 = brand::SIZE_WAVE_HEIGHT;
    pub const LENGTH: f64 = brand::SIZE_WAVE_LENGTH;
    pub const STROKE: f64 = brand::SIZE_WAVE_STROKE;
    pub const TRACK: f64 = brand::SIZE_PROGRESS_TRACK;
}

/// State layers of the content color over a container.
pub mod state {
    use super::brand;

    pub const HOVER: f64 = brand::STATE_HOVER;
    pub const PRESSED: f64 = brand::STATE_PRESSED;
    pub const DISABLED_CONTAINER: f64 = brand::DISABLED_CONTAINER;
    /// Disabled selection marks and other non-text affordances.
    pub const DISABLED_CONTENT: f64 = brand::DISABLED_CONTENT;
    /// New task's shortcut hint over its label color.
    pub const SHORTCUT: f64 = brand::SHORTCUT_OPACITY;
    pub const SCRIM: f64 = brand::SCRIM_OPACITY;
    pub const PLOT_STUB_OPACITY: f64 = brand::PLOT_STUB_OPACITY;
}

/// Layout: the shell's widths, columns, limits and window size. Component
/// sizes, shapes and motion are the brand book's (the modules above); a
/// constant here only places things.
pub mod layout {
    /// The sidebar, and the Settings navigation that replaces it.
    pub const SIDEBAR_WIDTH: f64 = crate::brand::SIZE_SIDEBAR;
    /// The sidebar's side gutter.
    pub const SIDEBAR_GUTTER: f64 = 8.0;
    /// The sidebar's top and bottom padding.
    pub const SIDEBAR_PADDING: f64 = 12.0;
    /// The right pane (Changes, Agents, Details, Terminal).
    pub const PANEL_WIDTH: f64 = crate::brand::SIZE_PANE;
    /// The narrowest the task area gets beside the full sidebar; a smaller
    /// window folds the sidebar to its rail.
    pub const MAIN_MIN_WIDTH: f64 = 480.0;
    /// The conversation's column and its composer.
    pub const READING_WIDTH: f64 = crate::brand::SIZE_READING;
    /// The home screen's composer and suggestions.
    pub const HOME_WIDTH: f64 = 760.0;
    /// From the main card's top to the Home question.
    pub const HOME_TOP: f64 = crate::brand::SIZE_HOME_TOP;
    /// The main card's distance from the window's top, right and bottom
    /// edges, beside the sidebar on the window ground.
    pub const CARD_GAP: f64 = super::spacing::SM;
    /// The main card's corner.
    pub const CARD_RADIUS: f64 = crate::brand::RADIUS_EXTRA_LARGE;
    /// The daemon screen's column.
    pub const OFFLINE_WIDTH: f64 = crate::brand::SIZE_OFFLINE;
    /// The Usage popover beside the sidebar.
    pub const USAGE_POPOVER_WIDTH: f64 = crate::brand::SIZE_USAGE_POPOVER;
    /// A thin quota meter.
    pub const METER: f64 = crate::brand::SIZE_METER;
    /// The brand mark in the sidebar's title row and the daemon screen's
    /// shape.
    pub const MARK: f64 = crate::brand::SIZE_MARK;
    pub const MARK_LARGE: f64 = crate::brand::SIZE_MARK_LG;
    /// A sidebar row's state-mark slot (the dots sit centred in it).
    pub const MARK_SLOT: f64 = crate::brand::SIZE_ICON_XS;
    /// A task row's dots: waiting for you, idle.
    pub const DOT_WAIT: f64 = crate::brand::SIZE_DOT_WAIT;
    pub const DOT_IDLE: f64 = crate::brand::SIZE_DOT;
    /// The shape loader of a working row.
    pub const LOADER_SMALL: f64 = crate::brand::SIZE_LOADER_SM;
    /// The sidebar Usage row's ring.
    pub const ROW_RING: f64 = crate::brand::SIZE_ROW_RING;
    /// A badge's height and least width (the Inbox count).
    pub const BADGE: f64 = crate::brand::SIZE_BADGE;
    /// A worker row's step in under its manager: past the mark slot and
    /// its gap, so its mark sits under the manager's title.
    pub const KID_INDENT: f64 = MARK_SLOT + super::spacing::SM;
    /// The label column of every form.
    pub const LABEL_WIDTH: f64 = 200.0;
    /// Where a form's controls start: past the label column and its gap
    /// (a note or check under a field lines up with the field).
    pub const FIELD_INDENT: f64 = LABEL_WIDTH + super::spacing::SM;
    /// A confirmation dialog's width.
    pub const DIALOG_WIDTH: f64 = crate::brand::SIZE_DIALOG;
    /// A list of items inside a dialog before it scrolls.
    pub const DIALOG_LIST_MAX_HEIGHT: f64 = 240.0;
    pub const MOVE_DIALOG_WIDTH: f64 = 480.0;
    pub const MOVE_LIST_MAX_HEIGHT: f64 = 320.0;
    /// An enum dropdown ("low", "never", a role).
    pub const ENUM_WIDTH: f64 = 200.0;
    /// A number box.
    pub const NUMBER_WIDTH: f64 = 120.0;
    /// The composer's folder field (a new task outside the projects).
    pub const FOLDER_WIDTH: f64 = 280.0;
    /// One nesting step of a Markdown list in an answer.
    pub const LIST_INDENT: f64 = 20.0;
    /// A Markdown list's marker column ("•", "12."): the item's text starts
    /// after it and wraps under itself.
    pub const LIST_MARKER_WIDTH: f64 = 22.0;
    /// A nested row's indent (a task under its project, a worker under its
    /// manager): its label starts under its parent's label.
    pub const TREE_INDENT: f64 = 28.0;
    /// The composer's field grows with its text up to this height.
    pub const COMPOSER_MAX_HEIGHT: f64 = 200.0;
    /// The composer's field below its first line (the design's 44 DIP).
    pub const COMPOSER_MIN_HEIGHT: f64 = 44.0;
    /// The menu part of a split button (Start task ▾).
    pub const SPLIT_MENU: f64 = crate::brand::SIZE_SPLIT_MENU;
    /// Home's bar beside the trimmed project and model labels: +, the
    /// place and access chips, Start task with its menu, the gaps and the
    /// card insets, so the bar fits 760 with Auto (a layout budget).
    pub const HOME_BAR_FIXED: f64 = 480.0;
    /// Hide the keyboard hint in a composer narrower than this.
    pub const COMPOSER_HINT_MIN_WIDTH: f64 = 560.0;
    /// Bottom row space reserved for fixed controls, padding and gaps.
    pub const COMPOSER_ACTIONS_WIDTH: f64 = 190.0;
    pub const COMPOSER_LABEL_MIN_WIDTH: f64 = 80.0;
    /// A multiline source editor (TOML, JSON, a script).
    pub const SOURCE_MIN_HEIGHT: f64 = 240.0;
    pub const SOURCE_MAX_HEIGHT: f64 = 420.0;
    /// The hand-off dialog (`Handoff.dc.html`): ContentDialog's maximum.
    pub const HANDOFF_DIALOG_WIDTH: f64 = 548.0;
    /// The hand-off dialog's scrolling body.
    pub const DIALOG_MAX_HEIGHT: f64 = 520.0;
    /// A dropdown inside the hand-off dialog.
    pub const DIALOG_PICKER_WIDTH: f64 = 136.0;
    /// A status dot in a row.
    pub const DOT: f64 = crate::brand::SIZE_DOT_WAIT;
    /// Dividers.
    pub const HAIRLINE: f64 = crate::brand::SIZE_HAIRLINE;
    /// The decision panel's `tertiary` outline.
    pub const DECISION_OUTLINE: f64 = 2.0;
    /// The decision panel's leading inset inside its outline (the design's
    /// 18 from the card's edge).
    pub const DECISION_LEADING: f64 = 16.0;
    /// The attention strip's inset around its buttons.
    pub const STRIP_INSET: f64 = 6.0;
    /// Extend the ripple past the control so its final frame fills the shape.
    pub const RIPPLE_INSET: f64 = 24.0;
    pub const FOCUS_RING: f64 = crate::brand::SIZE_FOCUS_RING;
    pub const STATUS_STROKE: f64 = crate::brand::SIZE_STATUS_STROKE;
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
    pub const USAGE_MENU_WIDTH: f64 = 320.0;
    pub const EFFORT_MENU_MIN_WIDTH: f64 = 300.0;
    /// The scrolling list of a popover.
    pub const POPOVER_LIST_HEIGHT: f64 = 360.0;
    /// The check column of a popover row.
    pub const CHECK_COLUMN: f64 = 16.0;
    /// How far the drawn wave reaches: the widest indeterminate bar it
    /// fills (a 4K screen at 100 %); a bar clips it to its own width.
    pub const WAVE_SPAN: f64 = 3840.0;
    /// A quota ring (Models & Usage).
    pub const RING: f64 = crate::brand::SIZE_RING_SM;
    /// The composer's usage ring.
    pub const USAGE_RING: f64 = crate::brand::SIZE_RING;
    /// The neutral usage ring's stroke (the progress ring's track).
    pub const USAGE_RING_STROKE: f64 = crate::brand::SIZE_RING_STROKE;
}

/// The pages in the main card (`screens.css` `.page`): Project, Inbox,
/// Agents, Automations and Settings.
pub mod page {
    use super::brand;

    /// A list page's column at most, centred.
    pub const WIDTH: f64 = brand::SIZE_PAGE;
    /// A Settings page's column at most, centred.
    pub const SETTINGS_WIDTH: f64 = brand::SIZE_SETTINGS_PAGE;
    /// A dropdown at the end of a settings row.
    pub const SETTINGS_CONTROL: f64 = brand::SIZE_SETTINGS_CONTROL;
    /// The key column of a key-value line in a card.
    pub const KEY_COLUMN: f64 = brand::SIZE_KEY_COLUMN;
    /// A page's search field (Agents).
    pub const SEARCH: f64 = brand::SIZE_PAGE_SEARCH;
    /// The rule editor's word column and its Repeat count.
    pub const SENTENCE_WORD: f64 = brand::SIZE_SENTENCE_WORD;
    pub const REPEAT_FIELD: f64 = brand::SIZE_REPEAT_FIELD;
    /// The Agents tree's Model, Activity, CPU and Memory columns.
    pub const FLEET_MODEL: f64 = brand::SIZE_FLEET_MODEL;
    pub const FLEET_ACTIVITY: f64 = brand::SIZE_FLEET_ACTIVITY;
    pub const FLEET_NUMBER: f64 = brand::SIZE_FLEET_NUMBER;
    /// A branch's preparation state on the project page.
    pub const BRANCH_STATE: f64 = brand::SIZE_BRANCH_STATE;
    /// A theme's swatch in Settings › General.
    pub const THEME_SWATCH_WIDTH: f64 = brand::SIZE_THEME_SWATCH_WIDTH;
    pub const THEME_SWATCH_HEIGHT: f64 = brand::SIZE_THEME_SWATCH_HEIGHT;

    /// Type roles of a page: its title (`headline-medium`), a section
    /// heading (`title-small`, muted), a stat's number (`title-large`),
    /// an age (`label-small`).
    pub const TITLE: brand::TypeStyle = brand::HEADLINE_MEDIUM;
    pub const HEADING: brand::TypeStyle = brand::TITLE_SMALL;
    pub const STAT: brand::TypeStyle = brand::TITLE_LARGE;
    pub const AGE: brand::TypeStyle = brand::LABEL_SMALL;
}
