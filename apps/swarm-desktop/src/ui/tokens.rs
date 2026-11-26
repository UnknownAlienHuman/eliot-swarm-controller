//! The desktop's one set of layout constants (brand book
//! `docs/design/fluent-design-system.md` §4.3–4.4): sizes, spacing, type
//! ramp and the few shapes the app draws itself. Colors are not here: stock
//! WinUI controls keep their own theme resources and states, and the few
//! custom marks take Reactor's [`windows_reactor::ThemeBrush`] values, which
//! follow System, Light, Dark and High Contrast by themselves.
//!
//! Shapes, heights and padding of standard controls belong to their WinUI
//! templates; a constant here only places things or sizes the app's own
//! surfaces and marks.

/// Corners the app draws itself (§4.4 `content.radius`): standard controls
/// keep their template's corners.
pub mod radius {
    /// A small app shape beside controls (a badge, a fixed chip, a status
    /// pill): WinUI's `ControlCornerRadius`.
    pub const CONTROL: f64 = 4.0;
    /// A standalone app surface: the main card, a settings group, the
    /// composer, a message bubble.
    pub const CONTAINER: f64 = 8.0;
    /// The failed state's rounded-square mark.
    pub const STATUS_MARK: f64 = 2.0;
}

/// Heights the layout reserves: minimums, never clipping heights for
/// standard controls.
pub mod height {
    /// A row, a button, a menu entry.
    pub const CONTROL: f64 = 32.0;
    /// A row under its parent.
    pub const SUB_ROW: f64 = CONTROL;
    /// A chip.
    pub const CHIP: f64 = 32.0;
    /// The New task button.
    pub const NEW_TASK: f64 = 40.0;
    /// The conversation header.
    pub const HEADER: f64 = 52.0;
    /// A page's title row.
    pub const TITLE_ROW: f64 = 40.0;
    /// A status pill or file chip.
    pub const BADGE: f64 = 24.0;
    /// A labelled form row.
    pub const FORM_ROW: f64 = 40.0;
    /// A two-line list row.
    pub const TWO_LINE_ROW: f64 = 48.0;
    /// A suggestion card on Home.
    pub const SUGGESTION: f64 = 40.0;
}

/// The WinUI type ramp (§4.3) in the roles the views use; the font is the
/// system UI font (Segoe UI Variable).
pub mod text {
    /// A type role: size in DIPs and weight.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct TypeStyle {
        pub size: f64,
        pub weight: u16,
    }

    const CAPTION_RAMP: TypeStyle = TypeStyle {
        size: 12.0,
        weight: 400,
    };
    const BODY_RAMP: TypeStyle = TypeStyle {
        size: 14.0,
        weight: 400,
    };
    const BODY_STRONG_RAMP: TypeStyle = TypeStyle {
        size: 14.0,
        weight: 600,
    };
    const SUBTITLE_RAMP: TypeStyle = TypeStyle {
        size: 20.0,
        weight: 600,
    };
    const TITLE_RAMP: TypeStyle = TypeStyle {
        size: 28.0,
        weight: 600,
    };

    /// The Home title (Title).
    pub const HERO: TypeStyle = TITLE_RAMP;
    /// A card or dialog title (Body Strong).
    pub const TITLE: TypeStyle = BODY_STRONG_RAMP;
    /// A panel or conversation title (Subtitle).
    pub const HEADER: TypeStyle = SUBTITLE_RAMP;
    pub const BODY: TypeStyle = BODY_RAMP;
    /// A button or row label (Body).
    pub const BUTTON: TypeStyle = BODY_RAMP;
    pub const CHIP: TypeStyle = BODY_RAMP;
    /// A section label (Caption).
    pub const LABEL: TypeStyle = CAPTION_RAMP;
    pub const CAPTION: TypeStyle = CAPTION_RAMP;
    /// A selected row's label (Body Strong).
    pub const EMPHASIZED: TypeStyle = BODY_STRONG_RAMP;
    /// A row's trailing word, age or count (Caption).
    pub const TRAILING: TypeStyle = CAPTION_RAMP;
    /// A group heading (Body Strong).
    pub const SMALL_TITLE: TypeStyle = BODY_STRONG_RAMP;
    /// A page title (Title).
    pub const PAGE: TypeStyle = TITLE_RAMP;
    /// A worker row's label under its manager (Caption size).
    pub const KID_SIZE: f64 = CAPTION_RAMP.size;
}

/// Spacing between and inside groups.
pub mod spacing {
    /// Between rows of one list.
    pub const ROW_GAP: f64 = 2.0;
    pub const XS: f64 = 4.0;
    pub const SM: f64 = 8.0;
    /// Between a chip's icon, label and chevron.
    pub const CHIP_GAP: f64 = 6.0;
    /// A field's leading text inset.
    pub const FIELD_LEADING_INSET: f64 = 10.0;
    /// A row's leading inset; icon to label.
    pub const ROW: f64 = 10.0;
    /// A small block's inset.
    pub const INSET: f64 = 12.0;
    /// A text block's inset.
    pub const TEXT_INSET: f64 = 14.0;
    pub const MD: f64 = 16.0;
    pub const LG: f64 = 24.0;
    /// Between Home's groups.
    pub const HOME_GROUP_GAP: f64 = 28.0;
    pub const XL: f64 = 32.0;
}

/// Glyph sizes (§4.4 `icon.inline`): the glyph itself, not its hit target.
pub mod icon {
    /// Rows, menus and icon buttons.
    pub const ROW: f64 = 16.0;
    /// Chips, tool rows and fields.
    pub const SMALL: f64 = 16.0;
    /// Chevrons and inline marks.
    pub const CHEVRON: f64 = 12.0;
}

/// Opacity the layout uses for a non-interactive overlay state.
pub mod state {
    /// The sidebar's last list while the daemon is not reachable.
    pub const DISABLED_CONTENT: f64 = 0.38;
    /// A shortcut hint beside a label.
    pub const SHORTCUT: f64 = 0.7;
    /// The stub bar of an unknown chart sample.
    pub const PLOT_STUB_OPACITY: f64 = 0.38;
}

/// Widths and placements of the app's own layout.
pub mod layout {
    /// The sidebar, and the Settings navigation that replaces it, at its
    /// default width; its handle keeps it between the bounds (at most 360,
    /// so a 900-DIP window keeps the main area's 520 beside it).
    pub const SIDEBAR_WIDTH: f64 = 264.0;
    pub const SIDEBAR_MIN_WIDTH: f64 = 200.0;
    pub const SIDEBAR_MAX_WIDTH: f64 = 360.0;
    /// The sidebar hidden to its icons.
    pub const RAIL_WIDTH: f64 = super::height::CONTROL + 2.0 * SIDEBAR_GUTTER;
    /// The sidebar's side gutter.
    pub const SIDEBAR_GUTTER: f64 = 8.0;
    pub const SIDEBAR_PADDING: f64 = 12.0;
    /// The right pane at its default width; its handle keeps it between the
    /// bounds, and at the wide width (its maximum) its content takes the
    /// wide layout, as in Full view.
    pub const PANEL_WIDTH: f64 = 480.0;
    pub const PANEL_WIDE_WIDTH: f64 = READING_WIDTH;
    pub const PANEL_MIN_WIDTH: f64 = 320.0;
    pub const PANEL_MAX_WIDTH: f64 = PANEL_WIDE_WIDTH;
    /// A panel's drag handle, and one arrow-key step of it.
    pub const SPLITTER: f64 = 6.0;
    pub const SPLIT_STEP: f64 = 10.0;
    /// The §6.2 size classes by client width: Medium from 900, Wide from
    /// 1280, Wider from 1520.
    pub const MEDIUM_FROM: f64 = 900.0;
    pub const WIDE_FROM: f64 = 1280.0;
    pub const WIDER_FROM: f64 = 1520.0;
    /// §6.2 docking: the pane docks when the window holds the sidebar,
    /// the main area's required width, the pane and this separation.
    pub const MAIN_REQUIRED_WIDTH: f64 = 520.0;
    pub const DOCK_GAP: f64 = 16.0;
    pub const FILTER_WIDTH: f64 = 168.0;
    /// The conversation and composer column.
    pub const READING_WIDTH: f64 = 720.0;
    pub const HOME_WIDTH: f64 = 760.0;
    pub const HOME_TOP: f64 = 168.0;
    /// The main card's distance from the window's edges.
    pub const CARD_GAP: f64 = super::spacing::SM;
    /// The main card's corner (§4.4 `content.radius`).
    pub const CARD_RADIUS: f64 = super::radius::CONTAINER;
    pub const OFFLINE_WIDTH: f64 = 480.0;
    pub const USAGE_POPOVER_WIDTH: f64 = 340.0;
    /// The daemon screen's marks.
    pub const MARK: f64 = 22.0;
    /// A row's state-mark slot.
    pub const MARK_SLOT: f64 = 16.0;
    pub const DOT_WAIT: f64 = 8.0;
    pub const DOT_IDLE: f64 = 6.0;
    pub const LOADER_SMALL: f64 = 12.0;
    /// A worker row's indent past its manager's mark.
    pub const KID_INDENT: f64 = MARK_SLOT + super::spacing::SM;
    pub const LABEL_WIDTH: f64 = 200.0;
    /// Help under a form row's control starts here.
    pub const FIELD_INDENT: f64 = LABEL_WIDTH + super::spacing::SM;
    pub const DIALOG_WIDTH: f64 = 420.0;
    pub const DIALOG_LIST_MAX_HEIGHT: f64 = 240.0;
    pub const MOVE_DIALOG_WIDTH: f64 = 480.0;
    pub const MOVE_LIST_MAX_HEIGHT: f64 = 320.0;
    pub const ENUM_WIDTH: f64 = 200.0;
    pub const NUMBER_WIDTH: f64 = 120.0;
    pub const FOLDER_WIDTH: f64 = 280.0;
    pub const LIST_INDENT: f64 = 20.0;
    /// A list item's marker column.
    pub const LIST_MARKER_WIDTH: f64 = 22.0;
    /// One level of the Agents tree.
    pub const TREE_INDENT: f64 = 28.0;
    pub const COMPOSER_MAX_HEIGHT: f64 = 200.0;
    pub const COMPOSER_MIN_HEIGHT: f64 = 44.0;
    pub const SOURCE_MIN_HEIGHT: f64 = 240.0;
    pub const SOURCE_MAX_HEIGHT: f64 = 420.0;
    /// The hand-off dialog: ContentDialog's maximum.
    pub const HANDOFF_DIALOG_WIDTH: f64 = 548.0;
    /// The Goal dialog's card, and its goal field's height range: about
    /// three lines to start with, scrolling past about eight.
    pub const GOAL_DIALOG_WIDTH: f64 = 520.0;
    pub const GOAL_TEXT_MIN_HEIGHT: f64 = 76.0;
    pub const GOAL_TEXT_MAX_HEIGHT: f64 = 180.0;
    /// The hand-off dialog's scrolling body.
    pub const DIALOG_MAX_HEIGHT: f64 = 520.0;
    /// A dropdown inside the hand-off dialog.
    pub const DIALOG_PICKER_WIDTH: f64 = 136.0;
    /// A status dot in a row.
    pub const DOT: f64 = DOT_WAIT;
    /// The dot of a status pill.
    pub const BADGE_DOT: f64 = DOT_IDLE;
    /// Dividers and the app's own strokes (§4.4 `content.stroke`).
    pub const HAIRLINE: f64 = 1.0;
    /// The decision panel's outline.
    pub const DECISION_OUTLINE: f64 = 2.0;
    /// The decision panel's leading inset inside its outline.
    pub const DECISION_LEADING: f64 = 16.0;
    /// The attention strip's inset around its buttons.
    pub const STRIP_INSET: f64 = 6.0;
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
    /// The model picker above the composer. WinUI's default FlyoutPresenter
    /// is at most `FlyoutThemeMaxWidth` (456) wide and scrolls sideways past
    /// it; inside its `FlyoutContentPadding` (16 a side) and border (1, or
    /// 2 in high contrast) 420 is left, so the picker never scrolls sideways.
    pub const PICKER_WIDTH: f64 = 420.0;
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
    /// A quota ring (Models & Usage).
    pub const RING: f64 = 16.0;
    /// A quota ring's stroke.
    pub const USAGE_RING_STROKE: f64 = 2.0;
}

/// The pages in the main card: Project, Inbox, Agents, Automations and
/// Settings.
pub mod page {
    use super::text::TypeStyle;

    /// A list page's column at most, centred (§4.4 `page.max`).
    pub const WIDTH: f64 = 960.0;
    /// A Settings page's column at most, centred.
    pub const SETTINGS_WIDTH: f64 = 760.0;
    /// A dropdown at the end of a settings row.
    pub const SETTINGS_CONTROL: f64 = 220.0;
    /// The key column of a key-value line in a card.
    pub const KEY_COLUMN: f64 = 120.0;
    /// A page's search field (Agents).
    pub const SEARCH: f64 = 240.0;
    /// The rule editor's word column and its Repeat count.
    pub const SENTENCE_WORD: f64 = 72.0;
    pub const REPEAT_FIELD: f64 = 96.0;
    /// The Agents tree's star shares of a row's width: the task (its
    /// title is the text that matters) and each of Model and Activity.
    pub const FLEET_TASK_SHARE: f64 = 3.0;
    pub const FLEET_TEXT_SHARE: f64 = 2.0;
    /// Activity's share in a narrow main area (no Model column): half the
    /// task's, enough for a short status ("Working · 4 min").
    pub const FLEET_NARROW_ACTIVITY_SHARE: f64 = 1.5;
    /// The Agents tree's CPU and Memory columns (fixed content).
    pub const FLEET_NUMBER: f64 = 60.0;
    /// A branch's preparation state on the project page.
    pub const BRANCH_STATE: f64 = 96.0;

    /// Type roles of a page: its title, a section heading, a stat's
    /// number, an age.
    pub const TITLE: TypeStyle = super::text::PAGE;
    pub const HEADING: TypeStyle = super::text::SMALL_TITLE;
    pub const STAT: TypeStyle = super::text::HEADER;
    pub const AGE: TypeStyle = super::text::CAPTION;
}
