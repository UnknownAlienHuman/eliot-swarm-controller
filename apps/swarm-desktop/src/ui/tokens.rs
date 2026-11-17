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
    /// The only circle: the icon-only Send button (half its side).
    pub const CIRCLE: f64 = super::height::CONTROL / 2.0;
    /// Check boxes keep the M3 default corner.
    pub const CHECK_BOX: f64 = 2.0;
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
    /// The user's message bubble takes at most this share of the column.
    pub const BUBBLE_SHARE: f64 = 0.8;
    /// The narrowest content column.
    pub const MIN_COLUMN: f64 = 320.0;
    /// First window client size; the user resizes freely after.
    pub const WINDOW_WIDTH: f64 = 1440.0;
    pub const WINDOW_HEIGHT: f64 = 900.0;
    /// The model picker above the composer (OpenCode's dialog width).
    pub const PICKER_WIDTH: f64 = 520.0;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scale_has_three_radii_one_circle_and_three_heights() {
        let mut radii = [radius::CONTROL, radius::CONTAINER, radius::COMPOSER];
        radii.sort_by(f64::total_cmp);
        assert_eq!(radii, [8.0, 12.0, 16.0]);
        assert_eq!(radius::CIRCLE * 2.0, height::CONTROL);
        assert_eq!(
            [height::SUB_ROW, height::CONTROL, height::FORM_ROW],
            [28.0, 32.0, 36.0]
        );
        // A form row holds one control.
        const { assert!(height::FORM_ROW > height::CONTROL) };
    }

    #[test]
    fn title_body_and_label_have_distinct_sizes() {
        let styles = [
            text::PAGE_TITLE,
            text::SECTION_TITLE,
            text::BODY,
            text::SECONDARY,
        ];
        let sizes: Vec<f64> = styles.iter().map(|style| style.size).collect();
        assert_eq!(sizes, [22.0, 16.0, 14.0, 12.0]);
        const { assert!(text::SECTION_TITLE.size > text::BODY.size) };
        const { assert!(text::SECONDARY.size < text::BODY.size) };
    }

    #[test]
    fn spacing_sits_on_the_four_point_grid() {
        for value in [
            spacing::XS,
            spacing::SM,
            spacing::INSET,
            spacing::MD,
            spacing::LG,
            layout::TREE_INDENT,
            layout::LABEL_WIDTH,
        ] {
            assert_eq!(value % 4.0, 0.0, "{value}");
        }
    }
}
