//! Material 3 at the application root: WinUI lightweight styling from the
//! M3 roles, shape scale and state layers.
//!
//! Two parts, both parsed by `XamlReader.Load` (the runtime equivalent of an
//! `App.xaml`, the mechanism Reactor itself uses to compile styles in
//! `native/winui/mod.rs`) and moved into the application's own resource
//! entries, under the keys WinUI generated for them (no type key is built
//! by hand):
//!
//! - **Roles.** WinUI's own keys (`microsoft.windowsappsdk.winui` 2.3.0
//!   `Themes/generic.xaml`) from the window's scheme: the accent ramp, list
//!   selection, the text box as an M3 outlined text field, the combo box as
//!   an M3 exposed dropdown menu, radio buttons and check boxes in M3
//!   selection-control roles, menus and combo-box popups on
//!   surface-container, the dialog on
//!   surface-container-high, the tooltip on inverse-surface and the focus
//!   ring in secondary. WinUI resolves a ThemeResource through the
//!   application's own entries before its merged `XamlControlsResources`,
//!   so these win, also in the templates' visual-state storyboards (the
//!   focused text box takes the M3 outline, observed live). Theme
//!   dictionaries inserted into `Application.Resources.ThemeDictionaries`
//!   at run time were not consulted for these keys (observed live: own
//!   entries applied, theme-dictionary entries did not), so the roles of
//!   the shown scheme are written as own entries and rewritten when the
//!   scheme changes ([`apply_scheme`]); under High Contrast they are
//!   removed and the system colors apply.
//! - **Styles.** The shapes, heights and paddings of `ui::tokens` that
//!   WinUI's own styles fix (`Padding="{StaticResource ButtonPadding}"`,
//!   one `ControlCornerRadius` for every control): one button style per
//!   shape under the keys Reactor's `ButtonStyle` looks up (see
//!   [`styles_xaml`]), text fields, combo boxes and check boxes, menu items
//!   as control-height rows, dialogs and menus with the container corner,
//!   and menus without the acrylic backdrop, on the solid M3 container. A
//!   Reactor `Thickness` or `CornerRadius` resource override reaches WinUI as
//!   zero (observed live), so shapes cannot be per button.
//!
//! Reactor 0.100.0 applies [`windows_reactor::ResourceOverrides`] only to a
//! `Button` (`native/winui/mod.rs` `set_resource_overrides` writes into that
//! element's own `Resources`); everything else here has no Reactor property.
//!
//! The window's title bar takes the same surface role ([`apply_title_bar`]).
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.Xaml.winmd`
//! (WinUI 2.3.0 metadata): `IApplicationStatics` slot 6 `get_Current`,
//! `IApplication` slot 6 `get_Resources`, `IXamlReaderStatics` slot 6
//! `Load`; and of `Microsoft.UI.winmd` (Windows App SDK 2.1.3, read with
//! System.Reflection.Metadata; the `IAppWindow` and `IAppWindowTitleBar` ids
//! match Reactor's own bindings): `IAppWindowStatics` slot 9
//! `GetFromWindowId`, `IAppWindow` slot 16 `get_TitleBar` and the
//! `IAppWindowTitleBar` color setters listed in [`title_bar_colors`]. Only
//! these calls are declared; Reactor's own bindings are crate-private.
#![allow(
    unsafe_code,
    reason = "Window resources require direct Win32 handles and resource APIs."
)]

mod templates;

use std::ffi::c_void;
use std::fmt::Write as _;

use windows_collections::IMap;
use windows_core::{GUID, HRESULT, HSTRING, IInspectable, Interface, RuntimeName};
use windows_reference::IReference;
use windows_sys::Win32::Foundation::{HWND, LPARAM};
use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress, LoadLibraryW};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetClassNameW, GetWindowThreadProcessId, IsWindowVisible,
};
use windows_sys::core::BOOL;

use crate::theme::{Rgb8, Roles, SHADOW_COLOR, Theme, state};
use crate::ui::tokens::{height, layout, motion, radius, spacing, text};
use crate::winrt::{Getter, InspectableSlots, get, winrt_interface};

/// A theme resource value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    Color(Rgb8),
    /// A brush of `color` at `alpha` (0–255).
    Brush(Rgb8, u8),
    /// A uniform corner radius in DIPs.
    Radius(f64),
    /// A thickness (left, top, right, bottom) in DIPs.
    Thickness([f64; 4]),
}

const fn opaque(color: Rgb8) -> Value {
    Value::Brush(color, 255)
}

/// `color` at `opacity` (0–1): an M3 state layer or disabled treatment.
fn layer(color: Rgb8, opacity: f64) -> Value {
    Value::Brush(color, alpha(opacity))
}

/// A brush WinUI draws as nothing.
fn clear(s: &Roles) -> Value {
    Value::Brush(s.surface, 0)
}

const fn uniform(value: f64) -> Value {
    Value::Thickness([value; 4])
}

/// `alpha` as a byte (an M3 state layer or the WinUI opacity steps).
fn alpha(value: f64) -> u8 {
    (value * 255.0).round() as u8
}

/// Keys that take one value in every state of a control.
fn every(keys: &[&'static str], value: Value) -> Vec<(&'static str, Value)> {
    keys.iter().map(|key| (*key, value)).collect()
}

/// The WinUI keys this window sets for one color scheme, with their values.
pub fn entries(s: &Roles) -> Vec<(&'static str, Value)> {
    let mut all = accent(s);
    all.extend(lift(s));
    all.extend(buttons(s));
    all.extend(lists(s));
    all.extend(text_field(s));
    all.extend(dropdown(s));
    all.extend(selection_controls(s));
    all.extend(overlays(s));
    all
}

/// The brushes of the templates' drawn lift (`templates::LIFT_KEY` and
/// friends): the theme's key and ambient shadow opacity (each band of
/// `theme::elevation` takes its share), the ring of the true-black themes,
/// the night themes' glow.
fn lift(s: &Roles) -> Vec<(&'static str, Value)> {
    use crate::theme::Lift;
    let (key, ambient, ring) = match s.lift {
        Lift::Shadow { key, ambient } => (
            layer(SHADOW_COLOR, key),
            layer(SHADOW_COLOR, ambient),
            clear(s),
        ),
        Lift::Ring(color) => (clear(s), clear(s), opaque(color)),
    };
    let glow = s
        .glow
        .map_or(clear(s), |glow| layer(glow.color, glow.alpha));
    vec![
        (templates::LIFT_KEY, key),
        (templates::LIFT_AMBIENT, ambient),
        (templates::LIFT_RING, ring),
        (templates::GLOW, glow),
    ]
}

/// The accent ramp and accent fills (checked boxes, progress, selection).
fn accent(s: &Roles) -> Vec<(&'static str, Value)> {
    let [dark3, dark2, dark1, light1, light2, light3] = s.accent_ramp;
    vec![
        // Older brushes and templates read these colors.
        ("SystemAccentColor", Value::Color(s.primary)),
        ("SystemAccentColorDark3", Value::Color(dark3)),
        ("SystemAccentColorDark2", Value::Color(dark2)),
        ("SystemAccentColorDark1", Value::Color(dark1)),
        ("SystemAccentColorLight1", Value::Color(light1)),
        ("SystemAccentColorLight2", Value::Color(light2)),
        ("SystemAccentColorLight3", Value::Color(light3)),
        ("AccentFillColorDefaultBrush", opaque(s.primary)),
        (
            "AccentFillColorSecondaryBrush",
            Value::Brush(s.primary, alpha(0.9)),
        ),
        (
            "AccentFillColorTertiaryBrush",
            Value::Brush(s.primary, alpha(0.8)),
        ),
        (
            "AccentFillColorSelectedTextBackgroundBrush",
            opaque(s.primary),
        ),
        // Text without its own brush takes the theme's text roles.
        ("TextFillColorPrimaryBrush", opaque(s.text)),
        ("TextFillColorSecondaryBrush", opaque(s.variant)),
        ("TextFillColorTertiaryBrush", opaque(s.secondary_text)),
        ("TextFillColorDisabledBrush", opaque(s.secondary_text)),
        ("AccentTextFillColorPrimaryBrush", opaque(s.primary_text)),
        ("AccentTextFillColorSecondaryBrush", opaque(s.primary_text)),
        ("AccentTextFillColorTertiaryBrush", opaque(s.primary_text)),
        ("TextOnAccentFillColorPrimaryBrush", opaque(s.on_primary)),
        ("TextOnAccentFillColorSecondaryBrush", opaque(s.on_primary)),
        (
            "TextOnAccentFillColorSelectedTextBrush",
            opaque(s.on_primary),
        ),
        // The Usage page's quota rings: WinUI's ProgressRing reads its own
        // key (the Windows accent otherwise), and its track.
        ("ProgressRingForegroundThemeBrush", opaque(s.primary)),
        ("ProgressRingBackgroundThemeBrush", opaque(s.container)),
        // The M3 focus indicator: secondary, on the surface.
        ("FocusStrokeColorOuter", Value::Color(s.primary)),
        ("FocusStrokeColorInner", Value::Color(s.surface)),
        ("FocusStrokeColorOuterBrush", opaque(s.primary)),
        ("FocusStrokeColorInnerBrush", opaque(s.surface)),
        ("SystemControlFocusVisualPrimaryBrush", opaque(s.primary)),
        ("SystemControlFocusVisualSecondaryBrush", opaque(s.surface)),
    ]
}

/// Buttons without a family (inside dialogs): the M3 text button, and
/// WinUI's AccentButtonStyle as the M3 filled button. The shape is the
/// button's style.
fn buttons(s: &Roles) -> Vec<(&'static str, Value)> {
    let mut all = vec![
        ("ButtonBackground", clear(s)),
        (
            "ButtonBackgroundPointerOver",
            layer(s.primary_text, state::HOVER),
        ),
        (
            "ButtonBackgroundPressed",
            layer(s.primary_text, state::PRESSED),
        ),
        ("ButtonBackgroundDisabled", clear(s)),
        ("ButtonForegroundDisabled", opaque(s.secondary_text)),
        ("AccentButtonBackground", opaque(s.primary)),
        (
            "AccentButtonBackgroundPointerOver",
            opaque(s.primary.mix(s.on_primary, state::HOVER)),
        ),
        (
            "AccentButtonBackgroundPressed",
            opaque(s.primary.mix(s.on_primary, state::PRESSED)),
        ),
        (
            "AccentButtonBackgroundDisabled",
            layer(s.text, state::DISABLED_CONTAINER),
        ),
        ("AccentButtonForegroundDisabled", opaque(s.secondary_text)),
    ];
    all.extend(every(
        &[
            "ButtonForeground",
            "ButtonForegroundPointerOver",
            "ButtonForegroundPressed",
        ],
        opaque(s.primary_text),
    ));
    all.extend(every(
        &[
            "ButtonBorderBrush",
            "ButtonBorderBrushPointerOver",
            "ButtonBorderBrushPressed",
            "ButtonBorderBrushDisabled",
        ],
        clear(s),
    ));
    all.extend(every(
        &[
            "AccentButtonForeground",
            "AccentButtonForegroundPointerOver",
            "AccentButtonForegroundPressed",
        ],
        opaque(s.on_primary),
    ));
    all
}

/// List selection: the secondary-container row with a primary indicator,
/// and on-surface state layers on hover and press.
fn lists(s: &Roles) -> Vec<(&'static str, Value)> {
    let selected_hover = s.selected.mix(s.text, state::HOVER);
    let selected_pressed = s.selected.mix(s.text, state::PRESSED);
    let mut all = vec![
        ("ListViewItemBackgroundSelected", opaque(s.selected)),
        (
            "ListViewItemBackgroundSelectedPointerOver",
            opaque(selected_hover),
        ),
        (
            "ListViewItemBackgroundSelectedPressed",
            opaque(selected_pressed),
        ),
        (
            "ListViewItemBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "ListViewItemBackgroundPressed",
            layer(s.text, state::PRESSED),
        ),
    ];
    all.extend(every(
        &[
            "ListViewItemForegroundSelected",
            "ListViewItemForegroundSelectedPointerOver",
            "ListViewItemForegroundSelectedPressed",
        ],
        opaque(s.text),
    ));
    all.extend(every(
        &[
            "ListViewItemSelectionIndicatorBrush",
            "ListViewItemSelectionIndicatorPointerOverBrush",
            "ListViewItemSelectionIndicatorPressedBrush",
        ],
        opaque(s.primary),
    ));
    all
}

/// The text box as an M3 outlined text field: no fill, an outline that
/// darkens on hover and becomes a 2-DIP primary line when focused.
fn text_field(s: &Roles) -> Vec<(&'static str, Value)> {
    let mut all = vec![
        ("TextControlBorderBrush", opaque(s.border)),
        ("TextControlBorderBrushPointerOver", opaque(s.text)),
        ("TextControlBorderBrushFocused", opaque(s.primary)),
        (
            "TextControlBorderBrushDisabled",
            layer(s.text, state::DISABLED_CONTAINER),
        ),
        ("TextControlElevationBorderBrush", opaque(s.border)),
        ("TextControlElevationBorderFocusedBrush", opaque(s.primary)),
        (
            "TextControlBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        ("TextControlForegroundDisabled", opaque(s.secondary_text)),
        (
            "TextControlPlaceholderForegroundDisabled",
            opaque(s.secondary_text),
        ),
        ("TextControlHeaderForeground", opaque(s.variant)),
        (
            "TextControlHeaderForegroundDisabled",
            opaque(s.secondary_text),
        ),
        ("TextControlSelectionHighlightColor", opaque(s.primary)),
        (
            "TextControlButtonBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "TextControlButtonBackgroundPressed",
            layer(s.text, state::PRESSED),
        ),
        ("TextControlBorderThemeThickness", uniform(layout::HAIRLINE)),
        (
            "TextControlBorderThemeThicknessFocused",
            uniform(layout::FOCUS_RING),
        ),
    ];
    all.extend(every(
        &[
            "TextControlBackground",
            "TextControlBackgroundFocused",
            "TextControlBackgroundDisabled",
            "TextControlButtonBackground",
            "TextControlButtonBorderBrush",
            "TextControlButtonBorderBrushPointerOver",
            "TextControlButtonBorderBrushPressed",
        ],
        clear(s),
    ));
    all.extend(every(
        &[
            "TextControlForeground",
            "TextControlForegroundPointerOver",
            "TextControlForegroundFocused",
        ],
        opaque(s.text),
    ));
    all.extend(every(
        &[
            "TextControlPlaceholderForeground",
            "TextControlPlaceholderForegroundPointerOver",
            "TextControlPlaceholderForegroundFocused",
            "TextControlButtonForeground",
            "TextControlButtonForegroundPointerOver",
            "TextControlButtonForegroundPressed",
        ],
        opaque(s.secondary_text),
    ));
    all
}

/// The combo box as an M3 exposed dropdown menu: the outlined field, and a
/// menu on surface-container whose selected item is secondary-container.
fn dropdown(s: &Roles) -> Vec<(&'static str, Value)> {
    let selected_hover = s.selected.mix(s.text, state::HOVER);
    let selected_pressed = s.selected.mix(s.text, state::PRESSED);
    let mut all = vec![
        ("ComboBoxBorderBrush", opaque(s.border)),
        ("ComboBoxBorderBrushPointerOver", opaque(s.text)),
        ("ComboBoxBorderBrushPressed", opaque(s.primary)),
        (
            "ComboBoxBorderBrushDisabled",
            layer(s.text, state::DISABLED_CONTAINER),
        ),
        ("ComboBoxBackgroundBorderBrushFocused", opaque(s.primary)),
        ("ComboBoxBackgroundBorderBrushUnfocused", opaque(s.border)),
        ("ComboBoxBorderThemeThickness", uniform(layout::HAIRLINE)),
        ("ComboBoxForegroundDisabled", opaque(s.secondary_text)),
        (
            "ComboBoxPlaceHolderForegroundDisabled",
            opaque(s.secondary_text),
        ),
        ("ComboBoxHeaderForeground", opaque(s.variant)),
        ("ComboBoxHeaderForegroundDisabled", opaque(s.secondary_text)),
        (
            "ComboBoxDropDownBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "ComboBoxDropDownBackgroundPointerPressed",
            layer(s.text, state::PRESSED),
        ),
        (
            "ComboBoxFocusedDropDownBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "ComboBoxFocusedDropDownBackgroundPointerPressed",
            layer(s.text, state::PRESSED),
        ),
        ("ComboBoxDropDownBackground", opaque(s.tonal)),
        ("ComboBoxDropDownForeground", opaque(s.text)),
        (
            "ComboBoxItemBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "ComboBoxItemBackgroundPressed",
            layer(s.text, state::PRESSED),
        ),
        (
            "ComboBoxItemBackgroundSelectedPointerOver",
            opaque(selected_hover),
        ),
        (
            "ComboBoxItemBackgroundSelectedPressed",
            opaque(selected_pressed),
        ),
        ("ComboBoxItemForegroundDisabled", opaque(s.secondary_text)),
        (
            "ComboBoxItemForegroundSelectedDisabled",
            opaque(s.secondary_text),
        ),
    ];
    all.extend(every(
        &[
            "ComboBoxBackground",
            "ComboBoxBackgroundFocused",
            "ComboBoxBackgroundUnfocused",
            "ComboBoxBackgroundDisabled",
            "ComboBoxDropDownBorderBrush",
            "ComboBoxLightDismissOverlayBackground",
            "ComboBoxItemBackground",
            "ComboBoxItemBackgroundDisabled",
            "ComboBoxItemBackgroundSelectedDisabled",
            "ComboBoxItemPillFillBrush",
            "ComboBoxItemBorderBrush",
            "ComboBoxItemBorderBrushPressed",
            "ComboBoxItemBorderBrushPointerOver",
            "ComboBoxItemBorderBrushDisabled",
            "ComboBoxItemBorderBrushSelected",
            "ComboBoxItemBorderBrushSelectedUnfocused",
            "ComboBoxItemBorderBrushSelectedPressed",
            "ComboBoxItemBorderBrushSelectedPointerOver",
            "ComboBoxItemBorderBrushSelectedDisabled",
        ],
        clear(s),
    ));
    all.extend([
        ("ComboBoxBackgroundPointerOver", layer(s.text, state::HOVER)),
        ("ComboBoxBackgroundPressed", layer(s.text, state::PRESSED)),
    ]);
    all.extend(every(
        &[
            "ComboBoxForeground",
            "ComboBoxForegroundPointerOver",
            "ComboBoxForegroundPressed",
            "ComboBoxForegroundFocused",
            "ComboBoxForegroundFocusedPressed",
            "ComboBoxItemForeground",
            "ComboBoxItemForegroundPressed",
            "ComboBoxItemForegroundPointerOver",
        ],
        opaque(s.text),
    ));
    all.extend(every(
        &[
            "ComboBoxPlaceHolderForeground",
            "ComboBoxPlaceHolderForegroundPointerOver",
            "ComboBoxPlaceHolderForegroundPressed",
            "ComboBoxPlaceHolderForegroundFocused",
            "ComboBoxPlaceHolderForegroundFocusedPressed",
        ],
        opaque(s.variant),
    ));
    all.extend(every(
        &[
            "ComboBoxItemBackgroundSelected",
            "ComboBoxItemBackgroundSelectedUnfocused",
        ],
        opaque(s.selected),
    ));
    all.extend(every(
        &[
            "ComboBoxItemForegroundSelected",
            "ComboBoxItemForegroundSelectedUnfocused",
            "ComboBoxItemForegroundSelectedPressed",
            "ComboBoxItemForegroundSelectedPointerOver",
        ],
        opaque(s.text),
    ));
    all
}

/// Radio buttons and check boxes in M3 selection-control roles: an
/// on-surface-variant ring or box at rest, primary when selected (a primary
/// ring with a primary dot; a primary box with an on-primary check), the
/// label in on-surface, with a readable disabled text role.
fn selection_controls(s: &Roles) -> Vec<(&'static str, Value)> {
    let disabled = layer(s.text, state::DISABLED_CONTENT);
    let disabled_text = opaque(s.secondary_text);
    let mut all = vec![
        ("RadioButtonForegroundDisabled", disabled_text),
        ("RadioButtonOuterEllipseStrokeDisabled", disabled),
        ("RadioButtonOuterEllipseCheckedStrokeDisabled", disabled),
        ("RadioButtonCheckGlyphFillDisabled", disabled),
        ("RadioButtonsHeaderForeground", opaque(s.variant)),
        ("CheckBoxForegroundUncheckedDisabled", disabled_text),
        ("CheckBoxForegroundCheckedDisabled", disabled_text),
        ("CheckBoxForegroundIndeterminateDisabled", disabled_text),
        ("CheckBoxCheckBackgroundStrokeUncheckedDisabled", disabled),
        ("CheckBoxCheckBackgroundFillCheckedDisabled", disabled),
        ("CheckBoxCheckBackgroundFillIndeterminateDisabled", disabled),
        (
            "CheckBoxCheckGlyphForegroundCheckedDisabled",
            opaque(s.surface),
        ),
        (
            "CheckBoxCheckGlyphForegroundIndeterminateDisabled",
            opaque(s.surface),
        ),
    ];
    all.extend(every(
        &[
            "RadioButtonForeground",
            "RadioButtonForegroundPointerOver",
            "RadioButtonForegroundPressed",
            "CheckBoxForegroundUnchecked",
            "CheckBoxForegroundUncheckedPointerOver",
            "CheckBoxForegroundUncheckedPressed",
            "CheckBoxForegroundChecked",
            "CheckBoxForegroundCheckedPointerOver",
            "CheckBoxForegroundCheckedPressed",
            "CheckBoxForegroundIndeterminate",
            "CheckBoxForegroundIndeterminatePointerOver",
            "CheckBoxForegroundIndeterminatePressed",
        ],
        opaque(s.text),
    ));
    all.extend(every(
        &[
            "RadioButtonOuterEllipseStroke",
            "RadioButtonOuterEllipseStrokePointerOver",
            "RadioButtonOuterEllipseStrokePressed",
            "CheckBoxCheckBackgroundStrokeUnchecked",
            "CheckBoxCheckBackgroundStrokeUncheckedPointerOver",
            "CheckBoxCheckBackgroundStrokeUncheckedPressed",
        ],
        opaque(s.variant),
    ));
    all.extend(every(
        &[
            "RadioButtonOuterEllipseCheckedStroke",
            "RadioButtonOuterEllipseCheckedStrokePointerOver",
            "RadioButtonOuterEllipseCheckedStrokePressed",
            "RadioButtonCheckGlyphFill",
            "RadioButtonCheckGlyphFillPointerOver",
            "RadioButtonCheckGlyphFillPressed",
            "CheckBoxCheckBackgroundStrokeChecked",
            "CheckBoxCheckBackgroundStrokeCheckedPointerOver",
            "CheckBoxCheckBackgroundStrokeCheckedPressed",
            "CheckBoxCheckBackgroundStrokeIndeterminate",
            "CheckBoxCheckBackgroundStrokeIndeterminatePointerOver",
            "CheckBoxCheckBackgroundStrokeIndeterminatePressed",
            "CheckBoxCheckBackgroundFillChecked",
            "CheckBoxCheckBackgroundFillCheckedPointerOver",
            "CheckBoxCheckBackgroundFillCheckedPressed",
            "CheckBoxCheckBackgroundFillIndeterminate",
            "CheckBoxCheckBackgroundFillIndeterminatePointerOver",
            "CheckBoxCheckBackgroundFillIndeterminatePressed",
        ],
        opaque(s.primary),
    ));
    all.extend(every(
        &[
            "CheckBoxCheckGlyphForegroundChecked",
            "CheckBoxCheckGlyphForegroundCheckedPointerOver",
            "CheckBoxCheckGlyphForegroundCheckedPressed",
            "CheckBoxCheckGlyphForegroundIndeterminate",
            "CheckBoxCheckGlyphForegroundIndeterminatePointerOver",
            "CheckBoxCheckGlyphForegroundIndeterminatePressed",
        ],
        opaque(s.on_primary),
    ));
    all.extend(every(
        &[
            "RadioButtonBackground",
            "RadioButtonBackgroundDisabled",
            "RadioButtonBorderBrush",
            "RadioButtonBorderBrushPointerOver",
            "RadioButtonBorderBrushPressed",
            "RadioButtonBorderBrushDisabled",
            "RadioButtonOuterEllipseFill",
            "RadioButtonOuterEllipseFillPointerOver",
            "RadioButtonOuterEllipseFillPressed",
            "RadioButtonOuterEllipseFillDisabled",
            "RadioButtonOuterEllipseCheckedFill",
            "RadioButtonOuterEllipseCheckedFillPointerOver",
            "RadioButtonOuterEllipseCheckedFillPressed",
            "RadioButtonOuterEllipseCheckedFillDisabled",
            "RadioButtonCheckGlyphStroke",
            "RadioButtonCheckGlyphStrokePointerOver",
            "RadioButtonCheckGlyphStrokePressed",
            "RadioButtonCheckGlyphStrokeDisabled",
            "RadioButtonCheckGlyphStrokeChecked",
            "RadioButtonCheckGlyphStrokeCheckedPointerOver",
            "RadioButtonCheckGlyphStrokeCheckedPressed",
            "RadioButtonCheckGlyphStrokeCheckedDisabled",
            "CheckBoxBackgroundUnchecked",
            "CheckBoxBackgroundUncheckedDisabled",
            "CheckBoxBackgroundChecked",
            "CheckBoxBackgroundCheckedDisabled",
            "CheckBoxBackgroundIndeterminate",
            "CheckBoxBackgroundIndeterminateDisabled",
            "CheckBoxBorderBrushUnchecked",
            "CheckBoxBorderBrushUncheckedPointerOver",
            "CheckBoxBorderBrushUncheckedPressed",
            "CheckBoxBorderBrushUncheckedDisabled",
            "CheckBoxBorderBrushChecked",
            "CheckBoxBorderBrushCheckedPointerOver",
            "CheckBoxBorderBrushCheckedPressed",
            "CheckBoxBorderBrushCheckedDisabled",
            "CheckBoxBorderBrushIndeterminate",
            "CheckBoxBorderBrushIndeterminatePointerOver",
            "CheckBoxBorderBrushIndeterminatePressed",
            "CheckBoxBorderBrushIndeterminateDisabled",
            "CheckBoxCheckBackgroundStrokeCheckedDisabled",
            "CheckBoxCheckBackgroundStrokeIndeterminateDisabled",
            "CheckBoxCheckBackgroundFillUnchecked",
            "CheckBoxCheckBackgroundFillUncheckedPointerOver",
            "CheckBoxCheckBackgroundFillUncheckedPressed",
            "CheckBoxCheckBackgroundFillUncheckedDisabled",
            "CheckBoxCheckGlyphForegroundUnchecked",
            "CheckBoxCheckGlyphForegroundUncheckedPointerOver",
            "CheckBoxCheckGlyphForegroundUncheckedPressed",
            "CheckBoxCheckGlyphForegroundUncheckedDisabled",
        ],
        clear(s),
    ));
    all.extend(every(
        &[
            "RadioButtonBackgroundPointerOver",
            "CheckBoxBackgroundUncheckedPointerOver",
            "CheckBoxBackgroundCheckedPointerOver",
            "CheckBoxBackgroundIndeterminatePointerOver",
        ],
        layer(s.text, state::HOVER),
    ));
    all.extend(every(
        &[
            "RadioButtonBackgroundPressed",
            "CheckBoxBackgroundUncheckedPressed",
            "CheckBoxBackgroundCheckedPressed",
            "CheckBoxBackgroundIndeterminatePressed",
        ],
        layer(s.text, state::PRESSED),
    ));
    all.extend(switches(s));
    all
}

/// Switches in M3 switch roles: selected, a primary track with an
/// on-primary handle; unselected, a surface-container-highest track with an
/// outline border and an outline handle; disabled at 12% / 38% on-surface.
fn switches(s: &Roles) -> Vec<(&'static str, Value)> {
    let mut all = every(
        &[
            "ToggleSwitchFillOn",
            "ToggleSwitchStrokeOn",
            "ToggleSwitchStrokeOnPointerOver",
            "ToggleSwitchStrokeOnPressed",
        ],
        opaque(s.primary),
    );
    all.extend(every(
        &[
            "ToggleSwitchKnobFillOn",
            "ToggleSwitchKnobFillOnPointerOver",
            "ToggleSwitchKnobFillOnPressed",
        ],
        opaque(s.on_primary),
    ));
    all.extend([
        ("ToggleSwitchFillOff", opaque(s.container)),
        (
            "ToggleSwitchFillOnPointerOver",
            opaque(s.primary.mix(s.on_primary, state::HOVER)),
        ),
        (
            "ToggleSwitchFillOnPressed",
            opaque(s.primary.mix(s.on_primary, state::PRESSED)),
        ),
        (
            "ToggleSwitchFillOffPointerOver",
            opaque(s.container.mix(s.text, state::HOVER)),
        ),
        (
            "ToggleSwitchFillOffPressed",
            opaque(s.container.mix(s.text, state::PRESSED)),
        ),
        (
            "ToggleSwitchContentForegroundDisabled",
            opaque(s.secondary_text),
        ),
        (
            "ToggleSwitchHeaderForegroundDisabled",
            opaque(s.secondary_text),
        ),
    ]);
    all.extend(every(
        &[
            "ToggleSwitchStrokeOff",
            "ToggleSwitchStrokeOffPointerOver",
            "ToggleSwitchStrokeOffPressed",
            "ToggleSwitchKnobFillOff",
            "ToggleSwitchKnobFillOffPointerOver",
            "ToggleSwitchKnobFillOffPressed",
        ],
        opaque(s.border),
    ));
    all.extend(every(
        &[
            "ToggleSwitchFillOnDisabled",
            "ToggleSwitchStrokeOffDisabled",
            "ToggleSwitchStrokeOnDisabled",
        ],
        layer(s.text, state::DISABLED_CONTAINER),
    ));
    all.extend(every(
        &[
            "ToggleSwitchKnobFillOffDisabled",
            "ToggleSwitchKnobFillOnDisabled",
        ],
        layer(s.text, state::DISABLED_CONTENT),
    ));
    all.push(("ToggleSwitchFillOffDisabled", clear(s)));
    all
}

/// Menus and popups: menus on surface-container (M3 menu), the dialog on
/// surface-container-high with the M3 scrim, the plain tooltip on
/// inverse-surface; the control corner for every control template that
/// reads `ControlCornerRadius` and the container corner for the popups
/// that read `OverlayCornerRadius` (flyouts, the combo-box list).
fn overlays(s: &Roles) -> Vec<(&'static str, Value)> {
    let mut all = vec![
        ("MenuFlyoutPresenterBackground", opaque(s.tonal)),
        ("NumberBoxPopupBackground", opaque(s.tonal)),
        ("NumberBoxPopupBorderBrush", opaque(s.outline)),
        ("NumberBoxPopupIndicatorForeground", opaque(s.variant)),
        ("AutoSuggestBoxSuggestionsListBackground", opaque(s.tonal)),
        (
            "AutoSuggestBoxSuggestionsListBorderBrush",
            opaque(s.outline),
        ),
        ("MenuFlyoutPresenterBorderBrush", opaque(s.outline)),
        ("MenuFlyoutSeparatorBackground", opaque(s.outline)),
        (
            "MenuFlyoutItemBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "MenuFlyoutItemBackgroundPressed",
            layer(s.text, state::PRESSED),
        ),
        (
            "MenuFlyoutSubItemBackgroundPointerOver",
            layer(s.text, state::HOVER),
        ),
        (
            "MenuFlyoutSubItemBackgroundPressed",
            layer(s.text, state::PRESSED),
        ),
        (
            "MenuFlyoutSubItemBackgroundSubMenuOpened",
            layer(s.text, state::HOVER),
        ),
        ("MenuFlyoutItemForegroundDisabled", opaque(s.secondary_text)),
        (
            "MenuFlyoutSubItemForegroundDisabled",
            opaque(s.secondary_text),
        ),
        (
            "MenuFlyoutSubItemChevronDisabled",
            layer(s.text, state::DISABLED_CONTENT),
        ),
        ("ContentDialogBackground", opaque(s.container)),
        ("ContentDialogTopOverlay", clear(s)),
        ("ContentDialogPadding", uniform(0.0)),
        ("ContentDialogForeground", opaque(s.text)),
        ("ContentDialogContentForegroundBrush", opaque(s.variant)),
        // The M3 scrim: black at 32%.
        ("ContentDialogSmokeFill", layer(SHADOW_COLOR, state::SCRIM)),
        ("ContentDialogBorderWidth", uniform(0.0)),
        ("ToolTipBackground", opaque(s.text)),
        ("ToolTipForeground", opaque(s.surface)),
        ("ControlCornerRadius", Value::Radius(radius::FIELD)),
        ("OverlayCornerRadius", Value::Radius(radius::CONTAINER)),
    ];
    all.extend(every(
        &[
            "MenuFlyoutItemBackground",
            "MenuFlyoutItemBackgroundDisabled",
            "MenuFlyoutSubItemBackground",
            "MenuFlyoutSubItemBackgroundDisabled",
            "ContentDialogBorderBrush",
            "ContentDialogSeparatorBorderBrush",
            "ToolTipBorderBrush",
        ],
        clear(s),
    ));
    all.extend(every(
        &[
            "MenuFlyoutItemForeground",
            "MenuFlyoutItemForegroundPointerOver",
            "MenuFlyoutItemForegroundPressed",
            "MenuFlyoutSubItemForeground",
            "MenuFlyoutSubItemForegroundPointerOver",
            "MenuFlyoutSubItemForegroundPressed",
            "MenuFlyoutSubItemForegroundSubMenuOpened",
        ],
        opaque(s.text),
    ));
    all.extend(every(
        &[
            "MenuFlyoutItemKeyboardAcceleratorTextForeground",
            "MenuFlyoutSubItemChevron",
            "MenuFlyoutSubItemChevronPointerOver",
            "MenuFlyoutSubItemChevronPressed",
            "MenuFlyoutSubItemChevronSubMenuOpened",
        ],
        opaque(s.variant),
    ));
    all
}

fn hex(color: Rgb8, alpha: u8) -> String {
    format!("#{alpha:02X}{:02X}{:02X}{:02X}", color.r, color.g, color.b)
}

const XAML_ROOT: &str = "<ResourceDictionary \
     xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\" \
     xmlns:x=\"http://schemas.microsoft.com/winfx/2006/xaml\">";

/// The roles of one scheme as one dictionary of XAML. Keys are fixed identifiers and values
/// numbers or hex colors, so nothing in it needs escaping.
pub fn dictionary_xaml(scheme: &Roles) -> String {
    let mut xaml = String::from(XAML_ROOT);
    for (key, value) in entries(scheme) {
        let _ = match value {
            Value::Color(color) => {
                write!(xaml, "<Color x:Key=\"{key}\">{}</Color>", hex(color, 255))
            }
            Value::Brush(color, alpha) => write!(
                xaml,
                "<SolidColorBrush x:Key=\"{key}\" Color=\"{}\"/>",
                hex(color, alpha)
            ),
            Value::Radius(radius) => {
                write!(
                    xaml,
                    "<CornerRadius x:Key=\"{key}\">{radius}</CornerRadius>"
                )
            }
            Value::Thickness([left, top, right, bottom]) => write!(
                xaml,
                "<Thickness x:Key=\"{key}\">{left},{top},{right},{bottom}</Thickness>"
            ),
        };
    }
    xaml.push_str("</ResourceDictionary>");
    xaml
}

/// The control styles from `ui::tokens`: the button styles under the keys
/// Reactor's `ButtonStyle` looks up (`native/winui/generated.rs`: Accent,
/// Subtle, TextLink), and the implicit styles of the other controls.
/// Button colors stay with the theme keys and the button families.
///
/// - implicit `Button`: every labelled button, row, chip and segment
///   (control corner, 12-DIP content inset, 32 DIPs, body weight);
/// - `SubtleButtonStyle`: icon buttons (control corner, no padding);
/// - `TextBlockButtonStyle`: the one circle, the icon-only Send button;
/// - `AccentButtonStyle`: suggestion cards and tool rows with hover lift.
///
/// Switches carry no On/Off words and no 154-DIP minimum: a Manage
/// models row ends in the bare switch (OpenCode's dialog).
///
/// Text fields never offer Windows text
/// prediction or spell check (`IsTextPredictionEnabled`,
/// `IsSpellCheckEnabled` false: owner review 2026-11-16, prediction junk in
/// a model id). Text fields, dropdowns and number boxes take the control
/// corner and height, check boxes the M3 default corner, menu items the control
/// height, menus the container corner and dialogs their larger corner. The
/// native templates retain each control's focus, input and selection behavior.
pub fn styles_xaml() -> String {
    use templates::{Lift, Look, button};
    let (pill, field, container, dialog, check, fab_radius) = (
        radius::PILL,
        radius::FIELD,
        radius::CONTAINER,
        radius::DIALOG,
        radius::CHECK_BOX,
        radius::FAB,
    );
    // Menu items are control-height rows: WinUI pads them from a
    // StaticResource (11,9,11,10), so only a style sets their height.
    let (min, inset, row) = (height::CONTROL, spacing::INSET, spacing::ROW);
    let font = format!(
        "<Setter Property=\"FontFamily\" Value=\"{}\"/>\
         <Setter Property=\"FontSize\" Value=\"{}\"/>",
        text::FAMILY,
        text::BODY.size,
    );
    let min_height = format!("<Setter Property=\"MinHeight\" Value=\"{min}\"/>");
    let control = format!("{font}{min_height}");
    let button_font = format!("{font}<Setter Property=\"FontWeight\" Value=\"Medium\"/>");
    let template = |look: Look| {
        format!(
            "<Setter Property=\"Template\"><Setter.Value>{}</Setter.Value></Setter>",
            button(look)
        )
    };
    // Rows, chips, segments and labelled buttons: flat pills. Icon buttons
    // use the same state animation on a circle.
    let flat = template(Look {
        lift: Lift::Flat,
        press: motion::PRESS_SCALE,
        radius: pill,
    });
    // The FAB: level 1 at rest, 2 under the pointer, a press to 0.94.
    let fab = template(Look {
        lift: Lift::Fab,
        press: motion::FAB_PRESS_SCALE,
        radius: fab_radius,
    });
    // Card pressables (suggestion cards, tool rows): the card corner,
    // lifting to level 1 under the pointer, a press to 0.97.
    let card = template(Look {
        lift: Lift::Hover,
        press: motion::PRESS_SCALE,
        radius: container,
    });
    // The transcript is the app's only ScrollView: anchored at its far
    // (bottom) edge, WinUI keeps it at the end while it is there and the
    // content grows, so a conversation opens at its newest turn.
    format!(
        "{XAML_ROOT}\
         <FontFamily x:Key=\"ContentControlThemeFontFamily\">{family}</FontFamily>\
         <x:Double x:Key=\"ControlContentThemeFontSize\">{font_size}</x:Double>\
         <Style TargetType=\"TextBlock\">{font}</Style>\
         <Style TargetType=\"RichTextBlock\">\
           {font}\
           <Setter Property=\"Foreground\" Value=\"{{ThemeResource TextFillColorPrimaryBrush}}\"/>\
         </Style>\
         <Style TargetType=\"Button\">\
           {button_font}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Padding\" Value=\"{row},0,{row},0\"/>\
           {min_height}\
           {flat}\
         </Style>\
         <Style x:Key=\"SubtleButtonStyle\" TargetType=\"Button\">\
           {button_font}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Padding\" Value=\"0\"/>\
           {flat}\
         </Style>\
         <Style x:Key=\"TextBlockButtonStyle\" TargetType=\"Button\">\
           {button_font}\
           <Setter Property=\"CornerRadius\" Value=\"{fab_radius}\"/>\
           <Setter Property=\"Padding\" Value=\"0\"/>\
           <Setter Property=\"BorderThickness\" Value=\"0\"/>\
           <Setter Property=\"HorizontalContentAlignment\" Value=\"Center\"/>\
           <Setter Property=\"VerticalContentAlignment\" Value=\"Center\"/>\
           {fab}\
         </Style>\
         <Style x:Key=\"AccentButtonStyle\" TargetType=\"Button\">\
           {button_font}\
           <Setter Property=\"CornerRadius\" Value=\"{container}\"/>\
           <Setter Property=\"Padding\" Value=\"{inset},0,{inset},0\"/>\
           {min_height}\
           <Setter Property=\"BorderThickness\" Value=\"0\"/>\
           {card}\
         </Style>\
         <Style TargetType=\"TextBox\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{field}\"/>\
           <Setter Property=\"IsTextPredictionEnabled\" Value=\"False\"/>\
           <Setter Property=\"IsSpellCheckEnabled\" Value=\"False\"/>\
         </Style>\
         <Style TargetType=\"ComboBox\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Template\"><Setter.Value>{combo_box}</Setter.Value></Setter>\
         </Style>\
         <Style TargetType=\"ComboBoxItem\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
         </Style>\
         <Style TargetType=\"AutoSuggestBox\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
         </Style>\
         <Style TargetType=\"NumberBox\">\
           {control}\
         </Style>\
         <Style TargetType=\"CheckBox\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{check}\"/>\
         </Style>\
         <Style TargetType=\"RadioButton\">\
           {control}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
         </Style>\
         <Style TargetType=\"ToggleSwitch\">\
           {control}\
           <Setter Property=\"Width\" Value=\"{switch_width}\"/>\
           <Setter Property=\"OnContent\" Value=\"\"/>\
           <Setter Property=\"OffContent\" Value=\"\"/>\
           <Setter Property=\"MinWidth\" Value=\"0\"/>\
         </Style>\
         <Style TargetType=\"ContentDialog\">\
           {font}\
           <Setter Property=\"CornerRadius\" Value=\"{dialog}\"/>\
           <Setter Property=\"Background\" Value=\"Transparent\"/>\
         </Style>\
         <Style TargetType=\"ScrollView\">\
           <Setter Property=\"VerticalAnchorRatio\" Value=\"1\"/>\
         </Style>\
         <Style TargetType=\"MenuFlyoutItem\">\
           {font}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Padding\" Value=\"{inset},0,{inset},0\"/>\
           <Setter Property=\"Height\" Value=\"{min}\"/>\
         </Style>\
         <Style TargetType=\"MenuFlyoutSubItem\">\
           {font}\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Padding\" Value=\"{inset},0,{inset},0\"/>\
           <Setter Property=\"Height\" Value=\"{min}\"/>\
         </Style>\
         <Style TargetType=\"MenuFlyoutPresenter\">\
           <Setter Property=\"SystemBackdrop\" Value=\"{{x:Null}}\"/>\
           <Setter Property=\"CornerRadius\" Value=\"{container}\"/>\
         </Style>\
         <Style TargetType=\"HyperlinkButton\">\
           <Setter Property=\"Width\" Value=\"{min}\"/>\
           <Setter Property=\"Height\" Value=\"{min}\"/>\
           <Setter Property=\"Padding\" Value=\"0\"/>\
           <Setter Property=\"CornerRadius\" Value=\"{pill}\"/>\
           <Setter Property=\"Background\" Value=\"Transparent\"/>\
           <Setter Property=\"Foreground\" Value=\"{{ThemeResource TextFillColorSecondaryBrush}}\"/>\
           <Setter Property=\"UseSystemFocusVisuals\" Value=\"True\"/>\
           <Setter Property=\"Template\"><Setter.Value>\
             <ControlTemplate TargetType=\"HyperlinkButton\">\
               <Grid Background=\"{{TemplateBinding Background}}\" CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
                 <VisualStateManager.VisualStateGroups>\
                   <VisualStateGroup x:Name=\"FocusStates\">\
                     <VisualState x:Name=\"Focused\">\
                       <VisualState.Setters>\
                         <Setter Target=\"ContentPresenter.Opacity\" Value=\"1\"/>\
                       </VisualState.Setters>\
                     </VisualState>\
                     <VisualState x:Name=\"Unfocused\"/>\
                     <VisualState x:Name=\"PointerFocused\"/>\
                   </VisualStateGroup>\
                 </VisualStateManager.VisualStateGroups>\
                 <ContentPresenter x:Name=\"ContentPresenter\" Opacity=\"0\" Content=\"{{TemplateBinding Content}}\" Foreground=\"{{TemplateBinding Foreground}}\" HorizontalAlignment=\"Center\" VerticalAlignment=\"Center\"/>\
               </Grid>\
             </ControlTemplate>\
           </Setter.Value></Setter>\
         </Style>\
         </ResourceDictionary>",
        family = text::FAMILY,
        font_size = text::BODY.size,
        switch_width = layout::SWITCH_WIDTH,
        combo_box = templates::combo_box(),
    )
}

/// Install the styles and the roles of the window's scheme (`None` under
/// High Contrast). Called once, on the UI thread, after Reactor merged
/// `XamlControlsResources` and before the first controls are created
/// (Reactor's `on_launched` installs those resources, then mounts the
/// component), so every control resolves these from its first template.
pub fn install(scheme: Option<&Roles>) -> Result<(), String> {
    native(|own| {
        insert_parsed(own, &styles_xaml())?;
        write_scheme(own, scheme)
    })
}

/// Rewrite the roles for a new scheme, or remove them under High Contrast
/// (`None`) so WinUI's system colors apply. The window rebuilds its
/// controls on a scheme change (the palette-keyed tree), and the new
/// controls resolve the new values.
pub fn apply_scheme(scheme: Option<&Roles>) -> Result<(), String> {
    native(|own| write_scheme(own, scheme))
}

type OwnEntries = IMap<IInspectable, IInspectable>;

fn native(work: impl FnOnce(&OwnEntries) -> windows_core::Result<()>) -> Result<(), String> {
    let run = || -> windows_core::Result<()> {
        let application = current_application()?;
        let own: OwnEntries = application_resources(&application)?.cast()?;
        work(&own)
    };
    run().map_err(|error| format!("{error} ({:#010X})", error.code().0))
}

fn write_scheme(own: &OwnEntries, scheme: Option<&Roles>) -> windows_core::Result<()> {
    match scheme {
        Some(scheme) => insert_parsed(own, &dictionary_xaml(scheme)),
        None => {
            // The keys are the same for both schemes.
            for (key, _) in entries(Theme::Light.roles()) {
                let key: IInspectable = IReference::<HSTRING>::from(key).into();
                if own.HasKey(&key)? {
                    own.Remove(&key)?;
                }
            }
            Ok(())
        }
    }
}

/// Parse one dictionary and move its entries, under the keys WinUI
/// generated for them, into the application's own entries.
fn insert_parsed(own: &OwnEntries, xaml: &str) -> windows_core::Result<()> {
    let parsed: OwnEntries = load_xaml(xaml)?.cast()?;
    let mut moved = Vec::new();
    for pair in &parsed {
        moved.push((pair.Key()?, pair.Value()?));
    }
    parsed.Clear()?;
    for (key, value) in moved {
        own.Insert(&key, &value)?;
    }
    Ok(())
}

#[repr(C)]
struct ApplicationStaticsVtbl {
    base: InspectableSlots,
    current: Getter,
}

#[repr(C)]
struct ApplicationVtbl {
    base: InspectableSlots,
    resources: Getter,
}

#[repr(C)]
struct XamlReaderStaticsVtbl {
    base: InspectableSlots,
    load: unsafe extern "system" fn(*mut c_void, *mut c_void, *mut *mut c_void) -> HRESULT,
}

winrt_interface!(
    IApplicationStatics,
    ApplicationStaticsVtbl,
    GUID::from_u128(0x4e0d09f5_4358_512c_a987_503b52848e95)
);
winrt_interface!(
    IApplication,
    ApplicationVtbl,
    GUID::from_u128(0x06a8f4e7_1146_55af_820d_ebd55643b021)
);
winrt_interface!(
    IXamlReaderStatics,
    XamlReaderStaticsVtbl,
    GUID::from_u128(0x82a4cd9e_435e_5aeb_8c4f_300cece45cae)
);

struct Application;
impl RuntimeName for Application {
    const NAME: &'static str = "Microsoft.UI.Xaml.Application";
}

struct XamlReader;
impl RuntimeName for XamlReader {
    const NAME: &'static str = "Microsoft.UI.Xaml.Markup.XamlReader";
}

fn current_application() -> windows_core::Result<IApplication> {
    let statics = windows_core::factory::<Application, IApplicationStatics>()?;
    // SAFETY: slot 6 of IApplicationStatics is get_Current.
    unsafe { get(&statics, statics.vtable().current) }?.cast()
}

fn application_resources(application: &IApplication) -> windows_core::Result<IInspectable> {
    // SAFETY: slot 6 of IApplication is get_Resources.
    unsafe { get(application, application.vtable().resources) }
}

fn load_xaml(xaml: &str) -> windows_core::Result<IInspectable> {
    let reader = windows_core::factory::<XamlReader, IXamlReaderStatics>()?;
    let xaml = HSTRING::from(xaml);
    let mut out = std::ptr::null_mut();
    // SAFETY: slot 6 of IXamlReaderStatics is Load(HSTRING) -> Object; the
    // HSTRING is borrowed for the call (its ABI is the handle itself).
    unsafe {
        (reader.vtable().load)(
            reader.as_raw(),
            std::mem::transmute_copy::<HSTRING, *mut c_void>(&xaml),
            &mut out,
        )
    }
    .ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: Load returned one owned reference.
    Ok(unsafe { IInspectable::from_raw(out) })
}

/// `Windows.UI.Color`, the value of the title bar's color properties.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct UiColor {
    a: u8,
    r: u8,
    g: u8,
    b: u8,
}

impl windows_core::imp::TypeKind for UiColor {
    type TypeKind = windows_core::imp::CopyType;
}

impl windows_core::RuntimeType for UiColor {
    const SIGNATURE: windows_core::imp::ConstBuffer =
        windows_core::imp::ConstBuffer::from_slice(b"struct(Windows.UI.Color;u1;u1;u1;u1)");
}

impl From<Rgb8> for UiColor {
    fn from(value: Rgb8) -> Self {
        Self {
            a: 255,
            r: value.r,
            g: value.g,
            b: value.b,
        }
    }
}

/// `IAppWindowTitleBar` setter slots (`Microsoft.UI.winmd`, Windows App SDK
/// 2.1.3) and the M3 role each takes: the window content's surface with
/// on-surface text, M3 state layers on the caption buttons, on-surface-variant
/// text while the window is inactive. Windows keeps its own close-button
/// hover and press colors.
pub fn title_bar_colors(s: &Roles) -> [(usize, Rgb8); 12] {
    let hover = s.surface.mix(s.text, state::HOVER);
    let pressed = s.surface.mix(s.text, state::PRESSED);
    [
        (7, s.surface),  // put_BackgroundColor
        (9, s.surface),  // put_ButtonBackgroundColor
        (11, s.text),    // put_ButtonForegroundColor
        (13, hover),     // put_ButtonHoverBackgroundColor
        (15, s.text),    // put_ButtonHoverForegroundColor
        (17, s.surface), // put_ButtonInactiveBackgroundColor
        (19, s.variant), // put_ButtonInactiveForegroundColor
        (21, pressed),   // put_ButtonPressedBackgroundColor
        (23, s.text),    // put_ButtonPressedForegroundColor
        (27, s.text),    // put_ForegroundColor
        (32, s.surface), // put_InactiveBackgroundColor
        (34, s.variant), // put_InactiveForegroundColor
    ]
}

/// Color the window's title bar in the shown scheme's surface role, so it
/// reads as part of the content in both schemes (Windows draws its own light
/// caption, a warm grey, beside the M3 surface otherwise). `None` (High
/// Contrast) clears the colors, so the system colors apply.
///
/// Reactor 0.100.0 sets only `AppWindowTitleBar.PreferredTheme` and exposes
/// neither the window handle nor its `AppWindow`, so this follows the Windows
/// App SDK's own interop path (`Microsoft.UI.Interop.h`): the process's
/// WinUI window, `Windowing_GetWindowIdFromWindow` from
/// `Microsoft.Internal.FrameworkUdk.dll`, `AppWindow.GetFromWindowId`, then
/// the `AppWindowTitleBar` color properties (supported for the default
/// title bar on Windows 11).
pub fn apply_title_bar(scheme: Option<&Roles>) -> Result<(), String> {
    let hwnd = own_window().ok_or("the window is not shown yet")?;
    #[cfg(target_env = "msvc")]
    set_window_icon(hwnd);
    let window_id = window_id(hwnd)?;
    let run = || -> windows_core::Result<()> {
        let statics = windows_core::factory::<AppWindow, IAppWindowStatics>()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: slot 9 of IAppWindowStatics is GetFromWindowId(WindowId,
        // out AppWindow); WindowId is one UInt64 passed by value.
        unsafe { (statics.vtable().get_from_window_id)(statics.as_raw(), window_id, &mut out) }
            .ok()?;
        if out.is_null() {
            return Err(windows_core::Error::empty());
        }
        // SAFETY: GetFromWindowId returned one owned reference.
        let app_window: IAppWindow = unsafe { IInspectable::from_raw(out) }.cast()?;
        // SAFETY: slot 16 of IAppWindow is get_TitleBar.
        let title_bar: IAppWindowTitleBar =
            unsafe { get(&app_window, app_window.vtable().title_bar) }?.cast()?;
        let colors: Vec<(usize, Option<IReference<UiColor>>)> = match scheme {
            Some(scheme) => title_bar_colors(scheme)
                .into_iter()
                .map(|(slot, color)| (slot, Some(IReference::from(UiColor::from(color)))))
                .collect(),
            None => title_bar_colors(Theme::Light.roles())
                .into_iter()
                .map(|(slot, _)| (slot, None))
                .collect(),
        };
        for (slot, value) in colors {
            let raw = title_bar.vtable().methods[slot - 6];
            // SAFETY: every listed slot of IAppWindowTitleBar is a
            // put_*Color(IReference<Windows.UI.Color>) setter; null clears it.
            let setter: Setter = unsafe { std::mem::transmute::<usize, Setter>(raw) };
            let argument = value
                .as_ref()
                .map_or(std::ptr::null_mut(), Interface::as_raw);
            // SAFETY: the setter belongs to this title bar's vtable and only
            // borrows the reference for the call.
            unsafe { setter(title_bar.as_raw(), argument) }.ok()?;
        }
        Ok(())
    };
    run().map_err(|error| format!("{error} ({:#010X})", error.code().0))
}

#[cfg(target_env = "msvc")]
fn set_window_icon(hwnd: HWND) {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        ICON_BIG, ICON_SMALL, LoadIconW, SendMessageW, WM_SETICON,
    };

    // Resource 101 is the icon group embedded by the Windows build script.
    let resource = std::ptr::without_provenance(101);
    // SAFETY: null selects this executable. LoadIconW returns a shared handle
    // that remains valid without a call to DestroyIcon.
    unsafe {
        let module = GetModuleHandleW(std::ptr::null());
        if module.is_null() {
            return;
        }
        let icon = LoadIconW(module, resource);
        if !icon.is_null() {
            SendMessageW(hwnd, WM_SETICON, ICON_SMALL as usize, icon as LPARAM);
            SendMessageW(hwnd, WM_SETICON, ICON_BIG as usize, icon as LPARAM);
        }
    }
}

/// This process's visible WinUI window (the desktop has one). The valid
/// clipboard owner for [`crate::clipboard`] copies.
pub(crate) fn own_window() -> Option<HWND> {
    struct Search {
        pid: u32,
        found: HWND,
    }
    unsafe extern "system" fn visit(hwnd: HWND, search: LPARAM) -> BOOL {
        // SAFETY: `search` is the `Search` passed to EnumWindows below, alive
        // for the whole enumeration.
        let search = unsafe { &mut *(search as *mut Search) };
        let mut pid = 0;
        // SAFETY: plain Win32 queries on a window handle EnumWindows gave us.
        unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
        // SAFETY: as above.
        if pid != search.pid || unsafe { IsWindowVisible(hwnd) } == 0 {
            return 1;
        }
        let mut class = [0u16; 64];
        // SAFETY: the buffer and its length match.
        let length = unsafe { GetClassNameW(hwnd, class.as_mut_ptr(), class.len() as i32) };
        if String::from_utf16_lossy(&class[..length.max(0) as usize]) == WINUI_WINDOW_CLASS {
            search.found = hwnd;
            return 0;
        }
        1
    }
    let mut search = Search {
        pid: std::process::id(),
        found: std::ptr::null_mut(),
    };
    // SAFETY: the callback only reads and writes `search`, which outlives the
    // call.
    unsafe { EnumWindows(Some(visit), &mut search as *mut Search as LPARAM) };
    (!search.found.is_null()).then_some(search.found)
}

/// The window class of a WinUI 3 desktop window.
pub(crate) const WINUI_WINDOW_CLASS: &str = "WinUIDesktopWin32WindowClass";

/// `Windowing_GetWindowIdFromWindow`, as `Microsoft.UI.Interop.h` loads it.
fn window_id(hwnd: HWND) -> Result<u64, String> {
    type GetWindowId = unsafe extern "system" fn(HWND, *mut u64) -> HRESULT;
    let dll: Vec<u16> = "Microsoft.Internal.FrameworkUdk.dll\0"
        .encode_utf16()
        .collect();
    // SAFETY: a NUL-terminated wide string; the module is the Windows App
    // SDK runtime WinUI already loaded (the header falls back to loading it).
    let mut module = unsafe { GetModuleHandleW(dll.as_ptr()) };
    if module.is_null() {
        // SAFETY: as above.
        module = unsafe { LoadLibraryW(dll.as_ptr()) };
    }
    if module.is_null() {
        return Err("Microsoft.Internal.FrameworkUdk.dll is not loaded".to_owned());
    }
    // SAFETY: a NUL-terminated export name of a loaded module.
    let export =
        unsafe { GetProcAddress(module, c"Windowing_GetWindowIdFromWindow".as_ptr().cast()) }
            .ok_or("Windowing_GetWindowIdFromWindow is not exported")?;
    // SAFETY: the export has the `PfnGetWindowIdFromWindow` signature of
    // Microsoft.UI.Interop.h (HWND, WindowId*) -> HRESULT; WindowId is one
    // UInt64.
    let get_window_id: GetWindowId = unsafe { std::mem::transmute(export) };
    let mut id = 0u64;
    // SAFETY: `id` is a valid out pointer for the call.
    unsafe { get_window_id(hwnd, &mut id) }
        .ok()
        .map_err(|error| error.to_string())?;
    Ok(id)
}

type Setter = unsafe extern "system" fn(*mut c_void, *mut c_void) -> HRESULT;

#[repr(C)]
struct AppWindowStaticsVtbl {
    base: InspectableSlots,
    create: [usize; 3],
    get_from_window_id: unsafe extern "system" fn(*mut c_void, u64, *mut *mut c_void) -> HRESULT,
}

#[repr(C)]
struct AppWindowVtbl {
    base: InspectableSlots,
    /// get_Id … put_Title (slots 6–15).
    before_title_bar: [usize; 10],
    title_bar: Getter,
}

#[repr(C)]
struct AppWindowTitleBarVtbl {
    base: InspectableSlots,
    /// Slots 6–38: the color properties, ExtendsContentIntoTitleBar, Height,
    /// IconShowOptions, insets, ResetToDefault and SetDragRectangles.
    methods: [usize; 33],
}

winrt_interface!(
    IAppWindowStatics,
    AppWindowStaticsVtbl,
    GUID::from_u128(0x3c315c24_d540_5d72_b518_b226b83627cb)
);
winrt_interface!(
    IAppWindow,
    AppWindowVtbl,
    GUID::from_u128(0xcfa788b3_643b_5c5e_ad4e_321d48a82acd)
);
winrt_interface!(
    IAppWindowTitleBar,
    AppWindowTitleBarVtbl,
    GUID::from_u128(0x5574efa2_c91c_5700_a363_539c71a7aaf4)
);

struct AppWindow;
impl RuntimeName for AppWindow {
    const NAME: &'static str = "Microsoft.UI.Windowing.AppWindow";
}
