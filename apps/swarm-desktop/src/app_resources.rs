//! WinUI accent and selection resources at the application root, from the
//! M3 roles.
//!
//! Without them every WinUI control that draws an accent (the ListView
//! selection pill and selected row, focus underlines of text fields, combo
//! box selections, check boxes) uses the Windows system accent next to the
//! M3 palette. Reactor 0.100.0 applies [`windows_reactor::ResourceOverrides`]
//! only to a `Button` (the one generated builder with `resource_overrides`;
//! `native/winui/mod.rs` `set_resource_overrides` writes into that element's
//! own `Resources`), and its `ResourceValue::Color` becomes a
//! `SolidColorBrush`, so it cannot set Color resources such as
//! `SystemAccentColor` either. The application root is reached directly:
//! `Application.Current.Resources.ThemeDictionaries["Light"|"Dark"]` get one
//! dictionary each, parsed by `XamlReader.Load`, the runtime equivalent of an
//! `App.xaml` and the mechanism Reactor itself uses to compile styles
//! (`native/winui/mod.rs`, `XamlReader::Load`).
//!
//! WinUI resolves a ThemeResource through the application's own entries and
//! theme dictionaries before its merged `XamlControlsResources`, so these
//! keys win; they follow the window's Light/Dark switch on their own, and
//! under High Contrast WinUI selects the `HighContrast` dictionary, which is
//! left alone: the system colors apply. The keys are WinUI's (lightweight
//! styling, `microsoft.windowsappsdk.winui` 2.3.0 `Themes/generic.xaml`).
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.Xaml.winmd`
//! (WinUI 2.3.0 metadata): `IApplicationStatics` slot 6 `get_Current`,
//! `IApplication` slot 6 `get_Resources`, `IResourceDictionary` slot 9
//! `get_ThemeDictionaries`, `IXamlReaderStatics` slot 6 `Load`. Only these
//! four calls are declared; Reactor's own bindings are crate-private.
#![allow(unsafe_code)]

use std::ffi::c_void;
use std::fmt::Write as _;

use windows_collections::IMap;
use windows_core::{GUID, HRESULT, HSTRING, IInspectable, IUnknown, Interface, RuntimeName};
use windows_reference::IReference;

use crate::theme::{Rgb8, Scheme, shape};

/// A theme resource value: a Color resource, a SolidColorBrush or a
/// uniform CornerRadius.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value {
    Color(Rgb8),
    /// A brush of `color` at `alpha` (0–255).
    Brush(Rgb8, u8),
    /// A uniform corner radius in whole DIPs.
    Radius(u8),
}

const fn opaque(color: Rgb8) -> Value {
    Value::Brush(color, 255)
}

/// `alpha` as a byte (an M3 state layer or the WinUI opacity steps).
fn alpha(value: f64) -> u8 {
    (value * 255.0).round() as u8
}

/// The WinUI keys this window sets for one color scheme, with their values.
pub fn entries(s: &Scheme) -> Vec<(&'static str, Value)> {
    let [dark3, dark2, dark1, light1, light2, light3] = s.accent_ramp;
    let selected_hover = s.secondary_container.mix(s.on_secondary_container, 0.08);
    let selected_pressed = s.secondary_container.mix(s.on_secondary_container, 0.12);
    vec![
        // The accent ramp: older brushes and templates read these colors.
        ("SystemAccentColor", Value::Color(s.primary)),
        ("SystemAccentColorDark3", Value::Color(dark3)),
        ("SystemAccentColorDark2", Value::Color(dark2)),
        ("SystemAccentColorDark1", Value::Color(dark1)),
        ("SystemAccentColorLight1", Value::Color(light1)),
        ("SystemAccentColorLight2", Value::Color(light2)),
        ("SystemAccentColorLight3", Value::Color(light3)),
        // Accent fills (checked boxes, focus underlines, selections).
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
        ("AccentTextFillColorPrimaryBrush", opaque(s.primary)),
        ("AccentTextFillColorSecondaryBrush", opaque(s.primary)),
        (
            "AccentTextFillColorTertiaryBrush",
            Value::Brush(s.primary, alpha(0.8)),
        ),
        ("TextOnAccentFillColorPrimaryBrush", opaque(s.on_primary)),
        (
            "TextOnAccentFillColorSecondaryBrush",
            Value::Brush(s.on_primary, alpha(0.7)),
        ),
        (
            "TextOnAccentFillColorSelectedTextBrush",
            opaque(s.on_primary),
        ),
        // WinUI's AccentButtonStyle as the M3 filled button (used where the
        // window palette cannot apply, such as inside a ContentDialog).
        ("AccentButtonBackground", opaque(s.primary)),
        (
            "AccentButtonBackgroundPointerOver",
            opaque(s.primary.mix(s.on_primary, 0.08)),
        ),
        (
            "AccentButtonBackgroundPressed",
            opaque(s.primary.mix(s.on_primary, 0.12)),
        ),
        (
            "AccentButtonBackgroundDisabled",
            Value::Brush(s.on_surface, alpha(0.12)),
        ),
        ("AccentButtonForeground", opaque(s.on_primary)),
        ("AccentButtonForegroundPointerOver", opaque(s.on_primary)),
        ("AccentButtonForegroundPressed", opaque(s.on_primary)),
        (
            "AccentButtonForegroundDisabled",
            Value::Brush(s.on_surface, alpha(0.38)),
        ),
        // List selection: the M3 secondary-container row with a primary
        // indicator, and on-surface state layers on hover and press.
        (
            "ListViewItemBackgroundSelected",
            opaque(s.secondary_container),
        ),
        (
            "ListViewItemBackgroundSelectedPointerOver",
            opaque(selected_hover),
        ),
        (
            "ListViewItemBackgroundSelectedPressed",
            opaque(selected_pressed),
        ),
        (
            "ListViewItemForegroundSelected",
            opaque(s.on_secondary_container),
        ),
        (
            "ListViewItemForegroundSelectedPointerOver",
            opaque(s.on_secondary_container),
        ),
        (
            "ListViewItemForegroundSelectedPressed",
            opaque(s.on_secondary_container),
        ),
        ("ListViewItemSelectionIndicatorBrush", opaque(s.primary)),
        (
            "ListViewItemSelectionIndicatorPointerOverBrush",
            opaque(s.primary),
        ),
        (
            "ListViewItemSelectionIndicatorPressedBrush",
            opaque(s.primary),
        ),
        (
            "ListViewItemBackgroundPointerOver",
            Value::Brush(s.on_surface, alpha(0.08)),
        ),
        (
            "ListViewItemBackgroundPressed",
            Value::Brush(s.on_surface, alpha(0.12)),
        ),
        // The one control radius (buttons, rows, chips, fields, combo
        // boxes). A Button's style reads `ControlCornerRadius` when the
        // style is applied, before Reactor sets a button's own resource
        // overrides, so it is set here, where WinUI resolves it first.
        ("ControlCornerRadius", Value::Radius(shape::SM as u8)),
    ]
}

fn hex(color: Rgb8, alpha: u8) -> String {
    format!("#{alpha:02X}{:02X}{:02X}{:02X}", color.r, color.g, color.b)
}

/// One theme dictionary as XAML. Keys are fixed identifiers and values hex
/// colors, so nothing in it needs escaping.
pub fn dictionary_xaml(scheme: &Scheme) -> String {
    let mut xaml = String::from(
        "<ResourceDictionary \
         xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\" \
         xmlns:x=\"http://schemas.microsoft.com/winfx/2006/xaml\">",
    );
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
        };
    }
    xaml.push_str("</ResourceDictionary>");
    xaml
}

/// Install the Light and Dark dictionaries on the running application.
/// Called once, on the UI thread, after Reactor merged
/// `XamlControlsResources` and before the first controls are created
/// (Reactor's `on_launched` installs those resources, then mounts the
/// component), so every control resolves these keys from its first template.
pub fn install() -> Result<(), String> {
    install_native().map_err(|error| format!("{error} ({:#010X})", error.code().0))
}

fn install_native() -> windows_core::Result<()> {
    let application = current_application()?;
    let themes = theme_dictionaries(&application)?;
    for (theme, dark) in [("Light", false), ("Dark", true)] {
        let dictionary = load_xaml(&dictionary_xaml(Scheme::of(dark)))?;
        let key: IInspectable = IReference::<HSTRING>::from(theme).into();
        themes.Insert(&key, &dictionary)?;
    }
    Ok(())
}

/// A WinRT interface pointer with a hand-declared vtable.
macro_rules! winrt_interface {
    ($name:ident, $vtable:ident, $iid:literal) => {
        #[repr(transparent)]
        #[derive(Clone, PartialEq, Eq, Debug)]
        struct $name(IUnknown);

        // SAFETY: a transparent wrapper over an owned COM pointer whose
        // vtable begins with the six IInspectable slots, as declared below;
        // the IID is the interface's id from Microsoft.UI.Xaml.winmd.
        unsafe impl Interface for $name {
            type Vtable = $vtable;
            const IID: GUID = GUID::from_u128($iid);
        }
    };
}

type Getter = unsafe extern "system" fn(*mut c_void, *mut *mut c_void) -> HRESULT;

/// IUnknown and IInspectable slots 0–5, never called through these structs.
type InspectableSlots = [usize; 6];

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
struct ResourceDictionaryVtbl {
    base: InspectableSlots,
    source: usize,
    set_source: usize,
    merged_dictionaries: usize,
    theme_dictionaries: Getter,
}

#[repr(C)]
struct XamlReaderStaticsVtbl {
    base: InspectableSlots,
    load: unsafe extern "system" fn(*mut c_void, *mut c_void, *mut *mut c_void) -> HRESULT,
}

winrt_interface!(
    IApplicationStatics,
    ApplicationStaticsVtbl,
    0x4e0d09f5_4358_512c_a987_503b52848e95
);
winrt_interface!(
    IApplication,
    ApplicationVtbl,
    0x06a8f4e7_1146_55af_820d_ebd55643b021
);
winrt_interface!(
    IResourceDictionary,
    ResourceDictionaryVtbl,
    0x1b690975_a710_5783_a6e1_15836f6186c2
);
winrt_interface!(
    IXamlReaderStatics,
    XamlReaderStaticsVtbl,
    0x82a4cd9e_435e_5aeb_8c4f_300cece45cae
);

struct Application;
impl RuntimeName for Application {
    const NAME: &'static str = "Microsoft.UI.Xaml.Application";
}

struct XamlReader;
impl RuntimeName for XamlReader {
    const NAME: &'static str = "Microsoft.UI.Xaml.Markup.XamlReader";
}

/// Call a getter slot and take ownership of the returned object.
///
/// # Safety
/// `getter` must be a slot of `this`'s vtable that returns one owned
/// IInspectable-derived pointer.
unsafe fn get<T: Interface>(this: &T, getter: Getter) -> windows_core::Result<IInspectable> {
    let mut out = std::ptr::null_mut();
    // SAFETY: the caller guarantees `getter` belongs to `this`.
    unsafe { getter(this.as_raw(), &mut out) }.ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: the getter returned one owned reference.
    Ok(unsafe { IInspectable::from_raw(out) })
}

fn current_application() -> windows_core::Result<IApplication> {
    let statics = windows_core::factory::<Application, IApplicationStatics>()?;
    // SAFETY: slot 6 of IApplicationStatics is get_Current.
    unsafe { get(&statics, statics.vtable().current) }?.cast()
}

fn theme_dictionaries(
    application: &IApplication,
) -> windows_core::Result<IMap<IInspectable, IInspectable>> {
    // SAFETY: slot 6 of IApplication is get_Resources.
    let resources: IResourceDictionary =
        unsafe { get(application, application.vtable().resources) }?.cast()?;
    // SAFETY: slot 9 of IResourceDictionary is get_ThemeDictionaries, an
    // IMap<Object, Object>.
    unsafe { get(&resources, resources.vtable().theme_dictionaries) }?.cast()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_scheme_sets_the_accent_list_and_text_keys_once() {
        for dark in [false, true] {
            let entries = entries(Scheme::of(dark));
            let mut keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
            for required in [
                "SystemAccentColor",
                "SystemAccentColorLight2",
                "SystemAccentColorDark1",
                "AccentFillColorDefaultBrush",
                "AccentTextFillColorPrimaryBrush",
                "TextOnAccentFillColorPrimaryBrush",
                "ListViewItemSelectionIndicatorBrush",
                "ListViewItemBackgroundSelected",
            ] {
                assert!(keys.contains(&required), "{required} dark={dark}");
            }
            let count = keys.len();
            keys.sort_unstable();
            keys.dedup();
            assert_eq!(keys.len(), count, "duplicate key dark={dark}");
        }
    }

    #[test]
    fn accents_are_the_scheme_primary_not_the_system_accent() {
        for dark in [false, true] {
            let s = Scheme::of(dark);
            let entries = entries(s);
            let value = |key: &str| entries.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
            assert_eq!(value("SystemAccentColor"), Some(Value::Color(s.primary)));
            assert_eq!(
                value("AccentFillColorDefaultBrush"),
                Some(Value::Brush(s.primary, 255))
            );
            assert_eq!(
                value("ListViewItemBackgroundSelected"),
                Some(Value::Brush(s.secondary_container, 255))
            );
            // WinUI's own mapping reads the fill from Dark1 (light theme) and
            // Light2 (dark theme): the ramp keeps those on the M3 primary.
            let ramp_slot = if dark {
                "SystemAccentColorLight2"
            } else {
                "SystemAccentColorDark1"
            };
            assert_eq!(value(ramp_slot), Some(Value::Color(s.primary)));
        }
    }

    #[test]
    fn dictionary_xaml_is_one_dictionary_with_argb_values() {
        let s = Scheme::of(true);
        let xaml = dictionary_xaml(s);
        assert!(xaml.starts_with("<ResourceDictionary "));
        assert!(xaml.ends_with("</ResourceDictionary>"));
        assert_eq!(xaml.matches("x:Key=").count(), entries(s).len());
        assert!(xaml.contains(&format!(
            "<Color x:Key=\"SystemAccentColor\">#FF{:02X}{:02X}{:02X}</Color>",
            s.primary.r, s.primary.g, s.primary.b
        )));
        assert!(xaml.contains(&format!(
            "<SolidColorBrush x:Key=\"AccentFillColorSecondaryBrush\" Color=\"#E6{:02X}{:02X}{:02X}\"/>",
            s.primary.r, s.primary.g, s.primary.b
        )));
    }
}
