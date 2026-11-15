//! Material 3 design tokens for the desktop client.
//!
//! Color: the M3 color roles generated from one seed by Google's
//! material-color-utilities (the pinned `material-colors` port, tonal-spot
//! scheme), the same generator and role set the `gpui-m3` donor uses
//! (`846e2b83`, `src/theme.rs`: `Scheme::from_seed_variant`). M3 has no
//! success/warning roles, so status colors take HCT tonal palettes at fixed
//! hues with the scheme's container/on-container tones. Type follows the M3
//! type scale and shape the M3 corner scale (both as in that donor file),
//! taken as DIPs. [`winui`] resolves the roles to WinUI brushes and to the
//! theme resource keys of the real controls; under Windows High Contrast it
//! maps nothing, so the system colors apply.

use std::sync::OnceLock;

use material_colors::{
    color::Rgb, hct::Hct, palette::TonalPalette, scheme::variant::SchemeTonalSpot,
};

/// The 8-point spacing grid in DIPs. `XS` is the one half step, for the
/// gap between a glyph and its label or between stacked text lines.
pub mod spacing {
    pub const XS: f64 = 4.0;
    pub const SM: f64 = 8.0;
    pub const MD: f64 = 16.0;
    pub const LG: f64 = 24.0;
}

/// The three corner radii (M3 small, medium, large) and the full round of
/// the 36-DIP controls. Nothing else rounds.
pub mod shape {
    /// Rows, chips, code blocks and fields.
    pub const SM: f64 = 8.0;
    /// Cards and menus.
    pub const MD: f64 = 12.0;
    /// The composer and message bubbles.
    pub const LG: f64 = 16.0;
    /// Buttons: half the control height.
    pub const FULL: f64 = 18.0;
}

/// The one type scale (size in DIPs, weight). Segoe UI Variable is the
/// system face; sizes follow the M3 roles.
pub mod type_scale {
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct TypeStyle {
        pub size: f64,
        pub weight: u16,
    }

    const fn style(size: f64, weight: u16) -> TypeStyle {
        TypeStyle { size, weight }
    }

    /// Page titles and the empty-state line.
    pub const TITLE_LARGE: TypeStyle = style(22.0, 400);
    /// The conversation title and section headings.
    pub const TITLE_MEDIUM: TypeStyle = style(16.0, 600);
    /// Card titles and emphasized rows.
    pub const TITLE_SMALL: TypeStyle = style(14.0, 600);
    /// Messages, rows and controls.
    pub const BODY_MEDIUM: TypeStyle = style(14.0, 400);
    /// Supporting lines.
    pub const BODY_SMALL: TypeStyle = style(12.0, 400);
    /// Section captions and chips.
    pub const LABEL_MEDIUM: TypeStyle = style(12.0, 600);
}

/// Secondary text lowers emphasis with opacity on the theme text brush, so
/// High Contrast still resolves the system color.
pub const MUTED_OPACITY: f64 = 0.72;

/// The one shell: sidebar, conversation, optional right panel.
pub mod layout {
    /// The sidebar (projects, Agents, recents, settings).
    pub const SIDEBAR_WIDTH: f64 = 280.0;
    /// The optional right panel (monitor or Agent details).
    pub const PANEL_WIDTH: f64 = 360.0;
    /// The conversation's reading column and the composer.
    pub const READING_WIDTH: f64 = 760.0;
    /// Buttons, rows and chips.
    pub const CONTROL_HEIGHT: f64 = 36.0;
    /// The composer's text field.
    pub const COMPOSER_FIELD_HEIGHT: f64 = 64.0;
    /// First window client size in DIPs; the user resizes freely after.
    pub const WINDOW_WIDTH: f64 = 1440.0;
    pub const WINDOW_HEIGHT: f64 = 900.0;
}

/// An opaque sRGB color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rgb8 {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl Rgb8 {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }

    /// `over` composited onto `self` at `amount` (an M3 state layer).
    pub fn mix(self, over: Rgb8, amount: f64) -> Rgb8 {
        let blend = |base: u8, top: u8| {
            (f64::from(base) + (f64::from(top) - f64::from(base)) * amount).round() as u8
        };
        Rgb8::new(
            blend(self.r, over.r),
            blend(self.g, over.g),
            blend(self.b, over.b),
        )
    }
}

impl From<Rgb> for Rgb8 {
    fn from(value: Rgb) -> Self {
        Rgb8::new(value.red, value.green, value.blue)
    }
}

/// Eliot's seed: the indigo the first desktop slice used as its accent.
pub const SEED: Rgb8 = Rgb8::new(0x49, 0x5A, 0xCC);

/// HCT hues of the status palettes M3 does not define.
const SUCCESS_HUE: f64 = 145.0;
const WARNING_HUE: f64 = 75.0;
const STATUS_CHROMA: f64 = 40.0;

/// The M3 color roles for one color scheme, plus success and warning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheme {
    pub dark: bool,
    pub primary: Rgb8,
    pub on_primary: Rgb8,
    pub primary_container: Rgb8,
    pub on_primary_container: Rgb8,
    pub secondary: Rgb8,
    pub on_secondary: Rgb8,
    pub secondary_container: Rgb8,
    pub on_secondary_container: Rgb8,
    pub tertiary: Rgb8,
    pub on_tertiary: Rgb8,
    pub tertiary_container: Rgb8,
    pub on_tertiary_container: Rgb8,
    pub error: Rgb8,
    pub on_error: Rgb8,
    pub error_container: Rgb8,
    pub on_error_container: Rgb8,
    pub surface: Rgb8,
    pub surface_dim: Rgb8,
    pub surface_bright: Rgb8,
    pub surface_container_lowest: Rgb8,
    pub surface_container_low: Rgb8,
    pub surface_container: Rgb8,
    pub surface_container_high: Rgb8,
    pub surface_container_highest: Rgb8,
    pub on_surface: Rgb8,
    pub on_surface_variant: Rgb8,
    pub outline: Rgb8,
    pub outline_variant: Rgb8,
    pub inverse_surface: Rgb8,
    pub inverse_on_surface: Rgb8,
    pub inverse_primary: Rgb8,
    pub success: Rgb8,
    pub success_container: Rgb8,
    pub on_success_container: Rgb8,
    pub warning: Rgb8,
    pub warning_container: Rgb8,
    pub on_warning_container: Rgb8,
    /// The WinUI accent ramp (`SystemAccentColorDark3`…`Light3`) from the
    /// primary tonal palette: tones 20, 30, 40, 60, 80, 90. WinUI fills with
    /// Dark1 in Light theme and Light2 in Dark theme, which are exactly the
    /// M3 primary of each scheme (tones 40 and 80).
    pub accent_ramp: [Rgb8; 6],
}

impl Scheme {
    /// The tonal-spot dynamic scheme of `seed` with M3 standard contrast.
    pub fn from_seed(seed: Rgb8, dark: bool) -> Self {
        let source = Hct::new(Rgb::new(seed.r, seed.g, seed.b));
        let s = SchemeTonalSpot::new(source, dark, None).scheme;
        let c = Rgb8::from;
        // Status palettes use the M3 accent tones of the scheme polarity:
        // 40/90/10 light, 80/30/90 dark.
        let (accent, container, on_container) = if dark { (80, 30, 90) } else { (40, 90, 10) };
        let success = TonalPalette::of(SUCCESS_HUE, STATUS_CHROMA);
        let warning = TonalPalette::of(WARNING_HUE, STATUS_CHROMA);
        Self {
            dark,
            primary: c(s.primary()),
            on_primary: c(s.on_primary()),
            primary_container: c(s.primary_container()),
            on_primary_container: c(s.on_primary_container()),
            secondary: c(s.secondary()),
            on_secondary: c(s.on_secondary()),
            secondary_container: c(s.secondary_container()),
            on_secondary_container: c(s.on_secondary_container()),
            tertiary: c(s.tertiary()),
            on_tertiary: c(s.on_tertiary()),
            tertiary_container: c(s.tertiary_container()),
            on_tertiary_container: c(s.on_tertiary_container()),
            error: c(s.error()),
            on_error: c(s.on_error()),
            error_container: c(s.error_container()),
            on_error_container: c(s.on_error_container()),
            surface: c(s.surface()),
            surface_dim: c(s.surface_dim()),
            surface_bright: c(s.surface_bright()),
            surface_container_lowest: c(s.surface_container_lowest()),
            surface_container_low: c(s.surface_container_low()),
            surface_container: c(s.surface_container()),
            surface_container_high: c(s.surface_container_high()),
            surface_container_highest: c(s.surface_container_highest()),
            on_surface: c(s.on_surface()),
            on_surface_variant: c(s.on_surface_variant()),
            outline: c(s.outline()),
            outline_variant: c(s.outline_variant()),
            inverse_surface: c(s.inverse_surface()),
            inverse_on_surface: c(s.inverse_on_surface()),
            inverse_primary: c(s.inverse_primary()),
            success: c(success.tone(accent)),
            success_container: c(success.tone(container)),
            on_success_container: c(success.tone(on_container)),
            warning: c(warning.tone(accent)),
            warning_container: c(warning.tone(container)),
            on_warning_container: c(warning.tone(on_container)),
            accent_ramp: [20, 30, 40, 60, 80, 90].map(|tone| c(s.primary_palette.tone(tone))),
        }
    }

    /// The generated scheme for `dark`, computed once per process.
    pub fn of(dark: bool) -> &'static Scheme {
        static LIGHT: OnceLock<Scheme> = OnceLock::new();
        static DARK: OnceLock<Scheme> = OnceLock::new();
        if dark {
            DARK.get_or_init(|| Scheme::from_seed(SEED, true))
        } else {
            LIGHT.get_or_init(|| Scheme::from_seed(SEED, false))
        }
    }

    /// Container and on-container colors for a status tone.
    pub fn status(&self, tone: StatusTone) -> (Rgb8, Rgb8) {
        match tone {
            StatusTone::Active => (self.primary_container, self.on_primary_container),
            StatusTone::Ready => (self.success_container, self.on_success_container),
            StatusTone::Attention => (self.tertiary_container, self.on_tertiary_container),
            StatusTone::Unknown => (self.warning_container, self.on_warning_container),
            StatusTone::Failed => (self.error_container, self.on_error_container),
            StatusTone::Pending => (self.secondary_container, self.on_secondary_container),
            StatusTone::Neutral => (self.surface_container_highest, self.on_surface_variant),
        }
    }
}

/// Semantic meaning of an Agent or command state, for status chips.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusTone {
    /// Working now (`busy`).
    Active,
    /// Ready for input (`idle`).
    Ready,
    /// Needs the owner (`waiting`: a pending request).
    Attention,
    /// Outcome not proven (`unknown`).
    Unknown,
    /// `failed` or `lost`.
    Failed,
    /// On its way (`created`, `starting`).
    Pending,
    /// Ended or anything else (`stopped`).
    Neutral,
}

impl StatusTone {
    /// The tone of an Agent state name as the daemon reports it.
    pub fn of_state(state: &str) -> Self {
        match state {
            "busy" | "running" => Self::Active,
            "idle" => Self::Ready,
            "waiting" => Self::Attention,
            "unknown" => Self::Unknown,
            "failed" | "lost" => Self::Failed,
            "created" | "starting" => Self::Pending,
            _ => Self::Neutral,
        }
    }
}

#[cfg(windows)]
pub mod winui {
    //! M3 roles as WinUI brushes, and as theme resource overrides for real
    //! `Button`s, the one control that takes Reactor `ResourceOverrides` in
    //! 0.100.0. The keys are WinUI's own (`microsoft-ui-xaml`
    //! `Button_themeresources.xaml`), so buttons keep their hover, pressed,
    //! disabled and focus states. Under High Contrast nothing is overridden
    //! and every role resolves to a live system brush.
    //!
    //! Surfaces are neutral; the primary color marks only the main action
    //! (filled) and the selection (the selected row). Status colors appear
    //! only as small dots and on a pending request.

    use windows_reactor::{Brush, Color, CornerRadius, ResourceOverrides, ThemeBrush};

    use super::{Rgb8, Scheme, StatusTone, shape};

    pub fn color(value: Rgb8) -> Color {
        Color::rgb(value.r, value.g, value.b)
    }

    /// `value` at `alpha` (0–1): a state layer over whatever is beneath.
    fn layer(value: Rgb8, alpha: f64) -> Color {
        Color::argb((alpha * 255.0).round() as u8, value.r, value.g, value.b)
    }

    /// The active palette: the generated M3 scheme, or the system colors
    /// under High Contrast. Cheap to copy into every view function.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Palette {
        scheme: Option<&'static Scheme>,
    }

    impl Palette {
        pub fn new(dark: bool, high_contrast: bool) -> Self {
            Self {
                scheme: (!high_contrast).then(|| Scheme::of(dark)),
            }
        }

        pub fn scheme(self) -> Option<&'static Scheme> {
            self.scheme
        }

        fn role(self, pick: fn(&Scheme) -> Rgb8, system: ThemeBrush) -> Brush {
            match self.scheme {
                Some(scheme) => Brush::Solid(color(pick(scheme))),
                None => Brush::Theme(system),
            }
        }

        /// The conversation and settings area (the window background).
        pub fn surface(self) -> Brush {
            self.role(|s| s.surface, ThemeBrush::SolidBackground)
        }

        /// The sidebar and the right panel: one step off the surface.
        pub fn sidebar(self) -> Brush {
            self.role(|s| s.surface_container_low, ThemeBrush::SolidBackground)
        }

        /// Cards (monitor, details, settings sections).
        pub fn card(self) -> Brush {
            self.role(|s| s.surface_container, ThemeBrush::CardBackground)
        }

        /// The composer, the user's message and code blocks.
        pub fn raised(self) -> Brush {
            self.role(|s| s.surface_container_high, ThemeBrush::CardBackground)
        }

        /// Text fields on a card or the sidebar.
        pub fn field(self) -> Brush {
            self.role(|s| s.surface_container_highest, ThemeBrush::CardBackground)
        }

        pub fn on_surface(self) -> Brush {
            self.role(|s| s.on_surface, ThemeBrush::PrimaryText)
        }

        /// Hairlines and field outlines.
        pub fn outline_variant(self) -> Brush {
            self.role(|s| s.outline_variant, ThemeBrush::CardStroke)
        }

        pub fn primary(self) -> Brush {
            self.role(|s| s.primary, ThemeBrush::AccentText)
        }

        pub fn tertiary(self) -> Brush {
            self.role(|s| s.tertiary, ThemeBrush::AccentText)
        }

        pub fn error(self) -> Brush {
            self.role(|s| s.error, ThemeBrush::SystemCritical)
        }

        /// The status dot of a tone; `None` for tones that need no mark in
        /// a list (ready, ended), so a calm fleet shows no dots at all.
        pub fn dot(self, tone: StatusTone) -> Option<Brush> {
            let pick: fn(&Scheme) -> Rgb8 = match tone {
                StatusTone::Active => |s| s.primary,
                StatusTone::Attention => |s| s.tertiary,
                StatusTone::Failed => |s| s.error,
                StatusTone::Unknown => |s| s.warning,
                StatusTone::Pending => |s| s.outline,
                StatusTone::Ready | StatusTone::Neutral => return None,
            };
            Some(self.role(pick, ThemeBrush::AccentText))
        }

        /// The header dot: every tone has one (ready is green, ended grey).
        pub fn header_dot(self, tone: StatusTone) -> Brush {
            self.dot(tone).unwrap_or_else(|| match tone {
                StatusTone::Ready => self.role(|s| s.success, ThemeBrush::AccentText),
                _ => self.role(|s| s.outline, ThemeBrush::PrimaryText),
            })
        }

        /// Background and text of a pending request card.
        pub fn attention(self) -> (Brush, Brush) {
            match self.scheme {
                Some(scheme) => {
                    let (container, on) = scheme.status(StatusTone::Attention);
                    (Brush::Solid(color(container)), Brush::Solid(color(on)))
                }
                None => (
                    Brush::Theme(ThemeBrush::CardBackground),
                    Brush::Theme(ThemeBrush::PrimaryText),
                ),
            }
        }

        /// Filled: the one main action of a surface.
        pub fn filled(self) -> ResourceOverrides {
            self.with(|s| family(s, Some(s.primary), s.on_primary, shape::FULL))
        }

        /// Tonal: a secondary action that should still read as a button.
        pub fn tonal(self) -> ResourceOverrides {
            self.with(|s| {
                family(
                    s,
                    Some(s.secondary_container),
                    s.on_secondary_container,
                    shape::FULL,
                )
            })
        }

        /// Text: low emphasis, neutral content (icon buttons too).
        pub fn text_button(self) -> ResourceOverrides {
            self.with(|s| family(s, None, s.on_surface_variant, shape::FULL))
        }

        /// A list row (sidebar, settings sections, monitor cards): neutral
        /// at rest, the selection in secondary-container.
        pub fn row(self, selected: bool) -> ResourceOverrides {
            self.with(|s| {
                if selected {
                    family(
                        s,
                        Some(s.secondary_container),
                        s.on_secondary_container,
                        shape::SM,
                    )
                } else {
                    family(s, None, s.on_surface, shape::SM)
                }
            })
        }

        /// A card that is one button (monitor cards): the card fill with
        /// state layers.
        pub fn card_button(self) -> ResourceOverrides {
            self.with(|s| family(s, Some(s.surface_container), s.on_surface, shape::MD))
        }

        /// A chip that opens a menu: neutral text, row-shaped.
        pub fn chip(self) -> ResourceOverrides {
            self.with(|s| family(s, None, s.on_surface_variant, shape::SM))
        }

        fn with(self, build: impl FnOnce(&Scheme) -> ResourceOverrides) -> ResourceOverrides {
            match self.scheme {
                Some(scheme) => build(scheme),
                None => ResourceOverrides::new(),
            }
        }
    }

    /// One button family on WinUI's default button keys: an optional fill
    /// with M3 state layers of its content color `on`, no outline, the M3
    /// disabled treatment (12% / 38% on-surface) and one corner radius.
    fn family(s: &Scheme, fill: Option<Rgb8>, on: Rgb8, radius: f64) -> ResourceOverrides {
        let clear = Color::transparent();
        let (rest, hover, pressed, disabled) = match fill {
            Some(fill) => (
                color(fill),
                color(fill.mix(on, 0.08)),
                color(fill.mix(on, 0.12)),
                layer(s.on_surface, 0.12),
            ),
            None => (clear, layer(on, 0.08), layer(on, 0.12), clear),
        };
        let mut r = ResourceOverrides::new()
            .set("ButtonBackground", rest)
            .set("ButtonBackgroundPointerOver", hover)
            .set("ButtonBackgroundPressed", pressed)
            .set("ButtonBackgroundDisabled", disabled)
            .set("ButtonForegroundDisabled", layer(s.on_surface, 0.38))
            .set("ButtonBorderBrushDisabled", clear)
            .set("ControlCornerRadius", CornerRadius::from(radius));
        for state in ["", "PointerOver", "Pressed"] {
            r = r
                .set(format!("ButtonForeground{state}"), color(on))
                .set(format!("ButtonBorderBrush{state}"), clear);
        }
        r
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn high_contrast_resolves_every_role_to_a_system_brush() {
            let palette = Palette::new(true, true);
            // Never a fixed color behind the window: a WinUI 3 window with no
            // root background shows black (Light-theme text invisible).
            assert_eq!(palette.surface(), Brush::Theme(ThemeBrush::SolidBackground));
            assert_eq!(palette.sidebar(), Brush::Theme(ThemeBrush::SolidBackground));
            assert_eq!(palette.on_surface(), Brush::Theme(ThemeBrush::PrimaryText));
            assert_eq!(palette.error(), Brush::Theme(ThemeBrush::SystemCritical));
            for overrides in [
                palette.filled(),
                palette.tonal(),
                palette.text_button(),
                palette.row(true),
                palette.card_button(),
                palette.chip(),
            ] {
                assert_eq!(overrides, ResourceOverrides::new());
            }
            assert_eq!(
                palette.attention(),
                (
                    Brush::Theme(ThemeBrush::CardBackground),
                    Brush::Theme(ThemeBrush::PrimaryText)
                )
            );
        }

        #[test]
        fn schemes_resolve_roles_to_their_generated_colors() {
            for dark in [false, true] {
                let scheme = Scheme::of(dark);
                let palette = Palette::new(dark, false);
                assert_eq!(palette.surface(), Brush::Solid(color(scheme.surface)));
                assert_eq!(palette.primary(), Brush::Solid(color(scheme.primary)));
                assert_ne!(palette.filled(), ResourceOverrides::new());
                assert_ne!(palette.row(true), palette.row(false));
            }
            assert_ne!(Palette::new(true, false), Palette::new(false, false));
        }

        #[test]
        fn only_states_that_need_a_look_carry_a_list_dot() {
            let palette = Palette::new(true, false);
            assert!(palette.dot(StatusTone::Ready).is_none());
            assert!(palette.dot(StatusTone::Neutral).is_none());
            for tone in [
                StatusTone::Active,
                StatusTone::Attention,
                StatusTone::Failed,
                StatusTone::Unknown,
                StatusTone::Pending,
            ] {
                assert!(palette.dot(tone).is_some(), "{tone:?}");
            }
            // The header always shows one.
            assert_ne!(
                palette.header_dot(StatusTone::Ready),
                palette.header_dot(StatusTone::Neutral)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WCAG relative luminance.
    fn luminance(color: Rgb8) -> f64 {
        let channel = |value: u8| {
            let value = f64::from(value) / 255.0;
            if value <= 0.03928 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * channel(color.r) + 0.7152 * channel(color.g) + 0.0722 * channel(color.b)
    }

    /// WCAG contrast ratio between two colors.
    fn contrast(a: Rgb8, b: Rgb8) -> f64 {
        let (a, b) = (luminance(a), luminance(b));
        (a.max(b) + 0.05) / (a.min(b) + 0.05)
    }

    #[test]
    fn content_roles_meet_text_contrast_on_their_containers() {
        for dark in [false, true] {
            let s = Scheme::of(dark);
            let pairs = [
                ("on_primary/primary", s.on_primary, s.primary),
                (
                    "on_primary_container",
                    s.on_primary_container,
                    s.primary_container,
                ),
                (
                    "on_secondary_container",
                    s.on_secondary_container,
                    s.secondary_container,
                ),
                (
                    "on_tertiary_container",
                    s.on_tertiary_container,
                    s.tertiary_container,
                ),
                (
                    "on_error_container",
                    s.on_error_container,
                    s.error_container,
                ),
                (
                    "on_success_container",
                    s.on_success_container,
                    s.success_container,
                ),
                (
                    "on_warning_container",
                    s.on_warning_container,
                    s.warning_container,
                ),
                ("on_surface", s.on_surface, s.surface),
                (
                    "on_surface/highest",
                    s.on_surface,
                    s.surface_container_highest,
                ),
                (
                    "on_surface_variant",
                    s.on_surface_variant,
                    s.surface_container,
                ),
            ];
            for (name, text, fill) in pairs {
                let ratio = contrast(text, fill);
                assert!(ratio >= 4.5, "{name} dark={dark}: contrast {ratio:.2}");
            }
        }
    }

    #[test]
    fn surface_containers_step_away_from_the_surface() {
        // Light containers darken toward highest; dark ones lighten.
        for dark in [false, true] {
            let s = Scheme::of(dark);
            let steps = [
                s.surface_container_lowest,
                s.surface_container_low,
                s.surface_container,
                s.surface_container_high,
                s.surface_container_highest,
            ]
            .map(luminance);
            for pair in steps.windows(2) {
                if dark {
                    assert!(pair[0] < pair[1], "dark steps {steps:?}");
                } else {
                    assert!(pair[0] > pair[1], "light steps {steps:?}");
                }
            }
        }
    }

    #[test]
    fn primary_keeps_the_seed_hue() {
        let seed = Hct::new(Rgb::new(SEED.r, SEED.g, SEED.b)).get_hue();
        for dark in [false, true] {
            let p = Scheme::of(dark).primary;
            let hue = Hct::new(Rgb::new(p.r, p.g, p.b)).get_hue();
            let distance = (hue - seed).abs().min(360.0 - (hue - seed).abs());
            assert!(distance < 15.0, "primary hue {hue:.1} vs seed {seed:.1}");
        }
    }

    #[test]
    fn every_agent_state_has_a_status_tone() {
        let tones = [
            ("busy", StatusTone::Active),
            ("idle", StatusTone::Ready),
            ("waiting", StatusTone::Attention),
            ("unknown", StatusTone::Unknown),
            ("failed", StatusTone::Failed),
            ("lost", StatusTone::Failed),
            ("created", StatusTone::Pending),
            ("starting", StatusTone::Pending),
            ("stopped", StatusTone::Neutral),
        ];
        for (state, tone) in tones {
            assert_eq!(StatusTone::of_state(state), tone, "{state}");
        }
    }

    #[test]
    fn state_layers_composite_toward_the_content_color() {
        let base = Rgb8::new(0, 0, 0);
        let on = Rgb8::new(255, 255, 255);
        assert_eq!(base.mix(on, 0.0), base);
        assert_eq!(base.mix(on, 1.0), on);
        assert_eq!(base.mix(on, 0.08), Rgb8::new(20, 20, 20));
    }
}
