//! Material 3 color for the desktop client.
//!
//! Color: the M3 color roles generated from one seed by Google's
//! material-color-utilities (the pinned `material-colors` port, tonal-spot
//! scheme), the same generator and role set the `gpui-m3` donor uses
//! (`846e2b83`, `src/theme.rs`: `Scheme::from_seed_variant`). M3 has no
//! success/warning roles, so status colors take HCT tonal palettes at fixed
//! hues with the scheme's container/on-container tones. Shape, height, type
//! and spacing live in `ui::tokens`, the one size scale. [`winui`] resolves
//! the roles to WinUI brushes and to the theme resource keys of the real
//! controls; under Windows High Contrast it maps nothing, so the system
//! colors apply.

use std::sync::OnceLock;

use material_colors::{
    color::Rgb, hct::Hct, palette::TonalPalette, scheme::variant::SchemeTonalSpot,
};

/// M3 state-layer opacities of the content color over a container.
pub mod state {
    pub const HOVER: f64 = 0.08;
    pub const PRESSED: f64 = 0.10;
    /// A disabled container: on-surface at this opacity.
    pub const DISABLED_CONTAINER: f64 = 0.12;
    /// Disabled content: on-surface at this opacity.
    pub const DISABLED_CONTENT: f64 = 0.38;
}

/// Secondary text lowers emphasis with opacity on the theme text brush, so
/// High Contrast still resolves the system color.
pub const MUTED_OPACITY: f64 = 0.72;

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
    /// The tone of a controlled Agent's typed lifecycle state.
    pub fn of_state(state: eliot_core::AgentState) -> Self {
        match state {
            eliot_core::AgentState::Busy => Self::Active,
            eliot_core::AgentState::Idle => Self::Ready,
            eliot_core::AgentState::Waiting => Self::Attention,
            eliot_core::AgentState::Unknown => Self::Unknown,
            eliot_core::AgentState::Failed | eliot_core::AgentState::Lost => Self::Failed,
            eliot_core::AgentState::Created | eliot_core::AgentState::Starting => Self::Pending,
            eliot_core::AgentState::Stopped => Self::Neutral,
        }
    }

    /// The tone of native observed-child activity, which has a distinct state
    /// vocabulary from a controlled Agent lifecycle.
    pub fn of_activity_state(state: &str) -> Self {
        match state {
            "running" => Self::Active,
            "idle" => Self::Ready,
            "interrupted" => Self::Attention,
            "unknown" => Self::Unknown,
            "lost" => Self::Failed,
            _ => Self::Neutral,
        }
    }
}

#[cfg(windows)]
pub mod winui {
    //! M3 roles as WinUI brushes, and as theme resource overrides for real
    //! `Button`s, the one control that takes Reactor `ResourceOverrides` in
    //! 0.100.0. The color keys are WinUI's own (`microsoft-ui-xaml`
    //! `Button_themeresources.xaml`), so buttons keep their hover, pressed,
    //! disabled and focus states. Shape and padding come from the button's
    //! style (`app_resources`, from `ui::tokens`): WinUI reads a Reactor
    //! `Thickness` or `CornerRadius` override as zero (observed live). One
    //! action pattern: primary filled, secondary outlined, tertiary text,
    //! destructive text in the error color. Under High Contrast nothing is
    //! overridden and every role resolves to a live system brush.
    //!
    //! Surfaces are neutral; the primary color marks only the main action
    //! (filled) and the selection (secondary-container). Status colors
    //! appear only as small dots, status chips and on a pending request.

    use windows_reactor::{Brush, Color, ResourceOverrides, ThemeBrush};

    use super::{Rgb8, Scheme, StatusTone, state};

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

        /// The sidebar and the right panel: one tonal step off
        /// the surface (surface-container-low).
        pub fn sidebar(self) -> Brush {
            self.role(|s| s.surface_container_low, ThemeBrush::SolidBackground)
        }

        /// Grouped content on the surface (settings lists, notices):
        /// surface-container-low.
        pub fn card(self) -> Brush {
            self.role(|s| s.surface_container_low, ThemeBrush::CardBackground)
        }

        /// Cards on the sidebar-toned panel (monitor, details):
        /// surface-container-high.
        pub fn card_high(self) -> Brush {
            self.role(|s| s.surface_container_high, ThemeBrush::CardBackground)
        }

        /// The composer (an M3 search-bar container) and code blocks:
        /// surface-container-high.
        pub fn raised(self) -> Brush {
            self.role(|s| s.surface_container_high, ThemeBrush::CardBackground)
        }

        /// A plot well inside a card: surface-container-highest.
        pub fn well(self) -> Brush {
            self.role(|s| s.surface_container_highest, ThemeBrush::CardBackground)
        }

        /// The user's message bubble: secondary-container.
        pub fn bubble(self) -> Brush {
            self.role(|s| s.secondary_container, ThemeBrush::CardBackground)
        }

        pub fn on_bubble(self) -> Brush {
            self.role(|s| s.on_secondary_container, ThemeBrush::PrimaryText)
        }

        pub fn on_surface(self) -> Brush {
            self.role(|s| s.on_surface, ThemeBrush::PrimaryText)
        }

        /// Hairlines and dividers.
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

        /// The M3 scrim behind a dialog: black at 32 %.
        pub fn scrim(self) -> Brush {
            Brush::Solid(Color::argb(82, 0, 0, 0))
        }

        /// A destructive confirmation's surface (the Force stop banner).
        pub fn error_container(self) -> Brush {
            self.role(|s| s.error_container, ThemeBrush::SystemCriticalBackground)
        }

        pub fn on_error_container(self) -> Brush {
            self.role(|s| s.on_error_container, ThemeBrush::PrimaryText)
        }

        /// The one warning text: the Full access level (owner review
        /// 2026-11-16: no orange or red except this warning and
        /// destructive actions).
        pub fn warning(self) -> Brush {
            self.role(|s| s.warning, ThemeBrush::SystemCritical)
        }

        /// The keyboard highlight of a popover row (secondary-container,
        /// the selection's role).
        pub fn highlight(self) -> Brush {
            self.role(|s| s.secondary_container, ThemeBrush::CardBackground)
        }

        /// A popover above the composer: surface-container, a hairline
        /// outline (the M3 menu container).
        pub fn popover(self) -> Brush {
            self.role(|s| s.surface_container, ThemeBrush::SolidBackground)
        }

        /// The status dot of a tone; `None` for tones that need no mark in
        /// a list (ready, ended), so a calm fleet shows no dots at all.
        /// Failed and unknown use the attention role: orange and red are
        /// kept for the access warning and destructive actions.
        pub fn dot(self, tone: StatusTone) -> Option<Brush> {
            let pick: fn(&Scheme) -> Rgb8 = match tone {
                StatusTone::Active => |s| s.primary,
                StatusTone::Attention | StatusTone::Failed | StatusTone::Unknown => |s| s.tertiary,
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

        /// Container and content of a status chip.
        pub fn status_chip(self, tone: StatusTone) -> (Brush, Brush) {
            match self.scheme {
                Some(scheme) => {
                    let (container, on) = scheme.status(tone);
                    (Brush::Solid(color(container)), Brush::Solid(color(on)))
                }
                None => (
                    Brush::Theme(ThemeBrush::CardBackground),
                    Brush::Theme(ThemeBrush::PrimaryText),
                ),
            }
        }

        /// Background and text of a pending request card.
        pub fn attention(self) -> (Brush, Brush) {
            self.status_chip(StatusTone::Attention)
        }

        /// Primary: filled, the one main action of a page (Save).
        pub fn filled(self) -> ResourceOverrides {
            self.with(|s| Family::new(Some(s.primary), s.on_primary).overrides(s))
        }

        /// Secondary: outlined (Validate, Reload, Duplicate, Add).
        pub fn outlined(self) -> ResourceOverrides {
            self.with(|s| {
                Family {
                    outline: Some(s.outline),
                    ..Family::new(None, s.primary)
                }
                .overrides(s)
            })
        }

        /// Tertiary: text (Discard, Edit as TOML, Refresh, Hide).
        pub fn text_button(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.primary).overrides(s))
        }

        /// Destructive: a text button in the error color.
        pub fn destructive(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.error).overrides(s))
        }

        /// A text button on an error container (the Force stop banner's
        /// Cancel): on-error-container.
        pub fn error_text_button(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.on_error_container).overrides(s))
        }

        /// A standard icon button: on-surface-variant, no container.
        pub fn icon_button(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.on_surface_variant).overrides(s))
        }

        /// A list or navigation row: the selection in secondary-container,
        /// on-surface otherwise (state layers on hover and press).
        pub fn row(self, selected: bool) -> ResourceOverrides {
            self.with(|s| {
                if selected {
                    Family::new(Some(s.secondary_container), s.on_secondary_container)
                } else {
                    Family::new(None, s.on_surface)
                }
                .overrides(s)
            })
        }

        /// The tonal row (New task): primary-container with
        /// on-primary-container, distinct from the selection's
        /// secondary-container.
        pub fn tonal_row(self) -> ResourceOverrides {
            self.with(|s| {
                Family::new(Some(s.primary_container), s.on_primary_container).overrides(s)
            })
        }

        /// A card that is one button (monitor cards): no fill of its own
        /// (the card's container shows through), on-surface state layers.
        pub fn card_button(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.on_surface).overrides(s))
        }

        /// An assist chip: outlined in outline-variant.
        /// A field that opens a list (the preset Model field): the
        /// outlined text field's outline and on-surface text, like the
        /// form's dropdowns.
        pub fn field_button(self) -> ResourceOverrides {
            self.with(|s| {
                Family {
                    outline: Some(s.outline),
                    ..Family::new(None, s.on_surface)
                }
                .overrides(s)
            })
        }

        pub fn chip(self) -> ResourceOverrides {
            self.with(|s| {
                Family {
                    outline: Some(s.outline_variant),
                    ..Family::new(None, s.on_surface_variant)
                }
                .overrides(s)
            })
        }

        /// The Full access row of the access menu and the new task's access
        /// chip at Full access: warning content, no outline.
        pub fn warning_row(self) -> ResourceOverrides {
            self.with(|s| Family::new(None, s.warning).overrides(s))
        }

        /// One segment of a segmented button: secondary-container when
        /// selected, on-surface otherwise, on the filled track.
        /// A segment: the selected one filled with primary, so it stands
        /// out on a highlighted picker row (whose fill is the selection's
        /// secondary-container).
        pub fn segment(self, selected: bool) -> ResourceOverrides {
            if selected {
                self.with(|s| Family::new(Some(s.primary), s.on_primary).overrides(s))
            } else {
                self.row(false)
            }
        }

        fn with(self, build: impl FnOnce(&Scheme) -> ResourceOverrides) -> ResourceOverrides {
            match self.scheme {
                Some(scheme) => build(scheme),
                None => ResourceOverrides::new(),
            }
        }
    }

    /// The key prefix of WinUI's default button template (every family's
    /// style reads these keys; the hand-off dialog's AccentButtonStyle
    /// button keeps the application's `AccentButton` theme keys).
    const BUTTON_KEYS: &str = "Button";

    /// One button family on WinUI's button keys: an optional container
    /// with M3 state layers of its content color `on`, an optional outline
    /// and the M3 disabled treatment (12% / 38% on-surface).
    struct Family {
        fill: Option<Rgb8>,
        on: Rgb8,
        outline: Option<Rgb8>,
    }

    impl Family {
        fn new(fill: Option<Rgb8>, on: Rgb8) -> Self {
            Self {
                fill,
                on,
                outline: None,
            }
        }

        fn overrides(&self, s: &Scheme) -> ResourceOverrides {
            let clear = Color::transparent();
            let on = self.on;
            let (rest, hover, pressed, disabled) = match self.fill {
                Some(fill) => (
                    color(fill),
                    color(fill.mix(on, state::HOVER)),
                    color(fill.mix(on, state::PRESSED)),
                    layer(s.on_surface, state::DISABLED_CONTAINER),
                ),
                None => (
                    clear,
                    layer(on, state::HOVER),
                    layer(on, state::PRESSED),
                    clear,
                ),
            };
            let border = self.outline.map_or(clear, color);
            let disabled_border = self
                .outline
                .map_or(clear, |_| layer(s.on_surface, state::DISABLED_CONTAINER));
            let key = |name: &str| format!("{BUTTON_KEYS}{name}");
            let mut r = ResourceOverrides::new()
                .set(key("Background"), rest)
                .set(key("BackgroundPointerOver"), hover)
                .set(key("BackgroundPressed"), pressed)
                .set(key("BackgroundDisabled"), disabled)
                .set(
                    key("ForegroundDisabled"),
                    layer(s.on_surface, state::DISABLED_CONTENT),
                )
                .set(key("BorderBrushDisabled"), disabled_border);
            for state in ["", "PointerOver", "Pressed"] {
                r = r
                    .set(key(&format!("Foreground{state}")), color(on))
                    .set(key(&format!("BorderBrush{state}")), border);
            }
            r
        }
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
                palette.outlined(),
                palette.text_button(),
                palette.destructive(),
                palette.icon_button(),
                palette.row(true),
                palette.card_button(),
                palette.chip(),
                palette.segment(true),
                palette.error_text_button(),
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
                assert_eq!(
                    palette.bubble(),
                    Brush::Solid(color(scheme.secondary_container))
                );
                assert_ne!(palette.filled(), ResourceOverrides::new());
                assert_ne!(palette.row(true), palette.row(false));
                assert_ne!(palette.destructive(), palette.text_button());
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
            (eliot_core::AgentState::Busy, StatusTone::Active),
            (eliot_core::AgentState::Idle, StatusTone::Ready),
            (eliot_core::AgentState::Waiting, StatusTone::Attention),
            (eliot_core::AgentState::Unknown, StatusTone::Unknown),
            (eliot_core::AgentState::Failed, StatusTone::Failed),
            (eliot_core::AgentState::Lost, StatusTone::Failed),
            (eliot_core::AgentState::Created, StatusTone::Pending),
            (eliot_core::AgentState::Starting, StatusTone::Pending),
            (eliot_core::AgentState::Stopped, StatusTone::Neutral),
        ];
        for (state, tone) in tones {
            assert_eq!(StatusTone::of_state(state), tone, "{state:?}");
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
