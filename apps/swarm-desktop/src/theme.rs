//! Color, elevation and state for the desktop: the brand book "Eliot
//! Swarm Material" (Material 3 Expressive, owner-approved 2026-11-24).
//!
//! A theme only swaps the color roles ([`Roles`]), which are the brand
//! book's tokens for that theme ([`crate::brand::SCHEMES`], generated from
//! `design/tokens.json`) under the names the views use. Every component
//! reads roles, never a raw color. Layout, density and motion never change
//! with the theme. [`winui`] resolves the roles to WinUI brushes and to
//! the theme resource keys of the real controls; under Windows High
//! Contrast it maps nothing, so the system colors apply.

pub use crate::tokens::state;

/// The design's elevation: level 1 (the main card, the composer, a
/// pressable under the pointer) and 2 (menus, popovers, the composer's
/// action under the pointer), each
/// Material Components' key and ambient shadow (offset, blur, spread, as
/// CSS `box-shadow`). WinUI's `ThemeShadow` draws nothing outside a popup
/// without receivers, so each shadow is drawn as the blur CSS renders: a
/// Gaussian falloff (σ = blur / 2) centred on the spread edge, in 1-DIP
/// bands that never overlap (a filled core, then rings), so a band's alpha
/// is exactly its share of the theme's opacity and the falloff reads as
/// one smooth shadow. The theme's [`super::Lift`] sets the opacity, or
/// replaces the shadow with a 1-DIP ring.
pub mod elevation {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Level {
        One,
        Two,
    }

    /// One shadow in DIPs, as CSS `box-shadow` takes it (no x offset).
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Shadow {
        pub offset: f64,
        pub blur: f64,
        pub spread: f64,
    }

    impl Shadow {
        const fn new(offset: f64, blur: f64, spread: f64) -> Self {
            Self {
                offset,
                blur,
                spread,
            }
        }
    }

    /// The key and the ambient shadow of `level` (material-web
    /// `elevation/internal/_elevation.scss`, the design's System board).
    pub const fn shadows(level: Level) -> [Shadow; 2] {
        match level {
            Level::One => [Shadow::new(1.0, 2.0, 0.0), Shadow::new(1.0, 3.0, 1.0)],
            Level::Two => [Shadow::new(1.0, 2.0, 0.0), Shadow::new(2.0, 6.0, 2.0)],
        }
    }

    /// The bands of `level`'s key and ambient shadows ([`bands`]), worked
    /// out once per level: the in-tree lifts draw them on every render.
    pub fn level_bands(level: Level) -> &'static [Vec<Band>; 2] {
        static CACHE: [std::sync::OnceLock<[Vec<Band>; 2]>; 2] =
            [const { std::sync::OnceLock::new() }; 2];
        let index = match level {
            Level::One => 0,
            Level::Two => 1,
        };
        CACHE[index].get_or_init(|| shadows(level).map(bands))
    }

    /// The night themes' glow around the FAB: a centred blur of `radius`.
    pub const fn glow(radius: f64) -> Shadow {
        Shadow::new(0.0, radius, 0.0)
    }

    /// One drawn band of a shadow: its outer edge `reach` DIPs past the
    /// element (negative inside it), its `width` (`None` for the filled
    /// core), how far it falls and its share (0–1) of the shadow's opacity.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Band {
        pub reach: f64,
        pub width: Option<f64>,
        pub offset: f64,
        pub share: f64,
    }

    /// The standard normal distribution function (Abramowitz and Stegun
    /// 7.1.26, absolute error below 1.5e-7).
    fn phi(x: f64) -> f64 {
        let z = x.abs() / std::f64::consts::SQRT_2;
        let t = 1.0 / (1.0 + 0.327_591_1 * z);
        let poly = t
            * (0.254_829_592
                + t * (-0.284_496_736
                    + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
        let erf = 1.0 - poly * (-z * z).exp();
        0.5 * (1.0 + if x < 0.0 { -erf } else { erf })
    }

    /// `shadow` as 1-DIP bands: the core is filled from where the shadow
    /// first shows past the element (its bottom edge, where the drop
    /// uncovers it), then a ring per DIP out to the end of the blur. Each
    /// band carries the falloff at its middle.
    pub fn bands(shadow: Shadow) -> Vec<Band> {
        let Shadow {
            offset,
            blur,
            spread,
        } = shadow;
        let band = |edge: f64, width: Option<f64>| Band {
            reach: spread + edge,
            width,
            offset,
            share: if blur > 0.0 {
                1.0 - phi((edge - 0.5) / (blur / 2.0))
            } else {
                1.0
            },
        };
        let end = blur.ceil();
        // Bands wholly under the element never show: start at the first
        // edge past the drop (the element covers the shadow up to
        // `spread + offset` inside its bottom edge).
        let core = (-end).max((-(spread + offset)).floor() + 1.0).min(end);
        let mut out = vec![band(core, None)];
        let mut edge = core + 1.0;
        while edge <= end {
            out.push(band(edge, Some(1.0)));
            edge += 1.0;
        }
        out
    }
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

    const fn hex(value: u32) -> Self {
        Self::new((value >> 16) as u8, (value >> 8) as u8, value as u8)
    }

    /// `over` composited onto `self` at `amount` (a state layer, a tint).
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

const BLACK: Rgb8 = Rgb8::new(0, 0, 0);
const WHITE: Rgb8 = Rgb8::new(255, 255, 255);
pub const SHADOW_COLOR: Rgb8 = BLACK;

/// The six themes of the design's Themes board.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Theme {
    Light,
    Dark,
    Amoled,
    Purple,
    Neon,
    Matrix,
}

impl Theme {
    pub const ALL: [Theme; 6] = [
        Theme::Light,
        Theme::Dark,
        Theme::Amoled,
        Theme::Purple,
        Theme::Neon,
        Theme::Matrix,
    ];

    /// The theme's roles, built once per process.
    pub fn roles(self) -> &'static Roles {
        use std::sync::OnceLock;
        static ROLES: OnceLock<[Roles; 6]> = OnceLock::new();
        let all = ROLES.get_or_init(|| Theme::ALL.map(Roles::new));
        &all[Theme::ALL
            .iter()
            .position(|theme| *theme == self)
            .unwrap_or_default()]
    }

    /// WinUI's own templates and the title bar follow this polarity.
    pub fn dark(self) -> bool {
        !matches!(self, Theme::Light | Theme::Purple)
    }
}

/// How raised things lift off their surface: Material's shadows at a key
/// and an ambient opacity, or (true-black themes) a hairline ring.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Lift {
    Shadow { key: f64, ambient: f64 },
    Ring(Rgb8),
}

/// A glow around the FAB (the night themes), drawn like a blur: `color`
/// at `alpha` reaching `radius` DIPs past the circle.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Glow {
    pub color: Rgb8,
    pub alpha: f64,
    pub radius: f64,
}

/// One theme's roles: the brand book's color tokens ([`crate::brand::Scheme`])
/// under the names the views use, and how raised things lift.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Roles {
    pub theme: Theme,
    pub dark: bool,
    /// Every color token of the theme, as the brand book lists them.
    pub scheme: crate::brand::Scheme,
    /// The main card: conversation, pages, Settings (`surface`).
    pub surface: Rgb8,
    /// The window ground and the sidebar (`surface-container`).
    pub sidebar: Rgb8,
    /// Menus and popovers (`surface-container`, with `overlay_border` in
    /// dark themes).
    pub menu: Rgb8,
    /// Dialogs, right-pane cards, segmented tracks, filled fields, plot
    /// wells (`surface-container-high`).
    pub container: Rgb8,
    /// The composer card, code blocks, tool rows and suggestion cards
    /// (`surface-container-low`).
    pub composer: Rgb8,
    /// Selection: a selected row, chip or toggled icon button, tonal
    /// buttons, the user's message bubble (`secondary-container`).
    pub selected: Rgb8,
    /// Content on the selection (`on-secondary-container`).
    pub on_selected: Rgb8,
    pub primary: Rgb8,
    /// Primary as text: text buttons, links, a selected icon.
    pub primary_text: Rgb8,
    pub on_primary: Rgb8,
    /// The New task button (`primary-container`, `on-primary-container`).
    pub primary_container: Rgb8,
    pub on_primary_container: Rgb8,
    /// Primary text (`on-surface`).
    pub text: Rgb8,
    /// Secondary text, captions, inactive icons (`on-surface-variant`).
    pub muted: Rgb8,
    pub secondary_text: Rgb8,
    /// Chips, icon buttons, an ended task's title (`on-surface-variant`).
    pub variant: Rgb8,
    /// Hairlines and dividers (`outline-variant`).
    pub outline: Rgb8,
    /// A chip's or outlined button's outline (`outline-variant`).
    pub border: Rgb8,
    /// A field's outline and the idle dot (`outline`).
    pub field_outline: Rgb8,
    /// Waiting for you: the attention dot, the Inbox counter, the
    /// attention strip, Steer (`tertiary`, `tertiary-container`).
    pub waiting: Rgb8,
    pub waiting_container: Rgb8,
    pub on_waiting_container: Rgb8,
    /// Stop turn, danger, Failed, Lost, Unknown (`error`).
    pub attention: Rgb8,
    pub attention_text: Rgb8,
    pub attention_container: Rgb8,
    pub on_attention_container: Rgb8,
    pub lift: Lift,
    pub glow: Option<Glow>,
    /// A tool row or suggestion card (`surface-container-low`).
    pub tonal: Rgb8,
    /// A running tool row (`surface-container-high`).
    pub running: Rgb8,
    /// WinUI's accent ramp (`SystemAccentColorDark3`…`Light3`). WinUI fills
    /// with Dark1 in Light and Light2 in Dark, which are the primary.
    pub accent_ramp: [Rgb8; 6],
}

/// `0xAARRGGBB` without its alpha.
const fn rgb(argb: u32) -> Rgb8 {
    Rgb8::hex(argb & 0x00ff_ffff)
}

/// The alpha of `0xAARRGGBB`, 0–1.
fn alpha(argb: u32) -> f64 {
    f64::from(argb >> 24) / 255.0
}

impl Roles {
    fn new(theme: Theme) -> Self {
        let index = Theme::ALL
            .iter()
            .position(|t| *t == theme)
            .unwrap_or_default();
        let s = crate::brand::SCHEMES[index];
        let primary = rgb(s.primary);
        let dark = theme.dark();
        // In-tree lifts draw `elevation-1`: a hairline ring in the
        // true-black themes, else its key and ambient shadow opacities.
        let lift = match crate::brand::ELEVATION_1[index] {
            [ring] if ring.blur == 0.0 => Lift::Ring(rgb(ring.color)),
            [key, ambient, ..] => Lift::Shadow {
                key: alpha(key.color),
                ambient: alpha(ambient.color),
            },
            _ => Lift::Ring(rgb(s.outline_variant)),
        };
        let glow = crate::brand::GLOW_SHADOW[index].first().map(|shadow| Glow {
            color: rgb(shadow.color),
            alpha: alpha(shadow.color),
            radius: shadow.blur,
        });
        let accent_ramp = if dark {
            [
                primary.mix(BLACK, 0.8),
                primary.mix(BLACK, 0.6),
                primary.mix(BLACK, 0.4),
                primary.mix(BLACK, 0.2),
                primary,
                primary.mix(WHITE, 0.3),
            ]
        } else {
            [
                primary.mix(BLACK, 0.4),
                primary.mix(BLACK, 0.2),
                primary,
                primary.mix(WHITE, 0.2),
                primary.mix(WHITE, 0.4),
                primary.mix(WHITE, 0.6),
            ]
        };
        Self {
            theme,
            dark,
            scheme: s,
            surface: rgb(s.surface),
            sidebar: rgb(s.surface_container),
            menu: rgb(s.surface_container),
            container: rgb(s.surface_container_high),
            composer: rgb(s.surface_container_low),
            selected: rgb(s.secondary_container),
            on_selected: rgb(s.on_secondary_container),
            primary,
            primary_text: rgb(s.primary_text),
            on_primary: rgb(s.on_primary),
            primary_container: rgb(s.primary_container),
            on_primary_container: rgb(s.on_primary_container),
            text: rgb(s.on_surface),
            muted: rgb(s.on_surface_variant),
            secondary_text: rgb(s.on_surface_variant),
            variant: rgb(s.on_surface_variant),
            outline: rgb(s.outline_variant),
            border: rgb(s.outline_variant),
            field_outline: rgb(s.outline),
            waiting: rgb(s.tertiary),
            waiting_container: rgb(s.tertiary_container),
            on_waiting_container: rgb(s.on_tertiary_container),
            attention: rgb(s.error),
            attention_text: rgb(s.error),
            attention_container: rgb(s.error_container),
            on_attention_container: rgb(s.on_error_container),
            lift,
            glow,
            tonal: rgb(s.surface_container_low),
            running: rgb(s.surface_container_high),
            accent_ramp,
        }
    }

    /// Container and content of a status chip.
    pub fn status(&self, tone: StatusTone) -> (Rgb8, Rgb8) {
        match tone {
            StatusTone::Active | StatusTone::Pending => (self.selected, self.on_selected),
            StatusTone::Attention => (self.waiting_container, self.on_waiting_container),
            StatusTone::Failed | StatusTone::Unknown => {
                (self.attention_container, self.on_attention_container)
            }
            StatusTone::Ready | StatusTone::Neutral => (self.container, self.variant),
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
}

#[cfg(windows)]
/// A quota window with less than this share left (percent) reads in the
/// attention color.
pub const QUOTA_LOW: f64 = 20.0;

#[cfg(windows)]
pub mod winui {
    //! The roles as WinUI brushes, and as theme resource overrides for real
    //! `Button`s, the one control that takes Reactor `ResourceOverrides` in
    //! 0.100.0. The color keys are WinUI's own (`Button_themeresources`),
    //! which the desktop's Material template reads (`app_resources`):
    //! the container takes `ButtonBackground`, the state layer and ripple
    //! the foreground. Shape and padding come from the button's style.
    //! Under High Contrast nothing is overridden and every role resolves to
    //! a live system brush.

    use windows_reactor::{Brush, Color, ResourceOverrides, ThemeBrush};

    use super::{Rgb8, Roles, StatusTone, Theme, state};

    pub fn color(value: Rgb8) -> Color {
        Color::rgb(value.r, value.g, value.b)
    }

    /// `value` at `alpha` (0–1): a state layer over whatever is beneath.
    fn layer(value: Rgb8, alpha: f64) -> Color {
        Color::argb((alpha * 255.0).round() as u8, value.r, value.g, value.b)
    }

    /// The active palette: the theme's roles, or the system colors under
    /// High Contrast. Cheap to copy into every view function.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Palette {
        roles: Option<&'static Roles>,
    }

    impl Palette {
        pub fn new(theme: Theme, high_contrast: bool) -> Self {
            Self {
                roles: (!high_contrast).then(|| theme.roles()),
            }
        }

        pub fn roles(self) -> Option<&'static Roles> {
            self.roles
        }

        fn role(self, pick: fn(&Roles) -> Rgb8, system: ThemeBrush) -> Brush {
            match self.roles {
                Some(roles) => Brush::Solid(color(pick(roles))),
                None => Brush::Theme(system),
            }
        }

        /// The conversation, Settings and pane background.
        pub fn surface(self) -> Brush {
            self.role(|r| r.surface, ThemeBrush::SolidBackground)
        }

        /// The sidebar.
        pub fn sidebar(self) -> Brush {
            self.role(|r| r.sidebar, ThemeBrush::SolidBackground)
        }

        /// A tool row, suggestion card, search field or settings card.
        pub fn tonal(self) -> Brush {
            self.role(|r| r.tonal, ThemeBrush::CardBackground)
        }

        /// The container role: the user's bubble, a plot well, a track.
        pub fn container(self) -> Brush {
            self.role(|r| r.container, ThemeBrush::CardBackground)
        }

        /// The composer card, code blocks, tool rows.
        pub fn composer(self) -> Brush {
            self.role(|r| r.composer, ThemeBrush::SolidBackground)
        }

        /// Menus and popovers.
        pub fn menu(self) -> Brush {
            self.role(|r| r.menu, ThemeBrush::SolidBackground)
        }

        /// The selection: a selected row, segment or pane tab, a status
        /// pill, a popover row under the keyboard.
        pub fn selected(self) -> Brush {
            self.role(|r| r.selected, ThemeBrush::CardBackground)
        }

        pub fn text(self) -> Brush {
            self.role(|r| r.text, ThemeBrush::PrimaryText)
        }

        /// Chips, icon buttons, an ended task's title.
        pub fn variant(self) -> Brush {
            self.role(|r| r.variant, ThemeBrush::PrimaryText)
        }

        /// Hints, counts, section labels.
        pub fn muted(self) -> Brush {
            self.role(|r| r.secondary_text, ThemeBrush::PrimaryText)
        }

        /// Hairlines and dividers.
        pub fn outline(self) -> Brush {
            self.role(|r| r.outline, ThemeBrush::CardStroke)
        }

        /// A chip's or field's outline.
        pub fn border(self) -> Brush {
            self.role(|r| r.border, ThemeBrush::CardStroke)
        }

        pub fn primary(self) -> Brush {
            self.role(|r| r.primary, ThemeBrush::AccentText)
        }

        /// Errors, failures, low quota, Full access.
        pub fn attention(self) -> Brush {
            self.role(|r| r.attention_text, ThemeBrush::SystemCritical)
        }

        /// An error or warning banner's surface.
        pub fn attention_container(self) -> Brush {
            self.role(
                |r| r.attention_container,
                ThemeBrush::SystemCriticalBackground,
            )
        }

        /// The status dot of a tone; `None` for tones that need no mark in
        /// a list (ready, ended), so a calm fleet shows no dots at all.
        pub fn dot(self, tone: StatusTone) -> Option<Brush> {
            let pick: fn(&Roles) -> Rgb8 = match tone {
                StatusTone::Active => |r| r.primary,
                StatusTone::Attention => |r| r.waiting,
                StatusTone::Failed | StatusTone::Unknown => |r| r.attention,
                StatusTone::Pending => |r| r.muted,
                StatusTone::Ready | StatusTone::Neutral => return None,
            };
            Some(self.role(pick, ThemeBrush::AccentText))
        }

        /// Container and content of a status chip.
        pub fn status_chip(self, tone: StatusTone) -> (Brush, Brush) {
            match self.roles {
                Some(r) => {
                    let (container, on) = r.status(tone);
                    (Brush::Solid(color(container)), Brush::Solid(color(on)))
                }
                None => (
                    Brush::Theme(ThemeBrush::CardBackground),
                    Brush::Theme(ThemeBrush::PrimaryText),
                ),
            }
        }

        /// Filled: the one main action of a page, the FAB.
        pub fn filled(self) -> ResourceOverrides {
            self.with(|r| Family::new(Some(r.primary), r.on_primary).overrides(r))
        }

        /// Outlined: in the primary color with the outline.
        pub fn outlined(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.muted),
                    ..Family::new(None, r.primary_text)
                }
                .overrides(r)
            })
        }

        /// Text: the primary color, no container.
        pub fn text_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.primary_text).overrides(r))
        }

        /// Destructive: a text button in the attention color.
        pub fn destructive(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.attention_text).overrides(r))
        }

        /// An icon button: the variant text color, no container.
        pub fn icon_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.variant).overrides(r))
        }

        /// A nav, project or task row: the selection in the selected role,
        /// the text color otherwise.
        pub fn row(self, selected: bool) -> ResourceOverrides {
            self.with(|r| {
                if selected {
                    Family::new(Some(r.selected), r.on_selected)
                } else {
                    Family::new(None, r.text)
                }
                .overrides(r)
            })
        }

        /// A destructive menu action, including the keyboard selection.
        pub fn destructive_row(self, selected: bool) -> ResourceOverrides {
            self.with(|r| {
                Family::new(selected.then_some(r.selected), r.attention_text).overrides(r)
            })
        }

        /// A chip: the variant text in an outline of the border tone.
        pub fn chip(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.border),
                    ..Family::new(None, r.variant)
                }
                .overrides(r)
            })
        }

        /// The outline of a chip in the attention tone (Full access).
        pub fn attention_outline(self) -> Brush {
            self.role(
                |r| r.attention.mix(r.surface, 0.55),
                ThemeBrush::SystemCritical,
            )
        }

        /// A chip that switches something on (New worktree): the chip's
        /// outline at rest, the selected role while on.
        pub fn toggle_chip(self, on: bool) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.border),
                    ..if on {
                        Family::new(Some(r.selected), r.on_selected)
                    } else {
                        Family::new(None, r.variant)
                    }
                }
                .overrides(r)
            })
        }

        /// A steered message: the selected role at half strength on the
        /// surface, so it never reads as an input field.
        pub fn steer(self) -> Brush {
            self.role(
                |r| r.surface.mix(r.selected, 0.5),
                ThemeBrush::CardBackground,
            )
        }

        /// A chip in the attention color (Full access in the composer).
        pub fn attention_chip(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.attention.mix(r.surface, 0.55)),
                    ..Family::new(None, r.attention_text)
                }
                .overrides(r)
            })
        }

        /// One segment of a segmented button or a pane tab: the selection
        /// in the selected role.
        pub fn segment(self, selected: bool) -> ResourceOverrides {
            self.with(|r| {
                if selected {
                    Family::new(Some(r.selected), r.on_selected)
                } else {
                    Family::new(None, r.variant)
                }
                .overrides(r)
            })
        }

        /// A field that opens a list (the preset Model field): the chip's
        /// outline with the text color.
        pub fn field_button(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.border),
                    ..Family::new(None, r.text)
                }
                .overrides(r)
            })
        }

        /// A suggestion card on the home screen: the tonal tone, its icon
        /// (the content color) in the primary role.
        pub fn suggestion(self) -> ResourceOverrides {
            self.with(|r| Family::new(Some(r.tonal), r.primary).overrides(r))
        }

        /// A tool row: tonal, or the running tint while it runs.
        pub fn tool_row(self, running: bool) -> ResourceOverrides {
            self.with(|r| {
                if running {
                    Family::new(Some(r.running), r.text)
                } else {
                    Family::new(Some(r.tonal), r.variant)
                }
                .overrides(r)
            })
        }

        /// The composer's Stop turn: `error` with `on-error`.
        pub fn stop_fab(self) -> ResourceOverrides {
            self.with(|r| {
                Family::new(
                    Some(super::rgb(r.scheme.error)),
                    super::rgb(r.scheme.on_error),
                )
                .overrides(r)
            })
        }

        /// The composer's Steer: `tertiary` with `on-tertiary`.
        pub fn steer_fab(self) -> ResourceOverrides {
            self.with(|r| {
                Family::new(
                    Some(super::rgb(r.scheme.tertiary)),
                    super::rgb(r.scheme.on_tertiary),
                )
                .overrides(r)
            })
        }

        /// Content on `error`: Stop's square.
        pub fn on_error(self) -> Brush {
            self.role(|r| super::rgb(r.scheme.on_error), ThemeBrush::PrimaryText)
        }

        /// The New task button: `primary-container` with
        /// `on-primary-container`.
        pub fn new_task(self) -> ResourceOverrides {
            self.with(|r| {
                Family::new(Some(r.primary_container), r.on_primary_container).overrides(r)
            })
        }

        /// A menu's or popover's 1-DIP edge: `overlay-border` (dark themes
        /// only; transparent in light ones).
        pub fn overlay_border(self) -> Brush {
            match self.roles {
                Some(r) => {
                    let v = r.scheme.overlay_border;
                    Brush::Solid(Color::argb(
                        (v >> 24) as u8,
                        (v >> 16) as u8,
                        (v >> 8) as u8,
                        v as u8,
                    ))
                }
                None => Brush::Theme(ThemeBrush::CardStroke),
            }
        }

        /// Settings usage status for a reported percentage remaining.
        pub fn quota_status(self, remaining: i64) -> Brush {
            match remaining {
                ..20 => self.attention(),
                _ => self.primary(),
            }
        }

        /// The composer's usage ring: a text button whose `ProgressRing`
        /// takes its arc (primary; attention under 20 % left) and its
        /// track (the outline) from this button's own resources, which
        /// WinUI's ring template resolves before the application's.
        pub fn usage_ring(self, remaining: f64) -> ResourceOverrides {
            self.with(|r| {
                let arc = if remaining < super::QUOTA_LOW {
                    r.attention
                } else {
                    r.primary
                };
                Family::new(None, r.variant)
                    .overrides(r)
                    .set("ProgressRingForegroundThemeBrush", color(arc))
                    .set("ProgressRingBackgroundThemeBrush", color(r.outline))
            })
        }

        fn with(self, build: impl FnOnce(&Roles) -> ResourceOverrides) -> ResourceOverrides {
            match self.roles {
                Some(roles) => build(roles),
                None => ResourceOverrides::new(),
            }
        }
    }

    /// The key prefix of WinUI's default button template (every family's
    /// style reads these keys; the hand-off dialog's AccentButtonStyle
    /// button keeps the application's `AccentButton` theme keys).
    const BUTTON_KEYS: &str = "Button";

    /// A button family: its container (none: transparent), its content
    /// color (which the template's state layer and ripple also take) and an
    /// optional outline.
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

        fn overrides(&self, r: &Roles) -> ResourceOverrides {
            let clear = Color::transparent();
            let on = self.on;
            let (rest, hover, pressed, disabled) = match self.fill {
                Some(fill) => (
                    color(fill),
                    color(fill.mix(on, state::HOVER)),
                    color(fill.mix(on, state::PRESSED)),
                    layer(r.text, state::DISABLED_CONTAINER),
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
                .map_or(clear, |_| layer(r.text, state::DISABLED_CONTAINER));
            let key = |name: &str| format!("{BUTTON_KEYS}{name}");
            let mut overrides = ResourceOverrides::new()
                .set(key("Background"), rest)
                .set(key("BackgroundPointerOver"), hover)
                .set(key("BackgroundPressed"), pressed)
                .set(key("BackgroundDisabled"), disabled)
                .set(key("ForegroundDisabled"), color(r.secondary_text))
                .set(key("BorderBrushDisabled"), disabled_border);
            for state in ["", "PointerOver", "Pressed"] {
                overrides = overrides
                    .set(key(&format!("Foreground{state}")), color(on))
                    .set(key(&format!("BorderBrush{state}")), border);
            }
            overrides
        }
    }
}
