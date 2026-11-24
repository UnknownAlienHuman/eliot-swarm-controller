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

/// The state layers, for the WinUI resources and button families (Windows
/// only, like their users).
#[cfg(windows)]
pub use crate::tokens::state;

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

/// How raised things lift off their surface, as the brand book draws
/// `elevation-1` (the main card, the composer, a pressable under the
/// pointer): Material's key and ambient shadow (offset, blur, spread and
/// color as CSS `box-shadow`), drawn as real Composition shadows by
/// `crate::shadow`; or, in the true-black themes, a 1-DIP ring. Menus,
/// popovers and dialogs keep WinUI's own `ThemeShadow`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Lift {
    Shadow(&'static [crate::brand::Shadow]),
    Ring(Rgb8),
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
    /// The night themes' glow behind the composer's main action
    /// (`glow-shadow`); empty in the other themes.
    pub glow: &'static [crate::brand::Shadow],
    /// A tool row or suggestion card (`surface-container-low`).
    pub tonal: Rgb8,
    /// A running tool row (`surface-container-high`).
    pub running: Rgb8,
    /// WinUI's accent ramp (`SystemAccentColorDark3`…`Light3`). WinUI fills
    /// with Dark1 in Light and Light2 in Dark, which are the primary.
    pub accent_ramp: [Rgb8; 6],
}

/// A brand-book color token (`0xAARRGGBB`) without its alpha.
pub(crate) const fn rgb(argb: u32) -> Rgb8 {
    Rgb8::hex(argb & 0x00ff_ffff)
}

/// The alpha of a brand-book color token (`0xAARRGGBB`), 0–1.
pub(crate) fn alpha(argb: u32) -> f64 {
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
        // A true-black theme's level is one layer without blur or offset
        // (`0 0 0 1px`): the ring. Every other theme's is its shadows.
        let lift = match crate::brand::ELEVATION_1[index] {
            [ring] if ring.blur == 0.0 && ring.y == 0.0 => Lift::Ring(rgb(ring.color)),
            layers => Lift::Shadow(layers),
        };
        let glow = crate::brand::GLOW_SHADOW[index];
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

        /// A chip that takes no input (a fixed fact): disabled, drawn as a
        /// chip at rest, its outline and content in the variant tone (the
        /// attention tone for Full access).
        pub fn fixed_chip(self, attention: bool) -> ResourceOverrides {
            self.with(|r| {
                let (outline, on) = if attention {
                    (r.attention.mix(r.surface, 0.55), r.attention_text)
                } else {
                    (r.border, r.variant)
                };
                ResourceOverrides::new()
                    .set("ButtonBackgroundDisabled", Color::transparent())
                    .set("ButtonBorderBrushDisabled", color(outline))
                    .set("ButtonForegroundDisabled", color(on))
            })
        }

        /// The outline of a chip in the attention tone (Full access).
        pub fn attention_outline(self) -> Brush {
            self.role(
                |r| r.attention.mix(r.surface, 0.55),
                ThemeBrush::SystemCritical,
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

        /// A suggestion card on the Home screen (`.suggest`):
        /// `surface-container-low` with an `outline-variant` hairline, its
        /// icon and label in `on-surface`.
        pub fn suggestion(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.border),
                    ..Family::new(Some(r.tonal), r.text)
                }
                .overrides(r)
            })
        }

        /// A work-log step or head (`.step`): no container, the variant
        /// content; a running step on `secondary-container` at 60 %.
        pub fn step_row(self, running: bool) -> ResourceOverrides {
            self.with(|r| {
                Family::new(running.then(|| r.surface.mix(r.selected, 0.6)), r.variant).overrides(r)
            })
        }

        /// A file chip in an answer (`.fchip`): `surface-container` with
        /// the text color.
        pub fn file_chip(self) -> ResourceOverrides {
            self.with(|r| Family::new(Some(r.sidebar), r.text).overrides(r))
        }

        /// A pane toggle in the header: pressed on `secondary-container`,
        /// otherwise an icon button's colors.
        pub fn toggle(self, on: bool) -> ResourceOverrides {
            self.with(|r| {
                if on {
                    Family::new(Some(r.selected), r.on_selected)
                } else {
                    Family::new(None, r.variant)
                }
                .overrides(r)
            })
        }

        /// An outlined labelled button (`.btn.out`: Changes +/−, Deny):
        /// the outline in `outline-variant`, the content on-surface-variant.
        pub fn outlined_plain(self) -> ResourceOverrides {
            self.with(|r| {
                Family {
                    outline: Some(r.border),
                    ..Family::new(None, r.variant)
                }
                .overrides(r)
            })
        }

        /// Tonal: `secondary-container` (Resume, Switch model, Hand off).
        pub fn tonal_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(Some(r.selected), r.on_selected).overrides(r))
        }

        /// A text button on the attention strip: `on-tertiary-container`.
        pub fn strip_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.on_waiting_container).overrides(r))
        }

        /// A text button on an error strip: `on-error-container`.
        pub fn error_strip_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.on_attention_container).overrides(r))
        }

        /// The attention strip (`tertiary-container`) and its content.
        pub fn strip(self) -> (Brush, Brush) {
            (
                self.token(|s| s.tertiary_container, ThemeBrush::Accent),
                self.token(|s| s.on_tertiary_container, ThemeBrush::PrimaryText),
            )
        }

        /// Waiting for you as text and outline (`tertiary`): the decision
        /// panel's kind line and its 2-DIP outline.
        pub fn tertiary(self) -> Brush {
            self.token(|s| s.tertiary, ThemeBrush::AccentText)
        }

        /// The code ground (`code`): a decision panel's subject.
        pub fn code(self) -> Brush {
            self.token(|s| s.code, ThemeBrush::CardBackground)
        }

        /// Primary as text: a status word, a link.
        pub fn primary_text(self) -> Brush {
            self.role(|r| r.primary_text, ThemeBrush::AccentText)
        }

        /// The composer's Resume: `secondary-container`.
        pub fn resume_fab(self) -> ResourceOverrides {
            self.tonal_button()
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

        /// Settings usage status for a reported percentage remaining.
        pub fn quota_status(self, remaining: i64) -> Brush {
            match remaining {
                ..20 => self.attention(),
                _ => self.primary(),
            }
        }

        /// The composer's context ring (`.ring`): its arc in `primary`, its
        /// track in `surface-container-highest`, which WinUI's ring template
        /// reads from the button's own resources.
        pub fn context_ring(self) -> ResourceOverrides {
            self.with(|r| {
                Family::new(None, r.variant)
                    .overrides(r)
                    .set("ProgressRingForegroundThemeBrush", color(r.primary))
                    .set(
                        "ProgressRingBackgroundThemeBrush",
                        color(super::rgb(r.scheme.surface_container_highest)),
                    )
            })
        }

        /// A brand-book color token by `pick`, or `system` under High
        /// Contrast.
        fn token(self, pick: fn(&crate::brand::Scheme) -> u32, system: ThemeBrush) -> Brush {
            match self.roles {
                Some(roles) => Brush::Solid(color(super::rgb(pick(&roles.scheme)))),
                None => Brush::Theme(system),
            }
        }

        /// A diff's added lines (`add`); the deleted sign is `error`.
        pub fn add(self) -> Brush {
            self.token(|s| s.add, ThemeBrush::PrimaryText)
        }

        /// Waiting for you: a task's attention dot (`tertiary`).
        pub fn waiting(self) -> Brush {
            self.token(|s| s.tertiary, ThemeBrush::AccentText)
        }

        /// An idle task's dot (`outline`).
        pub fn idle_dot(self) -> Brush {
            self.token(|s| s.outline, ThemeBrush::CardStroke)
        }

        /// A counter's container and content (`tertiary-container`,
        /// `on-tertiary-container`): the Inbox count.
        pub fn badge(self) -> (Brush, Brush) {
            (
                self.token(|s| s.tertiary_container, ThemeBrush::Accent),
                self.token(|s| s.on_tertiary_container, ThemeBrush::PrimaryText),
            )
        }

        /// Content on the selection (`on-secondary-container`).
        pub fn on_selected(self) -> Brush {
            self.token(|s| s.on_secondary_container, ThemeBrush::PrimaryText)
        }

        /// A meter's track (`secondary-container`).
        pub fn meter_track(self) -> Brush {
            self.token(|s| s.secondary_container, ThemeBrush::CardBackground)
        }

        /// A shape or icon in the color `pick` names where it is not a
        /// pressable (the brand mark, a failed row's error icon): the
        /// content color of a disabled carrier button, on no container.
        /// Reactor 0.100.0 colors an icon only through the control around
        /// it.
        pub fn carrier(self, pick: fn(&crate::brand::Scheme) -> u32) -> ResourceOverrides {
            self.with(|r| {
                let clear = Color::transparent();
                ResourceOverrides::new()
                    .set("ButtonBackgroundDisabled", clear)
                    .set("ButtonBorderBrushDisabled", clear)
                    .set(
                        "ButtonForegroundDisabled",
                        color(super::rgb(pick(&r.scheme))),
                    )
            })
        }

        /// The sidebar's Usage row: the row's colors, and its ring's arc
        /// (`primary`; `error` when little is left) and track
        /// (`surface-container-highest`), which WinUI's ring template
        /// resolves from the button's own resources.
        pub fn usage_row(self, selected: bool, low: bool) -> ResourceOverrides {
            let arc = if low {
                |s: &crate::brand::Scheme| s.error
            } else {
                |s: &crate::brand::Scheme| s.primary
            };
            self.with(|r| {
                let row = if selected {
                    Family::new(Some(r.selected), r.on_selected)
                } else {
                    Family::new(None, r.text)
                };
                row.overrides(r)
                    .set(
                        "ProgressRingForegroundThemeBrush",
                        color(super::rgb(arc(&r.scheme))),
                    )
                    .set(
                        "ProgressRingBackgroundThemeBrush",
                        color(super::rgb(r.scheme.surface_container_highest)),
                    )
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
