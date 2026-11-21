//! Color, type, elevation and state for the desktop: the approved design
//! (owner, 2026-11-19), `System.dc.html` and `Themes.dc.html` of the
//! eliot-design board.
//!
//! A theme only swaps the palette roles ([`Roles`]): surface, sidebar,
//! container, composer, selected, primary, on-primary, text, muted,
//! outline, attention, and the lift and glow of raised things. Every
//! component reads roles, never a raw color; the few in-between tones the
//! design draws (a chip's outline, a tool row's tint, a selected row's
//! hover) are fixed mixes of two roles ([`Roles::new`]), so all six themes
//! get them. Layout, density and motion never change with the theme.
//! [`winui`] resolves the roles to WinUI brushes and to the theme
//! resource keys of the real controls; under Windows High Contrast it maps
//! nothing, so the system colors apply.

/// The design's type roles in DIPs: one family (Segoe UI Variable, the
/// design's fallback for Roboto Flex, which Windows does not ship).
pub mod typography {
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct TypeStyle {
        pub size: f64,
        pub weight: u16,
    }
    const fn style(size: f64, weight: u16) -> TypeStyle {
        TypeStyle { size, weight }
    }
    /// The home greeting and a Settings page title.
    pub const HERO: TypeStyle = style(28.0, 500);
    /// The app title in the sidebar.
    pub const TITLE: TypeStyle = style(16.0, 600);
    /// The conversation header, a card or section title.
    pub const HEADER: TypeStyle = style(15.0, 500);
    /// Body text and rows.
    pub const BODY: TypeStyle = style(14.0, 400);
    /// Button labels and a project's name.
    pub const BUTTON: TypeStyle = style(14.0, 500);
    /// Chips, tool rows and the Monitor's rows.
    pub const CHIP: TypeStyle = style(13.0, 400);
    /// Section labels ("Projects").
    pub const LABEL: TypeStyle = style(12.0, 500);
    /// Hints, trailing counts and ages ("Ctrl+N", "2h").
    pub const CAPTION: TypeStyle = style(12.0, 400);
}

/// The design's elevation: level 1 (a pressable under the pointer), 2
/// (menus, popovers, the FAB under the pointer) and 3 (the composer), each
/// Material Components' key and ambient shadow (offset, blur, spread)
/// drawn as three concentric translucent layers per blur, so a WinUI
/// element without `ThemeShadow` receivers still lifts. The theme's
/// [`super::Lift`] sets their opacity, or replaces them with a 1-DIP ring.
pub mod elevation {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Level {
        One,
        Two,
        Three,
    }

    /// One drawn layer: how far it spreads past the element, how far it
    /// falls below it (DIPs) and its alpha (0–255) over the lift color.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Layer {
        pub spread: f64,
        pub offset: f64,
        pub alpha: u8,
    }

    /// (offset, blur, spread) of the key and the ambient shadow.
    fn shadows(level: Level) -> [(f64, f64, f64); 2] {
        match level {
            Level::One => [(1.0, 2.0, 0.0), (1.0, 3.0, 1.0)],
            Level::Two => [(1.0, 2.0, 0.0), (2.0, 6.0, 2.0)],
            Level::Three => [(1.0, 3.0, 0.0), (4.0, 8.0, 3.0)],
        }
    }

    /// Steps that draw one blur.
    pub const STEPS: usize = 3;

    /// The geometry of `level`'s layers, key layers first, then ambient,
    /// with the share of each shadow's opacity a layer carries.
    pub fn geometry(level: Level) -> Vec<(f64, f64, bool)> {
        let mut out = Vec::new();
        for (index, (offset, blur, spread)) in shadows(level).into_iter().enumerate() {
            for step in 0..STEPS {
                // Half-DIP steps keep the layers on whole device pixels at
                // 200 % scale.
                let reach = (blur * step as f64 / STEPS as f64 * 2.0).round() / 2.0;
                out.push((spread + reach, offset, index == 0));
            }
        }
        out
    }

    /// The layers of `level` under `lift`.
    pub fn layers(level: Level, lift: super::Lift) -> Vec<Layer> {
        let super::Lift::Shadow { key, ambient } = lift else {
            // A ring theme: one hairline around the element, no blur.
            return vec![Layer {
                spread: 1.0,
                offset: 0.0,
                alpha: 255,
            }];
        };
        geometry(level)
            .into_iter()
            .map(|(spread, offset, is_key)| Layer {
                spread,
                offset,
                alpha: ((if is_key { key } else { ambient }) / STEPS as f64 * 255.0).round() as u8,
            })
            .collect()
    }
}

/// State layers of the content color over a container (the design: hover
/// 8 %, pressed 12 %).
pub mod state {
    pub const HOVER: f64 = 0.08;
    pub const PRESSED: f64 = 0.12;
    /// The ripple of a press at its start, fading on release.
    pub const RIPPLE: f64 = 0.12;
    /// A disabled container: text at this opacity.
    pub const DISABLED_CONTAINER: f64 = 0.12;
    /// Disabled content: text at this opacity.
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

/// One theme's palette roles, exactly as the Themes board lists them, and
/// the tones the design draws between two roles.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Roles {
    pub theme: Theme,
    pub dark: bool,
    pub surface: Rgb8,
    pub sidebar: Rgb8,
    pub container: Rgb8,
    pub composer: Rgb8,
    pub selected: Rgb8,
    pub primary: Rgb8,
    pub on_primary: Rgb8,
    pub text: Rgb8,
    pub muted: Rgb8,
    pub outline: Rgb8,
    pub attention: Rgb8,
    pub lift: Lift,
    pub glow: Option<Glow>,
    /// Text between `text` and `muted`: chips, icon buttons, an ended
    /// task's title (the design's #46464F in Light).
    pub variant: Rgb8,
    /// A chip's, segment's and field's outline (#C6C5D0 in Light).
    pub border: Rgb8,
    /// A tool row, suggestion card or search field: halfway from surface to
    /// container (#F0EFF7 in Light).
    pub tonal: Rgb8,
    /// A selected row under the pointer (#D4D6FB in Light).
    pub selected_hover: Rgb8,
    /// A running tool row (#E8E9FF in Light).
    pub running: Rgb8,
    /// Placeholder text (#8F909A in Light).
    pub placeholder: Rgb8,
    /// The surface of an error or warning banner.
    pub attention_container: Rgb8,
    /// WinUI's accent ramp (`SystemAccentColorDark3`…`Light3`). WinUI fills
    /// with Dark1 in Light and Light2 in Dark, which are the primary.
    pub accent_ramp: [Rgb8; 6],
}

impl Roles {
    fn new(theme: Theme) -> Self {
        let soft = Lift::Shadow {
            key: 0.30,
            ambient: 0.15,
        };
        let deep = Lift::Shadow {
            key: 0.60,
            ambient: 0.40,
        };
        // surface, sidebar, container, composer, selected, primary,
        // on-primary, text, muted, outline, attention.
        let (c, lift, glow): ([u32; 11], Lift, Option<Glow>) = match theme {
            Theme::Light => (
                [
                    0xfbf8ff, 0xf5f2fa, 0xe9e8f1, 0xffffff, 0xdfe0ff, 0x4c58b0, 0xffffff, 0x1b1b21,
                    0x767680, 0xe4e1ec, 0xb3261e,
                ],
                soft,
                None,
            ),
            Theme::Dark => (
                [
                    0x131318, 0x1b1b21, 0x2a292f, 0x1f1f25, 0x353a66, 0xbac3ff, 0x1a2478, 0xe4e1e9,
                    0xc6c5d0, 0x35343b, 0xffb4ab,
                ],
                deep,
                None,
            ),
            Theme::Amoled => (
                [
                    0x000000, 0x0a0a0c, 0x16161a, 0x0f0f12, 0x23253f, 0x9fa8ff, 0x0b1240, 0xf2f0f7,
                    0xa3a2ad, 0x1f1f25, 0xff8a80,
                ],
                Lift::Ring(Rgb8::hex(0x1f1f25)),
                None,
            ),
            Theme::Purple => (
                [
                    0xfef7ff, 0xf7f0fb, 0xece6f0, 0xffffff, 0xe8def8, 0x6750a4, 0xffffff, 0x1d1b20,
                    0x79747e, 0xe7e0ec, 0xb3261e,
                ],
                soft,
                None,
            ),
            Theme::Neon => (
                [
                    0x0b0d17, 0x10131f, 0x1a1e30, 0x141828, 0x1f2b45, 0x00e5ff, 0x00141a, 0xe6f7ff,
                    0x8aa0b8, 0x24304a, 0xff4d8d,
                ],
                deep,
                Some(Glow {
                    color: Rgb8::hex(0x00e5ff),
                    alpha: 0.55,
                    radius: 12.0,
                }),
            ),
            Theme::Matrix => (
                [
                    0x050805, 0x070b07, 0x0d150d, 0x0a110a, 0x10301a, 0x00ff66, 0x001a0a, 0xc8ffd8,
                    0x5fa874, 0x133a1f, 0xff5c5c,
                ],
                Lift::Ring(Rgb8::hex(0x133a1f)),
                Some(Glow {
                    color: Rgb8::hex(0x00ff66),
                    alpha: 0.45,
                    radius: 10.0,
                }),
            ),
        };
        let [
            surface,
            sidebar,
            container,
            composer,
            selected,
            primary,
            on_primary,
            text,
            muted,
            outline,
            attention,
        ] = c.map(Rgb8::hex);
        let dark = theme.dark();
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
            surface,
            sidebar,
            container,
            composer,
            selected,
            primary,
            on_primary,
            text,
            muted,
            outline,
            attention,
            lift,
            glow,
            variant: text.mix(muted, 0.5),
            border: outline.mix(muted, 0.25),
            tonal: surface.mix(container, 0.5),
            selected_hover: selected.mix(primary, 0.08),
            running: surface.mix(selected, 0.6),
            placeholder: muted.mix(surface, 0.25),
            attention_container: surface.mix(attention, 0.12),
            accent_ramp,
        }
    }

    /// Container and content of a status chip.
    pub fn status(&self, tone: StatusTone) -> (Rgb8, Rgb8) {
        match tone {
            StatusTone::Active | StatusTone::Pending => (self.selected, self.text),
            StatusTone::Attention | StatusTone::Failed | StatusTone::Unknown => {
                (self.attention_container, self.attention)
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

        /// The composer, menus and popovers.
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
            self.role(|r| r.muted, ThemeBrush::PrimaryText)
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

        /// Content on primary: the FAB's glyph and Stop square.
        pub fn on_primary(self) -> Brush {
            self.role(|r| r.on_primary, ThemeBrush::PrimaryText)
        }

        /// Errors, failures, low quota, Full access.
        pub fn attention(self) -> Brush {
            self.role(|r| r.attention, ThemeBrush::SystemCritical)
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
                StatusTone::Attention | StatusTone::Failed | StatusTone::Unknown => |r| r.attention,
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
                    ..Family::new(None, r.primary)
                }
                .overrides(r)
            })
        }

        /// Text: the primary color, no container.
        pub fn text_button(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.primary).overrides(r))
        }

        /// Destructive: a text button in the attention color.
        pub fn destructive(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.attention).overrides(r))
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
                    Family::new(Some(r.selected), r.text)
                } else {
                    Family::new(None, r.text)
                }
                .overrides(r)
            })
        }

        /// A row in the attention color (Full access in a menu).
        pub fn attention_row(self) -> ResourceOverrides {
            self.with(|r| Family::new(None, r.attention).overrides(r))
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
                        Family::new(Some(r.selected), r.text)
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
                    ..Family::new(None, r.attention)
                }
                .overrides(r)
            })
        }

        /// One segment of a segmented button or a pane tab: the selection
        /// in the selected role.
        pub fn segment(self, selected: bool) -> ResourceOverrides {
            self.with(|r| {
                if selected {
                    Family::new(Some(r.selected), r.text)
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
                .set(
                    key("ForegroundDisabled"),
                    layer(r.text, state::DISABLED_CONTENT),
                )
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
