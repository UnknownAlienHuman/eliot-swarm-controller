//! The design's tactile control templates (XAML), with Uno.Themes'
//! Material button styles and Material Components' state layers as the
//! pattern, and their motion sampled from the brand book's springs (its
//! WinUI page, workaround 3).
//!
//! - **Button.** The container takes the button's own background, border
//!   and corner; a state layer in the content color shows on hover (8 %)
//!   and press (10 %); a ripple in the content color grows on press from
//!   its `RenderTransformOrigin`, which `crate::motion` moves to the press
//!   point, and fades on release. While pressed the button scales to 0.97
//!   (0.94 for the composer's action) and a round or square button's
//!   corner goes to `radius-small`. Card pressables lift to `elevation-1`
//!   under the pointer.
//! - **Switch.** Google's 52 × 32 switch on WinUI's `ToggleSwitch` parts:
//!   the knob travels and grows from 16 to 24 when it turns on, grows to 28
//!   while pressed, and shows its state layer under the pointer.
//! - **Progress bar.** The running turn's wave: a `Path` in the bar's
//!   foreground over the straight track, travelling one period per
//!   `wave-ms`; a determinate bar stays straight.
//!
//! **Motion.** Each visual state sets its resting values at once; the
//! movement between two states is a `VisualTransition` whose key frames
//! sample the brand book's spring (one key per 16-ms frame up to the
//! spring's settle time, starting from the state left): spatial-fast for
//! the press scale, the corner and the knob, effects-fast for state layers
//! and the lift. WinUI animates no `CornerRadius`, so the corner is one
//! `CornerRadius` object key per frame. When Windows turns animation
//! effects off, the templates carry no transitions and no wave travel:
//! every change is instant and the wave is still.
//!
//! The colors stay the lightweight-styling keys WinUI's own templates read
//! (`ButtonBackground`, `ToggleSwitchFillOn`… and their Disabled variants),
//! so the button families' per-button overrides (`theme::winui::Palette`)
//! and the scheme dictionary color them. Values are fixed numbers and
//! identifiers, so nothing here needs escaping.

use std::fmt::Write as _;

use crate::theme::state;
use crate::ui::tokens::motion::{
    DECELERATE, EFFECTS_FAST, EMPHASIZED, RIPPLE_MS, SPATIAL_FAST, Spring, WAVE_MS,
};
use crate::ui::tokens::{icon, layout, radius, switch, wave};

/// How a button lifts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lift {
    /// Flat: rows, chips, segments, labelled and icon buttons, the
    /// composer's action (whose night glow is a shadow in the composer).
    Flat,
    /// Flat at rest, `elevation-1` under the pointer: suggestion cards,
    /// tool rows.
    Hover,
}

/// What differs between the desktop's button styles.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Look {
    pub lift: Lift,
    /// The press scale.
    pub press: f64,
    /// The corner at rest (the style's own).
    pub radius: f64,
    /// Whether the corner goes to `radius-small` while pressed (every
    /// button and icon button; not the cards).
    pub morph: bool,
}

/// The theme values of a card's lift: whether WinUI's shadow shows (`1`,
/// the shadowed themes) or not (`0`), and the 1-DIP ring of the true-black
/// themes (transparent elsewhere). `app_resources` writes them for every
/// scheme, High Contrast included.
pub(super) const LIFT_SHADOW: &str = "EliotLiftShadow";
pub(super) const LIFT_RING: &str = "EliotLiftRing";

/// WinUI's depth for a card under the pointer: its `ThemeShadow` reaches
/// about as far as the brand book's `elevation-1` (a depth of 32 reached
/// about 10 DIPs, observed live).
const LIFT_DEPTH: f64 = 12.0;

/// One frame of a sampled spring.
const FRAME_MS: u64 = 16;

/// A key time in milliseconds, as XAML writes it.
fn key_time(ms: u64) -> String {
    format!("0:0:{}.{:03}", ms / 1000, ms % 1000)
}

/// `spring` moving a value from `from` to `to`, sampled once a frame up to
/// its settle time: `(milliseconds, value)`, the first `from`, the last
/// exactly `to`. The brand book's springs are underdamped (spatial) or
/// critically damped (effects); unit mass, so ω₀ = √stiffness.
fn sample(spring: Spring, from: f64, to: f64) -> Vec<(u64, f64)> {
    let omega = spring.stiffness.sqrt();
    let zeta = spring.damping;
    let at = |ms: u64| {
        let t = ms as f64 / 1000.0;
        let decay = (-zeta * omega * t).exp();
        let shape = if zeta < 1.0 {
            let damped = omega * (1.0 - zeta * zeta).sqrt();
            (damped * t).cos() + zeta * omega / damped * (damped * t).sin()
        } else {
            1.0 + omega * t
        };
        to + (from - to) * decay * shape
    };
    let mut out: Vec<(u64, f64)> = (0..spring.settle_ms.div_ceil(FRAME_MS))
        .map(|frame| (frame * FRAME_MS, at(frame * FRAME_MS)))
        .collect();
    out.push((spring.settle_ms, to));
    out
}

/// `property` of `target` through `samples`: the start at once, then
/// linear key frames.
fn frames(target: &str, property: &str, samples: &[(u64, f64)]) -> String {
    let mut out = format!(
        "<DoubleAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">"
    );
    for (index, (ms, value)) in samples.iter().enumerate() {
        let kind = if index == 0 { "Discrete" } else { "Linear" };
        let _ = write!(
            out,
            "<{kind}DoubleKeyFrame KeyTime=\"{}\" Value=\"{value:.4}\"/>",
            key_time(*ms)
        );
    }
    out + "</DoubleAnimationUsingKeyFrames>"
}

/// `property` of `target` from `from` to `to` on `spring`; nothing when it
/// does not move.
fn spring(target: &str, property: &str, spring: Spring, from: f64, to: f64) -> String {
    if from == to {
        return String::new();
    }
    frames(target, property, &sample(spring, from, to))
}

/// Both axes of a scale transform on `spring`.
fn scale_spring(target: &str, spring_: Spring, from: f64, to: f64) -> String {
    spring(target, "ScaleX", spring_, from, to) + &spring(target, "ScaleY", spring_, from, to)
}

/// A value set at once.
fn set(target: &str, property: &str, to: f64) -> String {
    format!(
        "<DoubleAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <DiscreteDoubleKeyFrame KeyTime=\"0\" Value=\"{to}\"/>\
         </DoubleAnimationUsingKeyFrames>"
    )
}

/// Both axes of a scale transform set at once.
fn scale_set(target: &str, to: f64) -> String {
    set(target, "ScaleX", to) + &set(target, "ScaleY", to)
}

/// `property` jumps to `from`, then eases to `to` over `ms`: one
/// animation, as WinUI refuses two on one property in a storyboard (a
/// fatal XAML error, observed live on the first press).
fn grow(target: &str, property: &str, from: f64, to: f64, ms: u64, spline: &str) -> String {
    format!(
        "<DoubleAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <DiscreteDoubleKeyFrame KeyTime=\"0\" Value=\"{from}\"/>\
           <SplineDoubleKeyFrame KeyTime=\"{}\" Value=\"{to}\" KeySpline=\"{spline}\"/>\
         </DoubleAnimationUsingKeyFrames>",
        key_time(ms)
    )
}

/// The corner of `target` through `samples`, one `CornerRadius` per frame
/// (WinUI interpolates no corner).
fn corner(target: &str, samples: &[(u64, f64)]) -> String {
    let mut out = format!(
        "<ObjectAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"CornerRadius\">"
    );
    for (ms, value) in samples {
        let _ = write!(
            out,
            "<DiscreteObjectKeyFrame KeyTime=\"{}\"><DiscreteObjectKeyFrame.Value>\
               <CornerRadius>{:.2}</CornerRadius>\
             </DiscreteObjectKeyFrame.Value></DiscreteObjectKeyFrame>",
            key_time(*ms),
            value.max(0.0)
        );
    }
    out + "</ObjectAnimationUsingKeyFrames>"
}

/// A property switched to a theme resource (the Disabled colors).
fn resource(target: &str, property: &str, key: &str) -> String {
    format!(
        "<ObjectAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <DiscreteObjectKeyFrame KeyTime=\"0\" Value=\"{{ThemeResource {key}}}\"/>\
         </ObjectAnimationUsingKeyFrames>"
    )
}

/// One transition between two states of a group.
fn transition(from: &str, to: &str, storyboard: &str) -> String {
    format!(
        "<VisualTransition From=\"{from}\" To=\"{to}\"><Storyboard>{storyboard}</Storyboard></VisualTransition>"
    )
}

/// A visual state group's transitions, or nothing.
fn transitions(all: &[String]) -> String {
    if all.is_empty() {
        String::new()
    } else {
        format!(
            "<VisualStateGroup.Transitions>{}</VisualStateGroup.Transitions>",
            all.concat()
        )
    }
}

/// The button's movements between Normal, PointerOver and Pressed.
fn button_transitions(look: Look) -> Vec<String> {
    let Look {
        lift,
        press,
        radius,
        morph,
    } = look;
    let (hover, pressed) = (state::HOVER, state::PRESSED);
    let layer = |from: f64, to: f64| spring("StateLayer", "Opacity", EFFECTS_FAST, from, to);
    let lifted = |from: f64, to: f64| match lift {
        Lift::Hover => spring("HoverShadow", "Opacity", EFFECTS_FAST, from, to),
        Lift::Flat => String::new(),
    };
    let shape = |from: f64, to: f64| {
        if morph {
            let samples = sample(SPATIAL_FAST, from, to);
            corner("Container", &samples) + &corner("Shape", &samples)
        } else {
            String::new()
        }
    };
    // The fill catches up with the growing ripple; both share the pressed
    // layer so their overlap cannot wash out the content.
    let ripple_in = grow("StateFill", "Opacity", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &set("Ripple", "Opacity", 1.0)
        + &grow("RippleScale", "ScaleX", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &grow("RippleScale", "ScaleY", 0.0, 1.0, RIPPLE_MS, DECELERATE);
    let ripple_out = set("StateFill", "Opacity", 1.0)
        + &grow("Ripple", "Opacity", 1.0, 0.0, RIPPLE_MS, EMPHASIZED);
    // A press sinks a lifted card back to its surface.
    let press_in = |layer_from: f64, lift_from: f64| {
        layer(layer_from, pressed)
            + &ripple_in
            + &scale_spring("Press", SPATIAL_FAST, 1.0, press)
            + &shape(radius, radius::PRESSED)
            + &lifted(lift_from, 0.0)
    };
    let press_out = |layer_to: f64, lift_to: f64| {
        layer(pressed, layer_to)
            + &ripple_out
            + &scale_spring("Press", SPATIAL_FAST, press, 1.0)
            + &shape(radius::PRESSED, radius)
            + &lifted(0.0, lift_to)
    };
    vec![
        transition(
            "Normal",
            "PointerOver",
            &(layer(0.0, hover) + &lifted(0.0, 1.0)),
        ),
        transition(
            "PointerOver",
            "Normal",
            &(layer(hover, 0.0) + &lifted(1.0, 0.0)),
        ),
        transition("Normal", "Pressed", &press_in(0.0, 0.0)),
        transition("PointerOver", "Pressed", &press_in(hover, 1.0)),
        transition("Pressed", "PointerOver", &press_out(hover, 1.0)),
        transition("Pressed", "Normal", &press_out(0.0, 0.0)),
    ]
}

/// The tactile button template; `motion` is Windows' animation effects.
pub(super) fn button(look: Look, motion: bool) -> String {
    let hover = look.lift == Lift::Hover;
    let lifted = |on: bool| {
        if hover {
            set("HoverShadow", "Opacity", f64::from(u8::from(on)))
        } else {
            String::new()
        }
    };
    // Each state's resting values, at once: the transitions move between
    // them. The corner rests at the button's own (TemplateBinding).
    let rest = |layer: f64, on: bool| {
        set("StateLayer", "Opacity", layer)
            + &set("StateFill", "Opacity", 1.0)
            + &set("Ripple", "Opacity", 0.0)
            + &scale_set("Press", 1.0)
            + &lifted(on)
    };
    let mut pressed = set("StateLayer", "Opacity", state::PRESSED)
        + &set("StateFill", "Opacity", 1.0)
        + &set("Ripple", "Opacity", f64::from(u8::from(motion)))
        + &scale_set("RippleScale", 1.0)
        + &scale_set("Press", look.press)
        + &lifted(false);
    if look.morph {
        let at_rest = [(0, radius::PRESSED)];
        pressed += &(corner("Container", &at_rest) + &corner("Shape", &at_rest));
    }
    let disabled = resource("Container", "Background", "ButtonBackgroundDisabled")
        + &resource("Container", "BorderBrush", "ButtonBorderBrushDisabled")
        + &resource("Content", "Foreground", "ButtonForegroundDisabled")
        + &set("StateLayer", "Opacity", 0.0)
        + &lifted(false);
    // A card's lift, shown under the pointer: a copy of its container at a
    // depth, casting WinUI's own shadow (a template holds no Composition
    // visual; WinUI draws a ThemeShadow in the tree without receivers,
    // observed live), or the true-black themes' ring.
    let layers = if hover {
        format!(
            "<Grid x:Name=\"HoverShadow\" Opacity=\"0\" IsHitTestVisible=\"False\">\
               <Border CornerRadius=\"{{TemplateBinding CornerRadius}}\" \
                 Background=\"{{TemplateBinding Background}}\" \
                 Opacity=\"{{ThemeResource {LIFT_SHADOW}}}\" Translation=\"0,0,{LIFT_DEPTH}\">\
                 <Border.Shadow><ThemeShadow/></Border.Shadow>\
               </Border>\
               <Border CornerRadius=\"{ring_radius}\" Margin=\"-{hairline}\" \
                 BorderThickness=\"{hairline}\" BorderBrush=\"{{ThemeResource {LIFT_RING}}}\"/>\
             </Grid>",
            ring_radius = look.radius + layout::HAIRLINE,
            hairline = layout::HAIRLINE,
        )
    } else {
        String::new()
    };
    let transitions = transitions(&if motion {
        button_transitions(look)
    } else {
        Vec::new()
    });
    format!(
        "<ControlTemplate TargetType=\"Button\">\
           <Grid x:Name=\"Root\" Background=\"Transparent\" RenderTransformOrigin=\"0.5,0.5\">\
             <Grid.RenderTransform><ScaleTransform x:Name=\"Press\"/></Grid.RenderTransform>\
             <VisualStateManager.VisualStateGroups>\
               <VisualStateGroup x:Name=\"CommonStates\">\
                 {transitions}\
                 <VisualState x:Name=\"Normal\"><Storyboard>{normal}</Storyboard></VisualState>\
                 <VisualState x:Name=\"PointerOver\"><Storyboard>{over}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Pressed\"><Storyboard>{pressed}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Disabled\"><Storyboard>{disabled}</Storyboard></VisualState>\
               </VisualStateGroup>\
             </VisualStateManager.VisualStateGroups>\
             {layers}\
             <Border x:Name=\"Container\" \
               Background=\"{{TemplateBinding Background}}\" \
               BorderBrush=\"{{TemplateBinding BorderBrush}}\" \
               BorderThickness=\"{{TemplateBinding BorderThickness}}\" \
               CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
               <Grid x:Name=\"Shape\" CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
                 <Grid x:Name=\"StateLayer\" Opacity=\"0\" IsHitTestVisible=\"False\">\
                   <Border x:Name=\"StateFill\" Background=\"{{TemplateBinding Foreground}}\"/>\
                   <Ellipse x:Name=\"Ripple\" Opacity=\"0\" Fill=\"{{TemplateBinding Foreground}}\" \
                     RenderTransformOrigin=\"0.5,0.5\" Stretch=\"Fill\" Margin=\"-{ripple_inset}\">\
                     <Ellipse.RenderTransform>\
                       <ScaleTransform x:Name=\"RippleScale\" ScaleX=\"0\" ScaleY=\"0\"/>\
                     </Ellipse.RenderTransform>\
                   </Ellipse>\
                 </Grid>\
                 <ContentPresenter x:Name=\"Content\" \
                   AutomationProperties.AccessibilityView=\"Raw\" \
                   Content=\"{{TemplateBinding Content}}\" \
                   ContentTemplate=\"{{TemplateBinding ContentTemplate}}\" \
                   ContentTransitions=\"{{TemplateBinding ContentTransitions}}\" \
                   Foreground=\"{{TemplateBinding Foreground}}\" \
                   FontFamily=\"{{TemplateBinding FontFamily}}\" \
                   FontSize=\"{{TemplateBinding FontSize}}\" \
                   FontWeight=\"{{TemplateBinding FontWeight}}\" \
                   Padding=\"{{TemplateBinding Padding}}\" \
                   HorizontalContentAlignment=\"{{TemplateBinding HorizontalContentAlignment}}\" \
                   VerticalContentAlignment=\"{{TemplateBinding VerticalContentAlignment}}\"/>\
               </Grid>\
             </Border>\
           </Grid>\
         </ControlTemplate>",
        normal = rest(0.0, false),
        over = rest(state::HOVER, true),
        ripple_inset = layout::RIPPLE_INSET,
    )
}

/// The switch's knob size, knob and track colors and state layer off or
/// on, set at once (everything but where the knob is).
fn switch_fill(on: bool, knob_off: f64) -> String {
    let (show, hide) = if on { (1.0, 0.0) } else { (0.0, 1.0) };
    scale_set("KnobSize", if on { 1.0 } else { knob_off })
        + &set("SwitchKnobOn", "Opacity", show)
        + &set("SwitchKnobOff", "Opacity", hide)
        + &set("SwitchKnobBounds", "Opacity", show)
        + &set("OuterBorder", "Opacity", hide)
        + &set("LayerOn", "Opacity", show)
        + &set("LayerOff", "Opacity", hide)
}

/// The switch off or on: the knob at its end, and [`switch_fill`].
fn switch_look(on: bool, travel: f64, knob_off: f64) -> String {
    set("KnobTranslateTransform", "X", if on { travel } else { 0.0 }) + &switch_fill(on, knob_off)
}

/// The switch turning on or off: the knob travels and changes size on
/// spatial-fast, the tracks and knobs cross-fade on effects-fast.
fn switch_turn(on: bool, travel: f64, knob_off: f64) -> String {
    let (from, to) = if on { (0.0, 1.0) } else { (1.0, 0.0) };
    let fade = |target: &str, from: f64, to: f64| spring(target, "Opacity", EFFECTS_FAST, from, to);
    spring(
        "KnobTranslateTransform",
        "X",
        SPATIAL_FAST,
        travel * from,
        travel * to,
    ) + &scale_spring(
        "KnobSize",
        SPATIAL_FAST,
        if on { knob_off } else { 1.0 },
        if on { 1.0 } else { knob_off },
    ) + &fade("SwitchKnobOn", from, to)
        + &fade("SwitchKnobOff", to, from)
        + &fade("SwitchKnobBounds", from, to)
        + &fade("OuterBorder", to, from)
        + &fade("LayerOn", from, to)
        + &fade("LayerOff", to, from)
}

/// Google's switch on WinUI's `ToggleSwitch` parts (`SwitchKnobBounds`,
/// `SwitchKnob`, `KnobTranslateTransform`, `SwitchThumb`, the lazy header):
/// a 52 × 32 track, off on `surface-container-highest` with a 2-DIP
/// `outline` ring and a 16-DIP `outline` knob, on in `primary` with a
/// 24-DIP `on-primary` knob; the knob grows to 28 while pressed and shows
/// its state layer (on-surface off, primary on) under the pointer. The
/// knob's cell is the track's height, so WinUI's own travel (bounds width
/// less the knob cell) is 20 DIPs; a drag that ends repositions the knob
/// with WinUI's own theme animation. No On or Off words.
pub(super) fn toggle_switch(motion: bool) -> String {
    let (width, height) = (switch::WIDTH, switch::HEIGHT);
    let travel = width - height;
    let track_radius = height / 2.0;
    let knob_off = switch::KNOB / switch::KNOB_ON;
    let grow_pressed = switch::KNOB_PRESSED / switch::KNOB_ON;
    let layer_inset = (height - switch::STATE_LAYER) / 2.0;
    let (hover, pressed) = (state::HOVER, state::PRESSED);
    let common =
        |layer: f64, grown: f64| set("KnobLayer", "Opacity", layer) + &scale_set("KnobGrow", grown);
    let disabled = resource("OuterBorder", "Fill", "ToggleSwitchFillOffDisabled")
        + &resource("OuterBorder", "Stroke", "ToggleSwitchStrokeOffDisabled")
        + &resource("SwitchKnobBounds", "Fill", "ToggleSwitchFillOnDisabled")
        + &resource("SwitchKnobOff", "Fill", "ToggleSwitchKnobFillOffDisabled")
        + &resource("SwitchKnobOn", "Fill", "ToggleSwitchKnobFillOnDisabled")
        + &common(0.0, 1.0);
    let common_transitions = if motion {
        let layer = |from: f64, to: f64| spring("KnobLayer", "Opacity", EFFECTS_FAST, from, to);
        let grown = |from: f64, to: f64| scale_spring("KnobGrow", SPATIAL_FAST, from, to);
        vec![
            transition("Normal", "PointerOver", &layer(0.0, hover)),
            transition("PointerOver", "Normal", &layer(hover, 0.0)),
            transition(
                "Normal",
                "Pressed",
                &(layer(0.0, pressed) + &grown(1.0, grow_pressed)),
            ),
            transition(
                "PointerOver",
                "Pressed",
                &(layer(hover, pressed) + &grown(1.0, grow_pressed)),
            ),
            transition(
                "Pressed",
                "PointerOver",
                &(layer(pressed, hover) + &grown(grow_pressed, 1.0)),
            ),
            transition(
                "Pressed",
                "Normal",
                &(layer(pressed, 0.0) + &grown(grow_pressed, 1.0)),
            ),
        ]
    } else {
        Vec::new()
    };
    // A drag from On starts from the on look (the Dragging state holds no
    // values of its own; the drag moves the knob itself); a drag that ends
    // repositions the knob with WinUI's own theme animation.
    let mut toggle_transitions = vec![transition("On", "Dragging", &switch_fill(true, knob_off))];
    if motion {
        let reposition = |offset: &str| {
            format!(
                "<RepositionThemeAnimation TargetName=\"SwitchKnob\"                    FromHorizontalOffset=\"{{Binding RelativeSource={{RelativeSource TemplatedParent}},                    Path=TemplateSettings.{offset}}}\"/>"
            )
        };
        toggle_transitions.extend([
            transition("Off", "On", &switch_turn(true, travel, knob_off)),
            transition("On", "Off", &switch_turn(false, travel, knob_off)),
            transition(
                "Dragging",
                "On",
                &(reposition("KnobCurrentToOnOffset") + &switch_fill(true, knob_off)),
            ),
            transition(
                "Dragging",
                "Off",
                &(reposition("KnobCurrentToOffOffset") + &switch_fill(false, knob_off)),
            ),
        ]);
    }
    format!(
        "<ControlTemplate TargetType=\"ToggleSwitch\">\
           <Grid Background=\"{{TemplateBinding Background}}\" \
             BorderBrush=\"{{TemplateBinding BorderBrush}}\" \
             BorderThickness=\"{{TemplateBinding BorderThickness}}\">\
             <VisualStateManager.VisualStateGroups>\
               <VisualStateGroup x:Name=\"CommonStates\">\
                 {common_transitions}\
                 <VisualState x:Name=\"Normal\"><Storyboard>{normal}</Storyboard></VisualState>\
                 <VisualState x:Name=\"PointerOver\"><Storyboard>{over}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Pressed\"><Storyboard>{press}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Disabled\"><Storyboard>{disabled}</Storyboard></VisualState>\
               </VisualStateGroup>\
               <VisualStateGroup x:Name=\"ToggleStates\">\
                 {toggle_transitions}\
                 <VisualState x:Name=\"Dragging\"/>\
                 <VisualState x:Name=\"Off\"><Storyboard>{off}</Storyboard></VisualState>\
                 <VisualState x:Name=\"On\"><Storyboard>{on}</Storyboard></VisualState>\
               </VisualStateGroup>\
             </VisualStateManager.VisualStateGroups>\
             <Grid.RowDefinitions><RowDefinition Height=\"Auto\"/><RowDefinition Height=\"*\"/></Grid.RowDefinitions>\
             <ContentPresenter x:Name=\"HeaderContentPresenter\" x:DeferLoadStrategy=\"Lazy\" Grid.Row=\"0\" \
               Content=\"{{TemplateBinding Header}}\" ContentTemplate=\"{{TemplateBinding HeaderTemplate}}\" \
               Foreground=\"{{ThemeResource ToggleSwitchHeaderForeground}}\" IsHitTestVisible=\"False\" \
               Margin=\"{{ThemeResource ToggleSwitchTopHeaderMargin}}\" TextWrapping=\"Wrap\" \
               VerticalAlignment=\"Top\" Visibility=\"Collapsed\" AutomationProperties.AccessibilityView=\"Raw\"/>\
             <Grid Grid.Row=\"1\" Width=\"{width}\" Height=\"{height}\" \
               HorizontalAlignment=\"Left\" VerticalAlignment=\"Center\">\
               <Grid x:Name=\"SwitchAreaGrid\" Background=\"Transparent\" CornerRadius=\"{track_radius}\" \
                 Control.IsTemplateFocusTarget=\"True\"/>\
               <Rectangle x:Name=\"OuterBorder\" RadiusX=\"{track_radius}\" RadiusY=\"{track_radius}\" \
                 Fill=\"{{ThemeResource ToggleSwitchFillOff}}\" \
                 Stroke=\"{{ThemeResource ToggleSwitchStrokeOff}}\" StrokeThickness=\"{outline}\"/>\
               <Rectangle x:Name=\"SwitchKnobBounds\" RadiusX=\"{track_radius}\" RadiusY=\"{track_radius}\" \
                 Fill=\"{{ThemeResource ToggleSwitchFillOn}}\" Opacity=\"0\"/>\
               <Grid x:Name=\"SwitchKnob\" Width=\"{height}\" Height=\"{height}\" HorizontalAlignment=\"Left\">\
                 <Grid.RenderTransform><TranslateTransform x:Name=\"KnobTranslateTransform\"/></Grid.RenderTransform>\
                 <Grid x:Name=\"KnobLayer\" Margin=\"{layer_inset}\" Opacity=\"0\" IsHitTestVisible=\"False\">\
                   <Ellipse x:Name=\"LayerOff\" Fill=\"{{ThemeResource TextFillColorPrimaryBrush}}\"/>\
                   <Ellipse x:Name=\"LayerOn\" Fill=\"{{ThemeResource AccentFillColorDefaultBrush}}\" Opacity=\"0\"/>\
                 </Grid>\
                 <Grid Width=\"{knob_on}\" Height=\"{knob_on}\" RenderTransformOrigin=\"0.5,0.5\">\
                   <Grid.RenderTransform><ScaleTransform x:Name=\"KnobGrow\"/></Grid.RenderTransform>\
                   <Grid RenderTransformOrigin=\"0.5,0.5\">\
                     <Grid.RenderTransform>\
                       <ScaleTransform x:Name=\"KnobSize\" ScaleX=\"{knob_off}\" ScaleY=\"{knob_off}\"/>\
                     </Grid.RenderTransform>\
                     <Ellipse x:Name=\"SwitchKnobOff\" Fill=\"{{ThemeResource ToggleSwitchKnobFillOff}}\"/>\
                     <Ellipse x:Name=\"SwitchKnobOn\" Fill=\"{{ThemeResource ToggleSwitchKnobFillOn}}\" Opacity=\"0\"/>\
                   </Grid>\
                 </Grid>\
               </Grid>\
               <Thumb x:Name=\"SwitchThumb\" AutomationProperties.AccessibilityView=\"Raw\">\
                 <Thumb.Template><ControlTemplate TargetType=\"Thumb\">\
                   <Rectangle Fill=\"Transparent\"/>\
                 </ControlTemplate></Thumb.Template>\
               </Thumb>\
             </Grid>\
           </Grid>\
         </ControlTemplate>",
        common_transitions = transitions(&common_transitions),
        toggle_transitions = transitions(&toggle_transitions),
        normal = common(0.0, 1.0),
        over = common(hover, 1.0),
        press = common(pressed, grow_pressed),
        off = switch_look(false, travel, knob_off),
        on = switch_look(true, travel, knob_off),
        outline = switch::OUTLINE,
        knob_on = switch::KNOB_ON,
    )
}

/// The wave as path data: quadratic halves of one period each, swinging
/// between the top and the bottom of the bar around its middle, from 0 to
/// past `span` (one period more, for its travel).
fn wave_path(span: f64) -> String {
    let (length, height) = (wave::LENGTH, wave::HEIGHT);
    let middle = height / 2.0;
    let mut path = format!("M0,{middle}");
    let mut x = 0.0;
    while x <= span + length {
        let _ = write!(
            path,
            " Q{},0 {},{middle} Q{},{height} {},{middle}",
            x + length / 4.0,
            x + length / 2.0,
            x + 3.0 * length / 4.0,
            x + length,
        );
        x += length;
    }
    path
}

/// The progress bar: an indeterminate bar is the brand book's wave (a
/// `Path` in the bar's foreground, `primary`, with round caps) over the
/// straight 4-DIP track in its background (`secondary-container`),
/// travelling one period per `wave-ms`, still when animation effects are
/// off; a determinate bar is the straight track with WinUI's own
/// `DeterminateProgressBarIndicator`, whose width WinUI sets. The wave
/// sits in a canvas (no layout clip moves with it) inside a grid whose
/// corner clips it to the bar. Still a real `ProgressBar`, with its own
/// automation peer.
///
/// WinUI's `ProgressBar` (Windows App SDK WinUI 2.3.0 `generic.xaml`)
/// looks up the parts `LayoutRoot` (a `Grid`) and the `Rectangle`s
/// `DeterminateProgressBarIndicator`, `IndeterminateProgressBarIndicator`
/// and `IndeterminateProgressBarIndicator2`, whose widths it sets, and
/// moves between the `CommonStates` named below. Every part is here (the
/// two indeterminate rectangles stay collapsed: the wave replaces them)
/// and every state exists, so no state change misses a part or a state;
/// each storyboard targets only this template's own names.
pub(super) fn progress_bar(motion: bool) -> String {
    let (track, stroke) = (wave::TRACK, wave::STROKE);
    let travel = if motion {
        format!(
            "<Storyboard RepeatBehavior=\"Forever\">\
               <DoubleAnimation Storyboard.TargetName=\"WaveShift\" Storyboard.TargetProperty=\"X\" \
                 From=\"0\" To=\"-{length}\" Duration=\"{duration}\"/>\
             </Storyboard>",
            length = wave::LENGTH,
            duration = key_time(WAVE_MS),
        )
    } else {
        String::new()
    };
    let wave_shown = "<VisualState.Setters>\
           <Setter Target=\"Wave.Opacity\" Value=\"1\"/>\
           <Setter Target=\"DeterminateProgressBarIndicator.Opacity\" Value=\"0\"/>\
         </VisualState.Setters>";
    format!(
        "<ControlTemplate TargetType=\"ProgressBar\">\
           <Grid x:Name=\"LayoutRoot\" Height=\"{height}\">\
             <VisualStateManager.VisualStateGroups>\
               <VisualStateGroup x:Name=\"CommonStates\">\
                 <VisualState x:Name=\"Normal\"/>\
                 <VisualState x:Name=\"Determinate\"/>\
                 <VisualState x:Name=\"Updating\"/>\
                 <VisualState x:Name=\"UpdatingError\"/>\
                 <VisualState x:Name=\"Error\"/>\
                 <VisualState x:Name=\"Paused\"/>\
                 <VisualState x:Name=\"Indeterminate\">{wave_shown}{travel}</VisualState>\
                 <VisualState x:Name=\"IndeterminateError\">{wave_shown}</VisualState>\
                 <VisualState x:Name=\"IndeterminatePaused\">{wave_shown}</VisualState>\
               </VisualStateGroup>\
             </VisualStateManager.VisualStateGroups>\
             <Rectangle x:Name=\"ProgressBarTrack\" Height=\"{track}\" RadiusX=\"{track_radius}\" \
               RadiusY=\"{track_radius}\" Fill=\"{{TemplateBinding Background}}\" VerticalAlignment=\"Center\"/>\
             <Rectangle x:Name=\"DeterminateProgressBarIndicator\" Height=\"{track}\" \
               RadiusX=\"{track_radius}\" RadiusY=\"{track_radius}\" Fill=\"{{TemplateBinding Foreground}}\" \
               HorizontalAlignment=\"Left\" VerticalAlignment=\"Center\"/>\
             <Rectangle x:Name=\"IndeterminateProgressBarIndicator\" Visibility=\"Collapsed\"/>\
             <Rectangle x:Name=\"IndeterminateProgressBarIndicator2\" Visibility=\"Collapsed\"/>\
             <Grid x:Name=\"Wave\" Opacity=\"0\" CornerRadius=\"{clip_radius}\">\
               <Canvas>\
                 <Path x:Name=\"WavePath\" Data=\"{path}\" Stroke=\"{{TemplateBinding Foreground}}\" \
                   StrokeThickness=\"{stroke}\" StrokeStartLineCap=\"Round\" StrokeEndLineCap=\"Round\">\
                   <Path.RenderTransform><TranslateTransform x:Name=\"WaveShift\"/></Path.RenderTransform>\
                 </Path>\
               </Canvas>\
             </Grid>\
           </Grid>\
         </ControlTemplate>",
        height = wave::HEIGHT,
        track_radius = track / 2.0,
        clip_radius = stroke / 2.0,
        path = wave_path(layout::WAVE_SPAN),
    )
}

/// WinUI's ComboBox template (`combo_box.xaml`, MIT) at the design's
/// dropdown chevron (the brand book's glyph and size; WinUI's is 12) with the focus highlight
/// following the pill: the highlight sits 4 DIPs outside the field.
pub(super) fn combo_box() -> String {
    const HIGHLIGHT_OUTSET: f64 = 4.0;
    include_str!("combo_box.xaml")
        .replace("@CHEVRON@", &icon::CHEVRON.to_string())
        .replace("@SYMBOLS_FAMILY@", &crate::fonts::families().symbols)
        .replace(
            "@CHEVRON_GLYPH@",
            &crate::brand::glyph::CHEVRON_DOWN
                .chars()
                .map(|c| format!("&#x{:X};", u32::from(c)))
                .collect::<String>(),
        )
        .replace(
            "@HIGHLIGHT_RADIUS@",
            &(radius::PILL + HIGHLIGHT_OUTSET).to_string(),
        )
}
