//! The design's tactile control templates (XAML), with Uno.Themes'
//! Material button styles and Material Components' state layers as the
//! pattern: the container takes the button's own background, border and
//! corner; a state layer in the content color fades in on hover (8 %) and
//! press (12 %); a ripple in the content color grows from the centre on
//! press and fades on release; things that lift cast the theme's shadow
//! (or ring) and glow, deeper under the pointer; every change runs on the
//! design's emphasized easing, 150 ms for states.
//!
//! WinUI's `ThemeShadow` draws nothing outside a popup unless code adds
//! receivers (observed live), so shadows are drawn layers
//! ([`crate::theme::elevation`]) whose brushes are theme resources
//! (`LIFT_KEY`, `LIFT_AMBIENT`, `LIFT_RING`, `GLOW`): the scheme
//! dictionary rewrites them per theme. The colors stay the
//! lightweight-styling keys WinUI's own button template reads
//! (`ButtonBackground`, `ButtonForeground`, `ButtonBorderBrush` and their
//! Disabled variants), so the button families' per-button overrides
//! (`theme::winui::Palette`) color it. Values are fixed numbers and
//! identifiers, so nothing here needs escaping.

use crate::theme::elevation::{self, Level};
use crate::theme::state;
use crate::ui::tokens::motion;

/// Emphasized easing, cubic-bezier(0.2, 0, 0, 1), as a WinUI key spline.
const EMPHASIZED: &str = "0.2,0 0,1";
/// The ripple's growth: emphasized decelerate, cubic-bezier(0.05, 0.7, 0.1, 1).
const DECELERATE: &str = "0.05,0.7 0.1,1";
/// The press, faster than a state change.
const PRESS_MS: u64 = 100;
/// The ripple's growth and fade.
const RIPPLE_MS: u64 = 300;

/// The theme brushes of a lift: the key and ambient shadow layers (each
/// already at its layer's share of the theme's opacity), the hairline ring
/// of the true-black themes and the night themes' glow. Transparent where
/// a theme has none.
pub(super) const LIFT_KEY: &str = "EliotLiftKey";
pub(super) const LIFT_AMBIENT: &str = "EliotLiftAmbient";
pub(super) const LIFT_RING: &str = "EliotLiftRing";
pub(super) const GLOW: &str = "EliotGlow";

/// How a button lifts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lift {
    /// Flat: rows, chips, segments, labelled and icon buttons.
    Flat,
    /// Flat at rest, level 1 under the pointer: suggestion cards, tool rows.
    Hover,
    /// The FAB: level 1 at rest, 2 under the pointer, with the glow.
    Fab,
}

/// What differs between the desktop's button styles.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Look {
    pub lift: Lift,
    /// The press scale (1: none).
    pub press: f64,
    /// The corner radius of the lifted layers (the button's own).
    pub radius: f64,
}

/// One key-spline animation of `property` on `target` to `to`.
fn animate(target: &str, property: &str, to: f64, ms: u64, spline: &str) -> String {
    format!(
        "<DoubleAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <SplineDoubleKeyFrame KeyTime=\"0:0:0.{ms:03}\" Value=\"{to}\" KeySpline=\"{spline}\"/>\
         </DoubleAnimationUsingKeyFrames>"
    )
}

/// `property` jumps to `from`, then animates to `to`: one animation, as
/// WinUI refuses two on one property in a storyboard (a fatal XAML error,
/// observed live on the first press).
fn grow(target: &str, property: &str, from: f64, to: f64, ms: u64, spline: &str) -> String {
    format!(
        "<DoubleAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <DiscreteDoubleKeyFrame KeyTime=\"0\" Value=\"{from}\"/>\
           <SplineDoubleKeyFrame KeyTime=\"0:0:0.{ms:03}\" Value=\"{to}\" KeySpline=\"{spline}\"/>\
         </DoubleAnimationUsingKeyFrames>"
    )
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

/// A property switched to a theme resource (the Disabled colors).
fn resource(target: &str, property: &str, key: &str) -> String {
    format!(
        "<ObjectAnimationUsingKeyFrames Storyboard.TargetName=\"{target}\" \
           Storyboard.TargetProperty=\"{property}\">\
           <DiscreteObjectKeyFrame KeyTime=\"0\" Value=\"{{ThemeResource {key}}}\"/>\
         </ObjectAnimationUsingKeyFrames>"
    )
}

fn scale(to: f64, ms: u64, spline: &str) -> String {
    animate("Press", "ScaleX", to, ms, spline) + &animate("Press", "ScaleY", to, ms, spline)
}

/// A pill grown by `spread` and fallen by `offset`, in `brush`.
fn layer_border(radius: f64, spread: f64, offset: f64, brush: &str) -> String {
    format!(
        "<Border CornerRadius=\"{corner}\" Margin=\"{left},{top},{left},{bottom}\" \
           Background=\"{{ThemeResource {brush}}}\"/>",
        corner = radius + spread,
        left = -spread,
        top = offset - spread,
        bottom = -offset - spread,
    )
}

/// One elevation level as a named group under the container: the theme's
/// shadow layers and its ring.
fn shadow(name: &str, level: Level, radius: f64, opacity: f64) -> String {
    let mut out =
        format!("<Grid x:Name=\"{name}\" Opacity=\"{opacity}\" IsHitTestVisible=\"False\">");
    for (spread, offset, is_key) in elevation::geometry(level) {
        out += &layer_border(
            radius,
            spread,
            offset,
            if is_key { LIFT_KEY } else { LIFT_AMBIENT },
        );
    }
    out += &format!(
        "<Border CornerRadius=\"{corner}\" Margin=\"-1\" BorderThickness=\"1\" \
           BorderBrush=\"{{ThemeResource {LIFT_RING}}}\"/>",
        corner = radius + 1.0
    );
    out + "</Grid>"
}

/// The FAB's glow: three layers reaching 12 DIPs, a blur's steps.
fn glow(radius: f64) -> String {
    let mut out = "<Grid IsHitTestVisible=\"False\">".to_owned();
    for spread in [4.0, 8.0, 12.0] {
        out += &layer_border(radius, spread, 0.0, GLOW);
    }
    out + "</Grid>"
}

/// The tactile button template.
pub(super) fn button(look: Look) -> String {
    let Look {
        lift,
        press,
        radius,
    } = look;
    let ms = motion::STATE_MS;
    // Normal and PointerOver share the release: the state layer settles,
    // the ripple fades, the press springs back.
    let release = |layer: f64, hovered: bool| {
        let mut out = animate("StateLayer", "Opacity", layer, ms, EMPHASIZED)
            + &animate("Ripple", "Opacity", 0.0, RIPPLE_MS, EMPHASIZED);
        if press < 1.0 {
            out += &scale(1.0, motion::POPOVER_MS, DECELERATE);
        }
        if lift != Lift::Flat {
            out += &animate(
                "HoverShadow",
                "Opacity",
                f64::from(u8::from(hovered)),
                ms,
                EMPHASIZED,
            );
        }
        out
    };
    let mut pressed = animate("StateLayer", "Opacity", state::PRESSED, PRESS_MS, EMPHASIZED)
        + &set("Ripple", "Opacity", state::RIPPLE)
        + &grow("RippleScale", "ScaleX", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &grow("RippleScale", "ScaleY", 0.0, 1.0, RIPPLE_MS, DECELERATE);
    if press < 1.0 {
        pressed += &scale(press, PRESS_MS, EMPHASIZED);
    }
    let mut disabled = resource("Container", "Background", "ButtonBackgroundDisabled")
        + &resource("Container", "BorderBrush", "ButtonBorderBrushDisabled")
        + &resource("Content", "Foreground", "ButtonForegroundDisabled")
        + &set("StateLayer", "Opacity", 0.0);
    let layers = match lift {
        Lift::Flat => String::new(),
        Lift::Hover => {
            // A press sinks back to the surface.
            pressed += &animate("HoverShadow", "Opacity", 0.0, PRESS_MS, EMPHASIZED);
            disabled += &set("HoverShadow", "Opacity", 0.0);
            shadow("HoverShadow", Level::One, radius, 0.0)
        }
        Lift::Fab => {
            pressed += &animate("HoverShadow", "Opacity", 0.0, PRESS_MS, EMPHASIZED);
            disabled += &set("HoverShadow", "Opacity", 0.0);
            disabled += &set("RestShadow", "Opacity", 0.0);
            glow(radius)
                + &shadow("RestShadow", Level::One, radius, 1.0)
                + &shadow("HoverShadow", Level::Two, radius, 0.0)
        }
    };
    let transform = if press < 1.0 {
        "<Grid.RenderTransform><ScaleTransform x:Name=\"Press\"/></Grid.RenderTransform>"
    } else {
        ""
    };
    format!(
        "<ControlTemplate TargetType=\"Button\">\
           <Grid x:Name=\"Root\" Background=\"Transparent\" RenderTransformOrigin=\"0.5,0.5\">\
             {transform}\
             <VisualStateManager.VisualStateGroups>\
               <VisualStateGroup x:Name=\"CommonStates\">\
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
               <Grid CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
                 <Border x:Name=\"StateLayer\" Opacity=\"0\" \
                   Background=\"{{TemplateBinding Foreground}}\"/>\
                 <Ellipse x:Name=\"Ripple\" Opacity=\"0\" Fill=\"{{TemplateBinding Foreground}}\" \
                   RenderTransformOrigin=\"0.5,0.5\" Stretch=\"Fill\" Margin=\"-24\">\
                   <Ellipse.RenderTransform>\
                     <ScaleTransform x:Name=\"RippleScale\" ScaleX=\"0\" ScaleY=\"0\"/>\
                   </Ellipse.RenderTransform>\
                 </Ellipse>\
                 <ContentPresenter x:Name=\"Content\" \
                   AutomationProperties.AccessibilityView=\"Raw\" \
                   Content=\"{{TemplateBinding Content}}\" \
                   ContentTemplate=\"{{TemplateBinding ContentTemplate}}\" \
                   ContentTransitions=\"{{TemplateBinding ContentTransitions}}\" \
                   Foreground=\"{{TemplateBinding Foreground}}\" \
                   Padding=\"{{TemplateBinding Padding}}\" \
                   HorizontalContentAlignment=\"{{TemplateBinding HorizontalContentAlignment}}\" \
                   VerticalContentAlignment=\"{{TemplateBinding VerticalContentAlignment}}\"/>\
               </Grid>\
             </Border>\
           </Grid>\
         </ControlTemplate>",
        normal = release(0.0, false),
        over = release(state::HOVER, true),
    )
}
