//! Material 3 control templates (XAML), the pattern of Uno.Themes'
//! Material button styles and Material Components' state layers: the
//! container takes the button's own background, border and corner; a
//! state layer in the content color fades in on hover (8 %) and press
//! (10 %); a ripple in the content color grows from the centre on press
//! and fades on release; a raised button casts a soft shadow that deepens
//! on hover; the press scales the button down a little, and every change
//! runs on M3's motion tokens.
//!
//! WinUI's `ThemeShadow` draws nothing outside a popup unless code adds
//! receivers (observed live; a style cannot), so the shadow is drawn
//! here: [`crate::theme::elevation`]'s key and ambient layers, each a few
//! concentric translucent pills under the container.
//!
//! The colors stay the lightweight-styling keys WinUI's own button
//! template reads (`ButtonBackground`, `ButtonForeground`,
//! `ButtonBorderBrush` and their Disabled variants), so the button
//! families' per-button overrides (`theme::winui::Palette`) still color
//! it. Values are fixed numbers and identifiers, so nothing here needs
//! escaping.

use crate::theme::{elevation, state};

/// M3 motion: the emphasized and emphasized-decelerate curves as WinUI
/// key splines (cubic-bezier(0.2, 0, 0, 1) and (0.05, 0.7, 0.1, 1)).
pub(super) const EMPHASIZED: &str = "0.2,0 0,1";
pub(super) const DECELERATE: &str = "0.05,0.7 0.1,1";

/// M3 durations in milliseconds: short2, short3, medium1, medium2.
pub(super) const SHORT2: u32 = 100;
pub(super) const SHORT3: u32 = 150;
pub(super) const MEDIUM1: u32 = 250;
pub(super) const MEDIUM2: u32 = 300;

/// How far a press scales a button down (a tactile press): a compact
/// button or chip, and a wide row or labelled button, which would travel
/// too far at the same scale.
pub(super) const PRESS_SMALL: f64 = 0.92;
pub(super) const PRESS_WIDE: f64 = 0.98;

/// A raised button's elevation at rest and under the pointer (M3 levels 3
/// and 4 for the FAB); `None` is a flat button.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Lift {
    pub rest: elevation::Level,
    pub hover: elevation::Level,
}

/// One elevation level's shadow as a named group of pills under the
/// container (the FAB's circle grown by each layer's spread).
fn shadow(name: &str, level: elevation::Level, opacity: f64) -> String {
    let mut out =
        format!("<Grid x:Name=\"{name}\" Opacity=\"{opacity}\" IsHitTestVisible=\"False\">");
    for layer in elevation::layers(level) {
        let (spread, dy) = (layer.spread, layer.offset);
        out += &format!(
            "<Border CornerRadius=\"{radius}\" Margin=\"{left},{top},{left},{bottom}\" \
               Background=\"#{alpha:02X}000000\"/>",
            left = -spread,
            top = dy - spread,
            bottom = -dy - spread,
            alpha = layer.alpha,
            radius = crate::ui::tokens::radius::PILL + spread,
        );
    }
    out + "</Grid>"
}

/// One key-spline animation of `property` on `target` to `to`.
fn animate(target: &str, property: &str, to: f64, ms: u32, spline: &str) -> String {
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
fn grow(target: &str, property: &str, from: f64, to: f64, ms: u32, spline: &str) -> String {
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

fn scale(to: f64, ms: u32, spline: &str) -> String {
    animate("Press", "ScaleX", to, ms, spline) + &animate("Press", "ScaleY", to, ms, spline)
}

/// What differs between the desktop's buttons.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Look {
    /// Raised (the FAB), or flat.
    pub lift: Option<Lift>,
    /// The key prefix of the Disabled colors: `Button`, or `AccentButton`
    /// for the style based on WinUI's AccentButtonStyle.
    pub keys: &'static str,
    /// The press scale.
    pub press: f64,
}

/// The Material button template.
pub(super) fn button(look: Look) -> String {
    let Look { lift, keys, press } = look;
    let hover_layer = state::HOVER;
    let press_layer = state::PRESSED;
    let ripple_peak = state::RIPPLE;
    // Normal and PointerOver share the release: the state layer settles,
    // the ripple fades, the press springs back.
    let release = |layer: f64, hover_shadow: f64| {
        let mut out = animate("StateLayer", "Opacity", layer, SHORT3, EMPHASIZED)
            + &animate("Ripple", "Opacity", 0.0, MEDIUM2, EMPHASIZED)
            + &scale(1.0, MEDIUM1, DECELERATE);
        if lift.is_some() {
            out += &animate("HoverShadow", "Opacity", hover_shadow, SHORT3, EMPHASIZED);
            out += &animate("RestShadow", "Opacity", 1.0, SHORT3, EMPHASIZED);
        }
        out
    };
    let mut pressed = animate("StateLayer", "Opacity", press_layer, SHORT2, EMPHASIZED)
        + &set("Ripple", "Opacity", ripple_peak)
        + &grow("RippleScale", "ScaleX", 0.0, 1.0, MEDIUM2, DECELERATE)
        + &grow("RippleScale", "ScaleY", 0.0, 1.0, MEDIUM2, DECELERATE)
        + &scale(press, SHORT2, EMPHASIZED);
    let mut disabled = resource(
        "Container",
        "Background",
        &format!("{keys}BackgroundDisabled"),
    ) + &resource(
        "Container",
        "BorderBrush",
        &format!("{keys}BorderBrushDisabled"),
    ) + &resource(
        "Content",
        "Foreground",
        &format!("{keys}ForegroundDisabled"),
    ) + &set("StateLayer", "Opacity", 0.0);
    let shadows = match lift {
        Some(Lift { rest, hover }) => {
            // The press settles back to the resting depth (M3: pressed is
            // the resting level).
            pressed += &animate("HoverShadow", "Opacity", 0.0, SHORT2, EMPHASIZED);
            disabled += &set("RestShadow", "Opacity", 0.0);
            disabled += &set("HoverShadow", "Opacity", 0.0);
            shadow("RestShadow", rest, 1.0) + &shadow("HoverShadow", hover, 0.0)
        }
        None => String::new(),
    };
    format!(
        "<ControlTemplate TargetType=\"Button\">\
           <Grid x:Name=\"Root\" Background=\"Transparent\" RenderTransformOrigin=\"0.5,0.5\">\
             <Grid.RenderTransform><ScaleTransform x:Name=\"Press\"/></Grid.RenderTransform>\
             <VisualStateManager.VisualStateGroups>\
               <VisualStateGroup x:Name=\"CommonStates\">\
                 <VisualState x:Name=\"Normal\"><Storyboard>{normal}</Storyboard></VisualState>\
                 <VisualState x:Name=\"PointerOver\"><Storyboard>{over}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Pressed\"><Storyboard>{pressed}</Storyboard></VisualState>\
                 <VisualState x:Name=\"Disabled\"><Storyboard>{disabled}</Storyboard></VisualState>\
               </VisualStateGroup>\
             </VisualStateManager.VisualStateGroups>\
             {shadows}\
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
        normal = release(0.0, 0.0),
        over = release(hover_layer, 1.0),
    )
}
