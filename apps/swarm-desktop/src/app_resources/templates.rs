//! The design's tactile control templates (XAML), with Uno.Themes'
//! Material button styles and Material Components' state layers as the
//! pattern: the container takes the button's own background, border and
//! corner; a state layer in the content color fades in on hover (8 %) and
//! press (12 %); a ripple in the content color grows on press from its
//! `RenderTransformOrigin`, which `crate::motion` moves to the press point
//! (the centre when that is unknown), and fades on release; things that
//! lift cast the theme's shadow (or ring) and glow, deeper under the
//! pointer; every change runs on the design's emphasized easing, 150 ms
//! for states.
//!
//! WinUI's `ThemeShadow` draws nothing outside a popup unless code adds
//! receivers (observed live), so shadows are drawn as Gaussian bands
//! ([`crate::theme::elevation`]) whose brushes are theme resources
//! (`LIFT_KEY`, `LIFT_AMBIENT`, `LIFT_RING`, `GLOW`): the scheme
//! dictionary rewrites them per theme. The colors stay the
//! lightweight-styling keys WinUI's own button template reads
//! (`ButtonBackground`, `ButtonForeground`, `ButtonBorderBrush` and their
//! Disabled variants), so the button families' per-button overrides
//! (`theme::winui::Palette`) color it. Values are fixed numbers and
//! identifiers, so nothing here needs escaping.

use crate::theme::elevation::{self, Band, Level};
use crate::theme::state;
use crate::ui::tokens::motion::{DECELERATE, EMPHASIZED, PRESS_MS, RIPPLE_MS, STATE_MS};
use crate::ui::tokens::{icon, layout, radius, spacing};

/// The theme brushes of a lift: the key and ambient shadows at the theme's
/// opacity (each band takes its share as its own opacity), the hairline
/// ring of the true-black themes and the night themes' glow. Transparent
/// where a theme has none.
/// The key and ambient shadow brushes of elevation levels 1 and 2.
pub(super) const LIFT_KEY: [&str; 2] = ["EliotLiftKey", "EliotLiftKey2"];
pub(super) const LIFT_AMBIENT: [&str; 2] = ["EliotLiftAmbient", "EliotLiftAmbient2"];
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
    /// The press scale.
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

/// One band of a drawn shadow ([`elevation::bands`]) around a corner of
/// `radius`, in `brush` at the band's share: the filled core, or a ring.
fn band_border(radius: f64, band: Band, brush: &str) -> String {
    let paint = match band.width {
        None => format!("Background=\"{{ThemeResource {brush}}}\""),
        Some(width) => {
            format!("BorderThickness=\"{width}\" BorderBrush=\"{{ThemeResource {brush}}}\"")
        }
    };
    format!(
        "<Border CornerRadius=\"{corner}\" Margin=\"{left},{top},{left},{bottom}\" \
           Opacity=\"{share:.4}\" {paint}/>",
        corner = (radius + band.reach).max(0.0),
        left = -band.reach,
        top = band.offset - band.reach,
        bottom = -band.offset - band.reach,
        share = band.share,
    )
}

/// One elevation level as a named group under the container: the theme's
/// ambient and key shadows and its ring.
fn shadow(name: &str, level: Level, radius: f64, opacity: f64) -> String {
    let mut out =
        format!("<Grid x:Name=\"{name}\" Opacity=\"{opacity}\" IsHitTestVisible=\"False\">");
    let [key, ambient] = elevation::level_bands(level);
    for band in ambient {
        out += &band_border(radius, *band, LIFT_AMBIENT[level.index()]);
    }
    for band in key {
        out += &band_border(radius, *band, LIFT_KEY[level.index()]);
    }
    out += &format!(
        "<Border CornerRadius=\"{corner}\" Margin=\"-{stroke}\" BorderThickness=\"{stroke}\" \
           BorderBrush=\"{{ThemeResource {LIFT_RING}}}\"/>",
        corner = radius + layout::HAIRLINE,
        stroke = layout::HAIRLINE,
    );
    out + "</Grid>"
}

/// The FAB's glow: a centred 12-DIP blur (the Neon board's).
fn glow(radius: f64) -> String {
    let mut out = "<Grid IsHitTestVisible=\"False\">".to_owned();
    for band in elevation::bands(elevation::glow(spacing::INSET)) {
        out += &band_border(radius, band, GLOW);
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
    let ms = STATE_MS;
    // Normal and PointerOver share the release: the state layer settles,
    // the ripple fades, the press springs back.
    let release = |layer: f64, hovered: bool| {
        let mut out = animate("StateLayer", "Opacity", layer, ms, EMPHASIZED)
            + &set("StateFill", "Opacity", 1.0)
            + &animate("Ripple", "Opacity", 0.0, RIPPLE_MS, EMPHASIZED)
            + &scale(1.0, STATE_MS, DECELERATE);
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
    // Both effects share the 12% pressed layer so their overlap cannot
    // wash out the content. The fill catches up with the growing ripple.
    let mut pressed = animate(
        "StateLayer",
        "Opacity",
        state::PRESSED,
        PRESS_MS,
        EMPHASIZED,
    ) + &grow("StateFill", "Opacity", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &set("Ripple", "Opacity", 1.0)
        + &grow("RippleScale", "ScaleX", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &grow("RippleScale", "ScaleY", 0.0, 1.0, RIPPLE_MS, DECELERATE)
        + &scale(press, PRESS_MS, EMPHASIZED);
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
             {layers}\
             <Border x:Name=\"Container\" \
               Background=\"{{TemplateBinding Background}}\" \
               BorderBrush=\"{{TemplateBinding BorderBrush}}\" \
               BorderThickness=\"{{TemplateBinding BorderThickness}}\" \
               CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
               <Grid CornerRadius=\"{{TemplateBinding CornerRadius}}\">\
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
        normal = release(0.0, false),
        over = release(state::HOVER, true),
        ripple_inset = layout::RIPPLE_INSET,
    )
}

/// WinUI's ComboBox template (`combo_box.xaml`, MIT) at the design's
/// dropdown chevron (14 DIPs; WinUI's is 12) with the focus highlight
/// following the pill: the highlight sits 4 DIPs outside the field.
pub(super) fn combo_box() -> String {
    const HIGHLIGHT_OUTSET: f64 = 4.0;
    include_str!("combo_box.xaml")
        .replace("@CHEVRON@", &icon::CHEVRON.to_string())
        .replace(
            "@HIGHLIGHT_RADIUS@",
            &(radius::PILL + HIGHLIGHT_OUTSET).to_string(),
        )
}
