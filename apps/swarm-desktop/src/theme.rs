//! Material 3 Expressive-inspired semantic theme for the desktop client.
//!
//! Small on purpose: one spacing/radius/type scale, a fixed seed accent pair
//! and semantic slots that resolve to live WinUI theme resources (so the
//! system light/dark mode, High Contrast and reduced-motion settings keep
//! working). No token pipeline, no gallery, no wrapper family.

/// 8-point based spacing scale in device-independent pixels.
pub mod spacing {
    pub const XS: f64 = 4.0;
    pub const SM: f64 = 8.0;
    pub const MD: f64 = 12.0;
}

/// Rounded supporting surfaces, not pill-per-cell decoration.
pub mod radius {
    pub const MD: f64 = 12.0;
}

/// Restrained type scale; the system Segoe UI face is left as the default.
pub mod type_scale {
    pub const TITLE: f64 = 20.0;
    pub const LABEL: f64 = 12.0;
    pub const BODY: f64 = 14.0;
}

/// Layout starting points from the issue: narrow navigation, bounded reading
/// width. These are initial choices, not measured requirements.
pub mod layout {
    pub const NAV_WIDTH: f64 = 248.0;
    pub const CONTENT_WIDTH: f64 = 560.0;
    pub const TRANSCRIPT_HEIGHT: f64 = 380.0;
    pub const AGENT_LIST_HEIGHT: f64 = 220.0;
}

/// Fixed M3-inspired seed accents, one per color scheme. The surrounding
/// surfaces and text stay on live WinUI theme brushes (see below) so the
/// system scheme always wins; only the prominent primary action carries the
/// seed color.
pub const ACCENT_LIGHT: (u8, u8, u8) = (0x49, 0x5A, 0xCC);
pub const ACCENT_DARK: (u8, u8, u8) = (0xB9, 0xC3, 0xFF);

#[cfg(windows)]
pub mod winui {
    use windows_reactor::{Brush, Color, CornerRadius, ResourceOverrides, ThemeBrush};

    use super::{ACCENT_DARK, ACCENT_LIGHT, radius};

    fn accent_color(dark: bool) -> Color {
        let (r, g, b) = if dark { ACCENT_DARK } else { ACCENT_LIGHT };
        Color::rgb(r, g, b)
    }

    /// Primary body text from the active WinUI theme.
    pub fn text_primary() -> Brush {
        Brush::Theme(ThemeBrush::PrimaryText)
    }

    /// Attention/destructive text from the active WinUI theme.
    pub fn text_critical() -> Brush {
        Brush::Theme(ThemeBrush::SystemCritical)
    }

    /// The prominent primary action: seed accent background, white text,
    /// rounded corners. State visuals (hover/pressed/disabled/focus) stay
    /// with the real WinUI Button; only the resting fill is overridden.
    pub fn primary_button(dark: bool) -> ResourceOverrides {
        ResourceOverrides::new()
            .set("ButtonBackground", accent_color(dark))
            .set("ButtonForeground", Color::rgb(0xFF, 0xFF, 0xFF))
            .set("ControlCornerRadius", CornerRadius::from(radius::MD))
    }
}
