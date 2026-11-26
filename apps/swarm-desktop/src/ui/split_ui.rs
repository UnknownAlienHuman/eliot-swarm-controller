//! The window's columns (`fluent-design-system.md` §6): the sidebar, the
//! main card and the docked context pane, with a drag handle on each
//! panel's inner edge.
//!
//! [`SplitShell`] owns both panel widths and is the one observer of the
//! window's size, so a drag or resize step re-renders it alone: the
//! window's subtrees reach it as input and compare equal. It posts to the
//! window only when the size class or the docking result changes (§6.2),
//! so a resize inside one class costs the window nothing.

use super::*;
use tokens::layout::{
    DOCK_GAP, MAIN_REQUIRED_WIDTH, PANEL_MAX_WIDTH, PANEL_MIN_WIDTH, PANEL_WIDE_WIDTH, PANEL_WIDTH,
    RAIL_WIDTH, SIDEBAR_MAX_WIDTH, SIDEBAR_MIN_WIDTH, SIDEBAR_WIDTH, SPLIT_STEP, SPLITTER,
};

/// The §6.2 size class of the client width in DIPs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SizeClass {
    /// Below 900: the sidebar opens as the native navigation pane.
    Compact,
    /// 900 to 1279: sidebar and main; the pane takes the main area.
    Medium,
    /// From 1280: sidebar, main and the pane side by side.
    Wide,
    /// From 1520: the Wide pane fits beside the main area.
    Wider,
}

impl SizeClass {
    fn of(width: f64) -> Self {
        if width < tokens::layout::MEDIUM_FROM {
            Self::Compact
        } else if width < tokens::layout::WIDE_FROM {
            Self::Medium
        } else if width < tokens::layout::WIDER_FROM {
            Self::Wide
        } else {
            Self::Wider
        }
    }
}

/// What the window lays out by: the size class, whether the requested
/// pane docks beside the main area (§6.2's docking condition) and whether
/// Home's column has its full width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Layout {
    pub(super) class: SizeClass,
    /// The requested pane would dock: computed with no pane open too, so
    /// the window already knows on the render that opens the pane whether
    /// it docks or lies over the main area. Computing it only for an open
    /// pane would render the opening once with a stale answer and lay it
    /// out again after this component observed the pane.
    pub(super) docks: bool,
    /// The pane at its wide width (720) would dock: the pane's Wide toggle
    /// shows only then (§6.2).
    pub(super) wide_docks: bool,
    /// The main card holds the Home column at its full width
    /// (`HOME_WIDTH` inside its side gutters): Home's composer then keeps
    /// its parameters and Start task on one line, and wraps them below it
    /// (§8.1).
    pub(super) full_home: bool,
    /// The main card holds the reading column at its full width
    /// (`READING_WIDTH` inside its side gutters), with no pane beside it /
    /// beside the requested pane docked: a conversation's composer then
    /// keeps its chips and its action on one line, and wraps the chips
    /// below it (§9.4).
    pub(super) full_reading: bool,
    pub(super) full_reading_docked: bool,
}

impl Layout {
    /// The layout of a client `width`: `sidebar` is the sidebar's width
    /// (the rail's in a compact window or when hidden by choice), `pane`
    /// the requested pane width, open or not (see [`Layout::docks`]).
    pub(super) fn of(width: f64, sidebar: f64, rail: bool, pane: f64) -> Self {
        let class = SizeClass::of(width);
        let sidebar = if rail || class == SizeClass::Compact {
            RAIL_WIDTH
        } else {
            sidebar
        };
        // The main card: the window less the sidebar, the card's right
        // gap and its two hairlines.
        let main = width - sidebar - tokens::layout::CARD_GAP - 2.0 * tokens::layout::HAIRLINE;
        let reading = tokens::layout::READING_WIDTH + 2.0 * tokens::spacing::XL;
        Self {
            class,
            docks: spare(width, sidebar, pane) >= 0.0,
            wide_docks: spare(width, sidebar, PANEL_WIDE_WIDTH) >= 0.0,
            full_home: main >= tokens::layout::HOME_WIDTH + 2.0 * tokens::spacing::XL,
            full_reading: main >= reading,
            full_reading_docked: main - pane - DOCK_GAP >= reading,
        }
    }
}

/// What a client `width` leaves beyond the sidebar, the main area's
/// required width, the docked `pane` (0 when none) and the gap: §6.2 docks
/// the pane while it is not negative, and a handle may widen its panel by
/// at most this much.
fn spare(width: f64, sidebar: f64, pane: f64) -> f64 {
    width - (sidebar + MAIN_REQUIRED_WIDTH + pane + DOCK_GAP)
}

/// The two resizable widths, as `[desktop] panels` keeps them: whole DIPs
/// (a handle rounds once, in [`SplitShell::resize`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct PanelWidths {
    pub(super) sidebar: f64,
    pub(super) pane: f64,
}

impl Default for PanelWidths {
    fn default() -> Self {
        Self {
            sidebar: SIDEBAR_WIDTH,
            pane: PANEL_WIDTH,
        }
    }
}

impl PanelWidths {
    /// Saved widths within the handles' bounds.
    pub(super) fn of(saved: Option<crate::settings::Panels>) -> Self {
        saved.map_or_else(Self::default, |saved| Self {
            sidebar: f64::from(saved.sidebar).clamp(SIDEBAR_MIN_WIDTH, SIDEBAR_MAX_WIDTH),
            pane: f64::from(saved.pane).clamp(PANEL_MIN_WIDTH, PANEL_MAX_WIDTH),
        })
    }

    /// `edge`'s width set to `width`: whole DIPs within its bounds. A
    /// handle and Settings › Panel widths both set a width this way.
    pub(super) fn with(mut self, edge: Edge, width: f64) -> Self {
        let width = width.round();
        match edge {
            Edge::Sidebar => self.sidebar = width.clamp(SIDEBAR_MIN_WIDTH, SIDEBAR_MAX_WIDTH),
            Edge::Pane => self.pane = width.clamp(PANEL_MIN_WIDTH, PANEL_MAX_WIDTH),
        }
        self
    }

    pub(super) fn saved(self) -> crate::settings::Panels {
        crate::settings::Panels {
            sidebar: self.sidebar as u32,
            pane: self.pane as u32,
        }
    }
}

/// A subtree the window built, compared by identity: SplitShell's own
/// renders (a drag step) pass the same one, which Reactor then skips
/// without walking it; a window render builds a new one.
#[derive(Clone)]
pub(super) struct Built(Rc<View>);

impl Built {
    pub(super) fn new(view: View) -> Self {
        Self(Rc::new(view))
    }
}

impl PartialEq for Built {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

/// The memo boundary around a [`Built`] subtree.
struct Subtree;

impl Component for Subtree {
    type Input = Built;
    type Message = ();

    fn create(_input: &Built, _context: &ComponentContext<Self>) -> Self {
        Self
    }

    fn view(&self, input: &Built, _context: &mut ViewContext<Self>) -> View {
        (*input.0).clone()
    }
}

fn subtree(built: &Built) -> View {
    View::component::<Subtree>(built.clone())
}

/// How the sidebar column shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Side {
    /// At its resizable width, with a handle.
    Panel,
    /// The rail: hidden by choice, or a compact window.
    Rail,
}

/// The window's subtrees and the panels' requested state.
#[derive(Clone, PartialEq)]
pub(super) struct SplitInput {
    pub(super) side: Built,
    pub(super) mode: Side,
    /// A compact window's sidebar, open in the native navigation pane.
    pub(super) overlay: Option<Built>,
    /// The main card's content: the conversation or a page.
    pub(super) main: Built,
    /// The open pane.
    pub(super) pane: Option<Built>,
    /// The pane lies over the main area (Full view, or a window too narrow
    /// to dock it) instead of docking beside it.
    pub(super) over: bool,
    /// The widths saved in `[desktop]` (defaults when absent).
    pub(super) widths: PanelWidths,
    pub(super) on_layout: Callback<Layout>,
    pub(super) on_widths: Callback<PanelWidths>,
    pub(super) on_overlay_closed: Callback<bool>,
}

/// Which panel a handle resizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Edge {
    Sidebar,
    Pane,
}

pub(super) enum SplitMsg {
    /// The window's client size.
    Size(WindowSize),
    Pressed(Edge, PointerEventInfo),
    /// A move over or captured by a handle; with the left button up (a
    /// release the handle never saw) the drag ends as on release.
    Moved(Edge, PointerEventInfo),
    /// Release or capture loss: the drag ends and the widths persist.
    Ended(Edge),
    Hover(Option<Edge>),
    /// An arrow key on the focused handle: DIPs toward the right.
    Step(Edge, f64),
    /// The arrow key is released (once per press or hold) or the focus
    /// leaves the handle: the arrow keys' widths persist.
    Stepped,
}

struct Drag {
    edge: Edge,
    start_x: f64,
    start_width: f64,
}

pub(super) struct SplitShell {
    /// The observed client width; none until the first size observation.
    window: Option<f64>,
    widths: PanelWidths,
    /// The saved widths last adopted (a reset in Settings changes them).
    adopted: PanelWidths,
    drag: Option<Drag>,
    hover: Option<Edge>,
    mode: Side,
    docked: bool,
    /// The layout last posted to the window.
    posted: Option<Layout>,
    /// The window has its icon (set once it is shown).
    icon_set: bool,
    on_layout: Callback<Layout>,
    on_widths: Callback<PanelWidths>,
}

impl SplitShell {
    /// Posts the layout of the observed width when it changed.
    fn post_layout(&mut self) {
        let Some(window) = self.window else {
            return;
        };
        let layout = Layout::of(
            window,
            self.widths.sidebar,
            self.mode == Side::Rail,
            self.widths.pane,
        );
        if self.posted != Some(layout) {
            self.posted = Some(layout);
            self.on_layout.call(layout);
        }
    }

    /// The sidebar's width beside the main area.
    fn sidebar_beside(&self) -> f64 {
        match self.mode {
            Side::Panel => self.widths.sidebar,
            Side::Rail => RAIL_WIDTH,
        }
    }

    /// `edge`'s width moved to `width` ([`PanelWidths::with`]), keeping the
    /// main area at its required width beside the other panel
    /// ([`spare`]). In a room already below that, the width cannot grow
    /// but can still narrow. Nothing moves before the first size
    /// observation.
    fn resize(&mut self, edge: Edge, width: f64) {
        let Some(window) = self.window else {
            return;
        };
        let pane = if self.docked { self.widths.pane } else { 0.0 };
        let current = self.width(edge);
        let most = (current + spare(window, self.sidebar_beside(), pane))
            .floor()
            .max(current);
        self.widths = self.widths.with(edge, width.round().min(most));
        self.post_layout();
    }

    /// The drag ends and the widths persist.
    fn end_drag(&mut self) {
        self.drag = None;
        self.persist();
    }

    fn width(&self, edge: Edge) -> f64 {
        match edge {
            Edge::Sidebar => self.widths.sidebar,
            Edge::Pane => self.widths.pane,
        }
    }

    fn persist(&mut self) {
        if self.widths != self.adopted {
            self.adopted = self.widths;
            self.on_widths.call(self.widths);
        }
    }

    /// A panel's drag handle on its inner edge: a 6-DIP strip with the
    /// resize cursor, highlighted under the pointer and while dragged;
    /// Left and Right move it by 10 DIPs (50 with Shift), and the widths
    /// persist when the key is released (one write per press or hold) and
    /// when the focus leaves it.
    fn handle(&self, edge: Edge, context: &mut ViewContext<Self>) -> View {
        let active = self.hover == Some(edge) || self.drag.as_ref().is_some_and(|d| d.edge == edge);
        let sender = context.sender();
        let keys = RoutedCallback::new(move |key: KeyEventInfo| {
            let step = if key.modifiers.contains(InputModifiers::SHIFT) {
                5.0 * SPLIT_STEP
            } else {
                SPLIT_STEP
            };
            let step = match key.key {
                VirtualKey::LEFT => -step,
                VirtualKey::RIGHT => step,
                _ => return false,
            };
            let _ = sender.send(SplitMsg::Step(edge, step));
            true
        });
        let sender = context.sender();
        let released = RoutedCallback::new(move |key: KeyEventInfo| {
            if !matches!(key.key, VirtualKey::LEFT | VirtualKey::RIGHT) {
                return false;
            }
            let _ = sender.send(SplitMsg::Stepped);
            true
        });
        let fill: Brush = if active {
            ThemeBrush::CardStroke.into()
        } else {
            Brush::Solid(Color::transparent())
        };
        let (name, align) = match edge {
            Edge::Sidebar => ("Resize sidebar", HorizontalAlignment::Right),
            Edge::Pane => ("Resize pane", HorizontalAlignment::Left),
        };
        Border::new()
            .width(SPLITTER)
            .horizontal_alignment(align)
            .background(fill)
            .automation_name(name)
            .is_tab_stop(true)
            .pointer_cursor(InputSystemCursorShape::SizeWestEast)
            .capture_pointer_on_press(true)
            .on_pointer_pressed(context.callback(move |info| SplitMsg::Pressed(edge, info)))
            .on_pointer_moved(context.callback(move |info| SplitMsg::Moved(edge, info)))
            .on_pointer_released(context.callback(move |_: PointerEventInfo| SplitMsg::Ended(edge)))
            .on_pointer_capture_lost(context.callback(move |()| SplitMsg::Ended(edge)))
            .on_pointer_entered(
                context.callback(move |_: PointerEventInfo| SplitMsg::Hover(Some(edge))),
            )
            .on_pointer_exited(context.callback(|_: PointerEventInfo| SplitMsg::Hover(None)))
            .on_preview_key_down(keys)
            .on_key_up(released)
            .on_lost_focus(context.callback(|_: FocusEventInfo| SplitMsg::Stepped))
            .tooltip(name)
    }
}

impl Component for SplitShell {
    type Input = SplitInput;
    type Message = SplitMsg;

    fn create(input: &SplitInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            window: None,
            widths: input.widths,
            adopted: input.widths,
            drag: None,
            hover: None,
            mode: input.mode,
            docked: input.pane.is_some() && !input.over,
            posted: None,
            icon_set: false,
            on_layout: input.on_layout.clone(),
            on_widths: input.on_widths.clone(),
        }
    }

    fn input_changed(&mut self, input: &SplitInput, _context: &ComponentContext<Self>) {
        self.mode = input.mode;
        self.docked = input.pane.is_some() && !input.over;
        self.on_layout = input.on_layout.clone();
        self.on_widths = input.on_widths.clone();
        if input.widths != self.adopted {
            self.widths = input.widths;
            self.adopted = input.widths;
        }
        self.post_layout();
    }

    fn update(&mut self, message: SplitMsg, _context: &ComponentContext<Self>) {
        match message {
            SplitMsg::Size(size) => {
                // Until the window is shown the next size observation
                // tries again.
                if !self.icon_set {
                    self.icon_set = crate::app_window::set_icon();
                }
                // The size the window restores to is read from its
                // placement as it closes (`window_place`).
                self.window = Some(size.width);
                self.post_layout();
            }
            SplitMsg::Pressed(edge, info) => {
                self.drag = Some(Drag {
                    edge,
                    start_x: info.window_x,
                    start_width: self.width(edge),
                });
            }
            SplitMsg::Moved(edge, info) => {
                let Some(drag) = self.drag.as_ref().filter(|drag| drag.edge == edge) else {
                    return;
                };
                if !info.is_left_button_pressed {
                    self.end_drag();
                    return;
                }
                let moved = info.window_x - drag.start_x;
                let width = match edge {
                    Edge::Sidebar => drag.start_width + moved,
                    Edge::Pane => drag.start_width - moved,
                };
                self.resize(edge, width);
            }
            SplitMsg::Ended(edge) => {
                if self.drag.as_ref().is_some_and(|drag| drag.edge == edge) {
                    self.end_drag();
                }
            }
            SplitMsg::Hover(edge) => self.hover = edge,
            SplitMsg::Step(edge, step) => {
                let width = match edge {
                    Edge::Sidebar => self.width(edge) + step,
                    Edge::Pane => self.width(edge) - step,
                };
                self.resize(edge, width);
            }
            SplitMsg::Stepped => self.persist(),
        }
    }

    fn view(&self, input: &SplitInput, context: &mut ViewContext<Self>) -> View {
        let gap = tokens::layout::CARD_GAP;
        let radius = tokens::layout::CARD_RADIUS;
        let side: View = match input.mode {
            Side::Rail => Border::new()
                .width(RAIL_WIDTH)
                .content(subtree(&input.side))
                .into(),
            Side::Panel => Grid::new()
                .children((
                    Border::new()
                        .width(self.widths.sidebar)
                        .content(subtree(&input.side)),
                    self.handle(Edge::Sidebar, context),
                ))
                .into(),
        };
        // The pane keeps one place in the tree whether it docks or lies
        // over the main area, so crossing the docking width moves it
        // without rebuilding it: docked, its own width behind a hairline
        // with its handle; over the main area, both columns on the opaque
        // base, the conversation beneath keeping its scroll, selection and
        // draft for Back to conversation.
        let pane: Option<View> = input.pane.as_ref().map(|pane| {
            let surface = Border::new().content(subtree(pane));
            let border = if input.over {
                Border::new()
                    .background(ThemeBrush::SolidBackground)
                    .corner_radius(radius)
                    .content(
                        surface
                            .background(ThemeBrush::CardBackground)
                            .corner_radius(radius),
                    )
            } else {
                Border::new()
                    .width(self.widths.pane)
                    .corner_radius(CornerRadius::new(0.0, radius, radius, 0.0))
                    .border_brush(ThemeBrush::CardStroke)
                    .border_thickness(Thickness::new(tokens::layout::HAIRLINE, 0.0, 0.0, 0.0))
                    .content(surface)
            };
            let cell = if input.over {
                Grid::new().grid_column(0).grid_column_span(2)
            } else {
                Grid::new().grid_column(1)
            };
            let handle = (!input.over).then(|| self.handle(Edge::Pane, context));
            cell.keyed_children(crate::views::slots([Some(border.into()), handle]))
                .into()
        });
        // The main card: a card surface on the window's base, 8 DIPs from
        // the window's top, right and bottom edges, beside the sidebar.
        let card: View = Border::new()
            .grid_column(1)
            .margin(Thickness::new(0.0, gap, gap, gap))
            .background(ThemeBrush::CardBackground)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
            .corner_radius(radius)
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .keyed_children(crate::views::slots([Some(subtree(&input.main)), pane])),
            )
            .into();
        context.on_window_size(context.callback(SplitMsg::Size));
        let columns = Grid::new()
            .background(ThemeBrush::SolidBackground)
            .columns([GridLength::Auto, GridLength::STAR])
            .keyed_children([KeyedView::new("side", side), KeyedView::new("card", card)]);
        // A compact window opens the sidebar over the main area in the
        // native navigation pane (light dismiss); the split view is always
        // there, so crossing 900 DIPs keeps the main card's tree.
        SplitView::new()
            .display_mode(SplitViewDisplayMode::Overlay)
            .is_pane_open(input.overlay.is_some())
            .open_pane_length(self.widths.sidebar)
            .pane(
                input
                    .overlay
                    .as_ref()
                    .map_or_else(|| Border::new().into(), subtree),
            )
            .content(columns)
            .on_pane_closed(input.on_overlay_closed.clone())
            .into()
    }
}
