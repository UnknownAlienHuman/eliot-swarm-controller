//! The window's columns (`fluent-design-system.md` §6): the sidebar, the
//! main card and the docked context pane, with a drag handle on each
//! panel's inner edge.
//!
//! [`SplitShell`] owns both panel widths, so a drag step re-renders it
//! alone: the window's subtrees reach it as input and compare equal.
//! [`Breakpoints`] is the one observer of the window's size; its view is
//! one empty element, and it posts to the window only when the size class
//! or the docking result changes (§6.2), so a resize inside one class
//! costs the window nothing.

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

/// What the window lays out by: the size class and whether the requested
/// pane docks beside the main area (§6.2's docking condition).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Layout {
    pub(super) class: SizeClass,
    pub(super) docks: bool,
}

impl Layout {
    /// The layout of a client `width`: `sidebar` is the sidebar's width
    /// (the rail's in a compact window or when hidden by choice), `pane`
    /// the requested pane width.
    pub(super) fn of(width: f64, sidebar: f64, rail: bool, pane: f64) -> Self {
        let class = SizeClass::of(width);
        let sidebar = if rail || class == SizeClass::Compact {
            RAIL_WIDTH
        } else {
            sidebar
        };
        Self {
            class,
            docks: width >= sidebar + MAIN_REQUIRED_WIDTH + pane + DOCK_GAP,
        }
    }
}

/// The two resizable widths, as `[desktop] panels` keeps them.
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

    pub(super) fn saved(self) -> crate::settings::Panels {
        crate::settings::Panels {
            sidebar: self.sidebar.round() as u32,
            pane: self.pane.round() as u32,
        }
    }
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
    pub(super) side: View,
    pub(super) mode: Side,
    /// A compact window's sidebar, open in the native navigation pane.
    pub(super) overlay: Option<View>,
    /// The main card's content: the conversation or a page.
    pub(super) main: View,
    /// The open pane.
    pub(super) pane: Option<View>,
    /// The pane lies over the main area (Full view, or a window too narrow
    /// to dock it) instead of docking beside it.
    pub(super) over: bool,
    /// The pane at its Wide preset (no handle).
    pub(super) wide: bool,
    /// The widths saved in `[desktop]` (defaults when absent).
    pub(super) widths: PanelWidths,
    pub(super) note: SizeNote,
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
    Pressed(Edge, PointerEventInfo),
    Moved(Edge, PointerEventInfo),
    /// Release or capture loss: the drag ends and the widths persist.
    Ended(Edge),
    Hover(Option<Edge>),
    /// An arrow key on the focused handle: DIPs toward the right.
    Step(Edge, f64),
    /// The arrow key is released: the widths persist.
    Stepped,
}

struct Drag {
    edge: Edge,
    start_x: f64,
    start_width: f64,
}

pub(super) struct SplitShell {
    widths: PanelWidths,
    /// The saved widths last adopted (a reset in Settings changes them).
    adopted: PanelWidths,
    drag: Option<Drag>,
    hover: Option<Edge>,
    mode: Side,
    docked: bool,
    note: SizeNote,
    on_widths: Callback<PanelWidths>,
}

impl SplitShell {
    /// The sidebar's width beside the main area.
    fn sidebar_beside(&self) -> f64 {
        match self.mode {
            Side::Panel => self.widths.sidebar,
            Side::Rail => RAIL_WIDTH,
        }
    }

    /// `edge`'s width moved to `width`, within its bounds and keeping the
    /// main area at its required width beside the other panel.
    fn resize(&mut self, edge: Edge, width: f64) {
        let window = self.note.width().unwrap_or(f64::MAX);
        match edge {
            Edge::Sidebar => {
                let pane = if self.docked { self.widths.pane } else { 0.0 };
                let room = window - pane - MAIN_REQUIRED_WIDTH - DOCK_GAP;
                self.widths.sidebar = width.min(room).clamp(SIDEBAR_MIN_WIDTH, SIDEBAR_MAX_WIDTH);
            }
            Edge::Pane => {
                let room = window - self.sidebar_beside() - MAIN_REQUIRED_WIDTH - DOCK_GAP;
                self.widths.pane = width.min(room).clamp(PANEL_MIN_WIDTH, PANEL_MAX_WIDTH);
            }
        }
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
    /// resize cursor, highlighted under the pointer and while focused or
    /// dragged; Left and Right move it by 10 DIPs (50 with Shift).
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
            .on_key_up(RoutedCallback::new({
                let sender = context.sender();
                move |key: KeyEventInfo| {
                    if matches!(key.key, VirtualKey::LEFT | VirtualKey::RIGHT) {
                        let _ = sender.send(SplitMsg::Stepped);
                        true
                    } else {
                        false
                    }
                }
            }))
            .tooltip(name)
    }
}

impl Component for SplitShell {
    type Input = SplitInput;
    type Message = SplitMsg;

    fn create(input: &SplitInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            widths: input.widths,
            adopted: input.widths,
            drag: None,
            hover: None,
            mode: input.mode,
            docked: input.pane.is_some() && !input.over,
            note: input.note.clone(),
            on_widths: input.on_widths.clone(),
        }
    }

    fn input_changed(&mut self, input: &SplitInput, _context: &ComponentContext<Self>) {
        self.mode = input.mode;
        self.docked = input.pane.is_some() && !input.over;
        self.on_widths = input.on_widths.clone();
        if input.widths != self.adopted {
            self.widths = input.widths;
            self.adopted = input.widths;
        }
    }

    fn update(&mut self, message: SplitMsg, _context: &ComponentContext<Self>) {
        match message {
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
                let moved = info.window_x - drag.start_x;
                let width = match edge {
                    Edge::Sidebar => drag.start_width + moved,
                    Edge::Pane => drag.start_width - moved,
                };
                self.resize(edge, width);
            }
            SplitMsg::Ended(edge) => {
                if self.drag.as_ref().is_some_and(|drag| drag.edge == edge) {
                    self.drag = None;
                    self.persist();
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
                .content(input.side.clone())
                .into(),
            Side::Panel => Grid::new()
                .children((
                    Border::new()
                        .width(self.widths.sidebar)
                        .content(input.side.clone()),
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
        let pane: Option<View> = input.pane.clone().map(|pane| {
            let surface = Border::new().content(pane);
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
                    .width(if input.wide {
                        PANEL_WIDE_WIDTH
                    } else {
                        self.widths.pane
                    })
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
            let handle = (!input.over && !input.wide).then(|| self.handle(Edge::Pane, context));
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
                    .keyed_children(crate::views::slots([Some(input.main.clone()), pane])),
            )
            .into();
        // The requested pane width decides docking: the Wide preset's, or
        // the handle's.
        let requested = if input.wide {
            PANEL_WIDE_WIDTH
        } else {
            self.widths.pane
        };
        let columns = Grid::new()
            .background(ThemeBrush::SolidBackground)
            .columns([GridLength::Auto, GridLength::STAR])
            .keyed_children([
                KeyedView::new("side", side),
                KeyedView::new("card", card),
                KeyedView::new(
                    "breakpoints",
                    View::component::<Breakpoints>(BreakpointsInput {
                        sidebar: self.widths.sidebar,
                        rail: input.mode == Side::Rail,
                        pane: requested,
                        note: input.note.clone(),
                        on_layout: input.on_layout.clone(),
                    }),
                ),
            ]);
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
                    .clone()
                    .unwrap_or_else(|| Border::new().into()),
            )
            .content(columns)
            .on_pane_closed(input.on_overlay_closed.clone())
            .into()
    }
}

#[derive(Clone, PartialEq)]
struct BreakpointsInput {
    /// The sidebar's resizable width.
    sidebar: f64,
    /// The rail shows instead (hidden by choice).
    rail: bool,
    /// The requested pane width.
    pane: f64,
    note: SizeNote,
    on_layout: Callback<Layout>,
}

/// The one window-size observer: an empty element (Reactor's fast path).
/// It keeps the size note for the `[desktop]` write at close and posts the
/// layout only when it changes.
struct Breakpoints {
    width: Option<f64>,
    posted: Option<Layout>,
    /// The window has its icon (set once it is shown).
    icon_set: bool,
    input: BreakpointsInput,
}

impl Breakpoints {
    fn post(&mut self) {
        let Some(width) = self.width else {
            return;
        };
        let input = &self.input;
        let layout = Layout::of(width, input.sidebar, input.rail, input.pane);
        if self.posted != Some(layout) {
            self.posted = Some(layout);
            input.on_layout.call(layout);
        }
    }
}

impl Component for Breakpoints {
    type Input = BreakpointsInput;
    type Message = WindowSize;

    fn create(input: &BreakpointsInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            width: None,
            posted: None,
            icon_set: false,
            input: input.clone(),
        }
    }

    fn input_changed(&mut self, input: &BreakpointsInput, _context: &ComponentContext<Self>) {
        self.input = input.clone();
        self.post();
    }

    fn update(&mut self, size: WindowSize, _context: &ComponentContext<Self>) {
        // Until the window is shown the next size observation tries again.
        if !self.icon_set {
            self.icon_set = crate::app_window::set_icon();
        }
        self.width = Some(size.width);
        // A maximized size is not the size the window restores to.
        let normal =
            size.width >= 640.0 && size.height >= 480.0 && !crate::window_place::maximized();
        self.input.note.observe(
            size.width,
            normal.then_some((size.width as u32, size.height as u32)),
        );
        self.post();
    }

    fn view(&self, _input: &BreakpointsInput, context: &mut ViewContext<Self>) -> View {
        context.on_window_size(context.callback(|size: WindowSize| size));
        Border::new().into()
    }
}
