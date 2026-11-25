//! Real shadows under in-tree elements (the brand book's WinUI page,
//! workaround 5): the composer card's `elevation-1` and the night themes'
//! `glow-shadow` behind the composer's main action, each as the brand
//! book's own layers (offset, blur, spread and color of CSS `box-shadow`).
//!
//! WinUI's `ThemeShadow` takes neither a color nor an opacity, and Reactor
//! 0.100.0 sets no `Shadow` on an element, so each shadow is Composition's
//! own: a host `Grid` in the element's cell, under it, is Reactor's
//! composition host (`ElementRef<Grid>::observe_composition_host`), which
//! hands over the window's `Compositor` and the host's size; its child
//! visual (`request_set_child_visual`) holds one `SpriteVisual` per layer
//! whose `DropShadow` takes the layer's blur, offset and color. A sprite
//! has no brush of its own, so only its shadow draws; the shadow's mask is
//! the element's rounded rectangle grown by the layer's spread (a
//! `ShapeVisual` rendered through a `CompositionVisualSurface`), so the
//! shadow follows the element's corner. The host's size events keep the
//! layers sized to the element.
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.winmd`
//! (Windows App SDK 2.1.3), read with System.Reflection.Metadata; each
//! vtable below asserts its slot offsets. Every call runs on the UI thread,
//! inside Reactor's host callbacks or a component's update, and fails
//! quietly: a shadow that cannot be built is left out and noted once in
//! `desktop.log`. Each function that calls through a hand-declared slot
//! allows `unsafe_code` for itself alone.

use std::cell::{Cell, RefCell};
use std::mem::offset_of;
use std::rc::Rc;

use windows_collections::IVector;
use windows_core::imp::ConstBuffer;
use windows_core::{GUID, HRESULT, IInspectable, IUnknown, Interface};
use windows_reactor::{CompositionHostEvent, ElementObservation, ElementRef, Grid};

use crate::brand;
use crate::winrt::{Getter, InspectableSlots, Raw, UiColor, get, winrt_interface};

/// `Windows.Foundation.Numerics.Vector2`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Vector2 {
    x: f32,
    y: f32,
}

/// `Windows.Foundation.Numerics.Vector3`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Vector3 {
    x: f32,
    y: f32,
    z: f32,
}

const SLOT: usize = size_of::<usize>();

/// A setter taking one object (an interface pointer, or null).
type SetObject = unsafe extern "system" fn(Raw, Raw) -> HRESULT;
type SetVector2 = unsafe extern "system" fn(Raw, Vector2) -> HRESULT;
type SetVector3 = unsafe extern "system" fn(Raw, Vector3) -> HRESULT;
type SetFloat = unsafe extern "system" fn(Raw, f32) -> HRESULT;
/// A factory method taking one object and returning one.
type CreateFrom = unsafe extern "system" fn(Raw, Raw, *mut Raw) -> HRESULT;

#[repr(C)]
struct CompositorVtbl {
    base: InspectableSlots,
    _6_7: [usize; 2],
    /// 8 `CreateColorBrush(Color)`.
    create_color_brush: unsafe extern "system" fn(Raw, UiColor, *mut Raw) -> HRESULT,
    /// 9 `CreateContainerVisual()`.
    create_container_visual: Getter,
    _10_21: [usize; 12],
    /// 22 `CreateSpriteVisual()`.
    create_sprite_visual: Getter,
    _23: usize,
    /// 24 `CreateSurfaceBrush(ICompositionSurface)`.
    create_surface_brush: CreateFrom,
}
const _: () = {
    assert!(offset_of!(CompositorVtbl, create_color_brush) == 8 * SLOT);
    assert!(offset_of!(CompositorVtbl, create_container_visual) == 9 * SLOT);
    assert!(offset_of!(CompositorVtbl, create_sprite_visual) == 22 * SLOT);
    assert!(offset_of!(CompositorVtbl, create_surface_brush) == 24 * SLOT);
};

#[repr(C)]
struct Compositor2Vtbl {
    base: InspectableSlots,
    _6_9: [usize; 4],
    /// 10 `CreateDropShadow()`.
    create_drop_shadow: Getter,
}
const _: () = assert!(offset_of!(Compositor2Vtbl, create_drop_shadow) == 10 * SLOT);

#[repr(C)]
struct Compositor5Vtbl {
    base: InspectableSlots,
    _6_19: [usize; 14],
    /// 20 `CreateRoundedRectangleGeometry()`.
    create_rounded_rectangle_geometry: Getter,
    /// 21 `CreateShapeVisual()`.
    create_shape_visual: Getter,
    _22: usize,
    /// 23 `CreateSpriteShape(CompositionGeometry)`.
    create_sprite_shape: CreateFrom,
}
const _: () = {
    assert!(offset_of!(Compositor5Vtbl, create_rounded_rectangle_geometry) == 20 * SLOT);
    assert!(offset_of!(Compositor5Vtbl, create_sprite_shape) == 23 * SLOT);
};

#[repr(C)]
struct CompositorWithVisualSurfaceVtbl {
    base: InspectableSlots,
    /// 6 `CreateVisualSurface()`.
    create_visual_surface: Getter,
}

#[repr(C)]
struct VisualVtbl {
    base: InspectableSlots,
    _6_20: [usize; 15],
    /// 21 `put_Offset(Vector3)`.
    put_offset: SetVector3,
    _22_35: [usize; 14],
    /// 36 `put_Size(Vector2)`.
    put_size: SetVector2,
}
const _: () = {
    assert!(offset_of!(VisualVtbl, put_offset) == 21 * SLOT);
    assert!(offset_of!(VisualVtbl, put_size) == 36 * SLOT);
};

#[repr(C)]
struct ContainerVisualVtbl {
    base: InspectableSlots,
    /// 6 `get_Children()`.
    children: Getter,
}

#[repr(C)]
struct VisualCollectionVtbl {
    base: InspectableSlots,
    _6_8: [usize; 3],
    /// 9 `InsertAtTop(Visual)`.
    insert_at_top: SetObject,
}
const _: () = assert!(offset_of!(VisualCollectionVtbl, insert_at_top) == 9 * SLOT);

#[repr(C)]
struct SpriteVisual2Vtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_Shadow(CompositionShadow)`.
    put_shadow: SetObject,
}
const _: () = assert!(offset_of!(SpriteVisual2Vtbl, put_shadow) == 7 * SLOT);

#[repr(C)]
struct DropShadowVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_BlurRadius(Single)`.
    put_blur_radius: SetFloat,
    _8: usize,
    /// 9 `put_Color(Color)`.
    put_color: unsafe extern "system" fn(Raw, UiColor) -> HRESULT,
    _10: usize,
    /// 11 `put_Mask(CompositionBrush)`.
    put_mask: SetObject,
    _12: usize,
    /// 13 `put_Offset(Vector3)`.
    put_offset: SetVector3,
    _14: usize,
    /// 15 `put_Opacity(Single)`.
    put_opacity: SetFloat,
}
const _: () = {
    assert!(offset_of!(DropShadowVtbl, put_blur_radius) == 7 * SLOT);
    assert!(offset_of!(DropShadowVtbl, put_mask) == 11 * SLOT);
    assert!(offset_of!(DropShadowVtbl, put_opacity) == 15 * SLOT);
};

#[repr(C)]
struct RoundedRectangleGeometryVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_CornerRadius(Vector2)`.
    put_corner_radius: SetVector2,
    _8_10: [usize; 3],
    /// 11 `put_Size(Vector2)`.
    put_size: SetVector2,
}
const _: () = assert!(offset_of!(RoundedRectangleGeometryVtbl, put_size) == 11 * SLOT);

#[repr(C)]
struct SpriteShapeVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_FillBrush(CompositionBrush)`.
    put_fill_brush: SetObject,
}

#[repr(C)]
struct ShapeVisualVtbl {
    base: InspectableSlots,
    /// 6 `get_Shapes()`.
    shapes: Getter,
}

#[repr(C)]
struct VisualSurfaceVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_SourceVisual(Visual)`.
    put_source_visual: SetObject,
    _8_10: [usize; 3],
    /// 11 `put_SourceSize(Vector2)`.
    put_source_size: SetVector2,
}
const _: () = assert!(offset_of!(VisualSurfaceVtbl, put_source_size) == 11 * SLOT);

/// The interfaces only passed as arguments (a runtime class goes by its
/// default interface): no slot is called on them.
#[repr(C)]
struct ArgumentVtbl {
    base: InspectableSlots,
}

winrt_interface!(
    ICompositor,
    CompositorVtbl,
    GUID::from_u128(0x95213c13_c4cb_57de_b267_d21ab901ae38)
);
winrt_interface!(
    ICompositor2,
    Compositor2Vtbl,
    GUID::from_u128(0xa9ffedad_3982_576d_a38a_c888ff605819)
);
winrt_interface!(
    ICompositor5,
    Compositor5Vtbl,
    GUID::from_u128(0xbb52d644_a030_5c19_b883_577ded739ae7)
);
winrt_interface!(
    ICompositorWithVisualSurface,
    CompositorWithVisualSurfaceVtbl,
    GUID::from_u128(0x5fcfe24a_690a_5378_acee_561e84bfb982)
);
winrt_interface!(
    IVisual,
    VisualVtbl,
    GUID::from_u128(0xc0eeab6c_c897_5ac6_a1c9_63abd5055b9b)
);
winrt_interface!(
    IContainerVisual,
    ContainerVisualVtbl,
    GUID::from_u128(0xc70dbce1_2c2f_5d8e_91a4_aae1121e6186)
);
winrt_interface!(
    IVisualCollection,
    VisualCollectionVtbl,
    GUID::from_u128(0xd002896d_67d8_5f69_ab70_581fa3bf370f)
);
winrt_interface!(
    ISpriteVisual2,
    SpriteVisual2Vtbl,
    GUID::from_u128(0x3cb43662_9ecf_5128_850d_9eb8437774f0)
);
winrt_interface!(
    IDropShadow,
    DropShadowVtbl,
    GUID::from_u128(0x977095d2_265f_5f58_9789_cb7f85e98c9e)
);
winrt_interface!(
    IRoundedRectangleGeometry,
    RoundedRectangleGeometryVtbl,
    GUID::from_u128(0x02eafc87_8d1f_5445_a416_d81baee8a750)
);
winrt_interface!(
    ISpriteShape,
    SpriteShapeVtbl,
    GUID::from_u128(0x982138f2_5781_509a_ba5d_112bcb0b98ef)
);
winrt_interface!(
    IShapeVisual,
    ShapeVisualVtbl,
    GUID::from_u128(0xa911c80b_a5a5_5aca_b8ff_c43f08f06143)
);
winrt_interface!(
    IVisualSurface,
    VisualSurfaceVtbl,
    GUID::from_u128(0x49e3624b_4d2e_53e7_9e02_f64831681134)
);
winrt_interface!(
    ICompositionBrush,
    ArgumentVtbl,
    GUID::from_u128(0x483924e7_99a5_5377_968b_dec6d40bbccd)
);
winrt_interface!(
    ICompositionShadow,
    ArgumentVtbl,
    GUID::from_u128(0x176b8bb5_4dae_59b2_b9a0_2499b243267c)
);
winrt_interface!(
    ICompositionSurface,
    ArgumentVtbl,
    GUID::from_u128(0x9ec612c3_a5d2_4f97_9df3_6b49ce736215)
);
winrt_interface!(
    ICompositionGeometry,
    ArgumentVtbl,
    GUID::from_u128(0x4e40bdb2_450b_5a81_9e9b_149417980cc4)
);
winrt_interface!(
    CompositionShape,
    ArgumentVtbl,
    GUID::from_u128(0xed75d4d8_437f_5640_9720_faae35ce5895)
);

// `ShapeVisual.Shapes` is an `IVector<CompositionShape>`, whose id derives
// from the element's signature.
impl windows_core::RuntimeType for CompositionShape {
    const SIGNATURE: ConstBuffer = ConstBuffer::from_slice(
        b"rc(Microsoft.UI.Composition.CompositionShape;{ed75d4d8-437f-5640-9720-faae35ce5895})",
    );
}

/// Call a setter slot of `this` with one object.
///
/// # Safety
/// `setter` must be a slot of `this`'s vtable that takes one object of
/// `argument`'s interface.
#[allow(unsafe_code, reason = "Calls a hand-declared WinRT setter slot.")]
unsafe fn set_object<T: Interface, A: Interface>(
    this: &T,
    setter: SetObject,
    argument: &A,
) -> windows_core::Result<()> {
    // SAFETY: the caller guarantees the slot; the setter borrows the
    // argument for the call.
    unsafe { setter(this.as_raw(), argument.as_raw()) }.ok()
}

/// Call a factory slot of `this` that takes one object and returns one.
///
/// # Safety
/// As [`set_object`], for a slot that returns one owned object.
#[allow(unsafe_code, reason = "Calls a hand-declared WinRT factory slot.")]
unsafe fn create_from<T: Interface, A: Interface>(
    this: &T,
    factory: CreateFrom,
    argument: &A,
) -> windows_core::Result<IInspectable> {
    let mut out = std::ptr::null_mut();
    // SAFETY: the caller guarantees the slot; the argument is borrowed.
    unsafe { factory(this.as_raw(), argument.as_raw(), &mut out) }.ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: the factory returned one owned reference.
    Ok(unsafe { IInspectable::from_raw(out) })
}

/// The window's compositor, through the interfaces the shadows use.
struct Compositor {
    base: ICompositor,
    shadows: ICompositor2,
    shapes: ICompositor5,
    surfaces: ICompositorWithVisualSurface,
}

impl Compositor {
    fn new(compositor: &IUnknown) -> windows_core::Result<Self> {
        Ok(Self {
            base: compositor.cast()?,
            shadows: compositor.cast()?,
            shapes: compositor.cast()?,
            surfaces: compositor.cast()?,
        })
    }

    #[allow(
        unsafe_code,
        reason = "Calls the compositor's CreateContainerVisual slot."
    )]
    fn container(&self) -> windows_core::Result<IContainerVisual> {
        // SAFETY: slot 9 of ICompositor is CreateContainerVisual.
        unsafe { get(&self.base, self.base.vtable().create_container_visual) }?.cast()
    }

    /// One layer: a brushless sprite whose drop shadow is masked by the
    /// element's rounded rectangle.
    #[allow(
        unsafe_code,
        reason = "Builds a layer's visuals through hand-declared Composition slots."
    )]
    fn layer(&self, layer: brand::Shadow) -> windows_core::Result<Layer> {
        let base = &self.base;
        let shapes = &self.shapes;
        // SAFETY: each call below is the slot its comment names, on the
        // interface whose vtable declares it; every argument is the
        // parameter's default interface, borrowed for the call.
        unsafe {
            let geometry: IRoundedRectangleGeometry =
                get(shapes, shapes.vtable().create_rounded_rectangle_geometry)?.cast()?;
            // ICompositor5 23 CreateSpriteShape(CompositionGeometry).
            let shape: ISpriteShape = create_from(
                shapes,
                shapes.vtable().create_sprite_shape,
                &geometry.cast::<ICompositionGeometry>()?,
            )?
            .cast()?;
            let mut black = std::ptr::null_mut();
            // ICompositor 8 CreateColorBrush(Color): the mask's opaque fill.
            (base.vtable().create_color_brush)(
                base.as_raw(),
                UiColor {
                    a: 255,
                    ..UiColor::default()
                },
                &mut black,
            )
            .ok()?;
            if black.is_null() {
                return Err(windows_core::Error::empty());
            }
            let black: ICompositionBrush = IInspectable::from_raw(black).cast()?;
            set_object(&shape, shape.vtable().put_fill_brush, &black)?;
            let mask: IShapeVisual = get(shapes, shapes.vtable().create_shape_visual)?.cast()?;
            let list: IVector<CompositionShape> = get(&mask, mask.vtable().shapes)?.cast()?;
            list.Append(&shape.cast::<CompositionShape>()?)?;
            // ICompositorWithVisualSurface 6 CreateVisualSurface.
            let surface: IVisualSurface =
                get(&self.surfaces, self.surfaces.vtable().create_visual_surface)?.cast()?;
            set_object(
                &surface,
                surface.vtable().put_source_visual,
                &mask.cast::<IVisual>()?,
            )?;
            // ICompositor 24 CreateSurfaceBrush(ICompositionSurface).
            let brush: ICompositionBrush = create_from(
                base,
                base.vtable().create_surface_brush,
                &surface.cast::<ICompositionSurface>()?,
            )?
            .cast()?;
            // ICompositor2 10 CreateDropShadow.
            let shadow: IDropShadow =
                get(&self.shadows, self.shadows.vtable().create_drop_shadow)?.cast()?;
            let drop = shadow.vtable();
            (drop.put_blur_radius)(shadow.as_raw(), layer.blur as f32).ok()?;
            (drop.put_color)(
                shadow.as_raw(),
                UiColor::from(crate::theme::rgb(layer.color)),
            )
            .ok()?;
            (drop.put_opacity)(shadow.as_raw(), crate::theme::alpha(layer.color) as f32).ok()?;
            (drop.put_offset)(
                shadow.as_raw(),
                Vector3 {
                    y: layer.y as f32,
                    ..Vector3::default()
                },
            )
            .ok()?;
            set_object(&shadow, drop.put_mask, &brush)?;
            // ICompositor 22 CreateSpriteVisual.
            let sprite: ISpriteVisual2 = get(base, base.vtable().create_sprite_visual)?.cast()?;
            set_object(
                &sprite,
                sprite.vtable().put_shadow,
                &shadow.cast::<ICompositionShadow>()?,
            )?;
            QA_LAYERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Layer {
                layer,
                sprite: sprite.cast()?,
                geometry,
                mask: mask.cast()?,
                surface,
            })
        }
    }
}

/// One brand-book layer's visuals, kept to follow the element's size.
struct Layer {
    layer: brand::Shadow,
    sprite: IVisual,
    geometry: IRoundedRectangleGeometry,
    mask: IVisual,
    surface: IVisualSurface,
}

impl Layer {
    /// Size the layer to an element of `width` × `height` at `radius`:
    /// the CSS spread grows the rectangle and its corner on every side.
    #[allow(
        unsafe_code,
        reason = "Sizes the layer through hand-declared Composition setters."
    )]
    fn fit(&self, width: f64, height: f64, radius: f64) -> windows_core::Result<()> {
        let spread = self.layer.spread;
        let size = Vector2 {
            x: (width + 2.0 * spread).max(0.0) as f32,
            y: (height + 2.0 * spread).max(0.0) as f32,
        };
        let corner = (radius + spread).max(0.0) as f32;
        // SAFETY: each call is the named slot of the interface whose
        // vtable declares it, with plain values.
        unsafe {
            (self.sprite.vtable().put_offset)(
                self.sprite.as_raw(),
                Vector3 {
                    x: -spread as f32,
                    y: -spread as f32,
                    z: 0.0,
                },
            )
            .ok()?;
            (self.sprite.vtable().put_size)(self.sprite.as_raw(), size).ok()?;
            (self.mask.vtable().put_size)(self.mask.as_raw(), size).ok()?;
            (self.geometry.vtable().put_size)(self.geometry.as_raw(), size).ok()?;
            (self.geometry.vtable().put_corner_radius)(
                self.geometry.as_raw(),
                Vector2 {
                    x: corner,
                    y: corner,
                },
            )
            .ok()?;
            (self.surface.vtable().put_source_size)(self.surface.as_raw(), size).ok()
        }
    }
}


// QA-1011 instrumentation (not committed).
pub static QA_LAYERS: std::sync::atomic::AtomicIsize = std::sync::atomic::AtomicIsize::new(0);
pub static QA_STATES: std::sync::atomic::AtomicIsize = std::sync::atomic::AtomicIsize::new(0);
fn qa_close_enabled() -> bool {
    std::env::var_os("ELIOT_QA_CLOSE").is_some()
}
#[repr(C)]
struct ClosableVtbl {
    base: InspectableSlots,
    close: unsafe extern "system" fn(Raw) -> HRESULT,
}
winrt_interface!(
    IClosable,
    ClosableVtbl,
    GUID::from_u128(0x30d5a829_7fa4_4026_83bb_d75bae4ea99e)
);
#[allow(unsafe_code, reason = "QA")]
fn qa_close<T: Interface>(object: &T) {
    if let Ok(c) = object.cast::<IClosable>() {
        let _ = unsafe { (c.vtable().close)(c.as_raw()) };
    }
}
fn qa_log(what: &str) {
    crate::startup::note(&format!(
        "qa-1011 shadow {what}: layers={} states={}",
        QA_LAYERS.load(std::sync::atomic::Ordering::SeqCst),
        QA_STATES.load(std::sync::atomic::Ordering::SeqCst)
    ));
}
impl Drop for Layer {
    fn drop(&mut self) {
        QA_LAYERS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if qa_close_enabled() {
            qa_close(&self.sprite);
            qa_close(&self.mask);
            qa_close(&self.surface);
            qa_close(&self.geometry);
        }
    }
}
impl Drop for State {
    fn drop(&mut self) {
        QA_STATES.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.drawn.clear();
        qa_log("state dropped");
    }
}

struct State {
    radius: f64,
    layers: &'static [brand::Shadow],
    compositor: Option<Compositor>,
    width: f64,
    height: f64,
    drawn: Vec<Layer>,
}

thread_local! {
    /// A failure is noted once per process, not once per frame.
    static NOTED: Cell<bool> = const { Cell::new(false) };
}

fn note(error: &windows_core::Error) {
    if !NOTED.with(|noted| noted.replace(true)) {
        crate::startup::note(&format!(
            "a Composition shadow was left out: {error} ({:#010X})",
            error.code().0
        ));
    }
}

impl State {
    /// Build the layers for the host (`None`: nothing to draw).
    #[allow(
        unsafe_code,
        reason = "Adds the layers through hand-declared Composition slots."
    )]
    fn build(&mut self) -> windows_core::Result<Option<IUnknown>> {
        self.drawn.clear();
        let Some(compositor) = &self.compositor else {
            return Ok(None);
        };
        if self.layers.is_empty() {
            return Ok(None);
        }
        let root = compositor.container()?;
        // SAFETY: slot 6 of IContainerVisual is get_Children.
        let children: IVisualCollection = unsafe { get(&root, root.vtable().children) }?.cast()?;
        for layer in self.layers {
            let layer = compositor.layer(*layer)?;
            layer.fit(self.width, self.height, self.radius)?;
            // SAFETY: slot 9 of IVisualCollection is InsertAtTop(Visual).
            unsafe { set_object(&children, children.vtable().insert_at_top, &layer.sprite) }?;
            self.drawn.push(layer);
        }
        qa_log("built");
        Ok(Some(root.cast()?))
    }

    fn fit(&self) -> windows_core::Result<()> {
        for layer in &self.drawn {
            layer.fit(self.width, self.height, self.radius)?;
        }
        Ok(())
    }
}

/// A real shadow under one element. Put [`ShadowHost::view`] in the
/// element's cell, before the element, so the host takes its size; the
/// layers ([`ShadowHost::set_layers`]) follow the theme.
pub struct ShadowHost {
    host: ElementRef<Grid>,
    state: Rc<RefCell<State>>,
    _observation: ElementObservation,
}

impl ShadowHost {
    /// A host for an element whose corner is `radius`, drawing `layers`.
    pub fn new(radius: f64, layers: &'static [brand::Shadow]) -> Self {
        let host = ElementRef::<Grid>::new();
        QA_STATES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let state = Rc::new(RefCell::new(State {
            radius,
            layers,
            compositor: None,
            width: 0.0,
            height: 0.0,
            drawn: Vec::new(),
        }));
        let observation = {
            let state = Rc::clone(&state);
            let target = host.clone();
            host.observe_composition_host(move |event| match event {
                CompositionHostEvent::Ready {
                    compositor,
                    width,
                    height,
                    ..
                } => {
                    let built = {
                        let mut state = state.borrow_mut();
                        state.width = width;
                        state.height = height;
                        Compositor::new(&compositor).and_then(|compositor| {
                            state.compositor = Some(compositor);
                            state.build()
                        })
                    };
                    match built {
                        Ok(visual) => show(&target, visual),
                        Err(error) => note(&error),
                    }
                }
                CompositionHostEvent::Metrics { width, height, .. } => {
                    let mut state = state.borrow_mut();
                    state.width = width;
                    state.height = height;
                    if let Err(error) = state.fit() {
                        note(&error);
                    }
                }
            })
        };
        Self {
            host,
            state,
            _observation: observation,
        }
    }

    /// Draw `layers` from now on (a theme change); empty draws nothing.
    pub fn set_layers(&self, layers: &'static [brand::Shadow]) {
        let built = {
            let mut state = self.state.borrow_mut();
            if std::ptr::eq(state.layers, layers) {
                return;
            }
            state.layers = layers;
            if state.compositor.is_none() {
                return;
            }
            state.build()
        };
        match built {
            Ok(visual) => show(&self.host, visual),
            Err(error) => note(&error),
        }
    }

    /// The host: an empty grid in the element's cell, under it.
    pub fn view(&self) -> Grid {
        Grid::new().element_ref(&self.host)
    }
}

/// Hand the host its child visual (`None` removes it).
fn show(host: &ElementRef<Grid>, visual: Option<IUnknown>) {
    let _bound = host.request_set_child_visual(visual, |result| {
        if let Err(error) = result {
            crate::startup::note(&format!("a Composition shadow was not shown: {error:?}"));
        }
    });
}
