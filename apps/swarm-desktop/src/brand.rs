//! The brand book "Eliot Swarm Material" as constants: the six themes'
//! color roles and shadows, and the size, shape, spacing, state, motion,
//! icon and type tokens, generated at build time from `design/tokens.json`
//! (`build_brand.rs`). `ui::tokens` names what the views use; `theme`
//! builds the palettes from [`SCHEMES`].

include!(concat!(env!("OUT_DIR"), "/brand.rs"));
