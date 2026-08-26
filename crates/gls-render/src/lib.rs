//! gls-render: animation engine and 3form renderer.
//!
//! Consumes the pipeline event stream and drives the terminal presentation.

mod dimension;
mod format;
mod grid;
mod queue;
mod renderer;
mod state;

pub use format::human_bytes;
pub use renderer::{OutputMode, RenderConfig, Renderer};
pub use state::EntryDisplayState;
