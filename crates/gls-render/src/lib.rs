//! gls-render: animation engine and 3form renderer.
//!
//! Consumes the pipeline event stream and drives the terminal presentation.

mod state;
mod renderer;
mod format;

pub use renderer::{Renderer, RenderConfig, OutputMode};
pub use state::EntryDisplayState;
