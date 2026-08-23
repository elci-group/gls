//! DISCOVER phase: fast, deterministic, zero-inference filesystem enumeration.

mod walk;
pub use walk::{discover, DiscoverConfig};
