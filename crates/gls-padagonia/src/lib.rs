//! Padagonia: persistent contextual history for `gls`.
//!
//! Every contextualised object gets a record.  When the object changes, a new
//! revision is appended rather than overwriting, enabling historical queries.

mod store;
pub use store::{CachedModel, Padagonia, PadagoniaConfig, SizeAggregateRecord};
