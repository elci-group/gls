//! gls-provider: the inference-provider catalogue, live model-availability
//! polling, and a persistent TTL cache in front of it.

mod cache;
mod catalog;
mod poll;

pub use cache::{new_client, resolve_models, MenuSource, ModelMenu, DEFAULT_TTL_SECS};
pub use catalog::{Provider, ProviderKind, PROVIDERS};
pub use poll::{efficiency_score, ModelInfo, PollError};
