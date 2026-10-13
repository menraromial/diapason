//! diapason-core: the protocol-agnostic part of the diapason load generator.
//!
//! Build a [`RunConfig`], pick a [`Protocol`] implementation, and run it with
//! a [`Runner`]:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # struct MyProtocol;
//! # impl diapason_core::Protocol for MyProtocol {
//! #     type Writer = W; type Reader = R;
//! #     async fn connect(&self) -> Result<(W, R), diapason_core::ConnectErrorKind> { unimplemented!() }
//! #     fn describe(&self) -> String { String::new() }
//! # }
//! # struct W; struct R;
//! # impl diapason_core::RequestWriter for W {
//! #     async fn write_request(&mut self, _: &diapason_core::RequestCtx) -> Result<(), diapason_core::FailKind> { Ok(()) }
//! # }
//! # impl diapason_core::ResponseReader for R {
//! #     async fn read_response(&mut self) -> Result<Option<diapason_core::Response>, diapason_core::FailKind> { Ok(None) }
//! # }
//! use diapason_core::{RunConfig, Runner};
//!
//! let cfg = RunConfig::from_toml(&std::fs::read_to_string("run.toml")?)?;
//! let (runner, windows) = Runner::new(cfg, MyProtocol).window_channel();
//! let handle = runner.spawn();
//! for w in windows {
//!     if w.completed == 0 && w.index > 10 {
//!         handle.stop(); // ends the schedule now; the drain follows
//!     }
//! }
//! let summary = handle.join()?;
//! assert!(summary.conservation.ok);
//! # Ok(())
//! # }
//! ```
//!
//! # Stability
//!
//! The public API is what this crate root exports and the public modules
//! listed below. Within a 0.x version, a minor version change may break it; a
//! patch version does not. The JSON summary carries its own `schema_version`,
//! raised whenever a field changes meaning or disappears; new fields may be
//! added without raising it. The accounting model and every field are
//! described in the repository README.

pub mod clock;
pub mod config;
pub mod dtr;
pub mod duration;
pub mod hist;
pub mod host;
pub mod protocol;
pub mod report;
pub mod summary;
pub mod template;
pub mod threshold;

mod dist;
mod engine;
mod fidelity;
mod live;
mod schedule;
mod stats;
mod timer;
mod trace;
mod window;
mod worker;

pub use config::{ConfigError, RunConfig};
pub use dist::{serve_agent, Coordinator};
pub use duration::Dur;
pub use engine::{RunError, RunHandle, Runner};
pub use protocol::{
    ConnectErrorKind, FailKind, Protocol, RequestCtx, RequestWriter, Response, ResponseReader,
};
pub use summary::{exit, AgentSummary, Distributed, Summary};
pub use window::{SliceStats, WindowSnapshot};
pub use worker::spawn_task;
