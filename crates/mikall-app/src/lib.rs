//! Application layer: use-case services and ports.
//!
//! This crate is the seam of the hexagon. Driving adapters (the iced GUI and
//! the IRC gateway) call the services in [`services`]; driven adapters
//! (libp2p transport, redb store, crypto, media) implement the traits in
//! [`ports`]. The BDD suite drives the same services over in-memory fakes —
//! if a behavior isn't reachable through this crate, it doesn't exist.

pub mod events;
pub mod ports;
pub mod services;

pub use events::{AppEvent, EventBus};
