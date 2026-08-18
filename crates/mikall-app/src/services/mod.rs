//! Use-case services — the driving ports of the hexagon.
//!
//! Both frontends (iced GUI, IRC gateway) call these and only these; the
//! BDD suite drives them over in-memory adapters.

mod calls;
mod chat;
mod dm;
mod identity;
mod presence;
mod router;
mod transfer;

pub use calls::{CallService, CallServiceError, CallSnapshot};
pub use chat::{ChatError, ChatService, MemberView, RenderedMessage};
pub use dm::{DmError, DmService};
pub use identity::{IdentityService, IdentityServiceError, Profile};
pub use presence::{PresenceError, PresenceService};
pub use router::InboundRouter;
pub use transfer::{TransferProgress, TransferService, TransferServiceError};
