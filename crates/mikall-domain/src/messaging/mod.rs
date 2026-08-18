//! Messaging bounded context: channels, DMs, and the causally ordered
//! message DAG that replaces a server's authoritative history.

mod channel;
mod dag;
mod dm;
mod message;
mod values;

pub use channel::{Channel, ChannelError, ChannelId, ChannelVisibility, Role};
pub use dag::{DagInsert, MessageDag};
pub use dm::{DmKey, DmKeyError, DmThread};
pub use message::{Message, MessageId, Verified};
pub use values::{
    ChannelName, ChannelNameError, IrcSafeLine, MessageBody, MessageBodyError, Nickname,
    NicknameError, Topic, TopicError,
};

use crate::shared::IdentityId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagingEvent {
    ChannelCreated {
        channel: ChannelId,
        name: ChannelName,
    },
    ChannelJoined {
        channel: ChannelId,
        who: IdentityId,
    },
    ChannelLeft {
        channel: ChannelId,
        who: IdentityId,
    },
    MessagePosted {
        channel: ChannelId,
        message: MessageId,
    },
    MessageReceived {
        channel: ChannelId,
        message: MessageId,
    },
    TopicChanged {
        channel: ChannelId,
        by: IdentityId,
        topic: Topic,
    },
    MemberRoleChanged {
        channel: ChannelId,
        member: IdentityId,
        role: Role,
    },
    DmReceived {
        from: IdentityId,
        message: MessageId,
    },
    /// A received message referenced parents we do not have yet; sync should
    /// backfill them.
    HistoryGapDetected {
        channel: ChannelId,
        missing: Vec<MessageId>,
    },
}
