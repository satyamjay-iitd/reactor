use bincode::{Decode, Encode};
use derive_more::{Add, Display, From};
use reactor_actor::ActorAddr;
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};

#[derive(
    Encode, Decode, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Copy, Display, From, Add,
)]
pub struct Ballot(pub u8);
impl Ballot {
    pub const MIN: Ballot = Ballot(0);
    pub const MAX: Ballot = Ballot(255);
}

pub type Val = u8;
pub type LeaderAddr = ActorAddr;
pub type AcceptorAddr = ActorAddr;

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum PaxosMsg {
    // Leader starts a new ballot
    A1(LeaderAddr, Ballot),
    // Leader asks for votes in a ballot
    A2(LeaderAddr, Ballot, Val),
    // Acceptor responds for a ballot with the last value and ballot it voted for.
    B1(
        AcceptorAddr,
        LeaderAddr,
        Ballot,
        Option<Ballot>,
        Option<Val>,
    ),
    // Acceptor votes for the value in a ballot
    B2(AcceptorAddr, LeaderAddr, Ballot, Val),
}
