use crate::SLEEP_MS;
use crate::common::{AcceptorAddr, Ballot, LeaderAddr, PaxosMsg, Val};
use log::info;
use rand::random;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction};
use std::borrow::Cow;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

struct BallotIterator {
    ballot: Ballot,
    addr: &'static str,
}

impl BallotIterator {
    fn new(addr: &'static str) -> Self {
        Self {
            ballot: Ballot(1),
            addr,
        }
    }
}

impl Iterator for BallotIterator {
    type Item = PaxosMsg;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_millis(10 * SLEEP_MS));

        if self.ballot != Ballot::MAX {
            self.ballot.0 += 1;
            Some(PaxosMsg::A1(LeaderAddr::from(self.addr), self.ballot))
        } else {
            None
        }
    }
}

struct Sender {
    addr: &'static str,
    acceptors: Vec<AcceptorAddr>,
}

impl Sender {
    fn new(addr: &'static str, acceptors: Vec<AcceptorAddr>) -> Self {
        Self { addr, acceptors }
    }
}
impl reactor_actor::ActorSend for Sender {
    type OMsg = PaxosMsg;

    async fn before_send<'a>(&'a mut self, output: &Self::OMsg) -> RouteTo<'a> {
        info!("{} Sending {:?}", self.addr, output);
        RouteTo::Multiple(Cow::Borrowed(&self.acceptors))
    }
}

struct Leader {
    addr: &'static str,
    acceptors: Vec<AcceptorAddr>,
    ballot: Ballot,
    max_val: HashMap<AcceptorAddr, Option<Val>>,
    proposed_val: Option<Val>,
    vote_count: u8,
    consensus_val: Option<Val>,
}
impl Leader {
    fn new(addr: &'static str, acceptors: Vec<AcceptorAddr>) -> Self {
        Self {
            addr,
            acceptors,
            ballot: Ballot::MIN,
            max_val: HashMap::new(),
            proposed_val: None,
            vote_count: 0,
            consensus_val: None,
        }
    }

    fn propose(&mut self) -> Vec<PaxosMsg> {
        info!(
            "Before self.max_val: {:?}, self.proposed_val: {:?}",
            self.max_val, self.proposed_val
        );
        if self.proposed_val.is_none() && 2 * self.max_val.len() > self.acceptors.len() {
            // We have heard back from the majority.
            let mut val_counts: HashMap<Option<Val>, u8> = HashMap::new();
            for v in self.max_val.values() {
                val_counts.entry(*v).and_modify(|c| *c += 1).or_insert(1);
            }
            for (v, c) in val_counts {
                if 2 * c > self.acceptors.len() as u8 {
                    // Majority agrees on a value! We can propose now!
                    self.proposed_val = Some(v.unwrap_or(random()));
                    break;
                }
            }
        }
        info!("After self.proposed_val: {:?}", self.proposed_val);

        if let Some(val) = self.proposed_val {
            // Proposed value was decided. Propose!
            vec![PaxosMsg::A2(LeaderAddr::from(self.addr), self.ballot, val)]
        } else {
            vec![]
        }
    }
}

impl reactor_actor::ActorProcess for Leader {
    type IMsg = PaxosMsg;
    type OMsg = PaxosMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        info!("{} received {:?}", self.addr, input);
        thread::sleep(Duration::from_millis(SLEEP_MS));
        match input {
            PaxosMsg::A1(l, b) => {
                self.ballot = b;
                self.vote_count = 0;
                self.max_val = HashMap::new();
                self.proposed_val = None;
                if self.consensus_val.is_none() {
                    // Start a ballot if we have not reached consensus
                    vec![PaxosMsg::A1(l, b)]
                } else {
                    info!(
                        "{} ignoring A1 message since consensus is reached on value {}",
                        self.addr,
                        self.consensus_val.unwrap()
                    );
                    vec![]
                }
            }
            PaxosMsg::B1(acceptor, leader, b, last_ballot, last_val) => {
                assert_eq!(leader, self.addr);
                if self.ballot == b {
                    if let Some(ballot) = last_ballot {
                        if ballot > self.ballot {
                            info!(
                                "Got a higher ballot {ballot} from {acceptor} than my current ballot {}",
                                self.ballot
                            );
                            return vec![];
                        }
                    }
                    self.max_val.insert(acceptor, last_val);
                }
                self.propose()
            }
            PaxosMsg::B2(_acceptor, leader, b, val) => {
                assert_eq!(leader, self.addr);
                if self.ballot == b {
                    assert_eq!(
                        val,
                        self.proposed_val.unwrap(),
                        "Acceptor voted for {val} but I had proposed {}",
                        self.proposed_val.unwrap()
                    );
                    self.vote_count += 1;
                }
                if 2 * self.vote_count > self.acceptors.len() as u8 {
                    info!("{} Reached consensus on value {val}!", self.addr);
                    // TODO: exit. We have reached consensus.
                    self.consensus_val = Some(val);
                }
                vec![]
            }
            _ => {
                panic!("Unexpected message")
            }
        }
    }
}

pub async fn actor(ctx: RuntimeCtx, acceptors: Vec<AcceptorAddr>) {
    BehaviourBuilder::new(
        Leader::new(ctx.addr, acceptors.clone()),
        BincodeCodec::default(),
    )
    .send(Sender::new(ctx.addr, acceptors))
    .generator(BallotIterator::new(ctx.addr))
    .on_send_failure(SendErrAction::Drop) // Retry up to 10 times with 100ms delay
    .build()
    .run(ctx)
    .await
    .unwrap();
}
