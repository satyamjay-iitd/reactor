use std::thread;
use std::time::Duration;

use crate::SLEEP_MS;
use crate::common::{AcceptorAddr, Ballot, PaxosMsg, Val};
use log::info;
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, SendErrAction};

struct Sender {
    addr: &'static str,
}

impl reactor_actor::ActorSend for Sender {
    type OMsg = PaxosMsg;

    async fn before_send<'a>(&'a mut self, output: &Self::OMsg) -> RouteTo<'a> {
        info!("{} sending {:?}", self.addr, output);
        RouteTo::Reply
    }
}

struct Acceptor {
    addr: &'static str,
    max_ballot: Ballot,
    last_value: Option<Val>,
    last_ballot: Option<Ballot>,
}

impl Acceptor {
    fn new(addr: &'static str) -> Self {
        Acceptor {
            addr,
            max_ballot: Ballot::MIN,
            last_value: None,
            last_ballot: None,
        }
    }
}
impl reactor_actor::ActorProcess for Acceptor {
    type IMsg = PaxosMsg;
    type OMsg = PaxosMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        info!("{} received {:?}", self.addr, input);
        thread::sleep(Duration::from_millis(SLEEP_MS));
        match input {
            PaxosMsg::A1(leader, ballot) => {
                if ballot > self.max_ballot {
                    self.max_ballot = ballot;
                    return vec![PaxosMsg::B1(
                        AcceptorAddr::from(self.addr),
                        leader,
                        ballot,
                        self.last_ballot,
                        self.last_value,
                    )];
                }
                info!("Ignoring A1 message. My max_ballot {}", self.max_ballot);
                vec![]
            }
            PaxosMsg::A2(leader, ballot, value) => {
                if ballot >= self.max_ballot {
                    self.max_ballot = ballot;
                    self.last_ballot = Some(ballot);
                    self.last_value = Some(value);
                    return vec![PaxosMsg::B2(
                        AcceptorAddr::from(self.addr),
                        leader,
                        ballot,
                        value,
                    )];
                }
                info!("Ignoring A2 message. My max_ballot {}", self.max_ballot);
                vec![]
            }
            _ => {
                panic!("Unexpected message")
            }
        }
    }
}

pub async fn acceptor(ctx: RuntimeCtx) {
    BehaviourBuilder::new(Acceptor::new(ctx.addr), BincodeCodec::default())
        .send(Sender { addr: ctx.addr })
        .on_send_failure(SendErrAction::Retry {
            attempts: Some(5),
            delay_ms: 100,
        })
        .build()
        .run(ctx)
        .await
        .unwrap();
}
