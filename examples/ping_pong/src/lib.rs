pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};

use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, actor};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::time::Duration;

use log::info;
#[cfg(feature = "chaos")]
use rand::random;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum PingPongMsg {
    Ping,
    Pong,
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Processor;
impl reactor_actor::ActorProcess for Processor {
    type IMsg = PingPongMsg;
    type OMsg = PingPongMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        std::thread::sleep(Duration::from_secs(1));
        info!("{input:?}");
        match input {
            PingPongMsg::Ping => vec![PingPongMsg::Pong],
            PingPongMsg::Pong => vec![PingPongMsg::Ping],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
struct Sender {
    other_addr: String,

    #[cfg(feature = "chaos")]
    drop: Vec<ActorAddrRef>,
}
impl reactor_actor::ActorSend for Sender {
    type OMsg = PingPongMsg;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        #[cfg(feature = "chaos")]
        {
            let b: bool = random();
            if b {
                warn!("Chaos! Dropping");
                return &self.drop;
            }
        }
        RouteTo::from(self.other_addr.as_str())
    }
}
impl Sender {
    fn new(other_actor: String) -> Self {
        Sender {
            other_addr: other_actor,
            #[cfg(feature = "chaos")]
            drop: vec![],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[actor]
pub fn pingpong(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let other_addr: String = payload
        .remove("other")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    RUNTIME.spawn(async move {
        BehaviourBuilder::new(Processor {}, BincodeCodec::default())
            .send(Sender::new(other_addr))
            .generator_if(ctx.addr == "pinger", || vec![PingPongMsg::Ping].into_iter())
            .build()
            .run(ctx)
            .await
            .unwrap();
    });
}
