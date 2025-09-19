use bincode::{Decode, Encode};
use log::info;
use reactor_actor::codec::BincodeCodec;
pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, actor};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::time::Duration;
use std::vec;

type Data = char;
type Bit = bool;

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
enum ABMsg {
    Write(Data, Bit),
    Ack(Bit),
    GeneratorMsg,
}

struct GeneratorIter {
    current: u8,
}
impl GeneratorIter {
    const MIN: u8 = 0;
    const MAX: u8 = 25;

    fn new() -> Self {
        GeneratorIter {
            current: GeneratorIter::MIN,
        }
    }
}
impl Iterator for GeneratorIter {
    type Item = ABMsg;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_secs(4));

        if self.current <= GeneratorIter::MAX {
            self.current += 1;
            Some(ABMsg::GeneratorMsg)
        } else {
            None
        }
    }
}

struct Writer {
    data: Data,
    bit: Bit,
}
impl Writer {
    fn new() -> Self {
        Writer {
            data: 'A',
            bit: true,
        }
    }
}

impl reactor_actor::ActorProcess for Writer {
    type IMsg = ABMsg;
    type OMsg = ABMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            ABMsg::GeneratorMsg => {
                let msg = ABMsg::Write(self.data, self.bit);
                info!("Writer: Sent: {msg:?}");
                vec![msg]
            }
            ABMsg::Ack(bit) => {
                info!("Writer: Recv: {input:?}");
                if bit == self.bit {
                    self.data = ((self.data as u8) + 1) as char;
                    self.bit = !self.bit;
                }
                vec![]
            }
            _ => panic!("Unexpected message at writer"),
        }
    }
}

struct Reader {
    data: Data,
    bit: Bit,
}
impl Reader {
    fn new() -> Self {
        Reader {
            data: 'A',
            bit: true,
        }
    }
}

impl reactor_actor::ActorProcess for Reader {
    type IMsg = ABMsg;
    type OMsg = ABMsg;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            ABMsg::GeneratorMsg => {
                let msg = ABMsg::Ack(self.bit);
                info!("Reader: Sent: {msg:?}");
                vec![msg]
            }
            ABMsg::Write(data, bit) => {
                info!("Reader: Recv: {input:?}");
                self.bit = bit;
                self.data = data;
                vec![]
            }
            _ => panic!("Unexpected message at reader"),
        }
    }
}

struct Sender {
    other_addr: String,
}
impl reactor_actor::ActorSend for Sender {
    type OMsg = ABMsg;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        RouteTo::from(self.other_addr.as_str())
    }
}
impl Sender {
    fn new(other_actor: String) -> Self {
        Sender {
            other_addr: other_actor,
        }
    }
}

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[actor]
pub fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let other_addr: String = payload
        .remove("other")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    RUNTIME.spawn(async move {
        BehaviourBuilder::new(Writer::new(), BincodeCodec::default())
            .send(Sender::new(other_addr))
            .generator(GeneratorIter::new())
            .build()
            .run(ctx)
            .await
            .unwrap();
    });
}

#[actor]
pub fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let other_addr: String = payload
        .remove("other")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    std::thread::sleep(Duration::from_secs(2));
    RUNTIME.spawn(async move {
        BehaviourBuilder::new(Reader::new(), BincodeCodec::default())
            .send(Sender::new(other_addr))
            .generator(GeneratorIter::new())
            .build()
            .run(ctx)
            .await
            .unwrap();
    });
}
