use bincode::{Decode, Encode};
use log::info;
use reactor_actor::codec::BincodeCodec;
pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx, actor};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use std::collections::HashMap;
use std::time::Duration;
use std::vec;

#[derive(Encode, Decode, Debug, Clone, Copy)]
struct Data(char);
#[derive(Encode, Decode, Debug, Clone, Eq, PartialEq, Copy)]
struct Bit(bool);

impl Data {
    const MIN: Data = Data('A');
    const MAX: Data = Data('Z');
}

impl Iterator for Data {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0 < Data::MAX.0 {
            self.0 = ((self.0 as u8) + 1) as char;
        } else {
            self.0 = Data::MIN.0;
        }
        Some(*self)
    }
}

impl Bit {
    const INIT: Bit = Bit(true);

    fn negate(&mut self) -> Bit {
        self.0 = !self.0;
        *self
    }
}

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
enum ABMsg {
    Write(Data, Bit),
    Ack(Bit),
    GeneratorMsg,
}

struct GeneratorIter;
impl Iterator for GeneratorIter {
    type Item = ABMsg;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_secs(4));
        Some(ABMsg::GeneratorMsg)
    }
}

struct Writer {
    data: Data,
    bit: Bit,
}
impl Writer {
    fn new() -> Self {
        Writer {
            data: Data::MIN,
            bit: Bit::INIT,
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
                    self.data.next();
                    self.bit.negate();
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
            data: Data::MIN,
            bit: Bit::INIT,
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
            .generator(GeneratorIter {})
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
            .generator(GeneratorIter {})
            .build()
            .run(ctx)
            .await
            .unwrap();
    });
}
