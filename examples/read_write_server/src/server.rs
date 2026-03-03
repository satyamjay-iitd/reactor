use crate::reader::{ReadAck, ReadOut};
use crate::writer::{WriteAck, WriteOut};
use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RuntimeCtx};
use reactor_actor::{RouteTo, SubDecoderStore};
use reactor_macros::msg_converter;

msg_converter! {
   Unions: [
       ServerIn = ReadOut, WriteOut;
       ServerOut = ReadAck, WriteAck;
   ];
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Server;

impl reactor_actor::ActorProcess for Server {
    type IMsg = ServerIn;
    type OMsg = ServerOut;

    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        match input {
            ServerIn::ReadOut(_) => vec![ServerOut::ReadAck(ReadAck)],
            ServerIn::WriteOut(_) => vec![ServerOut::WriteAck(WriteAck)],
        }
    }
}

impl ServerSender {
    fn new() -> Self {
        ServerSender {}
    }
}

struct ServerSender;

impl reactor_actor::ActorSend for ServerSender {
    type OMsg = ServerOut;

    fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        RouteTo::Reply
    }
}

pub(crate) async fn server(ctx: RuntimeCtx, decoder: SubDecoderStore<ServerIn>) {
    BehaviourBuilder::new(Server {}, BincodeCodec::default())
        .send(ServerSender::new())
        .sub_decoders(decoder)
        .ask_receiver_to_adapt()
        .build()
        .run(ctx)
        .await
        .unwrap();
}
