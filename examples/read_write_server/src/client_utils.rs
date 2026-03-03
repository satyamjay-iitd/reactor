use reactor_actor::{Msg, RouteTo};
use std::marker::PhantomData;

#[derive(Default, Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct GeneratorOut;

// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
pub struct ClientSender<R> {
    server_addr: String,
    response: PhantomData<R>,
}

impl<R: Msg> reactor_actor::ActorSend for ClientSender<R> {
    type OMsg = R;

    fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        RouteTo::from(self.server_addr.as_str())
    }
}

impl<R> ClientSender<R> {
    pub fn new(server_addr: String) -> Self {
        ClientSender {
            server_addr,
            response: PhantomData,
        }
    }
}
