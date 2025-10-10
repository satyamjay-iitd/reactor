// #![feature(log_syntax)]

use std::{borrow::Cow, collections::HashMap, marker::PhantomData};

use bincode::{Decode, Encode};
use err::ActorError;
use futures::future::join_all;
pub use inventory as __inventory;
use reactor_channel::{ReactorChannelTx, reactor_channel};
use recv::rx;
use send::tx;
use tokio::{
    sync::mpsc::{self},
    task::JoinHandle,
};
use tokio_util::codec::{Decoder, Encoder};
pub use tracing_shared::setup_shared_logger_ref;

mod chaos_manager;
pub mod codec;
pub mod err;
mod node_comm;
mod reactor_channel;
mod recv;
mod send;

pub use node_comm::{Connection, ControlInst, ControlReq, NodeComm};
pub use reactor_channel::{HasPriority, MAX_PRIO};
pub use reactor_macros::actor;

use crate::codec::ErrWithMsg;

pub type ActorSpawnCB = fn(RuntimeCtx, HashMap<String, serde_json::Value>);

pub struct ExportedFn {
    pub name: &'static str,
    pub func: ActorSpawnCB,
}

__inventory::collect!(ExportedFn);

#[macro_export]
macro_rules! register_actor {
    ($fn_name:path) => {
        $crate::__inventory::submit! {
            $crate::ExportedFn {
                name: stringify!($fn_name),
                func: $fn_name,
            }
        }
    };
}

#[unsafe(no_mangle)]
fn get_registered() -> Vec<String> {
    inventory::iter::<ExportedFn>()
        .map(|x| x.name.to_string())
        .collect()
}

static CHANNEL_SIZE: usize = 1 << 20;
/// Messages that can flow between the actors.
pub trait Msg: Send + Sync + std::fmt::Debug + HasPriority + 'static + Clone {}

/// Addr of the actors
pub type ActorAddr = String;

/// Represents the routing target of a message in the system.
///
/// `RouteTo` is used to specify where a message should be sent. It supports
/// multiple routing strategies:
///
/// - `Blackhole`: Drop the message silently without sending it.
/// - `Reply`: Route the message back to the original sender (used in request-reply patterns).
/// - `Single`: Route the message to a single actor by address.
/// - `Multiple`: Route the message to multiple addresses of actors.
///
/// The `'a` lifetime allows `RouteTo` to borrow from external string data
/// when possible, avoiding allocations when routing with references.
pub enum RouteTo<'a> {
    /// Drop the message; it will not be sent to any actor.
    Blackhole,
    /// Send the message back to the sender.
    Reply,
    /// Route to a single actor by address (either borrowed or owned).
    Single(Cow<'a, str>),
    /// Route to multiple actors by addresses (either borrowed or owned).
    Multiple(Cow<'a, [String]>),
}

/// Converts a borrowed `&str` into a `RouteTo::Single` with borrowed data.
/// Usage:
///      RouteTo::from("addr".to_string());
impl From<String> for RouteTo<'_> {
    fn from(value: String) -> Self {
        RouteTo::Single(Cow::Owned(value))
    }
}

/// Converts a borrowed `&str` into a `RouteTo::Single` with borrowed data.
/// Usage:
///      RouteTo::from(&self.other_addr.as_str());    // other_addr: String
impl<'a> From<&'a str> for RouteTo<'a> {
    fn from(value: &'a str) -> Self {
        RouteTo::Single(Cow::Borrowed(value))
    }
}

/// Converts a `Vec<String>` into a `RouteTo::Multiple` with owned address list.
/// Usage:
///      let x = vec!["addr1".to_string];
///      RouteTo::from(x);
impl From<Vec<String>> for RouteTo<'_> {
    fn from(value: Vec<String>) -> Self {
        RouteTo::Multiple(Cow::Owned(value))
    }
}

/// Converts a borrowed slice of `String`s into a `RouteTo::Multiple`.
/// Usage:
///      RouteTo::from(self.addrs.as_slice());  // addrs: Vec<String>
impl<'a> From<&'a [String]> for RouteTo<'a> {
    fn from(value: &'a [String]) -> Self {
        RouteTo::Multiple(Cow::Borrowed(value))
    }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct EmptyMsg;

impl HasPriority for EmptyMsg {}
impl Msg for EmptyMsg {}

/// Represents the action to take after receiving and decoding a message from a channel.
///
/// This enum is used by the message receiver to determine
/// how to handle an incoming message based on its content and the current channel state.
///
/// # Variants
///
/// - `PASS`:
///   Forward the message to the next stage in the pipeline (e.g., processor).
///
/// - `PANIC`:
///   Indicates a critical error. Triggers a panic, typically used to fail fast on invalid or unexpected input.
//
/// - `DROP`:
///   Silently discard the message without processing or forwarding.
///
/// - `SYNC(u16)`:
///   Perform a synchronization operation with the provided sync identifier (e.g., a sync round or epoch number).
///
/// - `CLOSE`:
///   Signals that the channel should be closed and no more messages will be received from it.
#[derive(Debug)]
pub enum ChannelAction {
    PASS,
    PANIC,
    DROP,
    SYNC(u16),
    CLOSE,
}

#[derive(Debug)]
enum R2PMsg<T> {
    Msg(T, &'static str),
    Exit,
    #[allow(dead_code)]
    AddPrio(mpsc::Receiver<R2PMsg<T>>),
    #[allow(dead_code)]
    RemoveLowPrio,
    SetMsgDuplication {
        factor: u32,
        probability: f32,
    },
    SetMsgLoss {
        probability: f32,
    },
    UnsetMsgLoss,
    UnsetMsgDuplication,
}

impl<T: Clone> Clone for R2PMsg<T> {
    fn clone(&self) -> Self {
        match self {
            R2PMsg::Msg(m, origin) => R2PMsg::Msg(m.clone(), origin),
            _ => panic!("Shouldn't Clone this"),
        }
    }
}
impl<T: PartialEq> PartialEq for R2PMsg<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (R2PMsg::AddPrio(_), _) => panic!("Can't Compare"),
            (R2PMsg::Msg(m1, _), R2PMsg::Msg(m2, _)) => m1.eq(m2),
            (R2PMsg::Exit, R2PMsg::Exit) => true,
            _ => false,
        }
    }
}

impl<T: HasPriority> HasPriority for R2PMsg<T> {
    fn priority(&self) -> usize {
        match self {
            R2PMsg::Msg(t, _) => t.priority(),
            R2PMsg::Exit => MAX_PRIO,
            R2PMsg::AddPrio(_) => MAX_PRIO,
            R2PMsg::RemoveLowPrio => MAX_PRIO,
            R2PMsg::SetMsgLoss { .. } => MAX_PRIO,
            R2PMsg::SetMsgDuplication { .. } => MAX_PRIO,
            R2PMsg::UnsetMsgLoss => MAX_PRIO,
            R2PMsg::UnsetMsgDuplication => MAX_PRIO,
        }
    }
}

/// The `ActorRecv` trait defines what action to take based on incoming message.
///
/// It defines a single asynchronous method, [`ActorRecv::after_recv`], that is called after a message is received.
///
/// # Type Parameters
/// - `IMsg`: The type of the message this actor receives. It must implement the [`Msg`] trait.
///
pub trait ActorRecv: Send + 'static {
    /// The input message type that this actor receives.
    type IMsg: Msg;
    /// Called after the actor receives a message.
    ///
    /// This asynchronous method is invoked with:
    /// - `worker_id`: A reference to the address of the sending actor.
    /// - `input`: A reference to the message that was received.
    ///
    /// It returns [`ChannelAction`] that determines how the actor should proceed.
    fn after_recv(
        &mut self,
        worker_id: &str,
        input: &Self::IMsg,
    ) -> impl std::future::Future<Output = ChannelAction> + Send;
}

pub struct NoOpActorRecv<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorRecv for NoOpActorRecv<M> {
    type IMsg = M;
    async fn after_recv(&mut self, _addr: &str, _input: &Self::IMsg) -> ChannelAction {
        panic!("This Shouldn't be used")
    }
}

/// The `ActorProcess` trait defines the processing logic for an actor that transforms
/// input messages into one or more output messages.
///
/// # Example
/// ```ignore
/// struct Incrementer;
///
/// impl ActorProcess for Incrementer {
///     type IMsg = i32;
///     type OMsg = i32;
///
///     fn process(&mut self, input: i32) -> Vec<i32> {
///         vec![input + 1]
///     }
/// }
///
pub trait ActorProcess: Send + 'static {
    /// The type of messages this actor accepts as input.
    type IMsg: Msg;

    /// The type of messages this actor produces as output.
    type OMsg: Msg;

    /// Processes an input message and returns a list of output messages.
    ///
    /// # Arguments
    ///
    /// * `input` - The input message to be processed.
    ///
    /// # Returns
    ///
    /// A vector of output messages of type [`Self::OMsg`].
    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg>;
}

/// The `ActorSend` trait defines how an actor determines the recipients of a message
/// before it is sent.
///
/// # Example
/// ```ignore
/// struct Broadcaster {
///     peers: Vec<ActorAddrRef>,
/// }
///
/// impl ActorSend for Broadcaster {
///     type OMsg = MyMessage;
///
///     async fn before_send<'a>(
///         &'a mut self,
///         _output: &Self::OMsg,
///     ) -> &'a Vec<ActorAddrRef> {
///         &self.peers
///     }
/// }
///
pub trait ActorSend: Send + 'static {
    /// The type of output messages that this actor sends.
    type OMsg: Msg;

    /// Called before an output message is sent.
    ///
    /// This asynchronous method returns the list of recipients that the message should be sent to.
    ///
    /// # Arguments
    ///
    /// * `output` - A reference to the message that is about to be sent.
    ///
    /// # Returns
    ///
    /// a list of [`ActorAddrRef`] indicating the recipient actors.
    fn before_send<'a>(
        &'a mut self,
        output: &Self::OMsg,
    ) -> impl std::future::Future<Output = RouteTo<'a>> + Send;
}
pub struct NoOpActorSend<M> {
    m: PhantomData<M>,
}
impl<M: Msg> ActorSend for NoOpActorSend<M> {
    type OMsg = M;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        panic!("This Shouldn't be used")
    }
}

/// Defines the action to take when sending a message fails. Variants:
/// - Drop: drops the message.
/// - Retry: retries sending the message, with parameters:
///   - attempts: maximum number of retry attempts (None for infinite retries).
///   - delay_ms: delay in milliseconds between retry attempts.
#[derive(Copy, Clone, Debug)]
pub enum SendErrAction {
    Drop,
    Retry {
        attempts: Option<u16>,
        delay_ms: u64,
    },
}

/// The `Behaviour` struct encapsulates the complete behavior of an actor,
/// including how it receives messages, processes them, sends output,
/// and optionally generates new messages.
///
/// # Type Parameters
///
/// - `R`: The type implementing the receiving behavior (must implement [`ActorRecv`]).
/// - `P`: The type implementing the processing behavior (must implement [`ActorProcess`]).
/// - `S`: The type implementing the sending behavior (must implement [`ActorSend`]).
/// - `M`: The type of message generated internally (e.g., from generators).
///
/// # Fields
///
/// - `recv`: Optional receiver logic implementing `ActorRecv`.
/// - `proc`: The core processing logic implementing `ActorProcess`.
/// - `send`: Optional sender logic implementing `ActorSend`.
/// - `generators`: A list of internal message generators, producing messages of type `M`.
/// - `on_send_failure`: Action to take when sending a message fails. Defaults to retrying 5 times with 100ms delay.
///
pub struct Behaviour<R, P, S, M: 'static, MCD> {
    recv: Option<R>,
    proc: P,
    send: Option<S>,
    generators: Vec<Box<dyn Iterator<Item = M> + Send>>,
    num_prios: u8,
    master_codec: MCD,
    sub_decoders: Option<SubDecoderStore<M>>,
    receiver_should_adapt: bool,
    on_send_failure: SendErrAction,
}

pub struct DecoderProvider<M> {
    pub decoder_cons:
        fn() -> Box<dyn tokio_util::codec::Decoder<Item = M, Error = std::io::Error> + Sync + Send>,
    pub any_to_m: fn(Box<dyn std::any::Any>) -> M,
}
pub type SubDecoderStore<M> = fn(&str) -> Option<DecoderProvider<M>>;

pub struct BehaviourBuilder<R, P, S, IM: 'static, OM, MCD> {
    recv: Option<R>,
    proc: P,
    send: Option<S>,
    generators: Vec<Box<dyn Iterator<Item = IM> + Send>>,
    num_prios: u8,
    master_codec: MCD,
    sub_decoders: Option<SubDecoderStore<IM>>,
    ask_recver_to_adapt: bool,
    m: PhantomData<OM>,
    on_send_failure: SendErrAction,
}

impl<P, IM, OM, MCD> BehaviourBuilder<NoOpActorRecv<IM>, P, NoOpActorSend<OM>, IM, OM, MCD> {
    pub fn new(proc: P, master_codec: MCD) -> Self {
        BehaviourBuilder {
            recv: None,
            proc,
            send: None,
            generators: vec![],
            num_prios: 1,
            m: PhantomData,
            master_codec,
            sub_decoders: None,
            ask_recver_to_adapt: false,
            on_send_failure: SendErrAction::Retry {
                attempts: Some(5),
                delay_ms: 100,
            },
        }
    }
}

impl<R, P, S, IM, OM, MCD> BehaviourBuilder<R, P, S, IM, OM, MCD> {
    pub fn recv<R1>(self, recv: R1) -> BehaviourBuilder<R1, P, S, IM, OM, MCD>
    where
        R1: ActorRecv<IMsg = IM>,
    {
        BehaviourBuilder {
            recv: Some(recv),
            proc: self.proc,
            send: self.send,
            generators: self.generators,
            num_prios: self.num_prios,
            m: self.m,
            master_codec: self.master_codec,
            sub_decoders: self.sub_decoders,
            ask_recver_to_adapt: self.ask_recver_to_adapt,
            on_send_failure: self.on_send_failure,
        }
    }
    pub fn send<S1>(self, send: S1) -> BehaviourBuilder<R, P, S1, IM, OM, MCD>
    where
        S1: ActorSend<OMsg = OM>,
    {
        BehaviourBuilder {
            recv: self.recv,
            proc: self.proc,
            send: Some(send),
            generators: self.generators,
            num_prios: self.num_prios,
            m: self.m,
            master_codec: self.master_codec,
            sub_decoders: self.sub_decoders,
            ask_recver_to_adapt: self.ask_recver_to_adapt,
            on_send_failure: self.on_send_failure,
        }
    }

    pub fn generator<I>(mut self, generator: I) -> BehaviourBuilder<R, P, S, IM, OM, MCD>
    where
        I: Iterator<Item = IM> + Send + 'static,
    {
        self.generators.push(Box::new(generator));
        self
    }

    pub fn generator_if<I, F>(
        mut self,
        condition: bool,
        generator_creator: F,
    ) -> BehaviourBuilder<R, P, S, IM, OM, MCD>
    where
        I: Iterator<Item = IM> + Send + 'static,
        F: FnOnce() -> I,
    {
        if condition {
            self.generators.push(Box::new(generator_creator()));
        }
        self
    }

    pub fn num_prios(mut self, num: u8) -> BehaviourBuilder<R, P, S, IM, OM, MCD> {
        self.num_prios = num;
        self
    }

    pub fn ask_receiver_to_adapt(mut self) -> BehaviourBuilder<R, P, S, IM, OM, MCD> {
        self.ask_recver_to_adapt = true;
        self
    }

    pub fn sub_decoders(
        mut self,
        decoders: SubDecoderStore<IM>,
    ) -> BehaviourBuilder<R, P, S, IM, OM, MCD> {
        self.sub_decoders = Some(decoders);
        self
    }

    pub fn on_send_failure(
        mut self,
        action: SendErrAction,
    ) -> BehaviourBuilder<R, P, S, IM, OM, MCD> {
        self.on_send_failure = action;
        self
    }

    pub fn build(self) -> Behaviour<R, P, S, IM, MCD> {
        Behaviour {
            recv: self.recv,
            proc: self.proc,
            send: self.send,
            generators: self.generators,
            num_prios: self.num_prios,
            master_codec: self.master_codec,
            sub_decoders: self.sub_decoders,
            receiver_should_adapt: self.ask_recver_to_adapt,
            on_send_failure: self.on_send_failure,
        }
    }
}

impl<R, P, S, M, MCD> Behaviour<R, P, S, M, MCD> {
    fn take_recv(&mut self) -> Option<R> {
        self.recv.take()
    }
    fn take_send(&mut self) -> Option<S> {
        self.send.take()
    }
    fn take_generators(&mut self) -> Vec<Box<dyn Iterator<Item = M> + Send>> {
        std::mem::take(&mut self.generators)
    }
}

#[derive(Debug)]
pub struct RuntimeCtx {
    pub addr: &'static str,
    pub node_comm: NodeComm,
}

impl RuntimeCtx {
    pub fn new(addr: &'static str, node_comm: NodeComm) -> Self {
        RuntimeCtx { addr, node_comm }
    }
}

impl<R, P, S, IM, OM, MCD> Behaviour<R, P, S, IM, MCD>
where
    IM: Msg,
    OM: Msg,
    R: ActorRecv<IMsg = IM>,
    P: ActorProcess<IMsg = IM, OMsg = OM>,
    S: ActorSend<OMsg = OM>,
    MCD: Encoder<OM> + Decoder<Item = IM, Error = std::io::Error> + Send + Sync + Clone + 'static,
    <MCD as Encoder<OM>>::Error: Send + 'static + ErrWithMsg<OM>,
{
    pub async fn run(mut self, ctx: RuntimeCtx) -> Result<(), ActorError> {
        // let my_addr = ctx.addr.to_string();
        let (p2s_tx, p2s_rx) = mpsc::unbounded_channel::<(OM, &'static str)>();
        let (r2p_tx, mut r2p_rx) = reactor_channel::<R2PMsg<IM>>(self.num_prios, CHANNEL_SIZE);

        let (controller_rx, controller_tx) = ctx.node_comm.split();

        let reciever = self.take_recv();
        let sender = self.take_send();
        let mut generators = self.take_generators();
        let mut processor = self.proc;

        let gen_handles: Vec<tokio::task::JoinHandle<_>> = generators
            .drain(..)
            .map(|gene| {
                let tx = r2p_tx.clone();
                tokio::spawn(generator(gene, tx))
            })
            .collect();

        let rx_handle = tokio::spawn(rx(
            ctx.addr,
            reciever,
            r2p_tx,
            self.master_codec.clone(),
            self.sub_decoders,
            controller_rx,
        ));

        let addr = ctx.addr;
        let mut chaos_manager = chaos_manager::ChaosManager::new();
        let proc_handle: JoinHandle<Result<(), ActorError>> =
            tokio::task::spawn_blocking(move || -> Result<(), ActorError> {
                tracing::info!("[ACTOR][{}] Processor Started", addr);
                loop {
                    match r2p_rx.recv() {
                        Some(R2PMsg::Msg(m, origin)) => {
                            // Dont apply chaos to messages comming from generator
                            let chaos_out = if origin.is_empty() {
                                vec![m]
                            } else {
                                chaos_manager.apply_chaos(m)
                            };
                            let chaos_out_len = chaos_out.len();
                            if chaos_out_len > 1 {
                                tracing::warn!(
                                    "[ACTOR][{}] Message Duplicated {} times",
                                    addr,
                                    chaos_out_len
                                );
                            } else if chaos_out_len == 0 {
                                tracing::warn!("[ACTOR][{}] Message Lost", addr);
                            }
                            for msg in chaos_out {
                                let processed = processor.process(msg);
                                for o in processed {
                                    p2s_tx.send((o, origin)).map_err(|_| ActorError::P2SErr)?;
                                }
                            }
                        }
                        Some(R2PMsg::AddPrio(new_rx)) => {
                            r2p_rx = r2p_rx.add_prio(new_rx);
                        }
                        Some(R2PMsg::RemoveLowPrio) => {
                            r2p_rx = r2p_rx.remove_prio();
                        }
                        Some(R2PMsg::Exit) => {
                            break;
                        }
                        Some(R2PMsg::SetMsgDuplication {
                            factor,
                            probability,
                        }) => {
                            tracing::info!(
                                "[ACTOR][{}] Setting Msg Duplication: factor={}, probability={}",
                                addr,
                                factor,
                                probability
                            );
                            chaos_manager.set_msg_duplication(factor, probability);
                        }
                        Some(R2PMsg::SetMsgLoss { probability }) => {
                            tracing::info!(
                                "[ACTOR][{}] Setting Msg Loss: probability={}",
                                addr,
                                probability
                            );
                            chaos_manager.set_msg_loss(probability);
                        }
                        Some(R2PMsg::UnsetMsgLoss) => {
                            tracing::info!("[ACTOR][{}] Unsetting Msg Loss", addr);
                            chaos_manager.unset_msg_loss();
                        }
                        Some(R2PMsg::UnsetMsgDuplication) => {
                            tracing::info!("[ACTOR][{}] Unsetting Msg Duplication", addr);
                            chaos_manager.unset_msg_duplication();
                        }
                        None => {
                            break;
                        }
                    }
                }
                tracing::info!("[ACTOR][{}] Processor Ended", addr);
                Ok(())
            });
        let tx_handle = tokio::spawn(tx(
            ctx.addr,
            sender,
            self.receiver_should_adapt,
            p2s_rx,
            controller_tx,
            self.master_codec,
            self.on_send_failure,
        ));
        rx_handle.await??;
        proc_handle.await??;
        tx_handle.await?;
        join_all(gen_handles).await;
        Ok(())
    }
}

async fn generator<G, M>(generator: G, p_tx: ReactorChannelTx<R2PMsg<M>>) -> Result<(), ActorError>
where
    G: Iterator<Item = M>,
    M: Msg + 'static,
{
    for m in generator {
        p_tx.send(R2PMsg::Msg(m, "")).await.unwrap();
    }
    Ok(())
}
