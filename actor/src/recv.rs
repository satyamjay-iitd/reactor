use core::panic;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt as _;
use rand::{Rng, SeedableRng as _, rngs::SmallRng};
use socket2::{Domain, Socket, Type};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    net::{TcpListener, tcp::OwnedReadHalf},
    sync::{Mutex, mpsc, watch},
    task::JoinSet,
};
use tokio_util::{
    codec::{Decoder, FramedRead},
    sync::CancellationToken,
};
use tracing::{error, warn};

use crate::{
    ActorRecv, ChannelAction, Msg, R2PMsg, SubDecoderStore,
    err::{ActorError, RecieverErr},
    node_comm::{ControlInst, LocalChannelRx},
    reactor_channel::ReactorChannelTx,
};

struct BoxedDecoder<M>(
    Box<dyn tokio_util::codec::Decoder<Item = M, Error = std::io::Error> + Sync + Send>,
);
impl<M> Decoder for BoxedDecoder<M> {
    type Item = M;

    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        self.0.decode(src)
    }
}

type ThrottleSignalStore = Arc<Mutex<HashMap<String, watch::Sender<(u64, u64)>>>>;

fn any_to_m<M: 'static>(msg: Box<dyn std::any::Any>) -> M {
    *(msg.downcast::<M>().unwrap())
}
/// Spawns tasks to receive messages from incoming network or local control channels,
/// decode them, and forward them for processing based on channel state.
///
/// This function listens for `ControlMsg` commands on a controller channel and handles two cases:
///
/// 1. **`StartTcpRecv(port)`**:
///    Binds a non-blocking TCP socket on the given port. For every incoming connection, it:
///     - Spawns a task (`parent_recv_subtask`) to receive bytes from the socket,
///     - Decodes messages using `decoder`,
///     - Passes messages to the processor via `p_tx`,
///     - Determines their routing via `after_recv` and shared state `CS`.
///
/// 2. **`StartLocalRecv(simplex_stream)`**:
///    Similar to the TCP case but uses a provided `simplex_stream` (e.g., for local testing or IPC),
///    spawning a new receive subtask accordingly.
///
/// # Type Parameters
/// - `M`: The type of decoded message. Must implement `Msg`.
/// - `CS`: The shared channel state type. Must implement `CState`.
/// - `AR`: A closure used to inspect each decoded message and state, returning a `ChannelAction`.
/// - `D`: A decoder capable of converting raw bytes into messages of type `M`.
///
/// # Arguments
/// - `after_recv`: A closure invoked on each message after decoding, allowing for routing decisions.
/// - `p_tx`: Unbounded channel sender to forward messages for further processing.
/// - `decoder`: The decoder used by all subtasks to interpret incoming byte streams.
/// - `controller_rx`: Control channel used to dynamically start new receiver tasks.
///
/// # Notes
/// - All state is shared via `Arc<Mutex<CS>>`, ensuring thread-safe mutation.
/// - `after_recv` and `decoder` must be `Clone` as they are moved into spawned tasks.
///
/// # Panics
/// - Will panic if socket binding, listening, or accepting fails (errors are unwrapped).
///   Spawns tasks to receive messages from incoming network or local control channels,
///   decode them, and forward them for processing based on channel state.
///
pub(crate) async fn rx<M, AR, D>(
    my_addr: &'static str,
    reciever: Option<AR>,
    p_tx: ReactorChannelTx<R2PMsg<M>>,
    decoder: D,
    sub_decoders: Option<SubDecoderStore<M>>,
    mut controller_rx: mpsc::Receiver<ControlInst>,
) -> Result<(), ActorError>
where
    D: Decoder<Item = M, Error = std::io::Error> + Clone + Send + Sync + 'static,
    AR: ActorRecv<IMsg = M> + 'static,
    M: Msg,
{
    tracing::info!("[ACTOR][{}] Rx Started", my_addr);
    let cancel_token = CancellationToken::new();
    let mut tcp_server_set: JoinSet<Result<(), ActorError>> = JoinSet::new();
    let mut local_recv_set = JoinSet::new();
    let channel_state = reciever.map(|reciever| Arc::new(Mutex::new(reciever)));
    let throttle_signals: ThrottleSignalStore = Arc::new(Mutex::new(HashMap::new()));

    while let Some(msg) = controller_rx.recv().await {
        match msg {
            ControlInst::StartTcpRecv(port) => {
                tcp_server_set.spawn(tcp_recv(
                    port,
                    cancel_token.clone(),
                    decoder.clone(),
                    sub_decoders,
                    p_tx.clone(),
                    channel_state.clone(),
                    throttle_signals.clone(),
                ));
            }
            ControlInst::StartLocalRecv(mut local_rx) => {
                let remote_addr = local_rx.recv().await.unwrap();
                let (remote_addr, msg_type) =
                    *(remote_addr.downcast::<(String, Option<String>)>().unwrap());
                let msg_transform = match (sub_decoders, msg_type) {
                    (Some(sub_decoders), Some(msg_type)) => {
                        sub_decoders(&msg_type).unwrap().any_to_m
                    }
                    _ => any_to_m,
                };

                let throttle_rx =
                    if let Some(throttle_tx) = throttle_signals.lock().await.get(&remote_addr) {
                        let rx = throttle_tx.subscribe();
                        // Resend value to trigger notification on the new rx;
                        let value = *throttle_tx.borrow();
                        throttle_tx.send(value).unwrap();
                        rx
                    } else {
                        let (throttle_tx, throttle_rx) = watch::channel((0, 0));
                        throttle_signals
                            .lock()
                            .await
                            .insert(remote_addr.clone(), throttle_tx);
                        throttle_rx
                    };
                local_recv_set.spawn(local_parent_recv_subtask(
                    remote_addr,
                    p_tx.clone(),
                    channel_state.clone(),
                    local_rx,
                    msg_transform,
                    throttle_rx,
                ));
            }
            ControlInst::SetMsgDuplication {
                factor,
                probability,
            } => {
                p_tx.send(R2PMsg::SetMsgDuplication {
                    factor,
                    probability,
                })
                .await
                .map_err(|_| ActorError::R2PErr)?;
            }
            ControlInst::SetMsgLoss { probability } => {
                p_tx.send(R2PMsg::SetMsgLoss { probability })
                    .await
                    .map_err(|_| ActorError::R2PErr)?;
            }
            ControlInst::SetMsgDelay {
                delay_range_ms,
                senders,
            } => {
                for sender in senders {
                    if let Some(throttle_tx) = throttle_signals.lock().await.get(&sender) {
                        throttle_tx.send(delay_range_ms).unwrap();
                    } else {
                        let (throttle_tx, _throttle_rx) = watch::channel((0, 0));
                        throttle_tx.send(delay_range_ms).unwrap();
                        throttle_signals
                            .lock()
                            .await
                            .insert(sender.clone(), throttle_tx);
                    }
                }
            }
            ControlInst::UnsetMsgLoss => {
                p_tx.send(R2PMsg::UnsetMsgLoss)
                    .await
                    .map_err(|_| ActorError::R2PErr)?;
            }
            ControlInst::UnsetMsgDuplication => {
                p_tx.send(R2PMsg::UnsetMsgDuplication)
                    .await
                    .map_err(|_| ActorError::R2PErr)?;
            }
            ControlInst::UnsetMsgDelay { senders } => {
                for sender in senders {
                    if let Some(throttle_signal) = throttle_signals.lock().await.get(&sender) {
                        throttle_signal.send((0, 0)).unwrap();
                    } else {
                        error!("Sender {sender} not found");
                    }
                }
            }
            ControlInst::Stop => {
                cancel_token.cancel();
                p_tx.send(R2PMsg::Exit)
                    .await
                    .map_err(|_| ActorError::R2PErr)?;
                break;
            }
        }
    }
    tcp_server_set.abort_all();
    local_recv_set.abort_all();
    tracing::info!("[ACTOR][{}] Rx Ended", my_addr);
    Ok(())
}

async fn tcp_recv<D, M, AR>(
    port: u16,
    cancel_token: CancellationToken,
    master_decoder: D,
    sub_decoders: Option<SubDecoderStore<M>>,
    p_tx: ReactorChannelTx<R2PMsg<M>>,
    cstate: Option<Arc<Mutex<AR>>>,
    throttle_signals: ThrottleSignalStore,
) -> Result<(), ActorError>
where
    M: Msg,
    D: Decoder<Item = M, Error = std::io::Error> + Clone + Send + Sync + 'static,
    AR: ActorRecv<IMsg = M> + 'static,
{
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
    socket
        .set_reuse_port(true)
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
    socket
        .bind(&SocketAddr::from((Ipv4Addr::new(0, 0, 0, 0), port)).into())
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
    socket
        .listen(128)
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;

    let parent_listener = TcpListener::from_std(socket.into())
        .map_err(|e| ActorError::RecieverErr(RecieverErr::TcpStartErr(e)))?;

    let mut remote_recv_set = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }
            accept_result = parent_listener.accept() => {
                let (socket, _) = accept_result.map_err(|e| {
                    ActorError::RecieverErr(RecieverErr::TcpStartErr(e))
                })?;
                let (mut rx, _) = socket.into_split();
                // Whenever an actor connects it first needs to tell us its
                // address, and message type.
                let (remote_addr, msg_type) = recv_remote_handshake(&mut rx).await;

                match (sub_decoders, msg_type){
                    (Some(sub_decoders), Some(msg_type)) => {
                        let decoder: Box<dyn tokio_util::codec::Decoder<Item = M, Error = std::io::Error> + Sync + Send> = (sub_decoders(&msg_type).unwrap().decoder_cons)();
                        let boxed_decoder = BoxedDecoder(decoder);
                        let (throttle_tx, throttle_rx) = watch::channel((0,0));
                        throttle_signals.lock().await.insert(remote_addr.clone(), throttle_tx);
                        remote_recv_set.spawn(remote_parent_recv_subtask(
                            remote_addr,
                            p_tx.clone(),
                            cstate.clone(),
                            FramedRead::new(rx, boxed_decoder),
                            throttle_rx
                        ));
                    },
                    (None, Some(_)) => {
                        panic!("I dont know how to decode your messages");
                    }
                    _ => {
                        let (throttle_tx, throttle_rx) = watch::channel((0,0));
                        throttle_signals.lock().await.insert(remote_addr.clone(), throttle_tx);
                        remote_recv_set.spawn(remote_parent_recv_subtask(
                            remote_addr,
                            p_tx.clone(),
                            cstate.clone(),
                            FramedRead::new(rx, master_decoder.clone()),
                            throttle_rx
                        ));

                    },
                }
            }
        }
    }
    remote_recv_set.abort_all();
    Ok(())
}

/// Receive actor name and message type
async fn recv_remote_handshake(rx: &mut OwnedReadHalf) -> (String, Option<String>) {
    // Step 1: Read 4 bytes as the length prefix
    let size = rx.read_u32().await.unwrap();
    // Step 2: Allocate a buffer of that size
    let mut buf = vec![0u8; size as usize];
    // Step 3: Read exactly that many bytes into the buffer
    rx.read_exact(&mut buf).await.unwrap();
    // Step 4: Convert to String
    let actor_addr = String::from_utf8(buf).unwrap();

    let size = rx.read_u32().await.unwrap();
    if size == 0 {
        return (actor_addr, None);
    }
    let mut buf = vec![0u8; size as usize];
    rx.read_exact(&mut buf).await.unwrap();
    let message_type = String::from_utf8(buf).unwrap();

    (actor_addr, Some(message_type))
}

async fn remote_parent_recv_subtask<M, AR, D, RX>(
    parent_addr: String,
    row_q: ReactorChannelTx<R2PMsg<M>>,
    cstate: Option<Arc<Mutex<AR>>>,
    mut framed_reader: FramedRead<RX, D>,
    mut throttle_signal: watch::Receiver<(u64, u64)>,
) where
    AR: ActorRecv<IMsg = M>,
    D: Decoder<Item = M>,
    RX: AsyncRead + Unpin,
    M: Msg,
{
    tracing::info!("[ACTOR] SubRx Started");
    let parent_addr = parent_addr.leak();
    let mut throttle_range = (0, 0);
    let mut rng = SmallRng::from_seed([0; 32]);
    loop {
        tokio::select! {
        _ = throttle_signal.changed() => {
            throttle_range= *throttle_signal.borrow_and_update();
        }
        msg = framed_reader.next() =>{
            if let Some(Ok(msg)) = msg {
                if throttle_range != (0,0){
                    let throttle_ms: u64 = rng.random_range(throttle_range.0..=throttle_range.1);
                    warn!("Throttling messages from {parent_addr} for {throttle_ms}ms");
                    tokio::time::sleep(Duration::from_millis(throttle_ms)).await;
                }
                if let Some(cstate) = cstate.as_ref() {
                    let action = cstate.lock().await.after_recv(parent_addr, &msg).await;
                    match action {
                        ChannelAction::PASS => {}
                        ChannelAction::PANIC => {
                            panic!()
                        }
                        ChannelAction::DROP => {
                            continue;
                        }
                        ChannelAction::SYNC(_) => todo!(),
                        ChannelAction::CLOSE => {
                            break;
                        }
                    }
                    if row_q.send(R2PMsg::Msg(msg, parent_addr)).await.is_err() {
                        break;
                    }
                } else if row_q.send(R2PMsg::Msg(msg, parent_addr)).await.is_err() {
                    break;
                }
            }
        }

        }
    }
    tracing::info!("[ACTOR] SubRx Ended");
}

async fn local_parent_recv_subtask<M, AR>(
    parent_addr: String,
    row_q: ReactorChannelTx<R2PMsg<M>>,
    after_recv: Option<Arc<Mutex<AR>>>,
    mut local_rx: LocalChannelRx,
    msg_transform: fn(Box<dyn std::any::Any>) -> M,
    mut throttle_signal: watch::Receiver<(u64, u64)>,
) where
    M: Msg + 'static,
    AR: ActorRecv<IMsg = M>,
{
    tracing::info!("[ACTOR] SubRx Started");
    let parent_addr = parent_addr.leak();
    let mut throttle_range = (0, 0);
    let mut rng = SmallRng::from_seed([0; 32]);

    loop {
        tokio::select! {
            _ = throttle_signal.changed() => {
                throttle_range = *throttle_signal.borrow_and_update();
            }
            msg = local_rx.recv() => {
                 if let Some(msg) = msg {
                    if throttle_range != (0,0){
                        let throttle_ms: u64 = rng.random_range(throttle_range.0..=throttle_range.1);
                        warn!("Throttling messages from {parent_addr} for {throttle_ms}ms");
                        tokio::time::sleep(Duration::from_millis(throttle_ms)).await;
                    }
                    let msg = msg_transform(msg);
                    // let msg = msg.downcast::<M>().unwrap();
                    // let msg: M = msg.into();
                    if let Some(cstate) = after_recv.as_ref() {
                        let action = cstate.lock().await.after_recv(parent_addr, &msg).await;
                        match action {
                            ChannelAction::PASS => {}
                            ChannelAction::PANIC => {
                                panic!()
                            }
                            ChannelAction::DROP => {
                                continue;
                            }
                            ChannelAction::SYNC(_) => todo!(),
                            ChannelAction::CLOSE => {
                                break;
                            }
                        }
                        if row_q.send(R2PMsg::Msg(msg, parent_addr)).await.is_err() {
                            break;
                        }
                    } else if row_q.send(R2PMsg::Msg(msg, parent_addr)).await.is_err() {
                        break;
                    }
                }
            }

        }
    }
    tracing::info!("[ACTOR] SubRx Ended");
}
