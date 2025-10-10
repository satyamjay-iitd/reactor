use std::{any::Any, collections::HashMap, time::Duration};

use futures::SinkExt as _;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinSet,
};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    ActorAddr, ActorSend, Msg, RouteTo, SendErrAction,
    codec::ErrWithMsg,
    node_comm::{Connection, ControlReq},
};

#[allow(clippy::type_complexity)]
pub(crate) async fn tx<M, E, BS>(
    my_addr: &'static str,
    before_send: Option<BS>,
    ask_receiver_to_adapt: bool,
    mut p_rx: mpsc::UnboundedReceiver<(M, &'static str)>,
    controller_tx: mpsc::Sender<ControlReq>,
    codec: E,
    on_send_failure: SendErrAction,
) where
    M: Msg,
    BS: ActorSend<OMsg = M>,
    E: Encoder<M> + 'static + Send + Clone,
    E::Error: Send + 'static + ErrWithMsg<M>,
{
    let mut addr_to_buff: HashMap<ActorAddr, mpsc::UnboundedSender<M>> = HashMap::new();
    let mut sub_senders = JoinSet::new();
    tracing::info!("[ACTOR][{}] Tx Started", my_addr);

    if let Some(mut before_send) = before_send {
        while let Some((m, origin)) = p_rx.recv().await {
            let receivers: RouteTo<'_> = before_send.before_send(&m).await;
            match receivers {
                RouteTo::Blackhole => {}
                RouteTo::Reply => send_msg(
                    my_addr,
                    ask_receiver_to_adapt,
                    controller_tx.clone(),
                    codec.clone(),
                    &mut addr_to_buff,
                    &mut sub_senders,
                    origin,
                    m,
                    on_send_failure,
                ),
                RouteTo::Single(send_to) => send_msg(
                    my_addr,
                    ask_receiver_to_adapt,
                    controller_tx.clone(),
                    codec.clone(),
                    &mut addr_to_buff,
                    &mut sub_senders,
                    &send_to,
                    m,
                    on_send_failure,
                ),
                RouteTo::Multiple(receivers) => {
                    let num_receivers = receivers.len();
                    if num_receivers == 0 {
                        continue;
                    }
                    for addr in &receivers[..num_receivers - 1] {
                        send_msg(
                            my_addr,
                            ask_receiver_to_adapt,
                            controller_tx.clone(),
                            codec.clone(),
                            &mut addr_to_buff,
                            &mut sub_senders,
                            addr,
                            m.clone(),
                            on_send_failure,
                        );
                    }
                    send_msg(
                        my_addr,
                        ask_receiver_to_adapt,
                        controller_tx.clone(),
                        codec.clone(),
                        &mut addr_to_buff,
                        &mut sub_senders,
                        &receivers[num_receivers - 1],
                        m,
                        on_send_failure,
                    );
                }
            }
        }
    } else {
        while p_rx.recv().await.is_some() {}
    }
    sub_senders.abort_all();
    tracing::info!("[ACTOR][{}] Tx Ended", my_addr);
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn send_msg<M, E>(
    my_addr: &'static str,
    ask_receiver_to_adapt: bool,
    controller_tx: mpsc::Sender<ControlReq>,
    codec: E,
    addr_to_buff: &mut HashMap<String, mpsc::UnboundedSender<M>>,
    sub_senders: &mut JoinSet<()>,
    addr: &str,
    m: M,
    on_send_failure: SendErrAction,
) where
    M: Msg,
    E: Encoder<M> + 'static + Send + Clone,
    E::Error: Send + 'static + ErrWithMsg<M>,
{
    if let Some(sender) = addr_to_buff.get(addr) {
        if let Err(e) = sender.send(m) {
            log::error!("[ACTOR] Failed to send message to {}: {}", addr, e);
        }
    } else {
        let (tx, rx) = mpsc::unbounded_channel::<M>();
        sub_senders.spawn(sender_task(
            my_addr,
            ask_receiver_to_adapt,
            (*addr).to_string(),
            rx,
            codec,
            controller_tx,
            on_send_failure,
        ));
        let _ = tx.send(m);
        addr_to_buff.insert(addr.to_string(), tx);
    }
}

async fn sender_task<M, E>(
    my_addr: &'static str,
    ask_receiver_to_adapt: bool,
    send_addr: ActorAddr,
    rx: mpsc::UnboundedReceiver<M>,
    encoder: E,
    controller_tx: mpsc::Sender<ControlReq>,
    on_send_failure: SendErrAction,
) where
    M: Msg,
    E: Encoder<M> + 'static + Send,
    E::Error: Send + 'static + ErrWithMsg<M>,
{
    async fn remote_sender<C, M>(
        my_addr: &'static str,
        ask_receiver_to_adapt: bool,
        mut tx: impl AsyncWrite + Unpin,
        mut rx: mpsc::UnboundedReceiver<M>,
        encoder: C,
        on_send_failure: SendErrAction,
    ) where
        M: Msg,
        C::Error: Send + 'static + ErrWithMsg<M>,
        C: Encoder<M> + 'static + Send,
    {
        log::info!("[ACTOR] SubTx Started");
        let decoder_name = std::any::type_name::<M>().to_string();
        send_remote_handshake(&mut tx, my_addr, decoder_name, ask_receiver_to_adapt).await;
        let mut framed_writer = FramedWrite::new(tx, encoder);

        while let Some(msg) = rx.recv().await {
            let Err(e) = framed_writer.send(msg).await else {
                continue;
            };
            let mut failed_msg = e.into_inner().unwrap();
            match on_send_failure {
                SendErrAction::Drop => {
                    log::warn!("[ACTOR] {} Failed to send message, dropping", my_addr);
                }
                SendErrAction::Retry {
                    attempts: retries_opt,
                    delay_ms,
                } => {
                    let mut attempts = 0;
                    loop {
                        if let Some(retries) = retries_opt
                            && attempts >= retries
                        {
                            log::error!(
                                "[ACTOR] {} Failed to send message, retries exhausted",
                                my_addr
                            );
                            break;
                        }

                        attempts += 1;

                        if let Err(err_msg) = framed_writer.send(failed_msg).await {
                            failed_msg = err_msg.into_inner().unwrap();

                            log::warn!(
                                "[ACTOR] {} Failed to send message, retries: {}",
                                my_addr,
                                attempts
                            );
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        log::info!("[ACTOR] SubTx Ended");
    }

    /// - `tx`: Channel to the destination actor.
    /// - `rx`: Internal channel for messages of type `M`, specific to this actor instance.
    /// - Note: `tx` and `rx` are independent — `rx` collects messages locally between helper functions and
    ///   `tx` delivers them to the actor. They don’t correlate directly.
    async fn local_sender<M: std::fmt::Debug + Send + 'static + Clone>(
        my_addr: &'static str,
        ask_receiver_to_adapt: bool,
        tx: mpsc::Sender<Box<dyn Any + Send>>,
        mut rx: mpsc::UnboundedReceiver<M>,
        on_send_failure: SendErrAction,
    ) {
        log::info!("[ACTOR] SubTx Started (Local)");
        let decoder_name = std::any::type_name::<M>().to_string();
        send_local_handshake(&tx, my_addr, decoder_name, ask_receiver_to_adapt).await;
        while let Some(msg) = rx.recv().await {
            let Err(e) = tx.send(Box::new(msg)).await else {
                continue;
            };
            let mut failed_msg: M = *e.0.downcast::<M>().unwrap();
            match on_send_failure {
                SendErrAction::Drop => {
                    log::warn!("[ACTOR] {} Failed to send message, dropping", my_addr);
                }
                SendErrAction::Retry {
                    attempts: retries_opt,
                    delay_ms,
                } => {
                    let mut attempts = 0;
                    loop {
                        if let Some(retries) = retries_opt
                            && attempts >= retries
                        {
                            log::error!(
                                "[ACTOR] {} Failed to send message, retries exhausted",
                                my_addr
                            );
                            break;
                        }

                        attempts += 1;

                        if let Err(e) = tx.send(Box::new(failed_msg)).await {
                            failed_msg = *e.0.downcast::<M>().unwrap();
                            log::warn!(
                                "[ACTOR] {} Failed to send message, retries: {}",
                                my_addr,
                                attempts,
                            );
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        log::info!("[ACTOR] SubTx Ended");
    }

    let mut attempts = 0;
    loop {
        let (c_tx, c_rx) = tokio::sync::oneshot::channel();
        log::debug!("[ACTOR] Sending Resolve request for address: {}", send_addr);
        controller_tx
            .send(ControlReq::Resolve {
                resp_tx: c_tx,
                addr: send_addr.clone(),
            })
            .await
            .unwrap();

        match c_rx.await.unwrap() {
            Connection::Remote(socket_addr) => {
                let tx = loop {
                    match TcpStream::connect(socket_addr).await {
                        Ok(s) => {
                            let (_, tx) = s.into_split();
                            break tx;
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            todo!();
                        }
                    }
                };

                remote_sender(
                    my_addr,
                    ask_receiver_to_adapt,
                    tx,
                    rx,
                    encoder,
                    on_send_failure,
                )
                .await;
                break;
            }
            Connection::Local(write_half) => {
                local_sender(
                    my_addr,
                    ask_receiver_to_adapt,
                    write_half,
                    rx,
                    on_send_failure,
                )
                .await;
                break;
            }
            Connection::CouldntResolve => match on_send_failure {
                SendErrAction::Drop => {
                    log::warn!(
                        "[ACTOR] {} Failed to resolve {}, dropping",
                        my_addr,
                        send_addr
                    );
                    break;
                }
                SendErrAction::Retry {
                    attempts: retries_opt,
                    delay_ms,
                } => {
                    if let Some(retries) = retries_opt
                        && attempts >= retries
                    {
                        log::error!(
                            "[ACTOR] {} Failed to resolve {}, retries exhausted",
                            my_addr,
                            send_addr
                        );
                        break;
                    }
                    attempts += 1;
                    log::warn!(
                        "[ACTOR] {} Failed to resolve {}, retrying {}",
                        my_addr,
                        send_addr,
                        attempts,
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            },
        };
    }
}

async fn send_remote_handshake(
    tx: &mut (impl AsyncWrite + Unpin),
    my_name: &str,
    type_name: String,
    ask_receiver_to_adapt: bool,
) {
    let bytes = my_name.as_bytes();
    let len = bytes.len();
    tx.write_u32(len as u32).await.unwrap();
    tx.write_all(bytes).await.unwrap();

    if ask_receiver_to_adapt {
        let bytes = type_name.as_bytes();
        let len = bytes.len();
        tx.write_u32(len as u32).await.unwrap();
        tx.write_all(bytes).await.unwrap();
    } else {
        tx.write_u32(0).await.unwrap();
    }
}
async fn send_local_handshake(
    tx: &mpsc::Sender<Box<dyn Any + Send>>,
    my_name: &str,
    type_name: String,
    ask_receiver_to_adapt: bool,
) {
    let to_send = if ask_receiver_to_adapt {
        (my_name.to_string(), Some(type_name.to_string()))
    } else {
        (my_name.to_string(), None)
    };
    tx.send(Box::new(to_send)).await.unwrap();
}
