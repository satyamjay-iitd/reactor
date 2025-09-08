use std::{any::Any, net::SocketAddr};

use tokio::sync::{mpsc, oneshot};

use crate::ActorAddr;

/// Type of channel that is used to send message from one local actor to the other.
pub type LocalChannelTx = mpsc::Sender<Box<dyn Any + Send>>;
pub type LocalChannelRx = mpsc::Receiver<Box<dyn Any + Send>>;

#[derive(Debug)]
pub enum Connection {
    Remote(SocketAddr),
    Local(LocalChannelTx),
    CouldntResolve,
}

pub enum ControlReq {
    Resolve {
        addr: ActorAddr,
        resp_tx: oneshot::Sender<Connection>,
    },
}

/// Instructions that are sent by the local controller to the actor
pub enum ControlInst {
    StartLocalRecv(LocalChannelRx),
    StartTcpRecv(u16),
    Stop,
    SetMsgLoss {
        probability: f32,
    },
    SetMsgDuplication {
        factor: u32,
        probability: f32,
    },
    SetMsgDelay {
        delay_range_ms: (u64, u64),
        senders: Vec<String>,
    },
    UnsetMsgLoss,
    UnsetMsgDuplication,
    UnsetMsgDelay {
        senders: Vec<String>,
    },
}

#[derive(Debug)]
pub struct NodeComm {
    pub controller_rx: mpsc::Receiver<ControlInst>,
    pub controller_tx: mpsc::Sender<ControlReq>,
}

impl NodeComm {
    pub fn new(
        controller_rx: mpsc::Receiver<ControlInst>,
        controller_tx: mpsc::Sender<ControlReq>,
    ) -> Self {
        NodeComm {
            controller_rx,
            controller_tx,
        }
    }
    pub fn split(self) -> (mpsc::Receiver<ControlInst>, mpsc::Sender<ControlReq>) {
        (self.controller_rx, self.controller_tx)
    }
}
