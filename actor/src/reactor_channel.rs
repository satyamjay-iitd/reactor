use tokio::sync::mpsc::{
    self, Receiver, Sender,
    error::{SendError, TryRecvError},
};

use crate::R2PMsg;

pub static MAX_PRIO: usize = 0;

#[derive(Clone)]
pub enum ReactorChannelTx<T> {
    SingleChannel(Sender<T>),
    MultiChannel(PriorityChannelTx<T>),
}

impl<T: HasPriority> ReactorChannelTx<T> {
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self {
            ReactorChannelTx::SingleChannel(tx) => tx.send(msg).await,
            ReactorChannelTx::MultiChannel(priority_channel_rx) => {
                priority_channel_rx.send(msg).await
            }
        }
    }
}

impl<T: HasPriority> ReactorChannelTx<R2PMsg<T>> {
    #[allow(dead_code)]
    pub async fn add_prio(self, channel_size: usize) -> ReactorChannelTx<R2PMsg<T>> {
        match self {
            ReactorChannelTx::SingleChannel(tx) => {
                let (new_tx, new_rx) = mpsc::channel(channel_size);
                new_tx.send(R2PMsg::AddPrio(new_rx)).await.unwrap();
                ReactorChannelTx::MultiChannel(PriorityChannelTx {
                    senders: vec![tx, new_tx],
                })
            }
            ReactorChannelTx::MultiChannel(mut priority_channel_tx) => {
                let (new_tx, new_rx) = mpsc::channel(channel_size);
                new_tx.send(R2PMsg::AddPrio(new_rx)).await.unwrap();
                priority_channel_tx.senders.push(new_tx);
                ReactorChannelTx::MultiChannel(priority_channel_tx)
            }
        }
    }
    #[allow(dead_code)]
    pub fn remove_prio(&mut self) {
        match self {
            ReactorChannelTx::SingleChannel(_) => panic!("Single Channel"),
            ReactorChannelTx::MultiChannel(priority_channel_tx) => {
                priority_channel_tx.remove_prio();
                priority_channel_tx
                    .send_blocking(R2PMsg::RemoveLowPrio)
                    .unwrap();
                if priority_channel_tx.curr_prios() == 1 {
                    *self =
                        ReactorChannelTx::SingleChannel(priority_channel_tx.senders.pop().unwrap())
                }
            }
        }
    }
}

pub enum ReactorChannelRx<T> {
    SingleChannel(Receiver<T>),
    MultiChannel(PriorityChannelRx<T>),
}
impl<T> ReactorChannelRx<T> {
    pub fn recv(&mut self) -> Option<T> {
        match self {
            ReactorChannelRx::SingleChannel(receiver) => receiver.blocking_recv(),
            ReactorChannelRx::MultiChannel(priority_channel_rx) => loop {
                match priority_channel_rx.try_recv() {
                    Ok(msg) => {
                        return Some(msg);
                    }
                    Err(TryRecvError::Empty) => {
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        return None;
                    }
                }
            },
        }
    }

    pub(crate) fn add_prio(self, new_rx: Receiver<T>) -> Self {
        match self {
            ReactorChannelRx::SingleChannel(receiver) => {
                ReactorChannelRx::MultiChannel(PriorityChannelRx {
                    receivers: vec![receiver, new_rx],
                })
            }
            ReactorChannelRx::MultiChannel(mut priority_channel_rx) => {
                priority_channel_rx.add_prio(new_rx);
                ReactorChannelRx::MultiChannel(priority_channel_rx)
            }
        }
    }
    pub(crate) fn remove_prio(self) -> Self {
        match self {
            ReactorChannelRx::SingleChannel(_) => panic!("Single Channel"),
            ReactorChannelRx::MultiChannel(mut priority_channel_rx) => {
                priority_channel_rx.remove_prio();
                if priority_channel_rx.curr_prios() == 1 {
                    ReactorChannelRx::SingleChannel(priority_channel_rx.receivers.pop().unwrap())
                } else {
                    ReactorChannelRx::MultiChannel(priority_channel_rx)
                }
            }
        }
    }
}

pub fn reactor_channel<T: HasPriority>(
    num_prios: u8,
    channel_size: usize,
) -> (ReactorChannelTx<T>, ReactorChannelRx<T>) {
    if num_prios == 1 {
        let (tx, rx) = mpsc::channel::<T>(channel_size);
        (
            ReactorChannelTx::SingleChannel(tx),
            ReactorChannelRx::SingleChannel(rx),
        )
    } else {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..num_prios {
            let (tx, rx) = mpsc::channel::<T>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        (
            ReactorChannelTx::MultiChannel(PriorityChannelTx { senders }),
            ReactorChannelRx::MultiChannel(PriorityChannelRx { receivers }),
        )
    }
}

/// Messages that can flow between the actors.
//pub trait Msg: Send + std::fmt::Debug {}
pub trait HasPriority {
    fn priority(&self) -> usize {
        MAX_PRIO
    }
}

#[derive(Clone)]
pub struct PriorityChannelTx<T> {
    senders: Vec<mpsc::Sender<T>>,
}

pub struct PriorityChannelRx<T> {
    receivers: Vec<mpsc::Receiver<T>>,
}

impl<T: HasPriority> PriorityChannelTx<T> {
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let idx = msg.priority();
        if let Some(tx) = self.senders.get(idx) {
            (*tx).send(msg).await
        } else {
            Err(SendError(msg))
        }
    }
    pub fn send_blocking(&self, msg: T) -> Result<(), SendError<T>> {
        let idx = msg.priority();
        if let Some(tx) = self.senders.get(idx) {
            (*tx).blocking_send(msg)
        } else {
            Err(SendError(msg))
        }
    }
}

impl<T> PriorityChannelTx<T> {
    pub fn remove_prio(&mut self) {
        self.senders.pop();
    }
    pub fn curr_prios(&self) -> u8 {
        self.senders.len() as u8
    }
}

impl<T> PriorityChannelRx<T> {
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut disconnected_count = 0;

        for rx in &mut self.receivers {
            match rx.try_recv() {
                Ok(msg) => return Ok(msg),
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => disconnected_count += 1,
            }
        }

        if disconnected_count == self.receivers.len() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub fn add_prio(&mut self, new_rx: mpsc::Receiver<T>) {
        self.receivers.push(new_rx);
    }

    fn remove_prio(&mut self) {
        self.receivers.pop();
    }

    fn curr_prios(&self) -> u8 {
        self.receivers.len() as u8
    }
}

#[cfg(test)]
mod tests {
    use crate::R2PMsg;

    use super::*;
    fn priority_channel<T: HasPriority>(
        num_prios: u8,
        channel_size: usize,
    ) -> (PriorityChannelTx<T>, PriorityChannelRx<T>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        for _ in 0..num_prios {
            let (tx, rx) = mpsc::channel::<T>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        (
            PriorityChannelTx { senders },
            PriorityChannelRx { receivers },
        )
    }

    // Dummy message type
    #[derive(Debug, Clone, PartialEq)]
    enum TestMsg {
        Low,
        Medium,
        High,
    }

    impl HasPriority for TestMsg {
        fn priority(&self) -> usize {
            match self {
                TestMsg::Low => 2,
                TestMsg::Medium => 1,
                TestMsg::High => MAX_PRIO,
            }
        }
    }

    #[tokio::test]
    async fn test_priority_order_stress() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 10000);

        for _ in 0..10000 {
            tx.send(R2PMsg::Msg(TestMsg::Low)).await.unwrap();
        }
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        for _ in 0..10000 {
            assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Low)));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_priority_order() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);

        tx.send(R2PMsg::Msg(TestMsg::Low)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Low)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_disconnected_behavior() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(2, 100);
        drop(tx.senders); // Drop all senders to simulate disconnect

        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[tokio::test]
    async fn test_exit_message() {
        let (tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);
        tx.send(R2PMsg::Exit).await.unwrap();
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(R2PMsg::Exit));
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
    }

    #[tokio::test]
    async fn test_empty_channel() {
        let (_tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(1, 100);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
    // fn test_partial_disconnect_behavior() {
    //     let (mut tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(2);

    //     // Drop only the High priority sender
    //     tx.senders.remove(Priority::High.to_index());
    //     println!("Getting the channel in high index");
    //     println!("{:?}", tx.senders.get(Priority::High.to_index()));
    //     // If you try to send to High now, it should fail (simulate disconnection)
    //     assert!(tx.senders.get(Priority::High.to_index()).is_none());

    //     // The channel should still be "connected" from the perspective of Medium/Low
    //     assert_ne!(rx.get_row(), PriorityChannelStatus::Disconnected);
    // }
    #[tokio::test]
    async fn test_partial_disconnect_behavior() {
        let (mut tx, mut rx) = priority_channel::<R2PMsg<TestMsg>>(3, 100);

        tx.remove_prio();

        tx.send(R2PMsg::Msg(TestMsg::High)).await.unwrap();
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::High)));
        tx.send(R2PMsg::Msg(TestMsg::Medium)).await.unwrap();
        assert_eq!(rx.try_recv(), Ok(R2PMsg::Msg(TestMsg::Medium)));
        let msg = R2PMsg::Msg(TestMsg::Low);
        match tx.send(msg.clone()).await {
            Ok(_) => panic!(),
            Err(send_err) => {
                assert_eq!(send_err.0, msg);
            }
        }
    }
}
