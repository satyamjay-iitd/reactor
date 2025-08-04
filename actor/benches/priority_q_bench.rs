/*use criterion::{Criterion, criterion_group, criterion_main};

fn send() {
    // TODO
}

fn bench_send(c: &mut Criterion) {
    // TODO
}

fn recv() {
    // TODO
}

fn bench_recv(c: &mut Criterion) {
    // TODO
}

criterion_group!(benches, bench_send, bench_recv);
criterion_main!(benches);*/

//use super::*;
//use std::hint::black_box;
use criterion::{Criterion, criterion_group, criterion_main};
use reactor_actor::{HasPriority, reactor_channel};
//use your_crate::{HasPriority, R2PMsg, reactor_channel};

#[derive(Clone)]
#[allow(dead_code)]
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
            TestMsg::High => 0,
        }
    }
}


fn send() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, _rx) = reactor_channel::<TestMsg>(3, 10000);

        for _ in 0..10000 {
            tx.send(TestMsg::Low).await.unwrap();
        }
    });
}

fn recv() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = reactor_channel::<TestMsg>(3, 10000);

        for _ in 0..10000 {
            tx.send(TestMsg::Low).await.unwrap();
        }

        for _ in 0..10000 {
            //black_box(rx.recv());
            rx.recv();
        }
    });
}

fn bench_send(c: &mut Criterion) {
    c.bench_function("priority_channel_send", |b| b.iter(|| send()));
}

fn bench_recv(c: &mut Criterion) {
    c.bench_function("priority_channel_recv", |b| b.iter(|| recv()));
}

criterion_group!(benches, bench_send, bench_recv);
criterion_main!(benches);
