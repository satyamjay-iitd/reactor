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

use rand::prelude::*;
use rand::rng;
use criterion::{Criterion, criterion_group, criterion_main};
use reactor_actor::{HasPriority, reactor_channel, ReactorChannelRx, ReactorChannelTx};

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

fn send_burst(tx: ReactorChannelTx<TestMsg>) {
    for _ in 0..10000 {
        let _ = tx.send(TestMsg::Low);
    }
}

fn recv(rx: &mut ReactorChannelRx<TestMsg>) {
    for _ in 0..10000 {
        let _ = rx.recv();
    }
}

fn random_priority_msg() -> TestMsg {
    match rng().random_range(0..=2) {
        0 => TestMsg::Low,
        1 => TestMsg::Medium,
        _ => TestMsg::High,
    }
}

fn mixed_random_send_recv() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = reactor_channel::<TestMsg>(3, 10000);

        for _ in 0..10000 {
            let msg = random_priority_msg();
            tx.send(msg).await.unwrap();
            rx.recv();
        }
    });
}

fn bench_mixed_random(c: &mut Criterion) {
    c.bench_function("priority_channel_mixed_random", |b| b.iter(|| mixed_random_send_recv()));
}

fn bench_send(c: &mut Criterion) {
    c.bench_function("priority_channel_send", |b| b.iter(|| send()));
}

fn bench_recv(c: &mut Criterion) {

    // Setup: fill the channel before benchmarking
    let (tx, mut rx) = reactor_channel::<TestMsg>(3, 10000);
    send_burst(tx); // Fill the pipeline before measuring receive

    c.bench_function("priority_channel_recv", |b| {
        b.iter(|| recv(&mut rx))
    });
}

criterion_group!(benches, bench_send, bench_recv, bench_mixed_random);
criterion_main!(benches);
