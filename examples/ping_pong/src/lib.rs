pub use reactor_actor::setup_shared_logger_ref;

use bincode::{Decode, Encode};

use reactor_actor::codec::BincodeCodec;
use reactor_actor::{BehaviourBuilder, RouteTo, RuntimeCtx};
use reactor_macros::{DefaultPrio, Msg as DeriveMsg};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use std::collections::HashMap;
use std::time::Duration;

use log::info;

use tracing::{Instrument, info_span};
use opentelemetry::{global, Context as OtelContext};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;

#[cfg(feature = "chaos")]
use rand::random;

// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////
#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub struct TracedMessage {
    pub payload: PingPongMsg,
    pub trace_metadata: HashMap<String, String>,
}

#[derive(Encode, Decode, Debug, Clone, DefaultPrio, DeriveMsg)]
pub enum PingPongMsg {
    Ping,
    Pong,
}

// //////////////////////////////////////////////////////////////////////////////
//                                  Processor
// //////////////////////////////////////////////////////////////////////////////
struct Processor {
    active_span: Option<tracing::Span>,
    actor_name: String,
}

impl reactor_actor::ActorProcess for Processor {
    type IMsg = TracedMessage;
    type OMsg = TracedMessage;

    // fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
    //     let output_payload;

    //     let parent_context = match input.payload {
    //         PingPongMsg::Ping => {
    //             debug_assert!(self.actor_name == "pinger", "ponger should not receive Ping messages");
                
    //             // Close existing span if any (continuing cycle)
    //             if let Some(span) = self.active_span.take() {
    //                 drop(span); // Close previous cycle span
    //             }

    //             // Create span in the right parent context
    //             let cycle_span = info_span!("ping_pong_cycle", actor = self.actor_name);
    //             self.active_span = Some(cycle_span);

    //             output_payload = PingPongMsg::Pong;
    //             None // Use current context (new root)
    //         }

    //         PingPongMsg::Pong => {
    //             debug_assert!(self.actor_name == "ponger", "pinger should not receive Pong messages");

    //             // Extract the parent context from the input message (trace_metadata)
    //             let propagator = TraceContextPropagator::new();
    //             let extracted_context = propagator.extract(&input.trace_metadata);
    //             tracing::info!("Extracted context from: {:?}", input.trace_metadata);

    //             output_payload = PingPongMsg::Ping;
    //             Some(extracted_context) // Use extracted context as parent
    //         }
    //     };

    //     // Attach the parent context to the current context (if exists)
    //     let _guard = parent_context.as_ref().map(|ctx| ctx.clone().attach());

    //     // Enter the active span context
    //     let _active_guard = self.active_span.as_ref().map(|span| span.enter());

    //     // Debug the current context
    //     let current_context = OtelContext::current();
    //     let span_ref = current_context.span();
    //     let span_context = span_ref.span_context();

    //     tracing::info!("Current span context valid: {}", span_context.is_valid());
    //     if span_context.is_valid() {
    //         tracing::info!("Trace ID: {}", span_context.trace_id());
    //         tracing::info!("Span ID: {}", span_context.span_id());
    //     }

    //     // Create injected trace metadata from current context
    //     let mut trace_metadata: HashMap<String, String> = HashMap::new();
    //     let propagator = TraceContextPropagator::new();
    //     let current_context = OtelContext::current();
    //     propagator.inject_context(&current_context, &mut trace_metadata);

    //     tracing::info!("Injecting context: {:?}", trace_metadata);

    //     // Create process span as a child of the active span
    //     let process_span = info_span!("process_message", message = ?input.payload);
    //     let _process_enter = process_span.enter();

    //     std::thread::sleep(Duration::from_secs(1));
    //     info!("{:?}", input.payload);

        
    //     vec![TracedMessage {
    //         payload: output_payload,
    //         trace_metadata: trace_metadata,
    //     }]
    // }
    fn process(&mut self, input: Self::IMsg) -> Vec<Self::OMsg> {
        let output_payload;

        match input.payload {
            PingPongMsg::Ping => {
                debug_assert!(self.actor_name == "pinger", "ponger should not receive Ping messages");
                
                // Close existing span if any (continuing cycle)
                if let Some(span) = self.active_span.take() {
                    drop(span); // Close previous cycle span
                }

                // Create span in the right parent context
                let cycle_span = info_span!("ping_pong_cycle", actor = self.actor_name);

                let clone_cycle_span = cycle_span.clone();
                let _process_enter =clone_cycle_span.enter();
                output_payload = PingPongMsg::Pong;
                std::thread::sleep(Duration::from_secs(1));
                tracing::info!("{:?}", input.payload);
                println!("Active span Id{:?}",cycle_span.id());

                let mut trace_metadata: HashMap<String, String> = HashMap::new();
                let propagator = TraceContextPropagator::new();
                let current_context = OtelContext::current();
                let current_context = current_context.with_remote_span_context(cycle_span.context().span().span_context().clone());
                println!("Current COntext: {:?}", current_context);
                propagator.inject_context(&current_context, &mut trace_metadata);

                self.active_span = Some(cycle_span);

                vec![TracedMessage {
                    payload: output_payload,
                    trace_metadata: trace_metadata,
                }]
            }


            PingPongMsg::Pong => {
                debug_assert!(self.actor_name == "ponger", "pinger should not receive Pong messages");

                // Extract the parent context from the input message (trace_metadata)
                let propagator = TraceContextPropagator::new();
                let extracted_context = propagator.extract(&input.trace_metadata);
                tracing::info!("Extracted context from: {:?}", input.trace_metadata);

                // self.active_span = extracted_context.

                let trace_metadata = HashMap::new();
                output_payload = PingPongMsg::Ping;
                vec![TracedMessage {
                    payload: output_payload,
                    trace_metadata: trace_metadata,
                }]
            }
        }

        // Attach the parent context to the current context (if exists)
        // let _guard = parent_context.as_ref().map(|ctx| ctx.clone().attach());

        // // Enter the active span context
        // let _active_guard = self.active_span.as_ref().map(|span| span.enter());

        // // Debug the current context
        // let current_context = OtelContext::current();
        // let span_ref = current_context.span();
        // let span_context = span_ref.span_context();

        // tracing::info!("Current span context valid: {}", span_context.is_valid());
        // if span_context.is_valid() {
        //     tracing::info!("Trace ID: {}", span_context.trace_id());
        //     tracing::info!("Span ID: {}", span_context.span_id());
        // }

        // // Create injected trace metadata from current context
        // let mut trace_metadata: HashMap<String, String> = HashMap::new();
        // let propagator = TraceContextPropagator::new();
        // // let current_context = OtelContext::current();
        // propagator.inject_context(&current_context, &mut trace_metadata);

        // tracing::info!("Injecting context: {:?}", trace_metadata);

        // Create process span as a child of the active span
        // let process_span = info_span!("process_message", message = ?input.payload);
        // let _process_enter = process_span.enter();

        
    }
}

impl Processor {
    fn new(actor_name: String) -> Self {
        Self { 
            active_span: None,
            actor_name: actor_name,
        }
    }
}
// //////////////////////////////////////////////////////////////////////////////
//                                  Sender
// //////////////////////////////////////////////////////////////////////////////
struct Sender {
    other_addr: String,

    #[cfg(feature = "chaos")]
    drop: Vec<ActorAddrRef>,
}
impl reactor_actor::ActorSend for Sender {
    type OMsg = TracedMessage;

    async fn before_send<'a>(&'a mut self, _output: &Self::OMsg) -> RouteTo<'a> {
        #[cfg(feature = "chaos")]
        {
            let b: bool = random();
            if b {
                warn!("Chaos! Dropping");
                return &self.drop;
            }
        }
        RouteTo::from(self.other_addr.as_str())
    }
}
impl Sender {
    fn new(other_actor: String) -> Self {
        Sender {
            other_addr: other_actor,
            #[cfg(feature = "chaos")]
            drop: vec![],
        }
    }
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

pub async fn actor(ctx: RuntimeCtx, other_addr: String) {
    BehaviourBuilder::new(Processor::new(ctx.addr.to_string()), BincodeCodec::default())
        .send(Sender::new(other_addr))
        .generator_if(ctx.addr == "pinger", || {
            vec![TracedMessage {
                payload: PingPongMsg::Ping,
                trace_metadata: HashMap::new(),
            }].into_iter()
        })
        .build()
        .run(ctx)
        .await
        .unwrap();
}

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub fn pingpong(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let other: String = payload
        .remove("other")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    RUNTIME.spawn(actor(ctx, other));
}
