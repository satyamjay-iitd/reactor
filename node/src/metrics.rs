//! `GET /metrics`: what each local actor has done, as Prometheus' text format
//! or as JSON. The actors count into their [`ActorMetrics`]; this reads them.

use std::fmt::Write as _;

use reactor_actor::{ActorState, MetricsSnapshot};
use serde::Serialize;

/// One actor's metrics, with what it runs.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActorReport {
    pub(crate) actor: String,
    pub(crate) op: String,
    pub(crate) lib: String,
    #[serde(flatten)]
    pub(crate) metrics: MetricsSnapshot,
}

/// `GET /metrics` as JSON.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MetricsResponse {
    /// When they were read, in ms since the Unix epoch.
    pub(crate) time_ms: u64,
    pub(crate) actors: Vec<ActorReport>,
}

/// A label value, escaped as the text format wants.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The metrics in Prometheus' text exposition format (0.0.4).
pub(crate) fn prometheus(actors: &[ActorReport]) -> String {
    let mut out = String::new();
    // Each metric: name, type, help, and its value per actor (with extra
    // labels, for the per-destination and the operators' own ones).
    type Values<'a> = Vec<(String, f64)>;
    let mut metric =
        |name: &str, kind: &str, help: &str, values: &dyn Fn(&ActorReport) -> Values| {
            let _ = writeln!(out, "# HELP reactor_actor_{name} {help}");
            let _ = writeln!(out, "# TYPE reactor_actor_{name} {kind}");
            for a in actors {
                for (extra, v) in values(a) {
                    let _ = writeln!(
                        out,
                        "reactor_actor_{name}{{actor=\"{}\",op=\"{}\"{extra}}} {v}",
                        escape(&a.actor),
                        escape(&a.op)
                    );
                }
            }
        };
    let one = |v: f64| vec![(String::new(), v)];
    metric("up", "gauge", "1 if the actor runs, 0 if it ended.", &|a| {
        one(matches!(a.metrics.state, ActorState::Starting | ActorState::Running) as u8 as f64)
    });
    metric(
        "failed",
        "gauge",
        "1 if the actor ended with an error.",
        &|a| one((a.metrics.state == ActorState::Failed) as u8 as f64),
    );
    metric(
        "received_total",
        "counter",
        "Messages that reached the processor.",
        &|a| one(a.metrics.received as f64),
    );
    metric("processed_total", "counter", "Messages processed.", &|a| {
        one(a.metrics.processed as f64)
    });
    metric(
        "outputs_total",
        "counter",
        "Messages the processor gave.",
        &|a| one(a.metrics.outputs as f64),
    );
    metric(
        "busy_seconds_total",
        "counter",
        "Time spent in the processor.",
        &|a| one(a.metrics.busy_ms / 1e3),
    );
    metric(
        "queued_inputs",
        "gauge",
        "Messages waiting to be processed.",
        &|a| one(a.metrics.queued_in as f64),
    );
    metric(
        "queued_outputs",
        "gauge",
        "Outputs waiting to be sent.",
        &|a| one(a.metrics.queued_out as f64),
    );
    metric(
        "last_active_timestamp_seconds",
        "gauge",
        "When the actor last processed a message (Unix time).",
        &|a| match a.metrics.last_active_ms {
            Some(t) => one(t as f64 / 1e3),
            None => vec![],
        },
    );
    let to = |to: &str| format!(",to=\"{}\"", escape(to));
    metric(
        "sent_messages_total",
        "counter",
        "Messages sent, by destination.",
        &|a| {
            a.metrics
                .sent
                .iter()
                .map(|(t, s)| (to(t), s.messages as f64))
                .collect()
        },
    );
    metric(
        "sent_bytes_total",
        "counter",
        "Encoded bytes sent, by destination (0 to actors on this node).",
        &|a| {
            a.metrics
                .sent
                .iter()
                .map(|(t, s)| (to(t), s.bytes as f64))
                .collect()
        },
    );
    metric(
        "send_failures_total",
        "counter",
        "Messages that couldn't be sent, by destination.",
        &|a| {
            a.metrics
                .sent
                .iter()
                .map(|(t, s)| (to(t), s.failed as f64))
                .collect()
        },
    );
    metric(
        "custom",
        "gauge",
        "The operator's own values, by name.",
        &|a| {
            a.metrics
                .custom
                .iter()
                .map(|(n, v)| (format!(",name=\"{}\"", escape(n)), *v))
                .collect()
        },
    );
    out
}

#[cfg(test)]
mod tests {
    use reactor_actor::ActorMetrics;

    use super::*;

    fn report(actor: &str) -> ActorReport {
        let m = ActorMetrics::new();
        m.set("latency \"p95\"", 7.5);
        let mut metrics = m.snapshot();
        (metrics.processed, metrics.outputs, metrics.busy_ms) = (1, 2, 1500.0);
        ActorReport {
            actor: actor.into(),
            op: "viz_tx".into(),
            lib: "viz_actor".into(),
            metrics,
        }
    }

    #[test]
    fn prometheus_text() {
        let text = prometheus(&[report("tx")]);
        assert!(text.contains("# TYPE reactor_actor_processed_total counter\n"));
        assert!(text.contains("reactor_actor_processed_total{actor=\"tx\",op=\"viz_tx\"} 1\n"));
        assert!(text.contains("reactor_actor_outputs_total{actor=\"tx\",op=\"viz_tx\"} 2\n"));
        assert!(
            text.contains("reactor_actor_busy_seconds_total{actor=\"tx\",op=\"viz_tx\"} 1.5\n")
        );
        assert!(
            text.contains("reactor_actor_up{actor=\"tx\",op=\"viz_tx\"} 1\n"),
            "{text}"
        );
        // Label values are escaped.
        assert!(
            text.contains("reactor_actor_custom{actor=\"tx\",op=\"viz_tx\",name=\"latency \\\"p95\\\"\"} 7.5\n"),
            "{text}"
        );
        // Every sample line is `name{labels} value`.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (_, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
        }
    }

    #[test]
    fn json_has_the_actor_and_its_counts() {
        let json = serde_json::to_value(MetricsResponse {
            time_ms: 1,
            actors: vec![report("tx")],
        })
        .unwrap();
        let a = &json["actors"][0];
        assert_eq!(a["actor"], "tx");
        assert_eq!(a["lib"], "viz_actor");
        assert_eq!(a["state"], "starting");
        assert_eq!(a["processed"], 1);
    }
}
