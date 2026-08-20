//! Session-lifecycle counters shared by the adapter, the acceptor and the
//! connector.
//!
//! A churn test reads these to decide whether it passed: after traffic
//! quiesces, `sessions_active`, `registrations_pending` and `tasks_live` must
//! all be zero, and `registrations_orphaned` must not have grown.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family, gauge::Gauge};
use prometheus_client::registry::Registry;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Why a control-plane admission decision was reached.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ControlEventLabels {
    /// The decision surface: `grant`, `membership`, `revocation` or `peer`.
    pub kind: String,
    /// A stable lowercase slug, `ok` for the accepting outcome.
    pub reason: String,
}

/// Registration, membership, revocation and peer-channel outcomes.
///
/// Registration decisions are taken on the Comch control thread and peer
/// refusals on a data worker, so the family is process-global: it is written
/// from several threads and every worker's admin endpoint reports the same
/// values.
pub fn control_events() -> &'static Family<ControlEventLabels, Counter> {
    static CONTROL_EVENTS: OnceLock<Family<ControlEventLabels, Counter>> = OnceLock::new();
    CONTROL_EVENTS.get_or_init(Family::default)
}

/// Count one control-plane admission decision.
pub fn record_control_event(kind: &str, reason: &str) {
    control_events()
        .get_or_create(&ControlEventLabels {
            kind: kind.to_string(),
            reason: reason.to_string(),
        })
        .inc();
}

#[derive(Debug, Default)]
pub struct SessionMetrics {
    /// Sessions the adapter opened.
    pub sessions_opened: Counter,
    /// Sessions the adapter closed.
    pub sessions_closed: Counter,
    /// Sessions the adapter is carrying.
    pub sessions_active: Gauge,
    /// Endpoints registered with the acceptor and not yet bound to a session.
    pub registrations_pending: Gauge,
    /// Registrations that arrived for a session that had already closed.
    pub registrations_orphaned: Counter,
    /// Endpoints aborted by a close, a poison or a worker stop.
    pub endpoints_aborted: Counter,
    /// Connection tasks the acceptor is driving.
    pub tasks_live: Gauge,
    /// Connection tasks cancelled by a close rather than by completion.
    pub tasks_cancelled: Counter,
    /// Backend channels a connector could not take.
    pub backend_take_errors: Counter,
    /// Linkerd selected a target outside the authoritative Service snapshot.
    pub backend_target_mismatches: Counter,
    /// Per-session outbound stacks built for DPUmesh frontend connections.
    pub session_stack_builds: Counter,
    /// Time spent cloning/configuring the per-session outbound template.
    pub session_stack_configure_nanoseconds: Counter,
    /// Time spent constructing the per-session outbound layers.
    pub session_stack_layers_nanoseconds: Counter,
    /// Time spent instantiating the target service from those layers.
    pub session_stack_service_nanoseconds: Counter,
    /// Slots withdrawn from reuse because their generation space ran out.
    pub slots_retired: Gauge,
    /// Sessions the adapter refused, by the cause it refused them for.
    pub sessions_declined: Family<DeclineLabels, Counter>,
}

/// Why the adapter refused a session.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DeclineLabels {
    pub reason: String,
}

impl SessionMetrics {
    /// Register the counters in the proxy's own registry. Cloning a metric
    /// shares its value, so the returned handle and the registry observe the
    /// same counters.
    pub fn register(registry: &mut Registry) -> Arc<Self> {
        let metrics = Self::default();
        registry.register(
            "sessions_opened",
            "DPUmesh sessions opened",
            metrics.sessions_opened.clone(),
        );
        registry.register(
            "sessions_closed",
            "DPUmesh sessions closed",
            metrics.sessions_closed.clone(),
        );
        registry.register(
            "sessions_active",
            "DPUmesh sessions currently carried by the adapter",
            metrics.sessions_active.clone(),
        );
        registry.register(
            "registrations_pending",
            "Endpoints registered with the acceptor and not yet bound to a session",
            metrics.registrations_pending.clone(),
        );
        registry.register(
            "registrations_orphaned",
            "Registrations that arrived after their session closed",
            metrics.registrations_orphaned.clone(),
        );
        registry.register(
            "endpoints_aborted",
            "DPUmesh endpoints aborted by a close, a poison or a worker stop",
            metrics.endpoints_aborted.clone(),
        );
        registry.register(
            "tasks_live",
            "Connection tasks the DPUmesh acceptor is driving",
            metrics.tasks_live.clone(),
        );
        registry.register(
            "tasks_cancelled",
            "Connection tasks cancelled by a session close",
            metrics.tasks_cancelled.clone(),
        );
        registry.register(
            "backend_take_errors",
            "Backend channels a connector could not take",
            metrics.backend_take_errors.clone(),
        );
        registry.register(
            "backend_target_mismatches",
            "Linkerd-selected targets rejected outside the authoritative DPUmesh Service",
            metrics.backend_target_mismatches.clone(),
        );
        registry.register(
            "session_stack_builds",
            "Per-session DPUmesh outbound stacks built",
            metrics.session_stack_builds.clone(),
        );
        registry.register(
            "session_stack_configure_nanoseconds",
            "Nanoseconds spent cloning and configuring per-session outbound templates",
            metrics.session_stack_configure_nanoseconds.clone(),
        );
        registry.register(
            "session_stack_layers_nanoseconds",
            "Nanoseconds spent constructing per-session outbound layers",
            metrics.session_stack_layers_nanoseconds.clone(),
        );
        registry.register(
            "session_stack_service_nanoseconds",
            "Nanoseconds spent instantiating target services from per-session outbound layers",
            metrics.session_stack_service_nanoseconds.clone(),
        );
        registry.register(
            "slots_retired",
            "Session slots withdrawn from reuse because their generations ran out",
            metrics.slots_retired.clone(),
        );
        registry.register(
            "sessions_declined",
            "DPUmesh sessions the adapter refused, by cause",
            metrics.sessions_declined.clone(),
        );
        registry.register(
            "control_events",
            "Registration, membership and revocation outcomes by kind and reason",
            control_events().clone(),
        );
        Arc::new(metrics)
    }

    /// Count one refused session.
    pub fn record_decline(&self, reason: &str) {
        self.sessions_declined
            .get_or_create(&DeclineLabels {
                reason: reason.to_string(),
            })
            .inc();
    }

    /// Record the synchronous phases of building one session-isolated stack.
    pub fn observe_stack_build(&self, configure: Duration, layers: Duration, service: Duration) {
        fn nanos(duration: Duration) -> u64 {
            duration.as_nanos().min(u64::MAX as u128) as u64
        }

        self.session_stack_builds.inc();
        self.session_stack_configure_nanoseconds
            .inc_by(nanos(configure));
        self.session_stack_layers_nanoseconds.inc_by(nanos(layers));
        self.session_stack_service_nanoseconds
            .inc_by(nanos(service));
    }

    /// Everything a quiesced worker must report as zero.
    pub fn quiescent(&self) -> bool {
        self.sessions_active.get() == 0
            && self.registrations_pending.get() == 0
            && self.tasks_live.get() == 0
    }

    /// One line for the DPU log, which is the only diagnostic a deployed run has.
    pub fn summary(&self) -> String {
        format!(
            "opened={} closed={} active={} pending={} orphaned={} aborted={} \
             tasks={} cancelled={} take_errors={} target_mismatches={} stack_builds={} \
             stack_configure_ns={} stack_layers_ns={} stack_service_ns={} retired_slots={}",
            self.sessions_opened.get(),
            self.sessions_closed.get(),
            self.sessions_active.get(),
            self.registrations_pending.get(),
            self.registrations_orphaned.get(),
            self.endpoints_aborted.get(),
            self.tasks_live.get(),
            self.tasks_cancelled.get(),
            self.backend_take_errors.get(),
            self.backend_target_mismatches.get(),
            self.session_stack_builds.get(),
            self.session_stack_configure_nanoseconds.get(),
            self.session_stack_layers_nanoseconds.get(),
            self.session_stack_service_nanoseconds.get(),
            self.slots_retired.get(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_shares_the_counters() {
        let mut registry = Registry::default();
        let metrics = SessionMetrics::register(&mut registry);
        metrics.sessions_opened.inc();
        metrics.sessions_active.inc();
        metrics.observe_stack_build(
            Duration::from_nanos(11),
            Duration::from_nanos(22),
            Duration::from_nanos(33),
        );

        let mut encoded = String::new();
        prometheus_client::encoding::text::encode(&mut encoded, &registry).unwrap();
        assert!(encoded.contains("sessions_opened_total 1"), "{encoded}");
        assert!(encoded.contains("sessions_active 1"), "{encoded}");
        assert!(
            encoded.contains("session_stack_builds_total 1"),
            "{encoded}"
        );
        assert!(
            encoded.contains("session_stack_layers_nanoseconds_total 22"),
            "{encoded}"
        );
        assert!(!metrics.quiescent());

        metrics.sessions_active.dec();
        assert!(metrics.quiescent());
    }
}
