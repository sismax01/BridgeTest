"""Core domain models for BridgeTest scenarios, chaos rules, twin state, and invariant reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class FaultType(str, Enum):
    """Supported downstream fault injection modes."""

    RATE_LIMIT_429 = "rate_limit_429"
    TRANSIENT_503 = "transient_503"
    GATEWAY_TIMEOUT_504 = "gateway_timeout_504"
    NETWORK_JITTER = "network_jitter"
    CORRUPT_JSON = "corrupt_json"
    PARTIAL_OUTAGE = "partial_outage"


class DriftType(str, Enum):
    """Supported inbound webhook schema drift and contract mutation types."""

    STRINGIFY_NUMBERS = "stringify_numbers"
    RENAME_FIELD = "rename_field"
    DROP_REQUIRED_FIELD = "drop_required_field"
    INJECT_EXTRA_FIELDS = "inject_extra_fields"
    NULLIFY_OPTIONAL_FIELD = "nullify_optional_field"
    TAMPER_SIGNATURE = "tamper_signature"


class InvariantType(str, Enum):
    """Built-in distributed state and business outcome invariant types."""

    EXACT_CARDINALITY = "exact_cardinality"
    CROSS_SYSTEM_CONSISTENCY = "cross_system_consistency"
    IDEMPOTENCY_ENFORCEMENT = "idempotency_enforcement"
    RETRY_BACKOFF_COMPLIANCE = "retry_backoff_compliance"
    POISON_PILL_CONTAINMENT = "poison_pill_containment"
    ZERO_SILENT_SWALLOW = "zero_silent_swallow"


@dataclass
class FaultRule:
    """Defines a fault injection behavior attached to a specific downstream twin service."""

    rule_id: str
    target_service: str  # "crm", "ledger", "notification", "dlq", or "*"
    fault_type: FaultType
    fail_first_n_per_key: int = 0
    probability: float = 0.0
    latency_ms: int = 0
    jitter_ms: int = 0
    retry_after_seconds: float = 0.05
    status_code: Optional[int] = None
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["fault_type"] = self.fault_type.value
        return data

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> FaultRule:
        return cls(
            rule_id=str(raw["rule_id"]),
            target_service=str(raw.get("target_service", "*")),
            fault_type=FaultType(raw["fault_type"]),
            fail_first_n_per_key=int(raw.get("fail_first_n_per_key", 0)),
            probability=float(raw.get("probability", 0.0)),
            latency_ms=int(raw.get("latency_ms", 0)),
            jitter_ms=int(raw.get("jitter_ms", 0)),
            retry_after_seconds=float(raw.get("retry_after_seconds", 0.05)),
            status_code=int(raw["status_code"]) if raw.get("status_code") is not None else None,
            enabled=bool(raw.get("enabled", True)),
        )


@dataclass
class DriftMutation:
    """Describes an inbound payload or header mutation applied to a subset of events."""

    mutation_id: str
    drift_type: DriftType
    apply_to_event_indices: List[int] = field(default_factory=list)
    source_field: Optional[str] = None
    target_field: Optional[str] = None
    extra_payload: Dict[str, Any] = field(default_factory=dict)
    expect_rejected_or_dlq: bool = False

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["drift_type"] = self.drift_type.value
        return data

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> DriftMutation:
        return cls(
            mutation_id=str(raw["mutation_id"]),
            drift_type=DriftType(raw["drift_type"]),
            apply_to_event_indices=[int(i) for i in raw.get("apply_to_event_indices", [])],
            source_field=raw.get("source_field"),
            target_field=raw.get("target_field"),
            extra_payload=dict(raw.get("extra_payload", {})),
            expect_rejected_or_dlq=bool(raw.get("expect_rejected_or_dlq", False)),
        )


@dataclass
class ChaosProfile:
    """Configures how inbound webhook events are dispatched and mutated."""

    duplicate_Deliveries_per_event: int = 1
    concurrent_duplicates: bool = False
    duplicate_delay_ms: int = 0
    out_of_order_delivery: bool = False
    signing_secret: str = "bt_whsec_prod_9f8e7d6c5b4a3210"
    mutations: List[DriftMutation] = field(default_factory=list)
    fault_rules: List[FaultRule] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "duplicate_deliveries_per_event": self.duplicate_Deliveries_per_event,
            "concurrent_duplicates": self.concurrent_duplicates,
            "duplicate_delay_ms": self.duplicate_delay_ms,
            "out_of_order_delivery": self.out_of_order_delivery,
            "signing_secret": self.signing_secret,
            "mutations": [m.to_dict() for m in self.mutations],
            "fault_rules": [f.to_dict() for f in self.fault_rules],
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> ChaosProfile:
        dup = raw.get(
            "duplicate_deliveries_per_event",
            raw.get("duplicate_Deliveries_per_event", 1),
        )
        return cls(
            duplicate_Deliveries_per_event=int(dup),
            concurrent_duplicates=bool(raw.get("concurrent_duplicates", False)),
            duplicate_delay_ms=int(raw.get("duplicate_delay_ms", 0)),
            out_of_order_delivery=bool(raw.get("out_of_order_delivery", False)),
            signing_secret=str(raw.get("signing_secret", "bt_whsec_prod_9f8e7d6c5b4a3210")),
            mutations=[DriftMutation.from_dict(m) for m in raw.get("mutations", [])],
            fault_rules=[FaultRule.from_dict(f) for f in raw.get("fault_rules", [])],
        )


@dataclass
class WebhookEvent:
    """Logical business event generated by the chaos producer."""

    event_id: str
    event_type: str
    idempotency_key: str
    correlation_id: str
    timestamp: int
    payload: Dict[str, Any]
    is_poison_expected: bool = False
    mutation_applied: Optional[str] = None
    tamper_signature: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> WebhookEvent:
        return cls(
            event_id=str(raw["event_id"]),
            event_type=str(raw.get("event_type", "order.completed")),
            idempotency_key=str(raw["idempotency_key"]),
            correlation_id=str(raw.get("correlation_id", raw["event_id"])),
            timestamp=int(raw.get("timestamp", 1761171720)),
            payload=dict(raw["payload"]),
            is_poison_expected=bool(raw.get("is_poison_expected", False)),
            mutation_applied=raw.get("mutation_applied"),
            tamper_signature=bool(raw.get("tamper_signature", False)),
        )


@dataclass
class JournalEntry:
    """Immutable flight-recorder entry for an inbound or downstream HTTP interaction."""

    seq_id: int
    timestamp_monotonic: float
    direction: str  # "inbound" or "downstream"
    service: str  # "sut", "crm", "ledger", "notification", "dlq"
    method: str
    path: str
    correlation_id: str
    idempotency_key: Optional[str]
    status_code: int
    latency_ms: float
    fault_injected: Optional[str]
    request_headers: Dict[str, str]
    request_body: Any
    response_body: Any
    attempt_index: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class InvariantSpec:
    """Declarative specification for an end-to-end invariant check."""

    invariant_id: str
    invariant_type: InvariantType
    description: str
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invariant_id": self.invariant_id,
            "invariant_type": self.invariant_type.value,
            "description": self.description,
            "params": self.params,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> InvariantSpec:
        return cls(
            invariant_id=str(raw["invariant_id"]),
            invariant_type=InvariantType(raw["invariant_type"]),
            description=str(raw.get("description", "")),
            params=dict(raw.get("params", {})),
        )


@dataclass
class InvariantResult:
    """Evaluation outcome of a single business or consistency invariant."""

    invariant_id: str
    invariant_type: InvariantType
    passed: bool
    description: str
    expected_summary: str
    actual_summary: str
    violations: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invariant_id": self.invariant_id,
            "invariant_type": self.invariant_type.value,
            "passed": self.passed,
            "description": self.description,
            "expected_summary": self.expected_summary,
            "actual_summary": self.actual_summary,
            "violations": self.violations,
            "metrics": self.metrics,
        }


@dataclass
class ScenarioSpec:
    """Complete chaos scenario definition combining events, chaos profile, and invariants."""

    scenario_id: str
    name: str
    description: str
    events: List[WebhookEvent]
    chaos: ChaosProfile
    invariants: List[InvariantSpec]
    quiescence_wait_ms: int = 80

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "name": self.name,
            "description": self.description,
            "events": [e.to_dict() for e in self.events],
            "chaos": self.chaos.to_dict(),
            "invariants": [i.to_dict() for i in self.invariants],
            "quiescence_wait_ms": self.quiescence_wait_ms,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> ScenarioSpec:
        return cls(
            scenario_id=str(raw["scenario_id"]),
            name=str(raw["name"]),
            description=str(raw.get("description", "")),
            events=[WebhookEvent.from_dict(e) for e in raw.get("events", [])],
            chaos=ChaosProfile.from_dict(raw.get("chaos", {})),
            invariants=[InvariantSpec.from_dict(i) for i in raw.get("invariants", [])],
            quiescence_wait_ms=int(raw.get("quiescence_wait_ms", 80)),
        )


@dataclass
class ScenarioResult:
    """Complete execution report of a scenario against a target pipeline."""

    scenario_id: str
    scenario_name: str
    target_name: str
    passed: bool
    duration_ms: float
    total_logical_events: int
    total_inbound_deliveries: int
    total_downstream_calls: int
    total_faults_injected: int
    twin_state_counts: Dict[str, int]
    invariant_results: List[InvariantResult]
    journal_summary: Dict[str, Any]

    @property
    def passed_invariants_count(self) -> int:
        return sum(1 for r in self.invariant_results if r.passed)

    @property
    def total_invariants_count(self) -> int:
        return len(self.invariant_results)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "scenario_name": self.scenario_name,
            "target_name": self.target_name,
            "passed": self.passed,
            "duration_ms": round(self.duration_ms, 2),
            "total_logical_events": self.total_logical_events,
            "total_inbound_deliveries": self.total_inbound_deliveries,
            "total_downstream_calls": self.total_downstream_calls,
            "total_faults_injected": self.total_faults_injected,
            "twin_state_counts": self.twin_state_counts,
            "passed_invariants": self.passed_invariants_count,
            "total_invariants": self.total_invariants_count,
            "invariant_results": [r.to_dict() for r in self.invariant_results],
            "journal_summary": self.journal_summary,
        }


@dataclass
class ComparisonReport:
    """Side-by-side comparison across two integration pipeline implementations."""

    naive_results: List[ScenarioResult]
    resilient_results: List[ScenarioResult]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "naive_results": [r.to_dict() for r in self.naive_results],
            "resilient_results": [r.to_dict() for r in self.resilient_results],
        }
