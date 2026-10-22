"""Standard 6-scenario chaos verification suite for BridgeTest."""

from __future__ import annotations

from typing import List

from bridgetest.models import (
    ChaosProfile,
    DriftMutation,
    DriftType,
    FaultRule,
    FaultType,
    InvariantSpec,
    InvariantType,
    ScenarioSpec,
    WebhookEvent,
)


def make_base_order_events(count: int = 4, prefix: str = "evt_ord") -> List[WebhookEvent]:
    """Generate a clean baseline batch of e-commerce order completion webhook events."""
    customers = [
        ("ord_1001", "ada.lovelace@analytical.io", 14900, "USD"),
        ("ord_1002", "grace.hopper@compilers.org", 29900, "USD"),
        ("ord_1003", "margaret.hamilton@apollo.gov", 45000, "USD"),
        ("ord_1004", "barbara.liskov@distributed.edu", 19500, "USD"),
        ("ord_1005", "radia.perlman@spanningtree.net", 32000, "USD"),
    ]
    events: List[WebhookEvent] = []
    for idx in range(count):
        order_id, email, cents, currency = customers[idx % len(customers)]
        eid = f"{prefix}_{idx + 1:02d}"
        events.append(
            WebhookEvent(
                event_id=eid,
                event_type="order.completed",
                idempotency_key=f"idem_{eid}",
                correlation_id=f"corr_{eid}",
                timestamp=1761171720 + idx,
                payload={
                    "order_id": f"{order_id}_{idx + 1}",
                    "customer_email": email,
                    "amount_cents": cents,
                    "currency": currency,
                    "coupon_code": "AUTUMN2026",
                },
            )
        )
    return events


def build_standard_chaos_suite() -> List[ScenarioSpec]:
    """Construct the 6 canonical BridgeTest scenarios spanning baseline to full multi-fault chaos."""

    # 1. Happy Path Baseline
    s1_events = make_base_order_events(3, prefix="evt_happy")
    s1 = ScenarioSpec(
        scenario_id="01_happy_path",
        name="Staging Happy-Path Baseline",
        description="Clean single-delivery webhooks with zero downstream faults or schema drift.",
        events=s1_events,
        chaos=ChaosProfile(duplicate_Deliveries_per_event=1),
        invariants=[
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="Exactly 3 orders persisted in CRM, Ledger, and Notification twins",
                params={"expected_valid_records": 3},
            ),
            InvariantSpec(
                invariant_id="inv_consistency",
                invariant_type=InvariantType.CROSS_SYSTEM_CONSISTENCY,
                description="CRM, Ledger, and Notification records match 1:1 without split-brain",
            ),
            InvariantSpec(
                invariant_id="inv_no_swallow",
                invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
                description="Every inbound order event is completely reconciled",
            ),
        ],
        quiescence_wait_ms=10,
    )

    # 2. Concurrent Duplicate Webhook Storm
    s2_events = make_base_order_events(4, prefix="evt_dup")
    s2 = ScenarioSpec(
        scenario_id="02_duplicate_storm",
        name="Concurrent 3x Duplicate Webhook Storm",
        description="Simulates at-least-once webhook retries firing 3 concurrent deliveries per event.",
        events=s2_events,
        chaos=ChaosProfile(
            duplicate_Deliveries_per_event=3,
            concurrent_duplicates=True,
        ),
        invariants=[
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="Exactly 4 unique orders persisted despite 12 inbound webhook deliveries",
                params={"expected_valid_records": 4},
            ),
            InvariantSpec(
                invariant_id="inv_idempotency",
                invariant_type=InvariantType.IDEMPOTENCY_ENFORCEMENT,
                description="All downstream writes enforce Idempotency-Key and prevent duplicate records",
            ),
            InvariantSpec(
                invariant_id="inv_consistency",
                invariant_type=InvariantType.CROSS_SYSTEM_CONSISTENCY,
                description="Cross-system state remains consistent under concurrent delivery race",
            ),
        ],
        quiescence_wait_ms=15,
    )

    # 3. Downstream HTTP 429 Rate Limit & Retry-After Compliance
    s3_events = make_base_order_events(3, prefix="evt_rl429")
    s3 = ScenarioSpec(
        scenario_id="03_rate_limit_429",
        name="CRM Rate-Limit (HTTP 429) & Retry-After Backoff",
        description="CRM twin rejects the first 2 attempts per key with HTTP 429 and Retry-After=0.02s.",
        events=s3_events,
        chaos=ChaosProfile(
            duplicate_Deliveries_per_event=1,
            fault_rules=[
                FaultRule(
                    rule_id="crm_burst_429",
                    target_service="crm",
                    fault_type=FaultType.RATE_LIMIT_429,
                    fail_first_n_per_key=2,
                    retry_after_seconds=0.02,
                )
            ],
        ),
        invariants=[
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="Zero dropped records: all 3 orders persisted after rate-limit recovery",
                params={"expected_valid_records": 3},
            ),
            InvariantSpec(
                invariant_id="inv_backoff",
                invariant_type=InvariantType.RETRY_BACKOFF_COMPLIANCE,
                description="Pipeline retries HTTP 429 and respects Retry-After >= 15ms",
                params={"min_backoff_seconds": 0.015},
            ),
            InvariantSpec(
                invariant_id="inv_no_swallow",
                invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
                description="No orders lost during upstream rate limiting",
            ),
        ],
        quiescence_wait_ms=15,
    )

    # 4. Partial Outage Cascade (Split-Brain Prevention)
    s4_events = make_base_order_events(3, prefix="evt_part503")
    s4 = ScenarioSpec(
        scenario_id="04_partial_cascade_503",
        name="Mid-Workflow Ledger & Notification 503 Outage",
        description="CRM succeeds on attempt 1, but Ledger and Notification fail with transient HTTP 503.",
        events=s4_events,
        chaos=ChaosProfile(
            duplicate_Deliveries_per_event=1,
            fault_rules=[
                FaultRule(
                    rule_id="ledger_transient_503",
                    target_service="ledger",
                    fault_type=FaultType.TRANSIENT_503,
                    fail_first_n_per_key=2,
                    retry_after_seconds=0.015,
                ),
                FaultRule(
                    rule_id="notif_transient_503",
                    target_service="notification",
                    fault_type=FaultType.GATEWAY_TIMEOUT_504,
                    fail_first_n_per_key=1,
                    retry_after_seconds=0.015,
                ),
            ],
        ),
        invariants=[
            InvariantSpec(
                invariant_id="inv_consistency",
                invariant_type=InvariantType.CROSS_SYSTEM_CONSISTENCY,
                description="Zero split-brain state across CRM, Ledger, and Notification twins",
            ),
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="All 3 orders eventually settle across all 3 downstream twins",
                params={"expected_valid_records": 3},
            ),
            InvariantSpec(
                invariant_id="inv_backoff",
                invariant_type=InvariantType.RETRY_BACKOFF_COMPLIANCE,
                description="Failed mid-workflow steps recover via backoff retries",
                params={"min_backoff_seconds": 0.01},
            ),
        ],
        quiescence_wait_ms=15,
    )

    # 5. Silent Schema Drift, Poison Pill & HMAC Tampering
    s5_events = make_base_order_events(5, prefix="evt_drift")
    s5 = ScenarioSpec(
        scenario_id="05_schema_drift_poison",
        name="Schema Drift, Poison Message & Tampered Signature",
        description="Injects stringified numbers, renamed fields, extra fields, missing required email, and HMAC tamper.",
        events=s5_events,
        chaos=ChaosProfile(
            duplicate_Deliveries_per_event=1,
            mutations=[
                DriftMutation(
                    mutation_id="mut_stringify_amount",
                    drift_type=DriftType.STRINGIFY_NUMBERS,
                    apply_to_event_indices=[0],
                    source_field="amount_cents",
                    expect_rejected_or_dlq=False,
                ),
                DriftMutation(
                    mutation_id="mut_alias_email",
                    drift_type=DriftType.RENAME_FIELD,
                    apply_to_event_indices=[1],
                    source_field="customer_email",
                    target_field="buyer_email",
                    expect_rejected_or_dlq=False,
                ),
                DriftMutation(
                    mutation_id="mut_extra_fields",
                    drift_type=DriftType.INJECT_EXTRA_FIELDS,
                    apply_to_event_indices=[2],
                    expect_rejected_or_dlq=False,
                ),
                DriftMutation(
                    mutation_id="mut_drop_email_poison",
                    drift_type=DriftType.DROP_REQUIRED_FIELD,
                    apply_to_event_indices=[3],
                    source_field="customer_email",
                    expect_rejected_or_dlq=True,
                ),
                DriftMutation(
                    mutation_id="mut_tamper_hmac",
                    drift_type=DriftType.TAMPER_SIGNATURE,
                    apply_to_event_indices=[4],
                    expect_rejected_or_dlq=True,
                ),
            ],
        ),
        invariants=[
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="3 recoverable schema-drifted events succeed; 2 poison/tampered events blocked",
                params={"expected_valid_records": 3},
            ),
            InvariantSpec(
                invariant_id="inv_poison_containment",
                invariant_type=InvariantType.POISON_PILL_CONTAINMENT,
                description="Missing-email poison event routed to DLQ and tampered HMAC rejected with 401",
            ),
            InvariantSpec(
                invariant_id="inv_no_swallow",
                invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
                description="Zero silent error swallowing across all 5 drifted/poisoned events",
            ),
        ],
        quiescence_wait_ms=15,
    )

    # 6. Full Production Chaos Gauntlet
    s6_events = make_base_order_events(5, prefix="evt_gauntlet")
    s6 = ScenarioSpec(
        scenario_id="06_full_chaos_gauntlet",
        name="Full Production Chaos Gauntlet (Combined Stress)",
        description="Combines 2x concurrent duplicates, CRM 429s, Ledger 503s, network jitter, and schema drift.",
        events=s6_events,
        chaos=ChaosProfile(
            duplicate_Deliveries_per_event=2,
            concurrent_duplicates=True,
            mutations=[
                DriftMutation(
                    mutation_id="gauntlet_stringify",
                    drift_type=DriftType.STRINGIFY_NUMBERS,
                    apply_to_event_indices=[0, 1],
                    source_field="amount_cents",
                    expect_rejected_or_dlq=False,
                ),
                DriftMutation(
                    mutation_id="gauntlet_poison_drop",
                    drift_type=DriftType.DROP_REQUIRED_FIELD,
                    apply_to_event_indices=[4],
                    source_field="customer_email",
                    expect_rejected_or_dlq=True,
                ),
            ],
            fault_rules=[
                FaultRule(
                    rule_id="gauntlet_jitter",
                    target_service="*",
                    fault_type=FaultType.NETWORK_JITTER,
                    latency_ms=2,
                    jitter_ms=4,
                ),
                FaultRule(
                    rule_id="gauntlet_crm_429",
                    target_service="crm",
                    fault_type=FaultType.RATE_LIMIT_429,
                    fail_first_n_per_key=1,
                    retry_after_seconds=0.015,
                ),
                FaultRule(
                    rule_id="gauntlet_ledger_503",
                    target_service="ledger",
                    fault_type=FaultType.TRANSIENT_503,
                    fail_first_n_per_key=1,
                    retry_after_seconds=0.015,
                ),
            ],
        ),
        invariants=[
            InvariantSpec(
                invariant_id="inv_cardinality",
                invariant_type=InvariantType.EXACT_CARDINALITY,
                description="4 valid orders persisted exactly once; 1 poison order isolated to DLQ",
                params={"expected_valid_records": 4},
            ),
            InvariantSpec(
                invariant_id="inv_consistency",
                invariant_type=InvariantType.CROSS_SYSTEM_CONSISTENCY,
                description="100% cross-system consistency across CRM, Ledger, and Notification",
            ),
            InvariantSpec(
                invariant_id="inv_idempotency",
                invariant_type=InvariantType.IDEMPOTENCY_ENFORCEMENT,
                description="Strict idempotency key enforcement across all retries and duplicates",
            ),
            InvariantSpec(
                invariant_id="inv_backoff",
                invariant_type=InvariantType.RETRY_BACKOFF_COMPLIANCE,
                description="All 429 and 503 downstream faults recovered with compliant backoff",
                params={"min_backoff_seconds": 0.01},
            ),
            InvariantSpec(
                invariant_id="inv_poison_containment",
                invariant_type=InvariantType.POISON_PILL_CONTAINMENT,
                description="Poison event captured in DLQ without polluting stateful twins",
            ),
            InvariantSpec(
                invariant_id="inv_no_swallow",
                invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
                description="Every event is either completely reconciled or recorded in DLQ",
            ),
        ],
        quiescence_wait_ms=20,
    )

    return [s1, s2, s3, s4, s5, s6]
