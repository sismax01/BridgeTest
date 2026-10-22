"""Business Outcome & Eventual Consistency Invariant Evaluation Engine."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List

from bridgetest.models import (
    InvariantResult,
    InvariantSpec,
    InvariantType,
    ScenarioSpec,
    WebhookEvent,
)
from bridgetest.twins import TwinCluster


class InvariantEngine:
    """Evaluates declarative business outcome invariants across stateful digital twins and causal journal."""

    def evaluate_all(
        self,
        scenario: ScenarioSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> List[InvariantResult]:
        """Evaluate all invariants specified in the scenario against final twin state and journal."""
        results: List[InvariantResult] = []
        for spec in scenario.invariants:
            results.append(self.evaluate_one(spec, prepared_events, cluster))
        return results

    def evaluate_one(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        itype = spec.invariant_type
        if itype == InvariantType.EXACT_CARDINALITY:
            return self._check_exact_cardinality(spec, prepared_events, cluster)
        if itype == InvariantType.CROSS_SYSTEM_CONSISTENCY:
            return self._check_cross_system_consistency(spec, prepared_events, cluster)
        if itype == InvariantType.IDEMPOTENCY_ENFORCEMENT:
            return self._check_idempotency_enforcement(spec, prepared_events, cluster)
        if itype == InvariantType.RETRY_BACKOFF_COMPLIANCE:
            return self._check_retry_backoff_compliance(spec, prepared_events, cluster)
        if itype == InvariantType.POISON_PILL_CONTAINMENT:
            return self._check_poison_pill_containment(spec, prepared_events, cluster)
        if itype == InvariantType.ZERO_SILENT_SWALLOW:
            return self._check_zero_silent_swallow(spec, prepared_events, cluster)

        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=itype,
            passed=False,
            description=spec.description,
            expected_summary="Recognized invariant type",
            actual_summary=f"Unsupported invariant type: {itype}",
            violations=[f"Unknown invariant type {itype}"],
        )

    def _check_exact_cardinality(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        valid_events = [e for e in prepared_events if not e.is_poison_expected and not e.tamper_signature]
        unique_valid_ids = sorted({e.event_id for e in valid_events})
        expected_valid_count = int(spec.params.get("expected_valid_records", len(unique_valid_ids)))
        services = spec.params.get("services", ["crm", "ledger", "notification"])

        snapshot = cluster.state_snapshot()
        violations: List[str] = []
        actual_counts: Dict[str, int] = {}

        for svc in services:
            records = snapshot.get(svc, [])
            actual_counts[svc] = len(records)
            if len(records) != expected_valid_count:
                violations.append(
                    f"Service '{svc}' persisted {len(records)} records, expected {expected_valid_count}"
                )

            by_event = Counter(r.get("external_event_id") for r in records)
            duplicates = {eid: cnt for eid, cnt in by_event.items() if cnt > 1}
            if duplicates:
                violations.append(
                    f"Service '{svc}' contains duplicate records for event_ids: {duplicates}"
                )

        expected_str = ", ".join(f"{s}={expected_valid_count}" for s in services)
        actual_str = ", ".join(f"{s}={actual_counts.get(s, 0)}" for s in services)
        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary=expected_str,
            actual_summary=actual_str,
            violations=violations,
            metrics={"expected_per_service": expected_valid_count, "actual_counts": actual_counts},
        )

    def _check_cross_system_consistency(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        snapshot = cluster.state_snapshot()
        crm_by_event = {r["external_event_id"]: r for r in snapshot["crm"] if "external_event_id" in r}
        ledger_by_event = {r["external_event_id"]: r for r in snapshot["ledger"] if "external_event_id" in r}
        notif_by_event = {r["external_event_id"]: r for r in snapshot["notification"] if "external_event_id" in r}

        all_reconciled_ids = sorted(set(crm_by_event) | set(ledger_by_event) | set(notif_by_event))
        violations: List[str] = []

        for eid in all_reconciled_ids:
            in_crm = eid in crm_by_event
            in_ledger = eid in ledger_by_event
            in_notif = eid in notif_by_event
            if not (in_crm and in_ledger and in_notif):
                violations.append(
                    f"Split-brain state for event '{eid}': crm={in_crm}, ledger={in_ledger}, notification={in_notif}"
                )
                continue

            crm_rec = crm_by_event[eid]
            ledger_rec = ledger_by_event[eid]
            if crm_rec.get("amount_cents") != ledger_rec.get("amount_cents"):
                violations.append(
                    f"Amount mismatch for event '{eid}': CRM={crm_rec.get('amount_cents')} vs Ledger={ledger_rec.get('amount_cents')}"
                )
            if crm_rec.get("customer_email") != ledger_rec.get("customer_email"):
                violations.append(
                    f"Customer email mismatch for event '{eid}': CRM='{crm_rec.get('customer_email')}' vs Ledger='{ledger_rec.get('customer_email')}'"
                )

        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary="100% atomic alignment across CRM, Ledger, and Notification twins",
            actual_summary=(
                f"{len(all_reconciled_ids)} events inspected; {len(violations)} cross-system drift violations"
            ),
            violations=violations,
            metrics={
                "events_inspected": len(all_reconciled_ids),
                "split_brain_or_mismatch_count": len(violations),
            },
        )

    def _check_idempotency_enforcement(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        downstream_entries = cluster.recorder.entries(direction="downstream")
        missing_headers: List[str] = []
        for entry in downstream_entries:
            if entry.service in ("crm", "ledger", "notification"):
                if not entry.idempotency_key:
                    missing_headers.append(
                        f"Seq #{entry.seq_id} {entry.method} {entry.path} (corr={entry.correlation_id}) missing Idempotency-Key header"
                    )

        snapshot = cluster.state_snapshot()
        duplicate_side_effects: List[str] = []
        for svc in ("crm", "ledger", "notification"):
            counts = Counter(r.get("external_event_id") for r in snapshot[svc])
            for eid, cnt in counts.items():
                if cnt > 1:
                    duplicate_side_effects.append(
                        f"Service '{svc}' executed {cnt} non-idempotent writes for event '{eid}'"
                    )

        violations = missing_headers + duplicate_side_effects
        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary="All downstream writes carry Idempotency-Key & produce at-most-once side effects",
            actual_summary=(
                f"{len(downstream_entries)} downstream calls checked; "
                f"{len(missing_headers)} missing keys, {len(duplicate_side_effects)} duplicate side effects"
            ),
            violations=violations,
            metrics={
                "downstream_calls": len(downstream_entries),
                "missing_idempotency_headers": len(missing_headers),
                "duplicate_side_effects": len(duplicate_side_effects),
            },
        )

    def _check_retry_backoff_compliance(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        min_backoff_sec = float(spec.params.get("min_backoff_seconds", 0.01))
        downstream = cluster.recorder.entries(direction="downstream")
        faulted = [e for e in downstream if e.fault_injected is not None or e.status_code in (429, 502, 503, 504)]

        violations: List[str] = []
        recovered_keys = 0

        grouped_by_service_and_corr: Dict[tuple[str, str], List[Any]] = {}
        for entry in downstream:
            grouped_by_service_and_corr.setdefault((entry.service, entry.correlation_id), []).append(entry)

        for (svc, corr_id), attempts in grouped_by_service_and_corr.items():
            had_transient_failure = any(a.status_code in (429, 502, 503, 504) for a in attempts)
            if not had_transient_failure:
                continue

            eventually_succeeded = any(200 <= a.status_code < 300 for a in attempts)
            if not eventually_succeeded:
                violations.append(
                    f"Service '{svc}' (corr={corr_id}) experienced transient HTTP {attempts[0].status_code} "
                    f"and failed to recover (total attempts={len(attempts)})"
                )
            else:
                recovered_keys += 1

            for idx in range(1, len(attempts)):
                delta = attempts[idx].timestamp_monotonic - attempts[idx - 1].timestamp_monotonic
                if delta < min_backoff_sec:
                    violations.append(
                        f"Service '{svc}' (corr={corr_id}) retry #{idx} fired after {delta*1000:.1f}ms, "
                        f"violating minimum backoff {min_backoff_sec*1000:.1f}ms"
                    )

        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary=f"All transient 429/5xx faults retried with backoff >= {min_backoff_sec*1000:.0f}ms until 2xx",
            actual_summary=(
                f"{len(faulted)} transient faults injected; {recovered_keys} workflows recovered; "
                f"{len(violations)} retry/backoff violations"
            ),
            violations=violations,
            metrics={
                "transient_faults_seen": len(faulted),
                "recovered_workflows": recovered_keys,
                "violations_count": len(violations),
            },
        )

    def _check_poison_pill_containment(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        poison_events = [e for e in prepared_events if e.is_poison_expected or e.tamper_signature]
        snapshot = cluster.state_snapshot()
        dlq_by_event = {r.get("external_event_id"): r for r in snapshot["dlq"]}

        violations: List[str] = []
        contained_count = 0

        for pevt in poison_events:
            eid = pevt.event_id
            leaked_services = [
                svc
                for svc in ("crm", "ledger", "notification")
                if any(r.get("external_event_id") == eid for r in snapshot[svc])
            ]
            if leaked_services:
                violations.append(
                    f"Poison/tampered event '{eid}' leaked into stateful services: {leaked_services}"
                )

            if pevt.tamper_signature:
                inbound_for_evt = [
                    e
                    for e in cluster.recorder.entries(direction="inbound", correlation_id=pevt.correlation_id)
                ]
                if not inbound_for_evt or any(e.status_code != 401 for e in inbound_for_evt):
                    statuses = [e.status_code for e in inbound_for_evt]
                    violations.append(
                        f"Tampered signature on event '{eid}' was not rejected with HTTP 401 (got {statuses})"
                    )
                else:
                    contained_count += 1
            else:
                if eid not in dlq_by_event:
                    violations.append(
                        f"Unrecoverable schema-drifted event '{eid}' was dropped without Dead-Letter Queue (DLQ) capture"
                    )
                else:
                    contained_count += 1

        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary=f"All {len(poison_events)} poison/tampered events isolated from core twins & routed to DLQ/401",
            actual_summary=f"{contained_count}/{len(poison_events)} poison events properly contained",
            violations=violations,
            metrics={
                "poison_events_total": len(poison_events),
                "contained_count": contained_count,
            },
        )

    def _check_zero_silent_swallow(
        self,
        spec: InvariantSpec,
        prepared_events: List[WebhookEvent],
        cluster: TwinCluster,
    ) -> InvariantResult:
        snapshot = cluster.state_snapshot()
        crm_ids = {r.get("external_event_id") for r in snapshot["crm"]}
        ledger_ids = {r.get("external_event_id") for r in snapshot["ledger"]}
        notif_ids = {r.get("external_event_id") for r in snapshot["notification"]}
        dlq_ids = {r.get("external_event_id") for r in snapshot["dlq"]}

        fully_reconciled = crm_ids & ledger_ids & notif_ids
        violations: List[str] = []
        accounted_for = 0

        unique_events = {e.event_id: e for e in prepared_events}
        for eid, evt in unique_events.items():
            if evt.tamper_signature:
                inbound = cluster.recorder.entries(direction="inbound", correlation_id=evt.correlation_id)
                if inbound and all(e.status_code == 401 for e in inbound):
                    accounted_for += 1
                else:
                    violations.append(f"Tampered event '{eid}' neither rejected with 401 nor tracked")
                continue

            if eid in fully_reconciled or eid in dlq_ids:
                accounted_for += 1
            else:
                violations.append(
                    f"Event '{eid}' was silently swallowed: neither fully reconciled in (CRM+Ledger+Notification) nor captured in DLQ"
                )

        return InvariantResult(
            invariant_id=spec.invariant_id,
            invariant_type=spec.invariant_type,
            passed=len(violations) == 0,
            description=spec.description,
            expected_summary=f"100% conservation of {len(unique_events)} unique inbound events (reconciled or DLQ)",
            actual_summary=f"{accounted_for}/{len(unique_events)} events accounted for; {len(violations)} silently swallowed",
            violations=violations,
            metrics={
                "unique_events": len(unique_events),
                "accounted_for": accounted_for,
                "swallowed_count": len(violations),
            },
        )
