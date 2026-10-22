"""Inbound Chaos Producer: webhook signing, duplicate delivery bursts, and schema drift mutators."""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Tuple

from bridgetest.journal import FlightRecorder
from bridgetest.models import ChaosProfile, DriftMutation, DriftType, WebhookEvent


def compute_webhook_signature(secret: str, timestamp: int, raw_body: bytes) -> str:
    """Compute an HMAC-SHA256 webhook signature header in `t=<ts>,v1=<hex>` format."""
    signed_payload = f"{timestamp}.".encode("utf-8") + raw_body
    digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify_webhook_signature(
    secret: str,
    signature_header: str,
    raw_body: bytes,
    tolerance_seconds: int = 300,
    current_timestamp: int | None = None,
) -> bool:
    """Verify an HMAC-SHA256 webhook signature header and check replay tolerance."""
    if not signature_header or "t=" not in signature_header or "v1=" not in signature_header:
        return False
    try:
        parts = dict(item.split("=", 1) for item in signature_header.split(",") if "=" in item)
        ts = int(parts["t"])
        provided_sig = parts["v1"]
    except (ValueError, KeyError):
        return False

    if current_timestamp is not None and abs(current_timestamp - ts) > tolerance_seconds:
        return False

    expected_header = compute_webhook_signature(secret, ts, raw_body)
    expected_sig = dict(item.split("=", 1) for item in expected_header.split(","))["v1"]
    return hmac.compare_digest(provided_sig, expected_sig)


class ChaosProducer:
    """Applies schema drift mutations and dispatches webhook deliveries with duplicate/concurrency chaos."""

    def __init__(self, profile: ChaosProfile, recorder: FlightRecorder) -> None:
        self.profile = profile
        self.recorder = recorder

    def prepare_events(self, base_events: List[WebhookEvent]) -> List[WebhookEvent]:
        """Clone and mutate events according to configured schema drift rules."""
        prepared: List[WebhookEvent] = []
        for idx, base in enumerate(base_events):
            evt = copy.deepcopy(base)
            for mutation in self.profile.mutations:
                if not mutation.apply_to_event_indices or idx in mutation.apply_to_event_indices:
                    evt = self._apply_mutation(evt, mutation)
            prepared.append(evt)

        if self.profile.out_of_order_delivery and len(prepared) > 1:
            prepared = list(reversed(prepared))
        return prepared

    def _apply_mutation(self, event: WebhookEvent, mutation: DriftMutation) -> WebhookEvent:
        payload = dict(event.payload)
        dtype = mutation.drift_type

        if dtype == DriftType.STRINGIFY_NUMBERS:
            field_name = mutation.source_field or "amount_cents"
            if field_name in payload:
                payload[field_name] = str(payload[field_name])
        elif dtype == DriftType.RENAME_FIELD:
            src = mutation.source_field or "customer_email"
            dst = mutation.target_field or "buyer_email"
            if src in payload:
                payload[dst] = payload.pop(src)
        elif dtype == DriftType.DROP_REQUIRED_FIELD:
            src = mutation.source_field or "customer_email"
            payload.pop(src, None)
        elif dtype == DriftType.INJECT_EXTRA_FIELDS:
            extra = mutation.extra_payload or {
                "_schema_version": "2026-10-experimental",
                "metadata_flags": {"beta_routing": True, "unexpected_nested": [1, 2, 3]},
            }
            payload.update(extra)
        elif dtype == DriftType.NULLIFY_OPTIONAL_FIELD:
            src = mutation.source_field or "coupon_code"
            payload[src] = None
        elif dtype == DriftType.TAMPER_SIGNATURE:
            event.tamper_signature = True

        event.payload = payload
        event.mutation_applied = mutation.mutation_id
        if mutation.expect_rejected_or_dlq:
            event.is_poison_expected = True
        return event

    def dispatch_all(
        self,
        events: List[WebhookEvent],
        target_handler: Callable[[Dict[str, str], bytes], Tuple[int, Dict[str, Any]]],
    ) -> Tuple[List[WebhookEvent], List[Tuple[str, int, Dict[str, Any]]]]:
        """Dispatch all prepared events (including configured duplicate storms) to the target handler."""
        prepared = self.prepare_events(events)
        deliveries: List[WebhookEvent] = []
        repeats = max(1, int(self.profile.duplicate_Deliveries_per_event))

        for evt in prepared:
            for _ in range(repeats):
                deliveries.append(evt)

        outcomes: List[Tuple[str, int, Dict[str, Any]]] = []

        def _send_one(evt: WebhookEvent) -> Tuple[str, int, Dict[str, Any]]:
            envelope = {
                "event_id": evt.event_id,
                "event_type": evt.event_type,
                "idempotency_key": evt.idempotency_key,
                "correlation_id": evt.correlation_id,
                "timestamp": evt.timestamp,
                "data": evt.payload,
            }
            raw_bytes = json.dumps(envelope, sort_keys=True).encode("utf-8")
            secret = "tampered_invalid_secret_key" if evt.tamper_signature else self.profile.signing_secret
            sig = compute_webhook_signature(secret, evt.timestamp, raw_bytes)
            headers = {
                "Content-Type": "application/json",
                "X-BridgeTest-Signature": sig,
                "X-BridgeTest-Event-ID": evt.event_id,
                "X-Correlation-ID": evt.correlation_id,
                "Idempotency-Key": evt.idempotency_key,
            }
            start_ts = time.perf_counter()
            status_code, resp_body = target_handler(headers, raw_bytes)
            elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
            self.recorder.record(
                direction="inbound",
                service="sut",
                method="POST",
                path="/webhooks/orders",
                correlation_id=evt.correlation_id,
                idempotency_key=evt.idempotency_key,
                status_code=status_code,
                latency_ms=elapsed_ms,
                fault_injected=evt.mutation_applied,
                request_headers=headers,
                request_body=envelope,
                response_body=resp_body,
            )
            return evt.event_id, status_code, resp_body

        if self.profile.concurrent_duplicates and len(deliveries) > 1:
            with ThreadPoolExecutor(max_workers=min(16, len(deliveries))) as pool:
                futures = [pool.submit(_send_one, d) for d in deliveries]
                for fut in futures:
                    outcomes.append(fut.result())
        else:
            for idx, d in enumerate(deliveries):
                if idx > 0 and self.profile.duplicate_delay_ms > 0:
                    time.sleep(self.profile.duplicate_delay_ms / 1000.0)
                outcomes.append(_send_one(d))

        return prepared, outcomes
