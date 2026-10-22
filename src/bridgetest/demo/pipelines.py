"""Reference Integration Pipelines: Naive (Happy-Path Only) vs. Resilient (Outbox + Backoff + Idempotency + DLQ)."""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Tuple

from bridgetest.chaos import verify_webhook_signature
from bridgetest.twins import TwinCluster


class NaiveWebhookPipeline:
    """
    A typical staging-grade webhook integration handler that passes happy-path tests
    but fails silently under production chaos:
      - No HMAC signature verification
      - No inbound deduplication or downstream Idempotency-Key propagation
      - No retry/backoff on HTTP 429 or 5xx responses
      - No transactional compensation or outbox tracking across CRM, Ledger, and Notification calls
      - Swallows schema validation errors without routing poison events to a Dead-Letter Queue
    """

    def __init__(self, twin_cluster: TwinCluster) -> None:
        self.cluster = twin_cluster

    def reset(self) -> None:
        """Stateless naive pipeline has no internal state to reset."""
        pass

    def handle_webhook(self, headers: Dict[str, str], raw_body: bytes) -> Tuple[int, Dict[str, Any]]:
        try:
            envelope = json.loads(raw_body.decode("utf-8"))
        except Exception:
            return 400, {"status": "bad_json"}

        event_id = str(envelope.get("event_id", "unknown"))
        correlation_id = str(envelope.get("correlation_id", event_id))
        data = envelope.get("data", {})
        if not isinstance(data, dict):
            return 200, {"status": "ignored"}

        # Naive extraction: assumes happy-path types and presence, no normalization or DLQ
        try:
            order_id = data["order_id"]
            customer_email = data["customer_email"]
            amount_cents = data["amount_cents"]
            currency = data.get("currency", "USD")
        except KeyError:
            # Silent swallow on missing field! Returns 200 OK to webhook sender without DLQ alert
            return 200, {"status": "swallowed_missing_field", "event_id": event_id}

        req_headers = {"X-Correlation-ID": correlation_id}

        # Step 1: Call CRM without Idempotency-Key or retry loop
        crm_status, _, _ = self.cluster.handle_request(
            service="crm",
            method="POST",
            path="/crm/contacts",
            headers=req_headers,
            body={
                "external_event_id": event_id,
                "order_id": order_id,
                "customer_email": customer_email,
                "amount_cents": amount_cents,
            },
        )
        if crm_status >= 400:
            # Drops the rest of the workflow without retry or DLQ
            return 200, {"status": "partial_drop_at_crm", "crm_status": crm_status}

        # Step 2: Call Ledger without Idempotency-Key or retry loop
        ledger_status, _, _ = self.cluster.handle_request(
            service="ledger",
            method="POST",
            path="/ledger/entries",
            headers=req_headers,
            body={
                "external_event_id": event_id,
                "order_id": order_id,
                "customer_email": customer_email,
                "amount_cents": amount_cents,
                "currency": currency,
            },
        )
        # Note: Even if ledger fails (503), CRM already committed! Naive pipeline continues or aborts leaving split-brain.
        if ledger_status >= 400:
            return 200, {"status": "split_brain_ledger_failed", "ledger_status": ledger_status}

        # Step 3: Call Notification without Idempotency-Key or retry loop
        notif_status, _, _ = self.cluster.handle_request(
            service="notification",
            method="POST",
            path="/notify/send",
            headers=req_headers,
            body={
                "external_event_id": event_id,
                "order_id": order_id,
                "recipient": customer_email,
                "channel": "email",
            },
        )
        return 200, {
            "status": "processed",
            "crm_status": crm_status,
            "ledger_status": ledger_status,
            "notification_status": notif_status,
        }


class ResilientOutboxPipeline:
    """
    A production-hardened integration workflow engineered for distributed resilience:
      - Cryptographic HMAC-SHA256 webhook signature verification (401 rejection on tamper)
      - Per-key striped mutex + idempotent workflow state machine preventing concurrent race duplicates
      - Tolerant reader schema normalization (coerces numeric strings, maps known field aliases, ignores extra fields)
      - Dead-Letter Queue (DLQ) routing for unrecoverable contract/domain violations
      - Step-level deterministic Idempotency-Key propagation (`<service>:<idempotency_key>`)
      - Exponential backoff retry loop honoring downstream `Retry-After` headers on HTTP 429/502/503/504
    """

    def __init__(
        self,
        twin_cluster: TwinCluster,
        signing_secret: str = "bt_whsec_prod_9f8e7d6c5b4a3210",
        max_retries: int = 4,
        base_backoff_seconds: float = 0.015,
    ) -> None:
        self.cluster = twin_cluster
        self.signing_secret = signing_secret
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self._global_lock = threading.RLock()
        self._key_locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)
        self._completed_events: Dict[str, Dict[str, Any]] = {}
        self._step_state: Dict[str, Dict[str, bool]] = defaultdict(dict)

    def reset(self) -> None:
        with self._global_lock:
            self._key_locks.clear()
            self._completed_events.clear()
            self._step_state.clear()

    def _get_key_lock(self, key: str) -> threading.RLock:
        with self._global_lock:
            return self._key_locks[key]

    def handle_webhook(self, headers: Dict[str, str], raw_body: bytes) -> Tuple[int, Dict[str, Any]]:
        norm_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        sig_header = norm_headers.get("x-bridgetest-signature", "")

        if not verify_webhook_signature(self.signing_secret, sig_header, raw_body):
            return 401, {"error": "invalid_signature", "message": "HMAC-SHA256 verification failed"}

        try:
            envelope = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            return 400, {"error": "malformed_json", "message": str(exc)}

        event_id = str(envelope.get("event_id", "unknown"))
        idempotency_key = str(
            norm_headers.get("idempotency-key") or envelope.get("idempotency_key") or event_id
        )
        correlation_id = str(
            norm_headers.get("x-correlation-id") or envelope.get("correlation_id") or event_id
        )

        key_lock = self._get_key_lock(idempotency_key)
        with key_lock:
            if idempotency_key in self._completed_events:
                cached = self._completed_events[idempotency_key]
                return 200, {
                    "status": "idempotent_short_circuit",
                    "event_id": event_id,
                    "idempotency_key": idempotency_key,
                    "outcome": cached,
                }

            raw_data = envelope.get("data")
            normalized, validation_error = self._normalize_and_validate(raw_data)
            if validation_error is not None:
                dlq_status, dlq_resp = self._dispatch_with_retry(
                    service="dlq",
                    path="/dlq/events",
                    correlation_id=correlation_id,
                    idempotency_key=f"dlq:{idempotency_key}",
                    payload={
                        "external_event_id": event_id,
                        "reason": validation_error,
                        "raw_payload": raw_data,
                    },
                )
                outcome = {
                    "status": "routed_to_dlq",
                    "reason": validation_error,
                    "dlq_status": dlq_status,
                    "dlq_response": dlq_resp,
                }
                self._completed_events[idempotency_key] = outcome
                return 202, outcome

            assert normalized is not None
            steps = [
                (
                    "crm",
                    "/crm/contacts",
                    {
                        "external_event_id": event_id,
                        "order_id": normalized["order_id"],
                        "customer_email": normalized["customer_email"],
                        "amount_cents": normalized["amount_cents"],
                    },
                ),
                (
                    "ledger",
                    "/ledger/entries",
                    {
                        "external_event_id": event_id,
                        "order_id": normalized["order_id"],
                        "customer_email": normalized["customer_email"],
                        "amount_cents": normalized["amount_cents"],
                        "currency": normalized["currency"],
                    },
                ),
                (
                    "notification",
                    "/notify/send",
                    {
                        "external_event_id": event_id,
                        "order_id": normalized["order_id"],
                        "recipient": normalized["customer_email"],
                        "channel": "email",
                    },
                ),
            ]

            step_results: Dict[str, int] = {}
            for service_name, path, step_payload in steps:
                if self._step_state[idempotency_key].get(service_name):
                    step_results[service_name] = 200
                    continue

                status_code, _ = self._dispatch_with_retry(
                    service=service_name,
                    path=path,
                    correlation_id=correlation_id,
                    idempotency_key=f"{service_name}:{idempotency_key}",
                    payload=step_payload,
                )
                step_results[service_name] = status_code
                if 200 <= status_code < 300:
                    self._step_state[idempotency_key][service_name] = True
                else:
                    # Exhausted all retries: route failure context to Dead-Letter Queue
                    self._dispatch_with_retry(
                        service="dlq",
                        path="/dlq/events",
                        correlation_id=correlation_id,
                        idempotency_key=f"dlq:{idempotency_key}",
                        payload={
                            "external_event_id": event_id,
                            "reason": f"downstream_exhausted_{service_name}_{status_code}",
                            "raw_payload": raw_data,
                        },
                    )
                    return 502, {
                        "status": "failed_downstream_routed_to_dlq",
                        "failed_service": service_name,
                        "step_results": step_results,
                    }

            outcome = {
                "status": "reconciled",
                "event_id": event_id,
                "step_results": step_results,
            }
            self._completed_events[idempotency_key] = outcome
            return 200, outcome

    def _normalize_and_validate(
        self, raw_data: Any
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Tolerant reader normalization with strict domain validation for unrecoverable drift."""
        if not isinstance(raw_data, dict):
            return None, "payload_not_an_object"

        order_id = raw_data.get("order_id") or raw_data.get("id")
        if not order_id or not isinstance(order_id, str):
            return None, "missing_required_field:order_id"

        # Support tolerant alias mapping (e.g. buyer_email -> customer_email) if present
        customer_email = raw_data.get("customer_email") or raw_data.get("buyer_email")
        if not customer_email or not isinstance(customer_email, str) or "@" not in customer_email:
            return None, "missing_or_invalid_field:customer_email"

        raw_amount = raw_data.get("amount_cents")
        if raw_amount is None or isinstance(raw_amount, bool):
            return None, "missing_required_field:amount_cents"

        try:
            amount_cents = int(raw_amount)
        except (ValueError, TypeError):
            return None, f"unparseable_amount_cents:{raw_amount}"

        if amount_cents <= 0:
            return None, f"non_positive_amount_cents:{amount_cents}"

        currency = str(raw_data.get("currency") or "USD").upper()
        return {
            "order_id": str(order_id),
            "customer_email": customer_email.strip(),
            "amount_cents": amount_cents,
            "currency": currency,
        }, None

    def _dispatch_with_retry(
        self,
        *,
        service: str,
        path: str,
        correlation_id: str,
        idempotency_key: str,
        payload: Dict[str, Any],
    ) -> Tuple[int, Any]:
        """Dispatch downstream request with Idempotency-Key and Retry-After / exponential backoff."""
        headers = {
            "X-Correlation-ID": correlation_id,
            "Idempotency-Key": idempotency_key,
        }
        last_status = 500
        last_body: Any = None

        for attempt in range(self.max_retries + 1):
            status_code, resp_headers, resp_body = self.cluster.handle_request(
                service=service,
                method="POST",
                path=path,
                headers=headers,
                body=payload,
            )
            last_status = status_code
            last_body = resp_body

            if 200 <= status_code < 300:
                return status_code, resp_body

            # Non-retryable client validation error (400, 401, 403, 404, 422)
            if 400 <= status_code < 500 and status_code != 429:
                return status_code, resp_body

            if attempt < self.max_retries:
                retry_after_hdr = (resp_headers or {}).get("Retry-After")
                if retry_after_hdr is not None:
                    try:
                        sleep_sec = max(float(retry_after_hdr), self.base_backoff_seconds)
                    except ValueError:
                        sleep_sec = self.base_backoff_seconds * (2 ** attempt)
                else:
                    sleep_sec = self.base_backoff_seconds * (2 ** attempt)
                time.sleep(sleep_sec)

        return last_status, last_body
