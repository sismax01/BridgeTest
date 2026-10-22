"""Deterministic and probabilistic fault injection engine for downstream digital twins."""

from __future__ import annotations

import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from bridgetest.models import FaultRule, FaultType


@dataclass
class FaultDecision:
    """Result of evaluating configured fault rules for a downstream request."""

    triggered: bool
    rule_id: Optional[str] = None
    fault_type: Optional[FaultType] = None
    status_code: int = 200
    headers: Optional[Dict[str, str]] = None
    response_body: Any = None
    raw_bytes: Optional[bytes] = None
    injected_latency_ms: float = 0.0


class FaultEngine:
    """Evaluates ordered fault rules against downstream twin requests with per-key attempt tracking."""

    def __init__(self, rules: Optional[List[FaultRule]] = None, seed: int = 42) -> None:
        self._lock = threading.RLock()
        self._rules: List[FaultRule] = list(rules or [])
        self._rng = random.Random(seed)
        self._key_failures: Dict[Tuple[str, str, str], int] = defaultdict(int)
        self._total_injected = 0

    def reset(self, rules: Optional[List[FaultRule]] = None, seed: int = 42) -> None:
        """Reset all fault counters and optionally load a new set of fault rules."""
        with self._lock:
            if rules is not None:
                self._rules = list(rules)
            self._rng = random.Random(seed)
            self._key_failures.clear()
            self._total_injected = 0

    @property
    def total_injected(self) -> int:
        with self._lock:
            return self._total_injected

    def evaluate(
        self,
        *,
        service: str,
        correlation_id: str,
        idempotency_key: Optional[str],
    ) -> FaultDecision:
        """Evaluate active rules for a request and inject latency or HTTP faults if matched."""
        tracking_token = idempotency_key or correlation_id or "global"
        total_latency_ms = 0.0

        with self._lock:
            matching_rules = [
                r
                for r in self._rules
                if r.enabled and (r.target_service == service or r.target_service == "*")
            ]

        for rule in matching_rules:
            if rule.fault_type == FaultType.NETWORK_JITTER:
                delay_ms = float(rule.latency_ms)
                if rule.jitter_ms > 0:
                    with self._lock:
                        delay_ms += self._rng.uniform(0.0, float(rule.jitter_ms))
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
                    total_latency_ms += delay_ms
                continue

            should_fail = False
            with self._lock:
                counter_key = (rule.rule_id, service, tracking_token)
                already_failed = self._key_failures[counter_key]
                if rule.fail_first_n_per_key > 0:
                    if already_failed < rule.fail_first_n_per_key:
                        should_fail = True
                elif rule.probability > 0.0:
                    if self._rng.random() < rule.probability:
                        should_fail = True

                if should_fail:
                    self._key_failures[counter_key] += 1
                    self._total_injected += 1

            if not should_fail:
                continue

            if rule.latency_ms > 0:
                time.sleep(rule.latency_ms / 1000.0)
                total_latency_ms += float(rule.latency_ms)

            if rule.fault_type == FaultType.RATE_LIMIT_429:
                retry_sec = rule.retry_after_seconds if rule.retry_after_seconds > 0 else 0.05
                return FaultDecision(
                    triggered=True,
                    rule_id=rule.rule_id,
                    fault_type=rule.fault_type,
                    status_code=rule.status_code or 429,
                    headers={"Retry-After": str(retry_sec)},
                    response_body={
                        "error": "rate_limit_exceeded",
                        "message": f"Upstream rate limit on {service}; retry after {retry_sec}s",
                        "retry_after_seconds": retry_sec,
                        "rule_id": rule.rule_id,
                    },
                    injected_latency_ms=total_latency_ms,
                )

            if rule.fault_type in (FaultType.TRANSIENT_503, FaultType.PARTIAL_OUTAGE):
                return FaultDecision(
                    triggered=True,
                    rule_id=rule.rule_id,
                    fault_type=rule.fault_type,
                    status_code=rule.status_code or 503,
                    headers={"Retry-After": str(rule.retry_after_seconds or 0.05)},
                    response_body={
                        "error": "service_unavailable",
                        "message": f"Downstream {service} experiencing transient outage",
                        "rule_id": rule.rule_id,
                    },
                    injected_latency_ms=total_latency_ms,
                )

            if rule.fault_type == FaultType.GATEWAY_TIMEOUT_504:
                return FaultDecision(
                    triggered=True,
                    rule_id=rule.rule_id,
                    fault_type=rule.fault_type,
                    status_code=rule.status_code or 504,
                    headers={},
                    response_body={
                        "error": "gateway_timeout",
                        "message": f"Upstream timeout waiting for {service}",
                        "rule_id": rule.rule_id,
                    },
                    injected_latency_ms=total_latency_ms,
                )

            if rule.fault_type == FaultType.CORRUPT_JSON:
                return FaultDecision(
                    triggered=True,
                    rule_id=rule.rule_id,
                    fault_type=rule.fault_type,
                    status_code=rule.status_code or 502,
                    headers={"Content-Type": "application/json"},
                    response_body={"error": "corrupt_upstream_payload"},
                    raw_bytes=b'{"status": "ok", "truncated_json": [1, 2, ',
                    injected_latency_ms=total_latency_ms,
                )

        return FaultDecision(
            triggered=False,
            injected_latency_ms=total_latency_ms,
        )
