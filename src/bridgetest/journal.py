"""Thread-safe causal flight recorder for capturing inbound webhooks and downstream twin interactions."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from bridgetest.models import JournalEntry


class FlightRecorder:
    """Thread-safe monotonic event journal tracking causal request/response chains and retry timings."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: List[JournalEntry] = []
        self._seq_counter = 0
        self._attempt_tracker: Dict[Tuple[str, str, str], int] = defaultdict(int)

    def reset(self) -> None:
        """Clear all recorded entries and reset counters."""
        with self._lock:
            self._entries.clear()
            self._seq_counter = 0
            self._attempt_tracker.clear()

    def record(
        self,
        *,
        direction: str,
        service: str,
        method: str,
        path: str,
        correlation_id: str,
        idempotency_key: Optional[str],
        status_code: int,
        latency_ms: float,
        fault_injected: Optional[str],
        request_headers: Dict[str, str],
        request_body: Any,
        response_body: Any,
    ) -> JournalEntry:
        """Append a new interaction entry to the causal journal in a thread-safe manner."""
        with self._lock:
            self._seq_counter += 1
            tracking_key = (
                direction,
                service,
                idempotency_key or correlation_id or f"anon-{self._seq_counter}",
            )
            self._attempt_tracker[tracking_key] += 1
            attempt_index = self._attempt_tracker[tracking_key]

            normalized_headers = {str(k).lower(): str(v) for k, v in (request_headers or {}).items()}
            entry = JournalEntry(
                seq_id=self._seq_counter,
                timestamp_monotonic=time.monotonic(),
                direction=direction,
                service=service,
                method=method.upper(),
                path=path,
                correlation_id=correlation_id or "uncorrelated",
                idempotency_key=idempotency_key,
                status_code=int(status_code),
                latency_ms=round(float(latency_ms), 3),
                fault_injected=fault_injected,
                request_headers=normalized_headers,
                request_body=request_body,
                response_body=response_body,
                attempt_index=attempt_index,
            )
            self._entries.append(entry)
            return entry

    def entries(
        self,
        *,
        direction: Optional[str] = None,
        service: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> List[JournalEntry]:
        """Return filtered journal entries in monotonic sequence order."""
        with self._lock:
            snapshot = list(self._entries)
        result = []
        for item in snapshot:
            if direction is not None and item.direction != direction:
                continue
            if service is not None and item.service != service:
                continue
            if correlation_id is not None and item.correlation_id != correlation_id:
                continue
            result.append(item)
        return result

    def retry_intervals_by_key(self, service: Optional[str] = None) -> Dict[Tuple[str, str], List[float]]:
        """Compute inter-attempt intervals (in seconds) for retried downstream requests."""
        downstream = self.entries(direction="downstream", service=service)
        grouped: Dict[Tuple[str, str], List[JournalEntry]] = defaultdict(list)
        for entry in downstream:
            key = entry.idempotency_key or entry.correlation_id
            grouped[(entry.service, key)].append(entry)

        intervals: Dict[Tuple[str, str], List[float]] = {}
        for group_key, items in grouped.items():
            if len(items) > 1:
                deltas = []
                for idx in range(1, len(items)):
                    delta = max(0.0, items[idx].timestamp_monotonic - items[idx - 1].timestamp_monotonic)
                    deltas.append(round(delta, 4))
                intervals[group_key] = deltas
        return intervals

    def summary(self) -> Dict[str, Any]:
        """Generate aggregate statistics across all recorded interactions."""
        with self._lock:
            snapshot = list(self._entries)

        inbound = [e for e in snapshot if e.direction == "inbound"]
        downstream = [e for e in snapshot if e.direction == "downstream"]
        faults = [e for e in downstream if e.fault_injected is not None]

        by_service: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "2xx": 0, "4xx": 0, "5xx": 0, "faults": 0})
        for entry in downstream:
            bucket = by_service[entry.service]
            bucket["total"] += 1
            if 200 <= entry.status_code < 300:
                bucket["2xx"] += 1
            elif 400 <= entry.status_code < 500:
                bucket["4xx"] += 1
            elif entry.status_code >= 500:
                bucket["5xx"] += 1
            if entry.fault_injected:
                bucket["faults"] += 1

        retries = self.retry_intervals_by_key()
        return {
            "total_entries": len(snapshot),
            "inbound_calls": len(inbound),
            "downstream_calls": len(downstream),
            "faults_injected": len(faults),
            "retried_keys_count": len(retries),
            "by_service": dict(by_service),
        }
