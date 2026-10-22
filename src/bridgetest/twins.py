"""Stateful Downstream Digital Twins (CRM, Payment Ledger, Notification Sink, Dead-Letter Queue)."""

from __future__ import annotations

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

from bridgetest.faults import FaultEngine
from bridgetest.journal import FlightRecorder
from bridgetest.models import FaultRule


class StatefulServiceTwin:
    """Thread-safe stateful store for a single simulated downstream service with idempotency support."""

    def __init__(self, service_name: str, required_fields: List[str]) -> None:
        self.service_name = service_name
        self.required_fields = list(required_fields)
        self._lock = threading.RLock()
        self._records: List[Dict[str, Any]] = []
        self._idempotency_index: Dict[str, Dict[str, Any]] = {}

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._idempotency_index.clear()

    def records(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._records]

    def process_write(
        self,
        *,
        payload: Dict[str, Any],
        idempotency_key: Optional[str],
        correlation_id: str,
    ) -> Tuple[int, Dict[str, Any]]:
        """Validate schema and persist record, honoring idempotency_key if provided by the caller."""
        if not isinstance(payload, dict):
            return 400, {
                "error": "invalid_payload",
                "message": "Request body must be a JSON object",
            }

        missing = [f for f in self.required_fields if f not in payload or payload[f] is None or payload[f] == ""]
        if missing:
            return 422, {
                "error": "schema_validation_failed",
                "service": self.service_name,
                "missing_fields": missing,
            }

        if "amount_cents" in payload:
            if not isinstance(payload["amount_cents"], int) or isinstance(payload["amount_cents"], bool):
                return 422, {
                    "error": "type_validation_failed",
                    "service": self.service_name,
                    "field": "amount_cents",
                    "message": f"Expected integer amount_cents, got {type(payload['amount_cents']).__name__}",
                }
            if payload["amount_cents"] <= 0:
                return 422, {
                    "error": "domain_validation_failed",
                    "service": self.service_name,
                    "field": "amount_cents",
                    "message": "amount_cents must be strictly positive",
                }

        with self._lock:
            if idempotency_key and idempotency_key in self._idempotency_index:
                existing = dict(self._idempotency_index[idempotency_key])
                return 200, {
                    "status": "idempotent_replay",
                    "service": self.service_name,
                    "record_id": existing["record_id"],
                    "idempotency_key": idempotency_key,
                    "record": existing,
                }

            record_id = f"{self.service_name}_{uuid.uuid4().hex[:10]}"
            record = {
                "record_id": record_id,
                "service": self.service_name,
                "correlation_id": correlation_id,
                "idempotency_key": idempotency_key,
                "created_at": time.time(),
                **payload,
            }
            self._records.append(record)
            if idempotency_key:
                self._idempotency_index[idempotency_key] = record

            return 201, {
                "status": "created",
                "service": self.service_name,
                "record_id": record_id,
                "idempotency_key": idempotency_key,
                "record": record,
            }


class TwinCluster:
    """Coordinates stateful downstream digital twins, fault injection, causal journaling, and HTTP serving."""

    def __init__(self, fault_rules: Optional[List[FaultRule]] = None) -> None:
        self.recorder = FlightRecorder()
        self.fault_engine = FaultEngine(fault_rules)

        self.crm = StatefulServiceTwin(
            service_name="crm",
            required_fields=["external_event_id", "order_id", "customer_email", "amount_cents"],
        )
        self.ledger = StatefulServiceTwin(
            service_name="ledger",
            required_fields=["external_event_id", "order_id", "customer_email", "amount_cents", "currency"],
        )
        self.notification = StatefulServiceTwin(
            service_name="notification",
            required_fields=["external_event_id", "order_id", "recipient", "channel"],
        )
        self.dlq = StatefulServiceTwin(
            service_name="dlq",
            required_fields=["external_event_id", "reason", "raw_payload"],
        )

        self._twins: Dict[str, StatefulServiceTwin] = {
            "crm": self.crm,
            "ledger": self.ledger,
            "notification": self.notification,
            "dlq": self.dlq,
        }
        self._http_server: Optional[ThreadingHTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._base_url: Optional[str] = None

    def reset(self, fault_rules: Optional[List[FaultRule]] = None, seed: int = 42) -> None:
        """Reset all twins, flight recorder entries, and fault injection counters."""
        self.recorder.reset()
        self.fault_engine.reset(fault_rules, seed=seed)
        for twin in self._twins.values():
            twin.reset()

    def handle_request(
        self,
        *,
        service: str,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Any,
    ) -> Tuple[int, Dict[str, str], Any]:
        """Process a request to a digital twin, applying fault rules and recording a journal entry."""
        start_ts = time.perf_counter()
        normalized_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        correlation_id = (
            normalized_headers.get("x-correlation-id")
            or (body.get("external_event_id") if isinstance(body, dict) else None)
            or "uncorrelated"
        )
        idempotency_key = normalized_headers.get("idempotency-key")

        if service not in self._twins:
            return 404, {"Content-Type": "application/json"}, {"error": f"Unknown twin service '{service}'"}

        decision = self.fault_engine.evaluate(
            service=service,
            correlation_id=str(correlation_id),
            idempotency_key=idempotency_key,
        )

        if decision.triggered:
            elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
            resp_headers = {"Content-Type": "application/json", **(decision.headers or {})}
            self.recorder.record(
                direction="downstream",
                service=service,
                method=method,
                path=path,
                correlation_id=str(correlation_id),
                idempotency_key=idempotency_key,
                status_code=decision.status_code,
                latency_ms=elapsed_ms,
                fault_injected=f"{decision.rule_id}:{decision.fault_type.value if decision.fault_type else 'unknown'}",
                request_headers=normalized_headers,
                request_body=body,
                response_body=decision.response_body,
            )
            if decision.raw_bytes is not None:
                return decision.status_code, resp_headers, decision.raw_bytes
            return decision.status_code, resp_headers, decision.response_body

        twin = self._twins[service]
        status_code, response_payload = twin.process_write(
            payload=body,
            idempotency_key=idempotency_key,
            correlation_id=str(correlation_id),
        )
        elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
        self.recorder.record(
            direction="downstream",
            service=service,
            method=method,
            path=path,
            correlation_id=str(correlation_id),
            idempotency_key=idempotency_key,
            status_code=status_code,
            latency_ms=elapsed_ms,
            fault_injected=None,
            request_headers=normalized_headers,
            request_body=body,
            response_body=response_payload,
        )
        return status_code, {"Content-Type": "application/json"}, response_payload

    def state_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return a deep snapshot of all records across every digital twin."""
        return {name: twin.records() for name, twin in self._twins.items()}

    def state_counts(self) -> Dict[str, int]:
        """Return record counts per twin service."""
        return {name: len(twin.records()) for name, twin in self._twins.items()}

    def start_http_server(self, host: str = "127.0.0.1", port: int = 0) -> str:
        """Start a multi-threaded HTTP server exposing twin endpoints on a local port."""
        if self._http_server is not None and self._base_url is not None:
            return self._base_url

        cluster = self

        class TwinRequestHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                pass  # Suppress noisy stdout access logs during tests

            def _send_json(self, status: int, headers: Dict[str, str], payload: Any) -> None:
                if isinstance(payload, bytes):
                    raw = payload
                else:
                    raw = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:
                if self.path == "/_admin/state":
                    self._send_json(
                        200,
                        {"Content-Type": "application/json"},
                        {
                            "counts": cluster.state_counts(),
                            "records": cluster.state_snapshot(),
                            "journal_summary": cluster.recorder.summary(),
                        },
                    )
                    return
                self._send_json(404, {"Content-Type": "application/json"}, {"error": "not_found"})

            def do_POST(self) -> None:
                content_len = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_len) if content_len > 0 else b"{}"
                try:
                    parsed_body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    parsed_body = None

                if self.path == "/_admin/reset":
                    cluster.reset()
                    self._send_json(200, {"Content-Type": "application/json"}, {"status": "reset"})
                    return

                parts = [p for p in self.path.strip("/").split("/") if p]
                service = parts[0] if parts else ""
                req_headers = {k: v for k, v in self.headers.items()}
                status, resp_headers, resp_payload = cluster.handle_request(
                    service=service,
                    method="POST",
                    path=self.path,
                    headers=req_headers,
                    body=parsed_body,
                )
                self._send_json(status, resp_headers, resp_payload)

        server = ThreadingHTTPServer((host, port), TwinRequestHandler)
        bound_host, bound_port = server.server_address
        self._http_server = server
        self._base_url = f"http://{bound_host}:{bound_port}"
        thread = threading.Thread(target=server.serve_forever, name="BridgeTestTwinServer", daemon=True)
        thread.start()
        self._server_thread = thread
        return self._base_url

    def stop_http_server(self) -> None:
        """Shut down the local HTTP twin server cleanly."""
        if self._http_server is not None:
            self._http_server.shutdown()
            self._http_server.server_close()
            self._http_server = None
            self._base_url = None
            self._server_thread = None

    def __enter__(self) -> TwinCluster:
        self.start_http_server()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop_http_server()
