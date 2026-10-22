"""Unit tests for FlightRecorder causal journaling and FaultEngine deterministic/probabilistic faults."""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bridgetest.faults import FaultEngine  # noqa: E402
from bridgetest.journal import FlightRecorder  # noqa: E402
from bridgetest.models import FaultRule, FaultType  # noqa: E402


class TestFlightRecorderAndFaultEngine(unittest.TestCase):
    def test_flight_recorder_monotonic_sequence_and_attempt_tracking(self) -> None:
        recorder = FlightRecorder()
        e1 = recorder.record(
            direction="downstream",
            service="crm",
            method="post",
            path="/crm/contacts",
            correlation_id="corr_1",
            idempotency_key="idem_1",
            status_code=429,
            latency_ms=4.2,
            fault_injected="rule_429:rate_limit_429",
            request_headers={"Idempotency-Key": "idem_1"},
            request_body={"order_id": "ord_1"},
            response_body={"error": "rate_limit"},
        )
        time.sleep(0.012)
        e2 = recorder.record(
            direction="downstream",
            service="crm",
            method="post",
            path="/crm/contacts",
            correlation_id="corr_1",
            idempotency_key="idem_1",
            status_code=201,
            latency_ms=2.1,
            fault_injected=None,
            request_headers={"Idempotency-Key": "idem_1"},
            request_body={"order_id": "ord_1"},
            response_body={"status": "created"},
        )

        self.assertEqual(e1.seq_id, 1)
        self.assertEqual(e2.seq_id, 2)
        self.assertEqual(e1.attempt_index, 1)
        self.assertEqual(e2.attempt_index, 2)

        intervals = recorder.retry_intervals_by_key(service="crm")
        self.assertIn(("crm", "idem_1"), intervals)
        self.assertEqual(len(intervals[("crm", "idem_1")]), 1)
        self.assertGreaterEqual(intervals[("crm", "idem_1")][0], 0.008)

        summary = recorder.summary()
        self.assertEqual(summary["downstream_calls"], 2)
        self.assertEqual(summary["faults_injected"], 1)
        self.assertEqual(summary["retried_keys_count"], 1)

    def test_fault_engine_fail_first_n_per_key_rate_limit(self) -> None:
        rules = [
            FaultRule(
                rule_id="rl_crm_2",
                target_service="crm",
                fault_type=FaultType.RATE_LIMIT_429,
                fail_first_n_per_key=2,
                retry_after_seconds=0.025,
            )
        ]
        engine = FaultEngine(rules=rules)

        # First two calls for key_A fail with 429; third succeeds
        d1 = engine.evaluate(service="crm", correlation_id="c1", idempotency_key="key_A")
        d2 = engine.evaluate(service="crm", correlation_id="c1", idempotency_key="key_A")
        d3 = engine.evaluate(service="crm", correlation_id="c1", idempotency_key="key_A")

        self.assertTrue(d1.triggered)
        self.assertEqual(d1.status_code, 429)
        self.assertEqual(d1.headers, {"Retry-After": "0.025"})
        self.assertTrue(d2.triggered)
        self.assertEqual(d2.status_code, 429)
        self.assertFalse(d3.triggered)

        # A distinct key_B still triggers its own 2 failures independently
        b1 = engine.evaluate(service="crm", correlation_id="c2", idempotency_key="key_B")
        self.assertTrue(b1.triggered)
        self.assertEqual(engine.total_injected, 3)

    def test_fault_engine_503_504_and_corrupt_json_modes(self) -> None:
        rules = [
            FaultRule(
                rule_id="ledger_503",
                target_service="ledger",
                fault_type=FaultType.TRANSIENT_503,
                fail_first_n_per_key=1,
            ),
            FaultRule(
                rule_id="notif_504",
                target_service="notification",
                fault_type=FaultType.GATEWAY_TIMEOUT_504,
                fail_first_n_per_key=1,
            ),
            FaultRule(
                rule_id="dlq_corrupt",
                target_service="dlq",
                fault_type=FaultType.CORRUPT_JSON,
                fail_first_n_per_key=1,
            ),
        ]
        engine = FaultEngine(rules=rules)

        res_ledger = engine.evaluate(service="ledger", correlation_id="c1", idempotency_key="k1")
        self.assertTrue(res_ledger.triggered)
        self.assertEqual(res_ledger.status_code, 503)

        res_notif = engine.evaluate(service="notification", correlation_id="c1", idempotency_key="k1")
        self.assertTrue(res_notif.triggered)
        self.assertEqual(res_notif.status_code, 504)

        res_corrupt = engine.evaluate(service="dlq", correlation_id="c1", idempotency_key="k1")
        self.assertTrue(res_corrupt.triggered)
        self.assertEqual(res_corrupt.status_code, 502)
        self.assertIsNotNone(res_corrupt.raw_bytes)


if __name__ == "__main__":
    unittest.main()
