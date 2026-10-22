"""Unit tests for the InvariantEngine verifying detection of split-brain, duplicates, and swallowed errors."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bridgetest.invariants import InvariantEngine  # noqa: E402
from bridgetest.models import InvariantSpec, InvariantType, WebhookEvent  # noqa: E402
from bridgetest.twins import TwinCluster  # noqa: E402


class TestInvariantEngine(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster = TwinCluster()
        self.engine = InvariantEngine()
        self.sample_event = WebhookEvent(
            event_id="evt_inv_01",
            event_type="order.completed",
            idempotency_key="idem_inv_01",
            correlation_id="corr_inv_01",
            timestamp=1761171720,
            payload={
                "order_id": "ord_501",
                "customer_email": "test@invariant.dev",
                "amount_cents": 9900,
                "currency": "USD",
            },
        )

    def test_detects_split_brain_cross_system_inconsistency(self) -> None:
        # Write only to CRM and Ledger, leaving Notification missing (split-brain)
        self.cluster.handle_request(
            service="crm",
            method="POST",
            path="/crm/contacts",
            headers={"Idempotency-Key": "crm:1", "X-Correlation-ID": "corr_inv_01"},
            body={
                "external_event_id": "evt_inv_01",
                "order_id": "ord_501",
                "customer_email": "test@invariant.dev",
                "amount_cents": 9900,
            },
        )
        self.cluster.handle_request(
            service="ledger",
            method="POST",
            path="/ledger/entries",
            headers={"Idempotency-Key": "ledger:1", "X-Correlation-ID": "corr_inv_01"},
            body={
                "external_event_id": "evt_inv_01",
                "order_id": "ord_501",
                "customer_email": "test@invariant.dev",
                "amount_cents": 9900,
                "currency": "USD",
            },
        )

        spec = InvariantSpec(
            invariant_id="test_split_brain",
            invariant_type=InvariantType.CROSS_SYSTEM_CONSISTENCY,
            description="Must detect missing notification record",
        )
        res = self.engine.evaluate_one(spec, [self.sample_event], self.cluster)
        self.assertFalse(res.passed)
        self.assertTrue(any("Split-brain state" in v for v in res.violations))

    def test_detects_silent_swallow_when_event_missing_everywhere(self) -> None:
        spec = InvariantSpec(
            invariant_id="test_swallow",
            invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
            description="Must detect silently dropped order event",
        )
        res = self.engine.evaluate_one(spec, [self.sample_event], self.cluster)
        self.assertFalse(res.passed)
        self.assertEqual(res.metrics["swallowed_count"], 1)

    def test_passes_when_poison_event_routed_to_dlq(self) -> None:
        poison_evt = WebhookEvent(
            event_id="evt_poison_99",
            event_type="order.completed",
            idempotency_key="idem_poison_99",
            correlation_id="corr_poison_99",
            timestamp=1761171720,
            payload={"order_id": "ord_bad"},
            is_poison_expected=True,
        )
        self.cluster.handle_request(
            service="dlq",
            method="POST",
            path="/dlq/events",
            headers={"Idempotency-Key": "dlq:idem_poison_99", "X-Correlation-ID": "corr_poison_99"},
            body={
                "external_event_id": "evt_poison_99",
                "reason": "missing_customer_email",
                "raw_payload": {"order_id": "ord_bad"},
            },
        )

        spec_poison = InvariantSpec(
            invariant_id="test_poison",
            invariant_type=InvariantType.POISON_PILL_CONTAINMENT,
            description="Poison event must be in DLQ and absent from CRM/Ledger",
        )
        res_poison = self.engine.evaluate_one(spec_poison, [poison_evt], self.cluster)
        self.assertTrue(res_poison.passed)

        spec_swallow = InvariantSpec(
            invariant_id="test_conservation",
            invariant_type=InvariantType.ZERO_SILENT_SWALLOW,
            description="DLQ capture satisfies event conservation",
        )
        res_swallow = self.engine.evaluate_one(spec_swallow, [poison_evt], self.cluster)
        self.assertTrue(res_swallow.passed)


if __name__ == "__main__":
    unittest.main()
