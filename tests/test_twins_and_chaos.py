"""Unit and integration tests for TwinCluster stateful HTTP servers, HMAC signing, and ChaosProducer mutations."""

from __future__ import annotations

import json
import sys
import unittest
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bridgetest.chaos import (  # noqa: E402
    ChaosProducer,
    compute_webhook_signature,
    verify_webhook_signature,
)
from bridgetest.models import (  # noqa: E402
    ChaosProfile,
    DriftMutation,
    DriftType,
    WebhookEvent,
)
from bridgetest.twins import TwinCluster  # noqa: E402


class TestTwinsAndChaosProducer(unittest.TestCase):
    def test_hmac_signature_computation_and_verification(self) -> None:
        secret = "whsec_test_secret_123"
        raw_body = b'{"event_id":"evt_01","amount_cents":1500}'
        ts = 1761171720
        sig_header = compute_webhook_signature(secret, ts, raw_body)

        self.assertTrue(
            verify_webhook_signature(secret, sig_header, raw_body, tolerance_seconds=300, current_timestamp=ts + 10)
        )
        # Tampered body fails verification
        self.assertFalse(
            verify_webhook_signature(secret, sig_header, b'{"event_id":"evt_01","amount_cents":9999}')
        )
        # Expired timestamp outside tolerance fails verification
        self.assertFalse(
            verify_webhook_signature(secret, sig_header, raw_body, tolerance_seconds=60, current_timestamp=ts + 500)
        )

    def test_twin_cluster_idempotency_and_http_server(self) -> None:
        with TwinCluster() as cluster:
            base_url = cluster.start_http_server()
            payload = {
                "external_event_id": "evt_100",
                "order_id": "ord_100",
                "customer_email": "alice@example.com",
                "amount_cents": 2500,
            }
            raw_bytes = json.dumps(payload).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "Idempotency-Key": "crm:idem_100",
                "X-Correlation-ID": "corr_100",
            }

            req1 = urllib.request.Request(f"{base_url}/crm/contacts", data=raw_bytes, headers=headers, method="POST")
            with urllib.request.urlopen(req1, timeout=5) as resp1:
                self.assertEqual(resp1.status, 201)
                body1 = json.loads(resp1.read().decode("utf-8"))
                self.assertEqual(body1["status"], "created")

            # Second request with identical Idempotency-Key returns 200 idempotent_replay without duplicating state
            req2 = urllib.request.Request(f"{base_url}/crm/contacts", data=raw_bytes, headers=headers, method="POST")
            with urllib.request.urlopen(req2, timeout=5) as resp2:
                self.assertEqual(resp2.status, 200)
                body2 = json.loads(resp2.read().decode("utf-8"))
                self.assertEqual(body2["status"], "idempotent_replay")
                self.assertEqual(body1["record_id"], body2["record_id"])

            self.assertEqual(cluster.state_counts()["crm"], 1)

    def test_chaos_producer_schema_drift_mutations_and_duplicate_fanout(self) -> None:
        cluster = TwinCluster()
        profile = ChaosProfile(
            duplicate_Deliveries_per_event=3,
            concurrent_duplicates=True,
            mutations=[
                DriftMutation(
                    mutation_id="m_stringify",
                    drift_type=DriftType.STRINGIFY_NUMBERS,
                    apply_to_event_indices=[0],
                    source_field="amount_cents",
                ),
                DriftMutation(
                    mutation_id="m_drop_email",
                    drift_type=DriftType.DROP_REQUIRED_FIELD,
                    apply_to_event_indices=[1],
                    source_field="customer_email",
                    expect_rejected_or_dlq=True,
                ),
            ],
        )
        producer = ChaosProducer(profile=profile, recorder=cluster.recorder)
        events = [
            WebhookEvent(
                event_id="evt_1",
                event_type="order.completed",
                idempotency_key="idem_1",
                correlation_id="corr_1",
                timestamp=1761171720,
                payload={"order_id": "o1", "customer_email": "a@b.com", "amount_cents": 1000},
            ),
            WebhookEvent(
                event_id="evt_2",
                event_type="order.completed",
                idempotency_key="idem_2",
                correlation_id="corr_2",
                timestamp=1761171721,
                payload={"order_id": "o2", "customer_email": "c@d.com", "amount_cents": 2000},
            ),
        ]

        received = []

        def _dummy_target(headers: dict, raw: bytes) -> tuple[int, dict]:
            received.append(json.loads(raw.decode("utf-8")))
            return 200, {"ok": True}

        prepared, outcomes = producer.dispatch_all(events, _dummy_target)
        self.assertEqual(len(prepared), 2)
        self.assertEqual(len(outcomes), 6)  # 2 events * 3 duplicate deliveries each
        self.assertEqual(prepared[0].payload["amount_cents"], "1000")
        self.assertNotIn("customer_email", prepared[1].payload)
        self.assertTrue(prepared[1].is_poison_expected)


if __name__ == "__main__":
    unittest.main()
