# BridgeTest — Chaos Engineering & End-to-End State Verification for Integration Pipelines

[CI](https://www.google.com/search?q=https://github.com/sismax01/BridgeTest/actions/workflows/ci.yml)




**BridgeTest** (`bridgetest`) is a standalone developer tool and chaos testing harness that subjects webhook-driven integration workflows (e.g., Make.com, n8n, FastAPI/Express orchestrators, event routers, and RevOps pipelines) to realistic distributed systems failures and verifies that the **final business state** across all downstream systems is idempotent, consistent, and loss-free.

## Why Happy-Path Integration Tests Fail in Production

In staging environments, webhook and API integration workflows almost always pass because tests send a single clean payload and assert a `200 OK` response. In production, distributed workflows silently corrupt business state under four recurring failure modes:

| Failure Mode | Real-World Trigger | Typical Happy-Path Pipeline Symptom | BridgeTest Invariant Assertion |
| :--- | :--- | :--- | :--- |
| **At-Least-Once Duplicate Storms** | Stripe, Shopify, or GitHub webhook retries firing concurrent or delayed duplicate deliveries | Duplicate CRM deals, double-booked ledger entries, and repeated customer emails | `exact_cardinality` & `idempotency_enforcement` |
| **Upstream Rate Limits & Timeouts** | HubSpot, Salesforce, or Slack returning `HTTP 429 Too Many Requests` (`Retry-After: 30`) or TCP drops | Silently dropped records or unhandled worker thread crashes | `eventual_consistency` within backoff window |
| **Mid-Workflow Partial Outages** | Step 1 (CRM) succeeds, Step 2 (Ledger) fails with `HTTP 503 Service Unavailable` | Orphaned state (e.g., deal created without linked invoice or entitlement) | `atomic_rollback` or `dead_letter_queue` state |
| **Silent Schema Drift & Poison Pills** | Upstream APIs stringifying numbers (`"14900"`), renaming fields (`buyer_email` -> `customer_email`), or dropping null keys | Downstream parsing exceptions, corrupt DB values, or silent default fallbacks | `schema_conformance` & `non_null_invariant` |

## System Architecture

BridgeTest implements a **Closed-Loop Integration Harness** that wraps both the ingress and egress boundaries of a target integration pipeline:

```text
+-------------------------------------------------------------+
|                     BridgeTest Runner                       |
|    (YAML / Python Scenario DSL + Invariant Evaluator)       |
+------------------------------+------------------------------+
                               |
            1. Mutate & Dispatch | 4. Inspect Joint State
           (Duplicates, Drift, HMAC) v & Causal Trace Graph
+------------------------------+------------------------------+
|                                                             |
|   +-----------------------------------------------------+   |
|   |         Target Integration Pipeline                 |   |
|   | (Make / n8n / Custom Webhook Router / Worker)       |   |
|   +--------------+----------------------+---------------+   |
|                  |                      |                   |
|       2. Call    |           3. Call    |                   |
|       Target 1   v           Target 2   v                   |
|   +--------------+-----+    +-----------+---------------+   |
|   |  CRM Twin          |    |  Ledger Twin              |   |
|   |  (Configurable     |    |  (Configurable            |   |
|   |   Chaos Server)    |    |   Chaos Server)           |   |
|   +--------------------+    +---------------------------+   |
|                                                             |
+-------------------------------------------------------------+
```

1. **Synthetic Event Generator**: Injects deterministic event streams configured with custom payload schemas, HMAC/JWT signing secrets, and duplicate burst patterns.
2. **Pluggable Chaos Fault Engine**: Intercepts downstream API calls using lightweight, configurable HTTP **Twin Servers** (e.g., CRM Twin, Ledger Twin) capable of simulating transient rate limits (`HTTP 429`), network jitter, service downtime (`HTTP 503`), payload schema shifts, and delayed responses.
3. **Multi-System Invariant Evaluator**: Queries the recorded memory state across all downstream Twins after scenario execution and runs cross-system state assertions.

## Core Invariant Library

BridgeTest tests behavior against verifiable state invariants rather than HTTP status codes:

- **`exact_cardinality`**: Asserts that despite $N$ duplicate webhook deliveries, exactly $K$ valid records exist in target systems.
- **`cross_system_consistency`**: Verifies that entity fields across multiple downstream twins (e.g., `crm.deal.amount == ledger.invoice.total`) match exactly without split-brain drift.
- **`idempotency_enforcement`**: Ensures that subsequent retries with identical `idempotency_key` or `event_id` headers yield cached responses and zero redundant mutations.
- **`retry_backoff_compliance`**: Checks that pipeline retry attempts respect downstream `Retry-After` headers and exponential backoff curves.
- **`schema_conformance`**: Confirms that stored entities conform to expected schema definitions without dropped properties or improper type coercions.

## Quickstart & Installation

### Requirements
- Python 3.10+
- `PyYAML>=6.0` (lightweight dependency footprint—HTTP twin servers, state modeling, and journaling are built on the Python 3.10+ standard library with zero external bloat)

### Installation

```bash
git clone https://github.com/sismax01/BridgeTest.git
cd BridgeTest
python -m venv venv
source venv/bin/activate  # Or `venv\Scripts\activate` on Windows
pip install -e .
```

## Usage Examples

### 1. Run the Included E2E Order Synchronization Scenario

BridgeTest includes a ready-to-run demonstration testing an order processing pipeline against concurrent duplicates, schema mutations, and temporary CRM rate limiting:

```bash
bridgetest run examples/scenarios/order_sync_chaos.yaml
# Or run the built-in interactive demo suite:
bridgetest demo
```

**Sample CLI Output:**

```text
[BridgeTest] Loading scenario: examples/scenarios/order_sync_chaos.yaml
[BridgeTest] Spin up TwinCluster (CRM Twin @ :54321, Ledger Twin @ :54322)...
[BridgeTest] Injecting 3 event deliveries (2 duplicates) for event_id=evt_ord_5001...
[BridgeTest] Applying mutation 'stringify_amount' to event index 0...
[BridgeTest] Arming fault rule 'crm_rate_limit_429' on target 'crm' (fail_first_n=2)...
[BridgeTest] Executing target pipeline: ResilientOutboxPipeline...

--------------------------------------------------------------------------------
SCENARIO EXECUTION SUMMARY
--------------------------------------------------------------------------------
Status: PASSED
Total Events Dispatched : 3
Total Invariants Checked : 4
Passed Invariants       : 4
Failed Invariants       : 0

Invariant Results:
  [PASS] inv_cardinality       : Exactly 1 valid order persisted across CRM, Ledger, and Notification
  [PASS] inv_consistency       : Zero split-brain state across downstream twins
  [PASS] inv_idempotency       : All downstream writes include Idempotency-Key
  [PASS] inv_backoff           : Retries respect Retry-After backoff interval
```

### 2. Run the Automated Test Suite

```bash
pytest tests/ -v
```

## Declarative YAML Scenario Format

BridgeTest scenarios are defined in human-readable YAML specification files:

```yaml
scenario_id: order_sync_resilience_test
description: Validate order synchronization resilience under network duplicates and rate limits

ingress:
  pipeline_type: internal_demo
  target_handler: ResilientOutboxPipeline

events:
  - event_id: "evt_ord_5001"
    event_type: "order.completed"
    idempotency_key: "idem_ord_5001"
    correlation_id: "corr_ord_5001"
    timestamp: 1761171801
    payload:
      order_id: "ord_5001"
      customer_email: "ada.lovelace@analytical.io"
      amount_cents: 18500
      currency: "USD"

chaos:
  duplicate_deliveries_per_event: 2
  concurrent_duplicates: true
  signing_secret: "bt_whsec_prod_9f8e7d6c5b4a3210"
  mutations:
    - mutation_id: "stringify_amount"
      drift_type: "stringify_numbers"
      apply_to_event_indices: [0]
      source_field: "amount_cents"
      expect_rejected_or_dlq: false
  fault_rules:
    - rule_id: "crm_rate_limit_429"
      target_service: "crm"
      fault_type: "rate_limit_429"
      fail_first_n_per_key: 2
      retry_after_seconds: 0.02

invariants:
  - invariant_id: "inv_cardinality"
    invariant_type: "exact_cardinality"
    description: "Exactly 1 valid order persisted across CRM, Ledger, and Notification"
    params:
      expected_valid_records: 1
  - invariant_id: "inv_consistency"
    invariant_type: "cross_system_consistency"
    description: "Zero split-brain state across downstream twins"
  - invariant_id: "inv_idempotency"
    invariant_type: "idempotency_enforcement"
    description: "All downstream writes include Idempotency-Key"
  - invariant_id: "inv_backoff"
    invariant_type: "retry_backoff_compliance"
    description: "Retries respect Retry-After backoff interval"
    params:
      min_backoff_seconds: 0.015
```

## Programmatic Python API

You can also embed `TwinCluster` and `ScenarioRunner` directly inside your Python test suite:

```python
from bridgetest import ScenarioRunner, TwinCluster
from bridgetest.demo.pipelines import ResilientOutboxPipeline

with TwinCluster() as cluster:
    # Expose real local HTTP endpoints (e.g., http://127.0.0.1:54321/crm/contacts)
    twin_base_url = cluster.start_http_server()

    runner = ScenarioRunner(cluster=cluster)
    scenario = runner.load_scenario_file("examples/scenarios/order_sync_chaos.yaml")
    pipeline = ResilientOutboxPipeline(twin_cluster=cluster)

    result = runner.run_scenario(
        scenario=scenario,
        target_handler=pipeline.handle_webhook,
        target_name="ResilientOutboxPipeline",
    )
    assert result.passed, f"Chaos invariants failed: {result.to_dict()}"
```

## License

Released under the MIT License. See `LICENSE` for details.
