# BridgeTest — Chaos Engineering & End-to-End State Verification for Integration Pipelines

**BridgeTest** (`bridgetest`) is a standalone developer tool and chaos testing harness that subjects webhook-driven integration workflows (e.g., n8n, Make, FastAPI/Express orchestrators, event routers, and RevOps pipelines) to realistic distributed systems failures and verifies that the **final business state** across all downstream systems is idempotent, consistent, and loss-free.

## Why Happy-Path Integration Tests Fail in Production

In staging environments, webhook and API integration workflows almost always pass because tests send a single clean payload and assert a `200 OK` response. In production, distributed workflows silently corrupt business state under four recurring failure modes:

| Failure Mode | Real-World Trigger | Typical Happy-Path Pipeline Symptom | BridgeTest Invariant Assertion |
| :--- | :--- | :--- | :--- |
| **At-Least-Once Duplicate Storms** | Stripe, Shopify, or GitHub webhook retries firing concurrent or delayed duplicate deliveries | Duplicate CRM deals, double-booked ledger entries, and repeated customer emails | `exact_cardinality` & `idempotency_enforcement` |
| **Upstream Rate Limits & Timeouts** | HubSpot, Salesforce, or Slack returning `HTTP 429 Too Many Requests` (`Retry-After`) or `504 Gateway Timeout` | Dropped customer records with no retry tracking or dead-letter capture | `retry_backoff_compliance` & `zero_silent_swallow` |
| **Mid-Workflow Partial Outages** | Step 1 (CRM) succeeds, Step 2 (Ledger) fails with `HTTP 503 Service Unavailable` | Split-brain distributed state: CRM shows an active deal while the payment ledger has zero record | `cross_system_consistency` |
| **Silent Schema Drift & Poison Pills** | Upstream APIs stringifying numbers (`"14900"`), renaming fields (`buyer_email`), or sending unrecoverable malformed payloads | Unhandled exceptions or silent `200 OK` swallows without alerting engineering via a Dead-Letter Queue (DLQ) | `poison_pill_containment` & `zero_silent_swallow` |

## System Architecture

BridgeTest implements a **Closed-Loop Integration Harness** that wraps both the ingress and egress boundaries of an integration pipeline:

```text
                       +-------------------------------------------------------+
                       |                 BridgeTest Runner                     |
                       |  (YAML / Python Scenario DSL + Invariant Evaluator)   |
                       +-----------+-------------------------------+-----------+
                                   |                               ^
         1. Mutate & Dispatch      |                               | 4. Inspect Joint State
         (Duplicates, Drift, HMAC) v                               |    & Causal Trace Graph
                       +-----------------------+       +-----------+-----------+
                       |    ChaosProducer      |       |     TwinCluster       |
                       |  - Concurrent Bursts  |       |  - CRMTwin            |
                       |  - Schema Mutators    |       |  - LedgerTwin         |
                       |  - HMAC-SHA256 Signer |       |  - NotificationTwin   |
                       +-----------+-----------+       |  - DeadLetterTwin     |
                                   |                   |  - FaultEngine        |
                                   | 2. Signed HTTP    |    (429/503/504/Jitter)|
                                   |    Webhooks       |  - FlightRecorder     |
                                   v                   +-----------+-----------+
                       +-----------------------+                   ^
                       |  System Under Test    |-------------------+
                       |  (Integration /       | 3. Downstream API Calls
                       |   Webhook Pipeline)   |    (Idempotency-Key, Retries)
                       +-----------------------+
```

### Core Components

1. **Ingress Chaos Producer (`bridgetest.chaos`)**:
   - **Duplicate Replay Storms**: Dispatches $N\times$ deliveries per logical event either sequentially with configurable delay or concurrently via a thread pool to expose race conditions in non-atomic deduplication checks.
   - **Contract & Schema Drift Mutators**: Deterministically mutates selected events using `stringify_numbers`, `rename_field`, `drop_required_field`, `inject_extra_fields`, `nullify_optional_field`, and `tamper_signature`.
   - **Cryptographic Signing**: Signs payloads with HMAC-SHA256 (`X-BridgeTest-Signature: t=<ts>,v1=<hex>`) and supports signature tampering to verify `401 Unauthorized` boundary enforcement.
2. **Stateful Downstream Digital Twins (`bridgetest.twins`)**:
   - Provides thread-safe, schema-validating in-memory and HTTP digital twins for **CRM** (`/crm/contacts`), **Payment Ledger** (`/ledger/entries`), **Notification Sink** (`/notify/send`), and **Dead-Letter Queue** (`/dlq/events`).
   - Built-in support for `Idempotency-Key` header deduplication (`201 Created` on first write, `200 OK` with `"status": "idempotent_replay"` on subsequent writes).
   - Can run in-process for sub-100ms CI test suites or spin up an ephemeral multi-threaded HTTP server (`127.0.0.1:<port>`) to test external services, Docker containers, or n8n/Make workflows.
3. **Deterministic & Probabilistic Fault Engine (`bridgetest.faults`)**:
   - Injects `rate_limit_429` (with `Retry-After` headers), `transient_503`, `gateway_timeout_504`, `network_jitter`, `partial_outage`, and `corrupt_json` responses.
   - Tracks failure counts per `(rule_id, service, idempotency_key)` tuple (`fail_first_n_per_key`), enabling deterministic verification of exponential backoff recovery.
4. **Causal Flight Recorder (`bridgetest.journal`)**:
   - Captures a monotonic, thread-safe trace of every inbound webhook and downstream twin interaction (`seq_id`, `correlation_id`, `idempotency_key`, `attempt_index`, `latency_ms`, `fault_injected`, and inter-attempt retry intervals).
5. **Business Outcome Invariant Engine (`bridgetest.invariants`)**:
   - Evaluates six formal distributed state invariants after quiescence:
     - `exact_cardinality`: Verifies exact record counts and zero duplicate records per `external_event_id`.
     - `cross_system_consistency`: Detects split-brain states and field-level drift (`amount_cents`, `customer_email`) across CRM, Ledger, and Notification twins.
     - `idempotency_enforcement`: Audits every downstream write in the causal journal for `Idempotency-Key` presence and zero duplicate side effects.
     - `retry_backoff_compliance`: Verifies that every transient `429`/`5xx` fault is retried until recovery and respects minimum backoff intervals.
     - `poison_pill_containment`: Verifies that unrecoverable schema-drifted payloads are isolated in the `DeadLetterTwin` (`dlq`) and tampered signatures are rejected with `HTTP 401`.
     - `zero_silent_swallow`: Enforces strict event conservation—every unique inbound event must either be completely reconciled across all target twins or explicitly recorded in the DLQ.

## Repository Structure

```text
bridgetest/
├── .github/workflows/ci.yml           # GitHub Actions CI matrix (Python 3.10, 3.11, 3.12)
├── pyproject.toml                     # Standard PEP 621 packaging & CLI entry point
├── setup.py                           # Editable installation support
├── requirements.txt                   # Runtime dependency declaration (PyYAML)
├── LICENSE                            # MIT License
├── README.md                          # Architecture & usage documentation
├── src/bridgetest/
│   ├── __init__.py                    # Public API exports
│   ├── models.py                      # Dataclasses & enums for scenarios, faults, and reports
│   ├── journal.py                     # Thread-safe FlightRecorder & retry interval analyzer
│   ├── faults.py                      # FaultEngine (429, 503, 504, jitter, corrupt JSON)
│   ├── twins.py                       # Stateful CRM, Ledger, Notification & DLQ HTTP Twins
│   ├── chaos.py                       # ChaosProducer, schema drift mutators & HMAC signing
│   ├── invariants.py                  # 6 business outcome & eventual consistency evaluators
│   ├── runner.py                      # ScenarioRunner & YAML/JSON loader
│   ├── reporter.py                    # Terminal table, Markdown, and JSON report formatters
│   ├── cli.py                         # CLI (`bridgetest demo`, `run`, `list-scenarios`)
│   └── demo/
│       ├── __init__.py
│       ├── pipelines.py               # NaiveWebhookPipeline vs. ResilientOutboxPipeline
│       └── scenarios.py               # Standard 6-scenario chaos verification suite
├── examples/
│   ├── run_demo.py                    # Standalone zero-install comparative demo runner
│   └── scenarios/
│       ├── happy_path_baseline.yaml   # Declarative YAML baseline scenario
│       └── order_sync_chaos.yaml      # Declarative YAML multi-fault chaos scenario
└── tests/
    ├── test_faults_and_journal.py     # Unit tests for FlightRecorder & FaultEngine
    ├── test_twins_and_chaos.py        # Unit tests for TwinCluster HTTP server & ChaosProducer
    ├── test_invariants.py             # Unit tests for split-brain & silent swallow detection
    └── test_runner_and_cli.py         # End-to-end integration tests for Runner & CLI
```

## Quick Start & Installation

### 1. Install in Editable Mode

```bash
git clone https://github.com/sismax01/BridgeTest.git
cd BridgeTest
python -m pip install -e ".[dev]"
```

### 2. Run the Automated Test Suite

BridgeTest's test suite uses standard library `unittest` (and is also fully compatible with `pytest`), running 11 unit and end-to-end integration tests in ~2.2 seconds:

```bash
python3 -m unittest discover -s tests -v
```

### 3. Run the End-to-End Comparative Demo

Execute the built-in 6-scenario chaos suite comparing `NaiveWebhookPipeline` (a typical happy-path integration) against `ResilientOutboxPipeline` (hardened with HMAC verification, per-key idempotency locks, tolerant schema normalization, `Retry-After` exponential backoff, and DLQ routing):

```bash
bridgetest demo
# Or run directly without installation:
python3 examples/run_demo.py
```

### 4. Run a Declarative YAML Scenario Against a Target Pipeline

```bash
# Run the chaos gauntlet YAML scenario against the resilient reference pipeline
bridgetest run examples/scenarios/order_sync_chaos.yaml --target resilient

# Run the exact same scenario against the naive pipeline to observe failure diagnostics
bridgetest run examples/scenarios/order_sync_chaos.yaml --target naive

# Point BridgeTest at any external HTTP webhook endpoint (e.g., FastAPI, Node.js, or n8n)
bridgetest run examples/scenarios/order_sync_chaos.yaml --target http://127.0.0.1:8080/webhooks/orders
```

## Sample Output (`bridgetest demo`)

When running `bridgetest demo`, BridgeTest executes all six scenarios against both pipelines and outputs per-invariant diagnostics followed by a side-by-side scorecard:

```text
[FAIL] Scenario: Concurrent 3x Duplicate Webhook Storm (02_duplicate_storm)
       Target: NaiveWebhookPipeline | Duration: 29.1ms | Events: 4 logical (12 deliveries) | Downstream Calls: 36 | Faults Injected: 0
       Final Twin State: CRM=12, LEDGER=12, NOTIFICATION=12, DLQ=0
         - [FAIL] inv_cardinality (exact_cardinality): crm=12, ledger=12, notification=12
             ! Violation: Service 'crm' persisted 12 records, expected 4
             ! Violation: Service 'crm' contains duplicate records for event_ids: {'evt_dup_01': 3, 'evt_dup_02': 3, 'evt_dup_03': 3, 'evt_dup_04': 3}
             ! Violation: Service 'ledger' persisted 12 records, expected 4
         - [FAIL] inv_idempotency (idempotency_enforcement): 36 downstream calls checked; 36 missing keys, 12 duplicate side effects
             ! Violation: Seq #1 POST /crm/contacts (corr=corr_evt_dup_01) missing Idempotency-Key header

[FAIL] Scenario: Mid-Workflow Ledger & Notification 503 Outage (04_partial_cascade_503)
       Target: NaiveWebhookPipeline | Duration: 16.8ms | Events: 3 logical (3 deliveries) | Downstream Calls: 6 | Faults Injected: 3
       Final Twin State: CRM=3, LEDGER=0, NOTIFICATION=0, DLQ=0
         - [FAIL] inv_consistency (cross_system_consistency): 3 events inspected; 3 cross-system drift violations
             ! Violation: Split-brain state for event 'evt_part503_01': crm=True, ledger=False, notification=False
             ! Violation: Split-brain state for event 'evt_part503_02': crm=True, ledger=False, notification=False
             ! Violation: Split-brain state for event 'evt_part503_03': crm=True, ledger=False, notification=False

[PASS] Scenario: Full Production Chaos Gauntlet (Combined Stress) (06_full_chaos_gauntlet)
       Target: ResilientOutboxPipeline | Duration: 86.0ms | Events: 5 logical (10 deliveries) | Downstream Calls: 21 | Faults Injected: 8
       Final Twin State: CRM=4, LEDGER=4, NOTIFICATION=4, DLQ=1
         - [PASS] inv_cardinality (exact_cardinality): crm=4, ledger=4, notification=4
         - [PASS] inv_consistency (cross_system_consistency): 4 events inspected; 0 cross-system drift violations
         - [PASS] inv_idempotency (idempotency_enforcement): 21 downstream calls checked; 0 missing keys, 0 duplicate side effects
         - [PASS] inv_backoff (retry_backoff_compliance): 8 transient faults injected; 8 workflows recovered; 0 retry/backoff violations
         - [PASS] inv_poison_containment (poison_pill_containment): 1/1 poison events properly contained
         - [PASS] inv_no_swallow (zero_silent_swallow): 5/5 events accounted for; 0 silently swallowed

3. COMPARATIVE SCORECARD (NAIVE VS. RESILIENT):
---------------------------------------------------------------------------------------------------
Scenario ID                | Chaos Injected               | Naive Pipeline     | Resilient Pipeline
---------------------------------------------------------------------------------------------------
01_happy_path              | None (Baseline)              | PASS (3/3)         | PASS (3/3)        
02_duplicate_storm         | 3x Concurrent Duplicate Burst | FAIL (1/3)         | PASS (3/3)        
03_rate_limit_429          | CRM HTTP 429 + Retry-After   | FAIL (0/3)         | PASS (3/3)        
04_partial_cascade_503     | Ledger/Notify HTTP 503 Outage | FAIL (0/3)         | PASS (3/3)        
05_schema_drift_poison     | Type Coercion + Poison + HMAC | FAIL (0/3)         | PASS (3/3)        
06_full_chaos_gauntlet     | Duplicates + 429 + 503 + Drift | FAIL (0/6)         | PASS (6/6)        
---------------------------------------------------------------------------------------------------

Summary: Naive Pipeline passed 1/6 scenarios | Resilient Pipeline passed 6/6 scenarios.
```

## Defining Custom Chaos Scenarios in YAML

Scenarios combine logical webhook events, a `chaos` profile (inbound mutations + downstream twin fault rules), and declarative `invariants`:

```yaml
scenario_id: "custom_crm_resilience"
name: "CRM Rate-Limit & Duplicate Burst Verification"
description: "Verifies that 2x concurrent webhook deliveries under CRM HTTP 429 rate limits produce zero duplicates and zero dropped orders."
quiescence_wait_ms: 20

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
