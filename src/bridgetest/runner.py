"""Scenario Runner orchestrating chaos dispatch, digital twin fault rules, and invariant verification."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

import yaml

from bridgetest.chaos import ChaosProducer
from bridgetest.invariants import InvariantEngine
from bridgetest.models import ScenarioResult, ScenarioSpec
from bridgetest.twins import TwinCluster

WebhookHandlerFn = Callable[[Dict[str, str], bytes], Tuple[int, Dict[str, Any]]]


def make_http_webhook_handler(webhook_url: str, timeout_seconds: float = 10.0) -> WebhookHandlerFn:
    """Create a target handler function that dispatches signed webhooks to an external HTTP endpoint."""

    def _handler(headers: Dict[str, str], raw_body: bytes) -> Tuple[int, Dict[str, Any]]:
        req = urllib.request.Request(webhook_url, data=raw_body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                body_bytes = resp.read()
                parsed = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
                return resp.status, parsed
        except urllib.error.HTTPError as exc:
            err_bytes = exc.read()
            try:
                parsed = json.loads(err_bytes.decode("utf-8")) if err_bytes else {"error": str(exc)}
            except Exception:
                parsed = {"error": err_bytes.decode("utf-8", errors="replace")}
            return exc.code, parsed
        except Exception as exc:
            return 599, {"error": "transport_exception", "message": str(exc)}

    return _handler


class ScenarioRunner:
    """Executes BridgeTest chaos scenarios against in-process or HTTP webhook pipelines."""

    def __init__(self, cluster: Optional[TwinCluster] = None) -> None:
        self.cluster = cluster or TwinCluster()
        self.invariant_engine = InvariantEngine()

    @staticmethod
    def load_scenario_file(path: Union[str, Path]) -> ScenarioSpec:
        """Load a ScenarioSpec from a YAML or JSON file on disk."""
        file_path = Path(path)
        raw_text = file_path.read_text(encoding="utf-8")
        if file_path.suffix.lower() in (".yaml", ".yml"):
            parsed = yaml.safe_load(raw_text)
        else:
            parsed = json.loads(raw_text)
        return ScenarioSpec.from_dict(parsed)

    def run_scenario(
        self,
        scenario: ScenarioSpec,
        target_handler: WebhookHandlerFn,
        target_name: str = "TargetPipeline",
        reset_callback: Optional[Callable[[], None]] = None,
    ) -> ScenarioResult:
        """Execute a single chaos scenario end-to-end and return its structured verification report."""
        self.cluster.reset(fault_rules=scenario.chaos.fault_rules, seed=42)
        if reset_callback is not None:
            reset_callback()

        producer = ChaosProducer(profile=scenario.chaos, recorder=self.cluster.recorder)
        start_ts = time.perf_counter()

        prepared_events, deliveries = producer.dispatch_all(scenario.events, target_handler)

        if scenario.quiescence_wait_ms > 0:
            time.sleep(scenario.quiescence_wait_ms / 1000.0)

        duration_ms = (time.perf_counter() - start_ts) * 1000.0
        invariant_results = self.invariant_engine.evaluate_all(
            scenario=scenario,
            prepared_events=prepared_events,
            cluster=self.cluster,
        )
        all_passed = all(r.passed for r in invariant_results)
        journal_summary = self.cluster.recorder.summary()

        return ScenarioResult(
            scenario_id=scenario.scenario_id,
            scenario_name=scenario.name,
            target_name=target_name,
            passed=all_passed,
            duration_ms=duration_ms,
            total_logical_events=len(prepared_events),
            total_inbound_deliveries=len(deliveries),
            total_downstream_calls=journal_summary["downstream_calls"],
            total_faults_injected=journal_summary["faults_injected"],
            twin_state_counts=self.cluster.state_counts(),
            invariant_results=invariant_results,
            journal_summary=journal_summary,
        )
