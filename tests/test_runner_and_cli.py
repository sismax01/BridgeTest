"""End-to-end integration tests for ScenarioRunner, YAML scenario loading, and CLI commands."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bridgetest.cli import main as cli_main  # noqa: E402
from bridgetest.cli import run_comparative_demo  # noqa: E402
from bridgetest.demo.pipelines import (  # noqa: E402
    NaiveWebhookPipeline,
    ResilientOutboxPipeline,
)
from bridgetest.runner import ScenarioRunner  # noqa: E402
from bridgetest.twins import TwinCluster  # noqa: E402


class TestRunnerAndCLI(unittest.TestCase):
    def test_comparative_demo_naive_fails_chaos_while_resilient_passes_all(self) -> None:
        report = run_comparative_demo(quiet=True)
        self.assertEqual(len(report.naive_results), 6)
        self.assertEqual(len(report.resilient_results), 6)

        # Scenario 01 (Happy Path) should pass on BOTH Naive and Resilient pipelines!
        self.assertTrue(report.naive_results[0].passed)
        self.assertTrue(report.resilient_results[0].passed)

        # Scenarios 02..06 (Chaos scenarios) MUST fail on NaiveWebhookPipeline
        for naive_chaos_res in report.naive_results[1:]:
            self.assertFalse(
                naive_chaos_res.passed,
                f"Expected NaiveWebhookPipeline to fail chaos scenario {naive_chaos_res.scenario_id}",
            )

        # All 6 scenarios MUST pass 100% of invariants on ResilientOutboxPipeline
        for resilient_res in report.resilient_results:
            self.assertTrue(
                resilient_res.passed,
                f"Expected ResilientOutboxPipeline to pass {resilient_res.scenario_id}, "
                f"violations: {[v.violations for v in resilient_res.invariant_results if not v.passed]}",
            )

    def test_yaml_scenario_loading_and_cli_execution(self) -> None:
        yaml_path = REPO_ROOT / "examples" / "scenarios" / "order_sync_chaos.yaml"
        scenario = ScenarioRunner.load_scenario_file(yaml_path)
        self.assertEqual(scenario.scenario_id, "06_full_chaos_gauntlet")

        cluster = TwinCluster()
        runner = ScenarioRunner(cluster=cluster)
        resilient = ResilientOutboxPipeline(twin_cluster=cluster)
        naive = NaiveWebhookPipeline(twin_cluster=cluster)

        res_pass = runner.run_scenario(scenario, resilient.handle_webhook, target_name="Resilient")
        self.assertTrue(res_pass.passed)

        res_fail = runner.run_scenario(scenario, naive.handle_webhook, target_name="Naive")
        self.assertFalse(res_fail.passed)

        with tempfile.TemporaryDirectory() as tmpdir:
            json_out = Path(tmpdir) / "result.json"
            exit_code = cli_main(["run", str(yaml_path), "--target", "resilient", "--json-out", str(json_out)])
            self.assertEqual(exit_code, 0)
            parsed = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertTrue(parsed["passed"])
            self.assertEqual(parsed["passed_invariants"], 6)


if __name__ == "__main__":
    unittest.main()
