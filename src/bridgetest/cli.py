"""Command-line interface for BridgeTest (`bridgetest demo`, `bridgetest run`, `bridgetest list-scenarios`)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from bridgetest.demo.pipelines import NaiveWebhookPipeline, ResilientOutboxPipeline
from bridgetest.demo.scenarios import build_standard_chaos_suite
from bridgetest.models import ComparisonReport, ScenarioResult
from bridgetest.reporter import BridgeTestReporter
from bridgetest.runner import ScenarioRunner, make_http_webhook_handler
from bridgetest.twins import TwinCluster


def run_comparative_demo(
    json_out: Optional[str] = None,
    markdown_out: Optional[str] = None,
    quiet: bool = False,
) -> ComparisonReport:
    """Execute the full 6-scenario chaos suite against both Naive and Resilient pipelines."""
    suite = build_standard_chaos_suite()
    naive_results: List[ScenarioResult] = []
    resilient_results: List[ScenarioResult] = []

    with TwinCluster() as cluster:
        runner = ScenarioRunner(cluster=cluster)
        naive_pipeline = NaiveWebhookPipeline(twin_cluster=cluster)
        resilient_pipeline = ResilientOutboxPipeline(twin_cluster=cluster)

        for scenario in suite:
            n_res = runner.run_scenario(
                scenario=scenario,
                target_handler=naive_pipeline.handle_webhook,
                target_name="NaiveWebhookPipeline",
                reset_callback=naive_pipeline.reset,
            )
            naive_results.append(n_res)

            r_res = runner.run_scenario(
                scenario=scenario,
                target_handler=resilient_pipeline.handle_webhook,
                target_name="ResilientOutboxPipeline",
                reset_callback=resilient_pipeline.reset,
            )
            resilient_results.append(r_res)

    report = ComparisonReport(naive_results=naive_results, resilient_results=resilient_results)

    if not quiet:
        print("=" * 98)
        print(" BRIDGETEST — INTEGRATION CHAOS & END-TO-END STATE VERIFICATION HARNESS")
        print("=" * 98)
        print("\n1. NAIVE PIPELINE EXECUTION (Happy-Path Only Implementation):")
        print("-" * 98)
        for res in naive_results:
            print(BridgeTestReporter.format_scenario_text(res))
            print()

        print("2. RESILIENT OUTBOX PIPELINE EXECUTION (Idempotent + Retry Backoff + DLQ):")
        print("-" * 98)
        for res in resilient_results:
            print(BridgeTestReporter.format_scenario_text(res))
            print()

        print("3. COMPARATIVE SCORECARD (NAIVE VS. RESILIENT):")
        print(BridgeTestReporter.format_comparison_table(report))

    if json_out:
        Path(json_out).write_text(BridgeTestReporter.to_json(report), encoding="utf-8")
    if markdown_out:
        Path(markdown_out).write_text(BridgeTestReporter.to_markdown(report), encoding="utf-8")

    return report


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for the `bridgetest` CLI."""
    parser = argparse.ArgumentParser(
        prog="bridgetest",
        description="BridgeTest: Chaos Engineering & End-to-End State Verification for Integration Pipelines",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_parser = subparsers.add_parser(
        "demo",
        help="Run the built-in comparative chaos suite against Naive vs. Resilient reference pipelines",
    )
    demo_parser.add_argument("--json-out", type=str, default=None, help="Path to write JSON scorecard")
    demo_parser.add_argument("--markdown-out", type=str, default=None, help="Path to write Markdown report")
    demo_parser.add_argument("--quiet", action="store_true", help="Suppress stdout report")

    run_parser = subparsers.add_parser(
        "run",
        help="Execute a YAML/JSON scenario file against a pipeline target",
    )
    run_parser.add_argument("scenario_file", type=str, help="Path to YAML or JSON scenario definition")
    run_parser.add_argument(
        "--target",
        type=str,
        default="resilient",
        help="Target pipeline: 'resilient', 'naive', or an external HTTP webhook URL (http://...)",
    )
    run_parser.add_argument("--json-out", type=str, default=None, help="Optional JSON output file path")

    list_parser = subparsers.add_parser(
        "list-scenarios",
        help="List all built-in chaos verification scenarios",
    )
    list_parser.add_argument("--format", choices=["text", "json"], default="text")

    args = parser.parse_args(argv)

    if args.command == "demo":
        report = run_comparative_demo(
            json_out=args.json_out,
            markdown_out=args.markdown_out,
            quiet=args.quiet,
        )
        all_resilient_passed = all(r.passed for r in report.resilient_results)
        return 0 if all_resilient_passed else 1

    if args.command == "list-scenarios":
        suite = build_standard_chaos_suite()
        if args.format == "json":
            print(json.dumps([s.to_dict() for s in suite], indent=2))
        else:
            for s in suite:
                print(f"{s.scenario_id:<26} | {s.name:<38} | {len(s.invariants)} invariants")
        return 0

    if args.command == "run":
        scenario = ScenarioRunner.load_scenario_file(args.scenario_file)
        with TwinCluster() as cluster:
            runner = ScenarioRunner(cluster=cluster)
            if args.target == "resilient":
                pipe = ResilientOutboxPipeline(twin_cluster=cluster)
                result = runner.run_scenario(
                    scenario, pipe.handle_webhook, target_name="ResilientOutboxPipeline", reset_callback=pipe.reset
                )
            elif args.target == "naive":
                pipe_n = NaiveWebhookPipeline(twin_cluster=cluster)
                result = runner.run_scenario(
                    scenario, pipe_n.handle_webhook, target_name="NaiveWebhookPipeline", reset_callback=pipe_n.reset
                )
            elif args.target.startswith("http://") or args.target.startswith("https://"):
                handler = make_http_webhook_handler(args.target)
                result = runner.run_scenario(scenario, handler, target_name=args.target)
            else:
                print(f"Error: Unknown target '{args.target}'. Use 'resilient', 'naive', or an HTTP URL.", file=sys.stderr)
                return 2

            print(BridgeTestReporter.format_scenario_text(result))
            if args.json_out:
                Path(args.json_out).write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
            return 0 if result.passed else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
