"""Terminal, Markdown, and JSON reporting utilities for BridgeTest scenario runs."""

from __future__ import annotations

import json
from typing import List

from bridgetest.models import ComparisonReport, ScenarioResult


class BridgeTestReporter:
    """Formats scenario results and pipeline comparison reports for CLI output, CI logs, and Markdown."""

    @staticmethod
    def format_scenario_text(result: ScenarioResult) -> str:
        """Render a clean, deterministic ASCII/UTF-8 report for a single ScenarioResult."""
        status_badge = "PASS" if result.passed else "FAIL"
        lines: List[str] = []
        lines.append(f"[{status_badge}] Scenario: {result.scenario_name} ({result.scenario_id})")
        lines.append(
            f"       Target: {result.target_name} | Duration: {result.duration_ms:.1f}ms | "
            f"Events: {result.total_logical_events} logical ({result.total_inbound_deliveries} deliveries) | "
            f"Downstream Calls: {result.total_downstream_calls} | Faults Injected: {result.total_faults_injected}"
        )
        counts_str = ", ".join(f"{k.upper()}={v}" for k, v in result.twin_state_counts.items())
        lines.append(f"       Final Twin State: {counts_str}")

        for inv in result.invariant_results:
            inv_badge = "PASS" if inv.passed else "FAIL"
            lines.append(
                f"         - [{inv_badge}] {inv.invariant_id} ({inv.invariant_type.value}): {inv.actual_summary}"
            )
            if not inv.passed:
                for violation in inv.violations[:3]:
                    lines.append(f"             ! Violation: {violation}")
        return "\n".join(lines)

    @staticmethod
    def format_comparison_table(report: ComparisonReport) -> str:
        """Render a side-by-side comparison scorecard between Naive and Resilient pipelines."""
        lines: List[str] = []
        header = (
            f"{'Scenario ID':<26} | {'Chaos Injected':<28} | "
            f"{'Naive Pipeline':<18} | {'Resilient Pipeline':<18}"
        )
        sep = "-" * len(header)
        lines.append(sep)
        lines.append(header)
        lines.append(sep)

        chaos_labels = {
            "01_happy_path": "None (Baseline)",
            "02_duplicate_storm": "3x Concurrent Duplicate Burst",
            "03_rate_limit_429": "CRM HTTP 429 + Retry-After",
            "04_partial_cascade_503": "Ledger/Notify HTTP 503 Outage",
            "05_schema_drift_poison": "Type Coercion + Poison + HMAC",
            "06_full_chaos_gauntlet": "Duplicates + 429 + 503 + Drift",
        }

        for naive, resilient in zip(report.naive_results, report.resilient_results):
            n_status = f"PASS ({naive.passed_invariants_count}/{naive.total_invariants_count})" if naive.passed else f"FAIL ({naive.passed_invariants_count}/{naive.total_invariants_count})"
            r_status = f"PASS ({resilient.passed_invariants_count}/{resilient.total_invariants_count})" if resilient.passed else f"FAIL ({resilient.passed_invariants_count}/{resilient.total_invariants_count})"
            chaos_desc = chaos_labels.get(naive.scenario_id, f"{naive.total_faults_injected} faults")
            lines.append(
                f"{naive.scenario_id:<26} | {chaos_desc:<28} | {n_status:<18} | {r_status:<18}"
            )
        lines.append(sep)
        return "\n".join(lines)

    @staticmethod
    def to_markdown(report: ComparisonReport) -> str:
        """Render a complete Markdown verification report suitable for GitHub Actions job summaries."""
        md: List[str] = []
        md.append("# BridgeTest Chaos Verification Report")
        md.append("")
        md.append("## Executive Scorecard: Naive vs. Resilient Integration Pipeline")
        md.append("")
        md.append("| Scenario ID | Scenario Name | Inbound Deliveries | Faults Injected | Naive Pipeline | Resilient Pipeline |")
        md.append("| :--- | :--- | :---: | :---: | :---: | :---: |")

        for naive, resilient in zip(report.naive_results, report.resilient_results):
            n_badge = f"PASS ({naive.passed_invariants_count}/{naive.total_invariants_count})" if naive.passed else f"FAIL ({naive.passed_invariants_count}/{naive.total_invariants_count})"
            r_badge = f"PASS ({resilient.passed_invariants_count}/{resilient.total_invariants_count})" if resilient.passed else f"FAIL ({resilient.passed_invariants_count}/{resilient.total_invariants_count})"
            md.append(
                f"| `{naive.scenario_id}` | {naive.scenario_name} | {resilient.total_inbound_deliveries} | "
                f"{resilient.total_faults_injected} | **{n_badge}** | **{r_badge}** |"
            )

        md.append("")
        md.append("## Detailed Failure Diagnostics Captured on Naive Pipeline")
        md.append("")
        for naive in report.naive_results:
            if naive.passed:
                continue
            md.append(f"### `{naive.scenario_id}` — {naive.scenario_name}")
            md.append(f"- **Final Twin State**: `{naive.twin_state_counts}`")
            for inv in naive.invariant_results:
                if not inv.passed:
                    md.append(f"- **Failed Invariant `{inv.invariant_id}` (`{inv.invariant_type.value}`)**:")
                    for v in inv.violations:
                        md.append(f"  - `{v}`")
            md.append("")
        return "\n".join(md)

    @staticmethod
    def to_json(report: ComparisonReport, indent: int = 2) -> str:
        """Serialize a ComparisonReport to formatted JSON."""
        return json.dumps(report.to_dict(), indent=indent)
