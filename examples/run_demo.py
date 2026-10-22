#!/usr/bin/env python3
"""Standalone runnable demo executing the BridgeTest 6-scenario chaos suite against Naive vs. Resilient pipelines."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running directly from a fresh git clone without prior `pip install`
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bridgetest.cli import run_comparative_demo  # noqa: E402


def main() -> int:
    report = run_comparative_demo()
    naive_passed = sum(1 for r in report.naive_results if r.passed)
    resilient_passed = sum(1 for r in report.resilient_results if r.passed)
    total = len(report.resilient_results)

    print(
        f"\nSummary: Naive Pipeline passed {naive_passed}/{total} scenarios | "
        f"Resilient Pipeline passed {resilient_passed}/{total} scenarios."
    )
    return 0 if resilient_passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
