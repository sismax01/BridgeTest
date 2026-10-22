"""Built-in reference integration pipelines and chaos verification scenarios for BridgeTest."""

from bridgetest.demo.pipelines import NaiveWebhookPipeline, ResilientOutboxPipeline
from bridgetest.demo.scenarios import build_standard_chaos_suite

__all__ = [
    "NaiveWebhookPipeline",
    "ResilientOutboxPipeline",
    "build_standard_chaos_suite",
]
