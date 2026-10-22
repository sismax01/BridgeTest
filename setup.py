"""Editable and legacy installation support for BridgeTest."""

from setuptools import find_packages, setup

setup(
    name="bridgetest",
    version="1.0.0",
    description="Chaos Engineering & End-to-End State Verification Framework for Integration Pipelines",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "PyYAML>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "bridgetest=bridgetest.cli:main",
        ],
    },
)
