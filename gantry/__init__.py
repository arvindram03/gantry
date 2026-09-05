# SPDX-License-Identifier: Apache-2.0
"""Gantry — reliability and execution layer for data movement and analysis.

The four core resources are Dataset, Movement, Analysis and Result. Movement and
Analysis are both Operations over Datasets, and both traverse one lifecycle:

    Plan -> Generate -> Validate -> Execute -> Verify -> Result

See docs/execution-plan-v1.md for the v1 build order.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
