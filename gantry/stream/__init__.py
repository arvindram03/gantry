# SPDX-License-Identifier: Apache-2.0
"""Streaming execution providers."""

from gantry.stream.api import StreamConnection, connect
from gantry.stream.capabilities import StreamCapabilities

__all__ = ["StreamCapabilities", "StreamConnection", "connect"]
