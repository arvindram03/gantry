# SPDX-License-Identifier: Apache-2.0
"""The one error a malformed policy raises."""

from __future__ import annotations


class PolicyConfigurationError(ValueError):
    """A policy that cannot mean anything, raised where it is written.

    A policy is trusted configuration, so the useful time to reject it is at
    construction — while the author is looking at it — rather than at admission,
    where the agent sees a refusal it cannot act on and the operator sees a
    denial that was really a typo.
    """
