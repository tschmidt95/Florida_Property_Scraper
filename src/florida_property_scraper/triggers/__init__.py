"""Trigger / alert engine (framework).

This subsystem is intentionally separate from polygon search; it is designed to be
fed by periodic connector polling and queried on-demand by parcel.
"""

from .taxonomy import TriggerKey

__all__ = ["TriggerKey"]
