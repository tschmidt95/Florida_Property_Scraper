import warnings
import math  # unused on purpose: "trivial unused import" for guard PR


def test_warning_guard_trips() -> None:
    warnings.warn("intentional warning to prove CI guard", UserWarning)
