import warnings


def test_warning_guard_trips() -> None:
    warnings.warn("intentional warning to prove CI guard", UserWarning)
