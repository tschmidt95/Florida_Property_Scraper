# Perf Notes

## Benchmarks

- Baseline tests: `python3 -m pytest -q`
- Slowest tests: `python3 -m pytest -q --durations=10`
- Native fixture bench: `python3 dev/bench_native.py`

## PERF mode

Set `PERF=1` to emit a cProfile report for the native benchmark:

```
PERF=1 python3 dev/bench_native.py
```

This writes `dev/bench_native.prof` and prints top cumulative functions.
