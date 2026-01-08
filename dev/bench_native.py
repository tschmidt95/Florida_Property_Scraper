import argparse
import cProfile
import os
import pstats
import time
from pathlib import Path

from florida_property_scraper.backend.native_adapter import NativeAdapter


COUNTIES = [
    "broward",
    "alachua",
    "orange",
    "palm_beach",
    "seminole",
]


def find_fixture(county):
    candidates = [
        Path("tests/fixtures") / f"{county}_realistic.html",
        Path("tests/fixtures") / f"{county}_sample.html",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def run_bench(counties, max_items, iterations=1):
    adapter = NativeAdapter()
    durations = []
    total_items = 0
    for _ in range(iterations):
        run_items = 0
        start = time.perf_counter()
        for county in counties:
            fixture = find_fixture(county)
            if not fixture:
                continue
            items = adapter.search(
                query="Smith",
                start_urls=[f"file://{fixture.resolve()}"],
                spider_name=f"{county}_spider",
                county_slug=county,
                max_items=max_items,
                live=False,
            )
            run_items += len(items)
        elapsed = time.perf_counter() - start
        durations.append(elapsed)
        total_items += run_items
    total_time = sum(durations)
    rate = total_items / total_time if total_time else 0.0
    print(f"Counties: {len(counties)} Items: {total_items} Time: {total_time:.4f}s Items/sec: {rate:.2f}")
    if iterations > 1:
        sorted_times = sorted(durations)
        p50 = sorted_times[int(0.50 * (len(sorted_times) - 1))]
        p95 = sorted_times[int(0.95 * (len(sorted_times) - 1))]
        print(f"p50: {p50:.4f}s p95: {p95:.4f}s iterations: {iterations}")


def main():
    parser = argparse.ArgumentParser(description="Native backend fixture benchmark")
    parser.add_argument("--max-items", type=int, default=50)
    parser.add_argument("--counties", default=",".join(COUNTIES))
    parser.add_argument("--iterations", type=int, default=1)
    args = parser.parse_args()

    counties = [c.strip() for c in args.counties.split(",") if c.strip()]
    if os.environ.get("PERF") == "1":
        profiler = cProfile.Profile()
        profiler.enable()
        run_bench(counties, args.max_items, iterations=args.iterations)
        profiler.disable()
        stats_path = Path("dev") / "bench_native.prof"
        profiler.dump_stats(stats_path)
        stats = pstats.Stats(profiler).strip_dirs().sort_stats("cumtime")
        stats.print_stats(10)
        print(f"Profile saved to {stats_path}")
    else:
        run_bench(counties, args.max_items, iterations=args.iterations)


if __name__ == "__main__":
    main()
