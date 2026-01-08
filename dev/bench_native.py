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


def run_bench(counties, max_items):
    adapter = NativeAdapter()
    total_items = 0
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
        total_items += len(items)
    elapsed = time.perf_counter() - start
    rate = total_items / elapsed if elapsed else 0.0
    print(f"Counties: {len(counties)} Items: {total_items} Time: {elapsed:.4f}s Items/sec: {rate:.2f}")


def main():
    parser = argparse.ArgumentParser(description="Native backend fixture benchmark")
    parser.add_argument("--max-items", type=int, default=50)
    parser.add_argument("--counties", default=",".join(COUNTIES))
    args = parser.parse_args()

    counties = [c.strip() for c in args.counties.split(",") if c.strip()]
    if os.environ.get("PERF") == "1":
        profiler = cProfile.Profile()
        profiler.enable()
        run_bench(counties, args.max_items)
        profiler.disable()
        stats_path = Path("dev") / "bench_native.prof"
        profiler.dump_stats(stats_path)
        stats = pstats.Stats(profiler).strip_dirs().sort_stats("cumtime")
        stats.print_stats(10)
        print(f"Profile saved to {stats_path}")
    else:
        run_bench(counties, args.max_items)


if __name__ == "__main__":
    main()
