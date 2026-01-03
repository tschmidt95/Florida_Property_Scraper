"""Simple Scrapy adapter that runs the Scrapy runner subprocess.

This implementation prefers running a separate subprocess (via
`scrapy_runner`) to avoid Twisted reactor reuse issues when running
multiple crawls in the same process (e.g., during tests).
"""

from typing import List, Dict, Any, Optional
import json
import subprocess
import sys
import time


class InMemoryPipeline:
    """Pipeline used by the runner subprocess for collecting items in memory.

    The subprocess can set `InMemoryPipeline.items_list` prior to running the
    crawl. The pipeline will append dictified items to that list so the
    subprocess can emit the aggregated results as JSON to stdout.
    """

    items_list = None

    @classmethod
    def from_crawler(cls, crawler):
        inst = cls()
        inst.items = cls.items_list if cls.items_list is not None else []
        return inst

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item


class ScrapyAdapter:
    def __init__(self, demo: bool = False, timeout: Optional[int] = None):
        self.demo = demo
        self.timeout = timeout

    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Run a search and return a list of result dicts.

        For demo mode this returns a deterministic fixture. For non-demo runs we
        invoke the `scrapy_runner` module in a subprocess which prints a JSON
        array of items to stdout.
        """
        if self.demo:
            return [{"address": "123 Demo St", "owner": "Demo Owner", "notes": "demo fixture"}]

        start_urls = kwargs.get("start_urls")
        spider_name = kwargs.get("spider_name") or ""
        if not start_urls:
            return []

        runner_cmd = [
            sys.executable,
            "-m",
            "florida_property_scraper.backend.scrapy_runner",
            "--spider-name",
            spider_name,
            "--start-urls",
            json.dumps(start_urls),
        ]

        MAX_RETRIES = 3
        delay = 0.05
        last_stdout = ""
        last_stderr = ""
        proc = None

        for attempt in range(1, MAX_RETRIES + 1):
            proc = subprocess.run(runner_cmd, capture_output=True, text=True)
            stdout = proc.stdout.strip()
            stderr = proc.stderr.strip()

            # Attempt to parse JSON output
            items = None
            try:
                if stdout:
                    items = json.loads(stdout)
            except Exception:
                items = None

            if isinstance(items, list) and items:
                return items

            # If the runner returned an error payload, stop retrying
            try:
                payload = json.loads(stdout) if stdout else None
                if isinstance(payload, dict) and payload.get("error"):
                    last_stdout = stdout
                    last_stderr = stderr
                    break
            except Exception:
                pass

            last_stdout = stdout
            last_stderr = stderr

            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= 2

        # Surface runner outputs to stderr for easier debugging in CI
        sys.stderr.write(f"Scrapy runner finished with returncode={proc.returncode if proc else 'N/A'}\n")
        sys.stderr.write(f"Runner STDOUT:\n{last_stdout}\n")
        if last_stderr:
            sys.stderr.write(f"Runner STDERR:\n{last_stderr}\n")

        return []
