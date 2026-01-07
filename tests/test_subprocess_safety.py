import subprocess

from florida_property_scraper.backend.scrapy_adapter import ScrapyAdapter


def test_subprocess_args_are_list(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        class Dummy:
            stdout = "[]"
            stderr = ""
            returncode = 0
        return Dummy()

    monkeypatch.setattr(subprocess, "run", fake_run)
    adapter = ScrapyAdapter(demo=False, live=False)
    adapter.search(
        "Smith",
        start_urls=["file://dummy"],
        spider_name="broward_spider",
    )
    assert calls
    args, kwargs = calls[0]
    assert isinstance(args, list)
    assert kwargs.get("shell") is False
