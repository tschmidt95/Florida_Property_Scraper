import gzip
import io
import random
import time
import urllib.parse
import urllib.request
import zlib
from http import cookiejar


RETRY_STATUS = {429, 500, 502, 503, 504}


class RetryConfig:
    def __init__(self, retries=2, base_delay=0.2, factor=2.0, jitter=0.1):
        self.retries = retries
        self.base_delay = base_delay
        self.factor = factor
        self.jitter = jitter


def compute_backoff_delays(retries, base_delay=0.2, factor=2.0, jitter=0.1, rand_fn=None):
    delays = []
    current = base_delay
    rand_fn = rand_fn or random.random
    for _ in range(retries):
        noise = (rand_fn() * 2 - 1) * jitter
        delays.append(max(0.0, current + noise))
        current *= factor
    return delays


class TokenBucket:
    def __init__(self, rate_per_sec=1.0, capacity=2.0):
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.last_check = time.time()

    def take(self):
        now = time.time()
        elapsed = now - self.last_check
        self.last_check = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        wait_time = (1.0 - self.tokens) / self.rate_per_sec
        self.tokens = 0.0
        return max(0.0, wait_time)


class HttpClient:
    def __init__(self, timeout=10, max_bytes=200_000, retry_config=None):
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.retry_config = retry_config or RetryConfig()
        self._buckets = {}
        self._cookies = cookiejar.CookieJar()
        proxy_handler = urllib.request.ProxyHandler({})
        self._opener = urllib.request.build_opener(
            proxy_handler,
            urllib.request.HTTPCookieProcessor(self._cookies),
        )

    def _get_bucket(self, host):
        bucket = self._buckets.get(host)
        if bucket is None:
            bucket = TokenBucket()
            self._buckets[host] = bucket
        return bucket

    def _read_body(self, response):
        data = response.read(self.max_bytes + 1)
        truncated = len(data) > self.max_bytes
        if truncated:
            data = data[: self.max_bytes]
        encoding = response.headers.get("Content-Encoding", "").lower()
        if encoding == "gzip":
            return gzip.decompress(data), truncated
        if encoding == "deflate":
            return zlib.decompress(data), truncated
        return data, truncated

    def _decode_body(self, data, response):
        charset = "utf-8"
        content_type = response.headers.get("Content-Type", "")
        if "charset=" in content_type:
            charset = content_type.split("charset=")[-1].split(";")[0].strip()
        return data.decode(charset, errors="replace")

    def build_form_request(self, url, form_fields):
        encoded = urllib.parse.urlencode(form_fields or {}).encode("utf-8")
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        return {"url": url, "method": "POST", "data": encoded, "headers": headers}

    def request(self, request_spec, allowed_hosts=None, sleep_fn=time.sleep, dry_run=False, fixture_map=None):
        if isinstance(request_spec, str):
            request_spec = {"url": request_spec, "method": "GET", "data": None, "headers": {}}
        url = request_spec.get("url")
        parsed = urllib.parse.urlparse(url)
        if dry_run:
            if fixture_map and url in fixture_map:
                return {"text": fixture_map[url], "final_url": url, "truncated": False, "status": 200}
            if parsed.scheme == "file":
                with open(parsed.path, "rb") as handle:
                    data = handle.read(self.max_bytes)
                return {"text": data.decode("utf-8", errors="replace"), "final_url": url, "truncated": False, "status": 200}
        if parsed.scheme in ("file", ""):
            with open(parsed.path, "rb") as handle:
                data = handle.read(self.max_bytes)
            return {"text": data.decode("utf-8", errors="replace"), "final_url": url, "truncated": False, "status": 200}
        if allowed_hosts is not None and parsed.hostname not in allowed_hosts:
            raise ValueError("Host not in allowlist")
        bucket = self._get_bucket(parsed.hostname or "")
        wait_time = bucket.take()
        if wait_time:
            sleep_fn(wait_time)
        headers = {
            "User-Agent": "florida-property-scraper-native",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Encoding": "gzip, deflate",
        }
        headers.update(request_spec.get("headers") or {})
        data = request_spec.get("data")
        req = urllib.request.Request(url, headers=headers, data=data, method=request_spec.get("method", "GET"))
        delays = compute_backoff_delays(
            self.retry_config.retries,
            self.retry_config.base_delay,
            self.retry_config.factor,
            self.retry_config.jitter,
        )
        attempts = len(delays) + 1
        last_error = None
        for attempt in range(attempts):
            try:
                with self._opener.open(req, timeout=self.timeout) as response:
                    status = getattr(response, "status", 200)
                    data_bytes, truncated = self._read_body(response)
                    text = self._decode_body(data_bytes, response)
                    if status in RETRY_STATUS and attempt < len(delays):
                        sleep_fn(delays[attempt])
                        continue
                    return {"text": text, "final_url": response.geturl(), "truncated": truncated, "status": status}
            except urllib.error.HTTPError as exc:
                last_error = exc
                status = getattr(exc, "code", None)
                if status in RETRY_STATUS and attempt < len(delays):
                    sleep_fn(delays[attempt])
                    continue
                raise
            except Exception as exc:  # pragma: no cover - error path
                last_error = exc
                if attempt < len(delays):
                    sleep_fn(delays[attempt])
                continue
        raise last_error
