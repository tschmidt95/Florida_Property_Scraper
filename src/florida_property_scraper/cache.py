import os
import time

_CACHE = {}
_STATS = {"hits": 0, "misses": 0}


def _cache_enabled():
    return os.environ.get("CACHE", "1") != "0"


def cache_get(key):
    if not _cache_enabled():
        return None
    entry = _CACHE.get(key)
    if not entry:
        _STATS["misses"] += 1
        return None
    expires_at, value = entry
    if expires_at < time.time():
        _CACHE.pop(key, None)
        _STATS["misses"] += 1
        return None
    _STATS["hits"] += 1
    return value


def cache_set(key, value, ttl=120):
    if not _cache_enabled():
        return
    _CACHE[key] = (time.time() + ttl, value)


def cache_clear():
    _CACHE.clear()
    _STATS["hits"] = 0
    _STATS["misses"] = 0


def cache_stats():
    return dict(_STATS)
