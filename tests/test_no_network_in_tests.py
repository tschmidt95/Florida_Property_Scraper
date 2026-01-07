import urllib.request

import pytest


def test_no_network_in_tests():
    with pytest.raises(RuntimeError):
        urllib.request.urlopen("http://example.com", timeout=1)
