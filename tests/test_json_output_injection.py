import json


def test_json_output_injection_safe():
    item = {
        "owner": "</json><evil>",
        "address": "__proto__",
        "county": "broward",
    }
    payload = json.dumps([item])
    loaded = json.loads(payload)
    assert loaded[0]["owner"] == "</json><evil>"
    assert loaded[0]["address"] == "__proto__"
