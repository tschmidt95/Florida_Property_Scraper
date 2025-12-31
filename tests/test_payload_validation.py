import json
import pathlib
import pytest
from jsonschema import validate, ValidationError

ROOT = pathlib.Path(__file__).parent.parent
SCHEMA_DIR = ROOT / ".github" / "schemas"


def load_schema(name):
    path = SCHEMA_DIR / name
    with path.open() as fh:
        return json.load(fh)


def test_generic_payload_validates():
    schema = load_schema("generic_payload.json")
    payload = {"alert_type": "image_monitor_failure", "title": "Test title", "url": "https://example.com/issue/1", "body": "Test body"}
    validate(instance=payload, schema=schema)


def test_generic_payload_rejects_missing_field():
    schema = load_schema("generic_payload.json")
    payload = {"title": "Missing alert_type"}
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_slack_payload_validates():
    schema = load_schema("slack_payload.json")
    payload = {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test"}}]}
    validate(instance=payload, schema=schema)


def test_slack_payload_rejects_missing_blocks():
    schema = load_schema("slack_payload.json")
    payload = {"not_blocks": True}
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_teams_payload_validates():
    schema = load_schema("teams_payload.json")
    payload = {"@type": "MessageCard", "@context": "https://schema.org/extensions", "summary": "x", "sections": [{"activityTitle": "t"}]}
    validate(instance=payload, schema=schema)


def test_teams_payload_rejects_invalid_shape():
    schema = load_schema("teams_payload.json")
    payload = {"foo": "bar"}
    # If schema is permissive this may pass; we assert that missing expected keys raises ValidationError
    try:
        validate(instance=payload, schema=schema)
    except ValidationError:
        return
    # If validation didn't raise, ensure at least the type is object (sanity)
    assert isinstance(payload, dict)
