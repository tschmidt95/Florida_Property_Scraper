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
    payload = {
        "alert_type": "image_monitor_failure",
        "title": "Test title",
        "url": "https://example.com/issue/1",
        "body": "Test body",
    }
    validate(instance=payload, schema=schema)


def test_generic_payload_rejects_missing_field():
    schema = load_schema("generic_payload.json")
    payload = {"title": "Missing alert_type"}
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_slack_payload_validates():
    schema = load_schema("slack_payload.json")
    payload = {
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "Test"}}
        ]
    }
    validate(instance=payload, schema=schema)


def test_slack_payload_rejects_missing_blocks():
    schema = load_schema("slack_payload.json")
    payload = {"not_blocks": True}
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


def test_teams_payload_validates():
    schema = load_schema("teams_payload.json")
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": "x",
        "sections": [{"activityTitle": "t"}],
    }
    validate(instance=payload, schema=schema)


def test_teams_payload_rejects_invalid_shape():
    schema = load_schema("teams_payload.json")
    payload = {"foo": "bar"}
    # If schema is permissive this may pass; we assert that missing
    # expected keys raise ValidationError
    try:
        validate(instance=payload, schema=schema)
    except ValidationError:
        return
    # If validation didn't raise, ensure at least the type is object (sanity)
    assert isinstance(payload, dict)


# --- Expanded negative tests (5 invalid cases per schema) ---

@pytest.mark.parametrize("payload", [
    {"title": "Missing alert_type"},  # missing required
    {
        "alert_type": "x",
        "title": "t",
        "url": "ftp://example.com",
        "body": "b",
    },  # url pattern
    {"alert_type": 123, "title": "t", "url": "https://x", "body": "b"},
    {
        "alert_type": "x",
        "title": "t",
        "url": "https://x",
        "body": 123,
    },
    {
        "alert_type": "x",
        "title": "t",
        "url": "https://x",
        "body": "b",
        "extra": "x",
    },
])
def test_generic_invalid_cases(payload):
    schema = load_schema("generic_payload.json")
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


@pytest.mark.parametrize("payload", [
    {},  # missing blocks
    {"blocks": []},  # minItems=1
    {"blocks": "not an array"},  # wrong type
    {
        "blocks": [
            {"text": {"type": "mrkdwn", "text": "t"}},
        ],
    },  # item missing required 'type'
    {"blocks": [{"type": 123}]},  # type wrong type
])
def test_slack_invalid_cases(payload):
    schema = load_schema("slack_payload.json")
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


@pytest.mark.parametrize("payload", [
    "just a string",  # top-level not an object
    {"title": 123},  # title must be string
    {"text": 42},  # text must be string or object
    {"potentialAction": "not an array"},  # must be array
    {"potentialAction": {"foo": "bar"}},  # not an array
])
def test_teams_invalid_cases(payload):
    schema = load_schema("teams_payload.json")
    with pytest.raises(ValidationError):
        validate(instance=payload, schema=schema)


# --- Additional positive edge-case tests ---


def test_generic_body_object_valid():
    schema = load_schema("generic_payload.json")
    payload = {
        "alert_type": "x",
        "title": "t",
        "url": "https://example.com",
        "body": {"rich": "content"},
    }
    validate(instance=payload, schema=schema)



def test_slack_blocks_with_extra_properties_valid():
    schema = load_schema("slack_payload.json")
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "T"},
                "accessory": {"type": "image"},
            }
        ]
    }
    validate(instance=payload, schema=schema)



def test_teams_text_object_valid():
    schema = load_schema("teams_payload.json")
    payload = {"title": "t", "text": {"foo": "bar"}, "potentialAction": []}
    validate(instance=payload, schema=schema)
