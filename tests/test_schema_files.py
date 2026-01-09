import json
import pathlib

SCHEMA_DIR = pathlib.Path(__file__).parent.parent / ".github" / "schemas"
SCHEMAS = ["generic_payload.json", "slack_payload.json", "teams_payload.json"]


def test_schemas_valid_json():
    for fname in SCHEMAS:
        path = SCHEMA_DIR / fname
        assert path.exists(), f"Schema {fname} must exist"
        with path.open() as fh:
            data = json.load(fh)
        # Basic checks
        assert isinstance(data, dict), f"Schema {fname} must be a JSON object"
        # Expect a draft-07 or $schema field or properties
        assert any(k in data for k in ("$schema", "properties", "type")), (
            f"Schema {fname} looks empty or invalid"
        )
