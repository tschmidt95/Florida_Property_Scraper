import tempfile
import unicodedata
from pathlib import Path


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def sanitize_path(path_str: str, project_root: Path) -> Path:
    if not path_str:
        raise ValueError("path required")
    normalized = unicodedata.normalize("NFKC", path_str)
    if ".." in normalized or ".." in Path(normalized).parts:
        raise ValueError("path traversal not allowed")
    if "\u202e" in normalized or "\u202d" in normalized:
        raise ValueError("unsafe unicode in path")

    tmp_root = Path(tempfile.gettempdir()).resolve()
    project_root = project_root.resolve()
    raw_path = Path(normalized)
    if raw_path.is_absolute():
        resolved = raw_path.resolve()
    else:
        resolved = (project_root / raw_path).resolve()

    allowed = any(
        _is_relative_to(resolved, root) for root in (project_root, tmp_root)
    )
    if not allowed:
        raise ValueError("path outside allowed roots")
    for parent in [resolved] + list(resolved.parents):
        if parent.exists() and parent.is_symlink():
            raise ValueError("symlink paths not allowed")
    return resolved


def neutralize_csv_field(value):
    text = "" if value is None else str(value)
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text
