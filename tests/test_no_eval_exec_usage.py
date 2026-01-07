from pathlib import Path


def test_no_eval_exec_usage():
    root = Path(__file__).resolve().parents[1] / "src"
    disallowed = ["eval(", "exec(", "os.system(", "popen(", "shell=True"]
    for path in root.rglob("*.py"):
        content = path.read_text(encoding="utf-8", errors="ignore")
        lower = content.lower()
        for token in disallowed:
            if token in lower:
                raise AssertionError(f"Disallowed token {token} in {path}")
