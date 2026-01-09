from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _tail_text(path: Path, *, lines: int) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    parts = data.splitlines()
    if lines <= 0:
        return "\n".join(parts)
    return "\n".join(parts[-lines:])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="enopro-run",
        description=(
            "Run a command and capture stdout/stderr to a temp file, then print only the last N lines. "
            "Useful when the VS Code terminal runner is flaky (ENOPRO)."
        ),
    )
    parser.add_argument("--tail", type=int, default=200, help="How many lines to print at the end")
    parser.add_argument("--keep", action="store_true", help="Keep the output file (prints its path)")
    parser.add_argument(
        "--cwd",
        default=None,
        help="Working directory for the command (default: current)",
    )
    parser.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        help="Command to run (use: -- <cmd> <args...>)",
    )

    args = parser.parse_args(argv)
    cmd = list(args.cmd)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        parser.error("No command provided. Use: scripts/enopro_run.py -- <cmd> <args...>")

    with tempfile.NamedTemporaryFile(prefix="enopro-run-", suffix=".log", delete=False) as handle:
        out_path = Path(handle.name)

    try:
        cwd = args.cwd or os.getcwd()
        with out_path.open("w", encoding="utf-8", errors="replace") as log:
            log.write("> " + " ".join(cmd) + "\n")
            log.flush()
            proc = subprocess.run(
                cmd,
                cwd=cwd,
                text=True,
                stdout=log,
                stderr=log,
            )

        tail = _tail_text(out_path, lines=int(args.tail))
        if tail:
            sys.stdout.write(tail + ("\n" if not tail.endswith("\n") else ""))

        if args.keep:
            sys.stdout.write(f"\n[enopro-run] full log: {out_path}\n")
        else:
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass
        return int(proc.returncode)
    finally:
        # Best-effort cleanup if something went wrong before we could unlink.
        if not args.keep:
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
