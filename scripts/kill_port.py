#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sys

from port_inspect import inspect_ports, _inode_to_pids  # type: ignore


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python scripts/kill_port.py <port>")
        return 2

    try:
        port = int(argv[1])
    except ValueError:
        print("port must be an int")
        return 2

    listeners = inspect_ports([port])
    if not listeners:
        print(f"port={port} FREE")
        return 0

    inodes = {l.inode for l in listeners}
    inode_pids = _inode_to_pids(inodes)

    killed: set[int] = set()
    for inode, pids in inode_pids.items():
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
                killed.add(pid)
            except ProcessLookupError:
                pass
            except PermissionError:
                pass
            except Exception:
                pass

    print(f"port={port} killed_pids={sorted(killed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
