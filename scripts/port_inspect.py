#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Listener:
    port: int
    proto: str  # tcp|tcp6
    state: str  # LISTEN
    inode: int


_TCP_LISTEN_STATE = "0A"  # /proc/net/tcp state hex for LISTEN
_SOCKET_INODE_RE = re.compile(r"socket:\[(\d+)\]")


def _parse_proc_net_tcp(path: str, proto: str) -> list[Listener]:
    listeners: list[Listener] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return listeners

    # First line is a header.
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 10:
            continue

        local_address = parts[1]  # e.g. 0100007F:1F40
        state = parts[3]  # e.g. 0A
        inode_s = parts[9]

        if state != _TCP_LISTEN_STATE:
            continue

        if ":" not in local_address:
            continue

        _addr_hex, port_hex = local_address.split(":", 1)
        try:
            port = int(port_hex, 16)
            inode = int(inode_s)
        except ValueError:
            continue

        listeners.append(Listener(port=port, proto=proto, state="LISTEN", inode=inode))

    return listeners


def _iter_pids() -> Iterable[int]:
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            yield int(name)
        except ValueError:
            continue


def _pid_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
    except Exception:
        return ""

    raw = raw.replace(b"\x00", b" ").strip()
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _inode_to_pids(inodes: set[int]) -> dict[int, list[int]]:
    inode_map: dict[int, set[int]] = {i: set() for i in inodes}

    # Scan /proc/<pid>/fd symlinks looking for socket:[inode]
    for pid in _iter_pids():
        fd_dir = f"/proc/{pid}/fd"
        try:
            fds = os.listdir(fd_dir)
        except Exception:
            continue

        for fd in fds:
            path = os.path.join(fd_dir, fd)
            try:
                target = os.readlink(path)
            except Exception:
                continue

            m = _SOCKET_INODE_RE.fullmatch(target)
            if not m:
                continue

            try:
                inode = int(m.group(1))
            except ValueError:
                continue

            if inode in inode_map:
                inode_map[inode].add(pid)

    return {inode: sorted(pids) for inode, pids in inode_map.items()}


def inspect_ports(ports: list[int]) -> list[Listener]:
    want = set(ports)
    listeners = []
    listeners.extend(_parse_proc_net_tcp("/proc/net/tcp", "tcp"))
    listeners.extend(_parse_proc_net_tcp("/proc/net/tcp6", "tcp6"))
    return [l for l in listeners if l.port in want]


def main() -> int:
    # Usage:
    #   python scripts/port_inspect.py
    #   python scripts/port_inspect.py 8000
    #   python scripts/port_inspect.py 8000 5173
    if len(os.sys.argv) > 1:
        ports: list[int] = []
        for a in os.sys.argv[1:]:
            try:
                ports.append(int(a))
            except ValueError:
                print(f"invalid port: {a}")
                return 2
    else:
        ports = [8000, 5173]

    listeners = inspect_ports(ports)

    by_port: dict[int, list[Listener]] = {p: [] for p in ports}
    for l in listeners:
        by_port.setdefault(l.port, []).append(l)

    all_inodes = {l.inode for l in listeners}
    inode_pids = _inode_to_pids(all_inodes) if all_inodes else {}

    print("== LISTENERS ==")
    for port in ports:
        ls = by_port.get(port) or []
        if not ls:
            print(f"port={port} FREE")
            continue

        # Stable sort output.
        ls_sorted = sorted(ls, key=lambda x: (x.proto, x.inode))
        for l in ls_sorted:
            pids = inode_pids.get(l.inode, [])
            cmdlines = [(_pid_cmdline(pid) or "").strip() for pid in pids]
            cmdlines = [c for c in cmdlines if c]
            print(
                f"port={l.port} proto={l.proto} state={l.state} inode={l.inode} "
                f"pids={pids} cmdline={cmdlines}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
