#!/usr/bin/env python3
"""
Aasan build log CLI.

Reads BUILD_LOG.md (canonical ship log) and prints filtered entries.
Each entry is a `## YYYY-MM-DD — title` block; tags come from the
`**Tags:**` line (space-separated `#tag` tokens).

Usage:
    python scripts/build_log.py                    # latest 5 entries
    python scripts/build_log.py --tag calendar     # filter by tag (no #)
    python scripts/build_log.py --tag v3 --tag goals   # multiple tags = AND
    python scripts/build_log.py --search voyage    # full-text search (case-insensitive)
    python scripts/build_log.py --since 2026-04-28
    python scripts/build_log.py --limit 20         # default 5; 0 = unlimited
    python scripts/build_log.py --no-color         # plain output for piping
    python scripts/build_log.py --tags-list        # list all tags + counts
"""

import argparse
import os
import re
import sys
from datetime import datetime

BUILD_LOG_PATH = os.environ.get(
    "AASAN_BUILD_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "BUILD_LOG.md"),
)

ENTRY_RE = re.compile(r"^## (\d{4}-\d{2}-\d{2}) — (.+)$")
TAGS_RE = re.compile(r"^\*\*Tags:\*\*\s*(.+)$")


# ANSI colors — disabled when stdout isn't a tty
class C:
    BOLD = "\033[1m"
    DIM = "\033[2m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    GRAY = "\033[90m"
    RESET = "\033[0m"

    @classmethod
    def off(cls):
        for k in dir(cls):
            if k.isupper():
                setattr(cls, k, "")


def parse_journal(path: str) -> list:
    if not os.path.exists(path):
        sys.exit(f"journal not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    entries = []
    current = None
    for line in lines:
        m = ENTRY_RE.match(line)
        if m:
            if current:
                entries.append(current)
            current = {
                "date": m.group(1),
                "title": m.group(2).strip(),
                "tags": [],
                "body": [],
            }
            continue
        if current is None:
            continue
        tag_m = TAGS_RE.match(line)
        if tag_m:
            current["tags"] = re.findall(r"#([a-z0-9_-]+)", tag_m.group(1).lower())
            continue
        # stop accumulating body once we hit the next entry; we already handled that above
        current["body"].append(line)
    if current:
        entries.append(current)
    return entries


def filter_entries(entries: list, tags: list, search: str, since: str) -> list:
    out = entries
    if tags:
        wanted = {t.lstrip("#").lower() for t in tags}
        out = [e for e in out if wanted.issubset(set(e["tags"]))]
    if search:
        needle = search.lower()
        def hit(e):
            blob = (e["title"] + " " + " ".join(e["body"])).lower()
            return needle in blob
        out = [e for e in out if hit(e)]
    if since:
        try:
            since_dt = datetime.fromisoformat(since).date()
            out = [e for e in out if datetime.fromisoformat(e["date"]).date() >= since_dt]
        except ValueError:
            sys.exit(f"--since must be YYYY-MM-DD, got: {since}")
    return out


def render(entry: dict) -> str:
    body = "\n".join(entry["body"]).strip()
    tags = " ".join(f"{C.BLUE}#{t}{C.RESET}" for t in entry["tags"])
    header = f"{C.BOLD}{entry['date']}{C.RESET} {C.GRAY}—{C.RESET} {C.BOLD}{C.CYAN}{entry['title']}{C.RESET}"
    return f"{header}\n{C.GRAY}{tags}{C.RESET}\n\n{body}\n"


def render_summary(entry: dict) -> str:
    tags = " ".join(f"{C.BLUE}#{t}{C.RESET}" for t in entry["tags"][:5])
    return f"  {C.BOLD}{entry['date']}{C.RESET}  {entry['title']}\n  {C.GRAY}└─{C.RESET} {tags}"


def list_tags(entries: list) -> None:
    counts = {}
    for e in entries:
        for t in e["tags"]:
            counts[t] = counts.get(t, 0) + 1
    width = max((len(t) for t in counts), default=0)
    for t, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {C.BLUE}#{t.ljust(width)}{C.RESET}  {n}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tag", action="append", default=[], help="Filter by tag (no #). Repeatable; multiple = AND.")
    p.add_argument("--search", default="", help="Full-text search (case-insensitive)")
    p.add_argument("--since", default="", help="Earliest date (YYYY-MM-DD)")
    p.add_argument("--limit", type=int, default=5, help="Max entries (0 = unlimited; default 5)")
    p.add_argument("--full", action="store_true", help="Show full body (default: summary)")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    p.add_argument("--tags-list", action="store_true", help="List all tags with counts and exit")
    p.add_argument("--path", default=BUILD_LOG_PATH, help="Path to BUILD_LOG.md")
    args = p.parse_args()

    if args.no_color or not sys.stdout.isatty():
        C.off()

    entries = parse_journal(args.path)

    if args.tags_list:
        list_tags(entries)
        return

    filtered = filter_entries(entries, args.tag, args.search, args.since)
    # Newest first (already so in BUILD_LOG.md, but enforce)
    filtered.sort(key=lambda e: e["date"], reverse=True)
    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    if not filtered:
        print(f"{C.GRAY}(no entries matched){C.RESET}")
        return

    n_total = len(parse_journal(args.path))
    print(f"{C.GRAY}{len(filtered)} of {n_total} entries{C.RESET}\n")

    for e in filtered:
        if args.full:
            print(render(e))
            print(C.GRAY + "─" * 60 + C.RESET)
        else:
            print(render_summary(e))
    if not args.full:
        print(f"\n{C.GRAY}(use --full for body, --search/--tag to filter){C.RESET}")


if __name__ == "__main__":
    main()
