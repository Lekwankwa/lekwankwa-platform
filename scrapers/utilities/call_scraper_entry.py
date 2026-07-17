"""
scrapers/utilities/call_scraper_entry.py — Lekwankwa Corporation

Shared helper for invoking a COUNTRY_ROUTER-mapped scraper entry point.

Two shapes exist across scraper modules:
  1. A real Python function accepting mode/since/extra kwargs directly,
     e.g. def scrape_usa_food_pricing(mode=..., since=...).
  2. A standalone CLI-style main() with no Python parameters at all — it
     builds its own argparse.ArgumentParser() and calls parser.parse_args(),
     reading directly from sys.argv.

Calling shape 2 with fn(mode=..., since=...) raises TypeError (main()
accepts no kwargs). Simply dropping the kwargs and calling fn() isn't
enough either: main()'s own parser then parses the *outer* process's
sys.argv, which still holds the outer script's own flags (e.g. --country
USA) that main()'s parser doesn't define, causing an unrecognized-argument
SystemExit instead.
"""
from __future__ import annotations

import inspect
import sys
from typing import Any, Callable


def call_scraper_entry(
    fn: Callable,
    mode: str,
    since: str | None,
    extra_kwargs: dict[str, Any] | None = None,
) -> None:
    """Call fn the way its signature expects — Python kwargs or CLI argv."""
    extra_kwargs = extra_kwargs or {}
    sig = inspect.signature(fn)
    accepts_var_kwargs = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )

    if sig.parameters or accepts_var_kwargs:
        call_kwargs = {"mode": mode, "since": since, **extra_kwargs}
        if not accepts_var_kwargs:
            call_kwargs = {k: v for k, v in call_kwargs.items() if k in sig.parameters}
        fn(**call_kwargs)
        return

    # Zero-parameter CLI entry point — swap in a compatible argv for the
    # duration of the call so its own parser doesn't choke on the outer
    # script's flags (e.g. --country), then restore it.
    argv_backup = sys.argv
    try:
        new_argv = [argv_backup[0], "--mode", mode]
        if since:
            new_argv += ["--since", since]
        for k, v in extra_kwargs.items():
            new_argv += [f"--{k}", str(v)]
        sys.argv = new_argv
        fn()
    finally:
        sys.argv = argv_backup
