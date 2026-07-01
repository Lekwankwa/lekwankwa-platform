"""
Shared pipeline runner for all validation orchestrators.

Usage in every run_all_*.py:

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # → validations/
    from pipeline_runner import run_pipeline

    PRODUCT = "..."
    STAGES  = [{"id": ..., "name": ..., "script": ..., "args": [...], "required": bool}, ...]

    if __name__ == "__main__":
        sys.exit(run_pipeline(PRODUCT, STAGES, scope=None, timeout=600))

Stage dict schema
-----------------
    id       : int | str   — stage number displayed in output (e.g. 1, "4b", "GX")
    name     : str         — human-readable name
    script   : str         — path to Python script, relative to CWD (project root)
    args     : list[str]   — extra CLI arguments forwarded to the script
    required : bool        — if True, a failure triggers the on_required_fail policy

Failure semantics
-----------------
    - A stage passes iff its subprocess exits with returncode 0 (status == "PASS").
      WARN, FAIL, TIMEOUT, and ERROR all count as failures.
    - on_required_fail controls what happens when a required stage fails:
        "stop"                       stop immediately; remaining stages do not run.
                                     (default — used by all EU27 orchestrators and
                                     USA wages_and_employment / housing)
        "continue"                   record the failure and keep running all remaining
                                     stages; pipeline_ok is set False but nothing stops.
                                     (USA trade_flows original behavior)
        "skip_required_run_optional" add SKIP for all remaining required stages, then
                                     still execute all remaining optional stages.
                                     (USA IMF original behavior)
    - Overall result is PASS iff pipeline_ok is True AND no stage recorded FAIL/TIMEOUT/ERROR.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def run_stage(stage: dict, *, timeout: int = 900) -> dict:
    """Execute one pipeline stage subprocess; return result dict."""
    cmd   = [sys.executable, stage["script"]] + list(stage.get("args", []))
    start = datetime.now()
    try:
        rc = subprocess.run(cmd, capture_output=False, text=True, timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        return {
            "stage": stage["id"], "name": stage["name"],
            "status": "TIMEOUT", "return_code": -1, "duration_sec": timeout,
        }
    except Exception as exc:
        return {
            "stage": stage["id"], "name": stage["name"],
            "status": "ERROR", "return_code": -1, "duration_sec": 0, "error": str(exc),
        }
    return {
        "stage": stage["id"], "name": stage["name"],
        "status": "PASS" if rc == 0 else "FAIL",
        "return_code": rc,
        "duration_sec": round((datetime.now() - start).total_seconds(), 2),
    }


def run_pipeline(
    product: str,
    stages: list[dict],
    *,
    scope: str | None = None,
    summary_path: Path | None = None,
    timeout: int = 900,
    on_required_fail: str = "stop",
) -> int:
    """
    Run stages in sequence according to the specified failure policy.

    Returns 0 (PASS) or 1 (FAIL) for sys.exit().

    Parameters
    ----------
    product          : vault product name, used in logging and the summary file name.
    stages           : list of stage dicts (id, name, script, args, required).
    scope            : optional label written to the summary JSON ("EU27", None for USA).
    summary_path     : override the auto-generated summary file path.
    timeout          : per-stage subprocess timeout in seconds.
    on_required_fail : "stop" | "continue" | "skip_required_run_optional"
                       Controls what happens when a required stage fails (see module docstring).
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if summary_path is None:
        scope_tag    = f"_{scope.lower()}" if scope else ""
        summary_path = Path(f"validation_summary_{product}{scope_tag}_{timestamp}.json")

    label = product.upper() + (f" ({scope})" if scope else "")
    sep   = "=" * 70

    print(sep)
    print(f"VALIDATION PIPELINE — {label}")
    print(f"Started   : {datetime.now().isoformat()}")
    print(f"Stages    : {len(stages)}")
    print(sep)

    results: list[dict[str, Any]] = []
    pipeline_ok = True
    t0 = datetime.now()

    i = 0
    while i < len(stages):
        stage = stages[i]
        print(f"\n{sep}\nSTAGE {stage['id']}: {stage['name'].upper()}\n{sep}")
        r = run_stage(stage, timeout=timeout)
        results.append(r)
        print(f"\n  Result: [{r['status']}] in {r['duration_sec']}s")

        if stage.get("required") and r["status"] != "PASS":
            pipeline_ok = False
            if on_required_fail == "stop":
                print(f"\n[FAIL] Required stage '{stage['name']}' failed — stopping pipeline.")
                break

            elif on_required_fail == "skip_required_run_optional":
                # Mark every remaining required stage as SKIP, then run remaining optional stages.
                print(f"\n[FAIL] Required stage '{stage['name']}' failed — "
                      f"skipping required stages, running optional.")
                remaining = stages[i + 1:]
                for s in remaining:
                    if s.get("required"):
                        results.append({
                            "stage": s["id"], "name": s["name"],
                            "status": "SKIP", "return_code": -1, "duration_sec": 0,
                        })
                for s in remaining:
                    if not s.get("required"):
                        print(f"\n{sep}\nSTAGE {s['id']}: {s['name'].upper()} (optional)\n{sep}")
                        r2 = run_stage(s, timeout=timeout)
                        results.append(r2)
                        print(f"\n  Result: [{r2['status']}] in {r2['duration_sec']}s")
                break

            # "continue" mode: pipeline_ok already False; loop keeps running normally.

        i += 1

    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] not in ("PASS", "SKIP"))
    overall = "PASS" if pipeline_ok and failed == 0 else "FAIL"
    total_s = round((datetime.now() - t0).total_seconds(), 2)

    print(f"\n{sep}")
    print(f"VALIDATION PIPELINE SUMMARY — {label}")
    print(sep)
    for r in results:
        print(f"  Stage {r['stage']}: [{r['status']:7}] {r['name']} ({r['duration_sec']}s)")
    print(f"\nOverall: [{overall}] | {passed}/{len(results)} passed | {total_s}s total")

    summary: dict[str, Any] = {
        "product":            product,
        "scope":              scope,
        "run_at":             timestamp,
        "overall":            overall,
        "stages_passed":      passed,
        "stages_failed":      failed,
        "total_duration_sec": total_s,
        "stage_results":      results,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nSummary: {summary_path}")
    return 0 if overall == "PASS" else 1
