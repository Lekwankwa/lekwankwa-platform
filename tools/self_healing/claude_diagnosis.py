"""
tools/self_healing/claude_diagnosis.py — Lekwankwa Corporation
Layer 3: Claude Sonnet 4.6 diagnosis and auto-fix for MAJOR_EXCEPTION events.

For SIMPLE fixes (1-5 lines): creates a branch, applies the patch via GitHub
API, opens a PR, and sends a notification email — no approval gate needed.

For COMPLEX fixes: escalates to the approval email flow.
"""
from __future__ import annotations

import base64
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

REPO = "lekwankwa/lekwankwa-platform"

SYSTEM_PROMPT = """You are the self-healing system for Lekwankwa Corporation's
sovereign data pipeline. Datasets covered: food_micropricing,
wages_and_employment, trade_flows, Housing_Supply_and_Shelter_Inflation,
global_macro. All data is ingested from open-government REST APIs and SDMX
endpoints across 32 countries.

When given an exception, traceback, and pipeline context, you must:
1. Diagnose the root cause in plain language (2-3 sentences)
2. Classify severity: CRITICAL (data delivery at risk) or
   HIGH (pipeline disrupted, delivery not yet at risk)
3. State which validation stage failed if applicable
   (PIT/Schema/Sanity/Temporal/Referential/Outlier/Changelog/Lineage/GX)
4. Propose a specific, actionable code fix
5. State exactly which file and function needs to change
6. ALWAYS provide the fix as a unified diff — no exceptions

The diff will be applied automatically via the GitHub API and a PR will be opened.
You must always produce a working diff. Return your response in this exact format:

ROOT_CAUSE: <2-3 sentences>
SEVERITY: CRITICAL|HIGH
VALIDATION_STAGE: <stage or N/A>
PROPOSED_FIX: <specific fix description>
FILE_AND_FUNCTION: <file path and function name>
DIFF: <the exact code change in unified diff format — always required>"""


def _read_source_file(program: str) -> tuple[str, str]:
    """
    Return (relative_path, file_content) for the given program path.
    Tries to resolve relative to the project root. Returns ('', '') if unreadable.
    """
    from pathlib import Path
    try:
        p = Path(program)
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.exists():
            return program, ""
        content = p.read_text(encoding="utf-8", errors="replace")
        # Make path relative to project root for use in diffs / GitHub API
        try:
            rel = str(p.relative_to(Path.cwd())).replace("\\", "/")
        except ValueError:
            rel = p.name
        return rel, content
    except Exception:
        return program, ""


def diagnose_with_claude(
    program: str,
    exception: Exception,
    context: dict[str, Any],
    traceback_str: str,
) -> str:
    """Call Claude Sonnet 4.6 to diagnose a MAJOR_EXCEPTION. Returns diagnosis text."""
    from tools.self_healing.secret_manager import get_secret
    import anthropic

    api_key = get_secret("anthropic-api-key")
    client  = anthropic.Anthropic(api_key=api_key)

    rel_path, file_content = _read_source_file(program)
    file_section = (
        f"\nSOURCE FILE ({rel_path}):\n```python\n{file_content}\n```\n"
        if file_content else
        f"\nSOURCE FILE: {program} (not readable — write diff against the path only)\n"
    )

    user_msg = f"""MAJOR EXCEPTION REPORT

Program:            {rel_path}
Product:            {context.get('product')}
Country:            {context.get('country')}
Source:             {context.get('source')}
Run date:           {context.get('run_date')}
Layer that failed:  {context.get('layer', 'SCRAPER')}
Validation finding: {context.get('finding', 'N/A')}

Exception type:     {type(exception).__name__}
Exception message:  {str(exception)}

Full traceback:
{traceback_str}
{file_section}
Diagnose root cause, classify severity, and provide the unified diff fix.
The diff MUST target the file above using the exact path shown."""

    log.info("[CLAUDE] Sending MAJOR_EXCEPTION to Claude Sonnet 4.6...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    diagnosis = response.content[0].text
    log.info("[CLAUDE] Diagnosis received (%d chars)", len(diagnosis))
    return diagnosis


def parse_complexity(diagnosis: str) -> str:
    """Extract FIX_COMPLEXITY from diagnosis text. Returns SIMPLE or COMPLEX."""
    for line in diagnosis.splitlines():
        if line.startswith("FIX_COMPLEXITY:"):
            return line.split(":", 1)[1].strip().upper()
    return "COMPLEX"


def parse_diff(diagnosis: str) -> str | None:
    """
    Extract the DIFF block from diagnosis.
    Handles both inline and markdown-fenced formats:
      DIFF: --- a/file ...
      DIFF:
      ```diff
      --- a/file ...
      ```
    Returns None if diff is N/A or missing.
    """
    lines = diagnosis.splitlines()
    diff_lines: list[str] = []
    in_diff = False
    in_fence = False

    for line in lines:
        if line.startswith("DIFF:"):
            rest = line[5:].strip()
            if rest in ("N/A", ""):
                in_diff = True   # diff may start on next line (fenced)
                continue
            if rest.startswith("```"):
                in_diff = True
                in_fence = True
                continue
            diff_lines.append(rest)
            in_diff = True
        elif in_diff:
            # opening fence
            if line.strip().startswith("```") and not in_fence and not diff_lines:
                in_fence = True
                continue
            # closing fence
            if in_fence and line.strip() == "```":
                break
            diff_lines.append(line)

    result = "\n".join(diff_lines).strip()
    return result if result else None


# ---------------------------------------------------------------------------
# Diff application helpers
# ---------------------------------------------------------------------------

def _parse_diff_target(diff_text: str) -> str | None:
    """Extract target file path from unified diff +++ header."""
    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            return line[6:].strip()
        if line.startswith("+++ ") and not line.startswith("+++ /dev/null"):
            return line[4:].strip()
    return None


def _apply_unified_diff(content: str, diff_text: str) -> str | None:
    """
    Apply a unified diff to string content.
    Returns modified content, or None if the patch cannot be applied cleanly.
    """
    hunk_re = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
    lines   = content.splitlines(keepends=True)
    result  = list(lines)
    offset  = 0

    diff_lines = diff_text.splitlines(keepends=True)
    i = 0
    while i < len(diff_lines):
        m = hunk_re.match(diff_lines[i])
        if not m:
            i += 1
            continue

        old_start = int(m.group(1)) - 1          # 0-indexed
        old_count = int(m.group(2)) if m.group(2) is not None else 1
        i += 1

        hunk_old: list[str] = []
        hunk_new: list[str] = []

        while i < len(diff_lines) and not hunk_re.match(diff_lines[i]):
            dl = diff_lines[i]
            if dl.startswith("---") or dl.startswith("+++") or dl.startswith("diff "):
                i += 1
                break
            if dl.startswith("-"):
                hunk_old.append(dl[1:])
            elif dl.startswith("+"):
                hunk_new.append(dl[1:])
            else:
                ctx = dl[1:] if dl.startswith(" ") else dl
                hunk_old.append(ctx)
                hunk_new.append(ctx)
            i += 1

        actual_start = old_start + offset
        if actual_start < 0 or actual_start + len(hunk_old) > len(result):
            log.error("[DIFF] Hunk at line %d out of bounds (file has %d lines)",
                      old_start + 1, len(result))
            return None

        result[actual_start : actual_start + len(hunk_old)] = hunk_new
        offset += len(hunk_new) - len(hunk_old)

    return "".join(result)


# ---------------------------------------------------------------------------
# Auto-apply via GitHub API (SIMPLE fixes only)
# ---------------------------------------------------------------------------

def apply_simple_fix(diff_text: str, program: str, context: dict[str, Any]) -> str | None:
    """
    Auto-apply a SIMPLE fix by:
      1. Creating a branch  self-healing/<product>-<timestamp>
      2. Applying the unified diff to the target file
      3. Committing and pushing via GitHub API
      4. Opening a pull request

    Returns the PR URL on success, None on failure.
    BLS API verify=False is BLOCKED. FRED/ALFRED/IMF verify=False is acceptable.
    """
    if not diff_text or diff_text.strip() in ("N/A", ""):
        log.warning("[GITHUB] No diff to apply")
        return None

    try:
        from tools.self_healing.secret_manager import get_secret
        import requests

        gh_token = get_secret("github-token")
        headers  = {
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        }

        target_file = _parse_diff_target(diff_text)
        if not target_file:
            log.error("[GITHUB] Could not determine target file from diff")
            return None

        log.info("[GITHUB] Auto-applying SIMPLE fix to %s", target_file)

        # ── Get default branch HEAD SHA ──────────────────────────────────────
        r = requests.get(
            f"https://api.github.com/repos/{REPO}/git/ref/heads/master",
            headers=headers, timeout=30,
        )
        if r.status_code != 200:
            log.error("[GITHUB] Could not get master HEAD: %s", r.status_code)
            return None
        base_sha = r.json()["object"]["sha"]

        # ── Create branch ────────────────────────────────────────────────────
        product   = context.get("product", "pipeline")
        ts        = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        branch    = f"self-healing/{product}-{ts}"
        r = requests.post(
            f"https://api.github.com/repos/{REPO}/git/refs",
            headers=headers,
            data=json.dumps({"ref": f"refs/heads/{branch}", "sha": base_sha}),
            timeout=30,
        )
        if r.status_code not in (200, 201):
            log.error("[GITHUB] Branch creation failed: %s", r.text[:200])
            return None
        log.info("[GITHUB] Branch created: %s", branch)

        # ── Get current file content ─────────────────────────────────────────
        r = requests.get(
            f"https://api.github.com/repos/{REPO}/contents/{target_file}",
            headers=headers,
            params={"ref": branch},
            timeout=30,
        )
        if r.status_code != 200:
            log.error("[GITHUB] File not found: %s", target_file)
            return None
        file_data    = r.json()
        current_sha  = file_data["sha"]
        current_text = base64.b64decode(file_data["content"]).decode("utf-8")

        # ── Apply the diff ───────────────────────────────────────────────────
        new_text = _apply_unified_diff(current_text, diff_text)
        if new_text is None:
            log.error("[GITHUB] Diff application failed — hunk mismatch")
            return None
        if new_text == current_text:
            log.warning("[GITHUB] Diff produced no change — already applied?")
            return None

        # ── Commit to branch ─────────────────────────────────────────────────
        commit_msg = (
            f"[AUTO-FIX] Self-healing patch: {context.get('product')} / "
            f"{context.get('country')}\n\n"
            f"Applied by Claude Sonnet 4.6 self-healing system.\n"
            f"Triggered by: {program}\n"
            f"Fix complexity: SIMPLE\n\n"
            f"Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
        )
        r = requests.put(
            f"https://api.github.com/repos/{REPO}/contents/{target_file}",
            headers=headers,
            data=json.dumps({
                "message": commit_msg,
                "content": base64.b64encode(new_text.encode("utf-8")).decode("utf-8"),
                "sha":     current_sha,
                "branch":  branch,
            }),
            timeout=30,
        )
        if r.status_code not in (200, 201):
            log.error("[GITHUB] Commit failed: %s", r.text[:200])
            return None
        log.info("[GITHUB] Committed fix to %s", branch)

        # ── Open pull request ────────────────────────────────────────────────
        r = requests.post(
            f"https://api.github.com/repos/{REPO}/pulls",
            headers=headers,
            data=json.dumps({
                "title": f"[AUTO-FIX] Self-healing: {context.get('product')} / {context.get('country')}",
                "head":  branch,
                "base":  "master",
                "body": (
                    f"## Automated Fix — Claude Sonnet 4.6\n\n"
                    f"**Product:** {context.get('product')}  \n"
                    f"**Country:** {context.get('country')}  \n"
                    f"**Source:** {context.get('source')}  \n"
                    f"**Run date:** {context.get('run_date')}  \n\n"
                    f"### File changed\n`{target_file}`\n\n"
                    f"### Diff\n```diff\n{diff_text}\n```\n\n"
                    f"This PR was opened automatically by the self-healing pipeline. "
                    f"Review and merge to apply the fix.\n\n"
                    f"🤖 Generated with [Claude Code](https://claude.com/claude-code)"
                ),
            }),
            timeout=30,
        )
        if r.status_code not in (200, 201):
            log.error("[GITHUB] PR creation failed: %s", r.text[:200])
            return None

        pr_url = r.json().get("html_url")
        log.info("[GITHUB] PR opened: %s", pr_url)
        return pr_url

    except Exception as exc:
        log.error("[GITHUB] Auto-apply failed: %s", exc)
        return None
