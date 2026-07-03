"""
tools/vault_audit.py — Lekwankwa Corporation
Programmatic gateway to the 9-stage + GX Universal + Bitemporal Core
validation pipeline.

Called by scraper run.py entry points after every successful vault write.
Routes to the correct product/country run_all_validations_*.py script
and returns a structured result the self-healing handler can act on.

Validation stages (per product):
  A1  PIT validation          (bitemporal_core.py)
  A2  Schema compliance
  A3  Sanity check
  A4  Temporal consistency
  A5  Referential integrity
  A6  Outlier extractor
  A7  Changelog generator
  A8  Lineage auditor
  B   Bitemporal Core         (bitemporal_core.py — shared root)
  GX  GX Universal validator  (gx_universal/universal_gx_validator.py)
"""
from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

REPO_ROOT  = Path(__file__).resolve().parents[1]
VAL_ROOT   = REPO_ROOT / "validations"

# Map (product, geo_scope) → run_all_validations script, relative to REPO_ROOT
_SCRIPT_MAP: dict[tuple[str, str], str] = {
    # food_micropricing
    ("food_micropricing",                    "USA"):    "validations/food_micropricing/run_all_validations_food_micropricing.py",
    ("food_micropricing",                    "EU27"):   "validations/food_micropricing/run_all_validations_food_eu27.py",
    ("food_micropricing",                    "non_eu"): "validations/food_micropricing/run_all_validations_food_non_eu.py",
    # wages_and_employment
    ("wages_and_employment",                 "USA"):    "validations/wages_and_employment/run_all_validations_employment.py",
    ("wages_and_employment",                 "EU27"):   "validations/wages_and_employment/run_all_validations_employment_eu27.py",
    ("wages_and_employment",                 "non_eu"): "validations/wages_and_employment/run_all_validations_wages_non_eu.py",
    # Housing_Supply_and_Shelter_Inflation
    ("Housing_Supply_and_Shelter_Inflation", "USA"):    "validations/housing/run_all_validations_housing.py",
    ("Housing_Supply_and_Shelter_Inflation", "EU27"):   "validations/housing/run_all_validations_housing_eu27.py",
    ("Housing_Supply_and_Shelter_Inflation", "non_eu"): "validations/housing/run_all_validations_housing_non_eu.py",
    # trade_flows
    ("trade_flows",                          "USA"):    "validations/trade_flows/run_all_validations_trade_flows.py",
    ("trade_flows",                          "EU27"):   "validations/trade_flows/run_all_validations_trade_eu27.py",
    ("trade_flows",                          "non_eu"): "validations/trade_flows/run_all_validations_trade_non_eu.py",
    # global_macro
    ("global_macro",                         "USA"):    "validations/imf_global_macro/run_all_validations_imf.py",
    ("global_macro",                         "EU27"):   "validations/imf_global_macro/run_all_validations_macro_eu27.py",
    ("global_macro",                         "non_eu"): "validations/imf_global_macro/run_all_validations_macro_non_eu.py",
}

EU27_MEMBERS = frozenset({
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU",
    "GRC","HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT",
    "ROU","SVK","SVN","ESP","SWE",
})
NON_EU_COUNTRIES = frozenset({"GBR","CAN","AUS","NOR"})


def _geo_scope(country: str) -> str:
    if country == "EU27":
        return "EU27"
    if country in EU27_MEMBERS:
        return "EU27"
    if country in NON_EU_COUNTRIES:
        return "non_eu"
    return "USA"   # default (USA, and catch-all for single-country)


@dataclass
class ValidationResult:
    overall: str           # "PASS" | "FAIL" | "ERROR"
    severity: str          # "NONE" | "HIGH" | "CRITICAL"
    code: str              # short code e.g. "VALIDATION_FAIL_STAGE_3"
    stage_results: list[dict] = field(default_factory=list)
    script_used: str = ""
    returncode: int = 0
    stdout_tail: str = ""  # last ~2000 chars of subprocess stdout+stderr (failures only)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall":      self.overall,
            "severity":     self.severity,
            "code":         self.code,
            "stages":       self.stage_results,
            "script_used":  self.script_used,
            "returncode":   self.returncode,
            "stdout_tail":  self.stdout_tail,
        }


def run_9_stage_validation(
    product: str,
    country: str,
    timeout: int = 3000,
) -> ValidationResult:
    """
    Run the full 9-stage + GX + Bitemporal Core validation pipeline for
    a product/country combination.

    Delegates to the appropriate run_all_validations_*.py via subprocess
    so each validation stage runs in a clean process (matches how the
    pipeline_runner.py orchestrator works).

    Returns ValidationResult with severity CRITICAL / HIGH / NONE.
    CRITICAL → do not write to vault, trigger self-healing.
    HIGH     → write to vault with warning, trigger self-healing.
    NONE     → all clear.
    """
    geo   = _geo_scope(country)
    key   = (product, geo)
    script_rel = _SCRIPT_MAP.get(key)

    if script_rel is None:
        log.warning("No validation script mapped for %s/%s (geo=%s) — skipping validation",
                    product, country, geo)
        return ValidationResult(
            overall="SKIP", severity="NONE",
            code="NO_VALIDATION_SCRIPT", script_used="",
        )

    script_path = REPO_ROOT / script_rel
    if not script_path.exists():
        log.error("Validation script not found: %s", script_path)
        return ValidationResult(
            overall="ERROR", severity="HIGH",
            code=f"VALIDATION_SCRIPT_MISSING:{script_rel}",
            script_used=script_rel,
        )

    log.info("[VALIDATION] Running %s for %s/%s ...", script_rel, product, country)
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        rc = result.returncode
        combined = (result.stdout or "") + (result.stderr or "")
        if rc == 0:
            log.info("[VALIDATION] PASS — %s/%s", product, country)
            return ValidationResult(
                overall="PASS", severity="NONE",
                code="ALL_STAGES_PASS", script_used=script_rel, returncode=0,
            )
        else:
            # Determine severity from stdout — look for CRITICAL or FAIL keywords
            out_upper = combined.upper()
            severity  = "CRITICAL" if "CRITICAL" in out_upper else "HIGH"
            # 2000 chars was nowhere near enough for a 10-stage pipeline's
            # print output — the actual failing stage and summary board were
            # always truncated off. 20000 chars comfortably covers the full
            # per-stage summary section even for verbose stages.
            tail = combined[-20000:]
            log.error("[VALIDATION] FAIL (rc=%d, severity=%s) — %s/%s\n%s",
                      rc, severity, product, country, tail)
            return ValidationResult(
                overall="FAIL", severity=severity,
                code=f"VALIDATION_FAIL_RC{rc}",
                script_used=script_rel, returncode=rc, stdout_tail=tail,
            )
    except subprocess.TimeoutExpired as exc:
        # subprocess still captures whatever the child produced before the
        # kill when capture_output=True was used — surface it, otherwise a
        # timeout tells us nothing about which stage was actually running.
        partial = (exc.output or "") + (exc.stderr or "")
        tail = partial[-20000:]
        log.error("[VALIDATION] TIMEOUT after %ds — %s/%s\n%s",
                  timeout, product, country, tail)
        return ValidationResult(
            overall="TIMEOUT", severity="HIGH",
            code="VALIDATION_TIMEOUT", script_used=script_rel, returncode=-1,
            stdout_tail=tail,
        )
    except Exception as exc:
        log.error("[VALIDATION] Unexpected error running %s: %s", script_rel, exc)
        return ValidationResult(
            overall="ERROR", severity="HIGH",
            code=f"VALIDATION_ERROR:{type(exc).__name__}",
            script_used=script_rel, returncode=-2,
        )
