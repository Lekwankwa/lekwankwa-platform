"""
temporal_validator.py
Lekwankwa Corporation

Central temporal coverage validator — Stage 4 Extension.
Reads a dataset-specific config file and checks year-level completeness
against the vault Hive partitions.

Usage:
  # Single dataset:
  python validations/temporal_coverage/temporal_validator.py \
      validations/temporal_coverage/config_wages_and_employment.json

  # All datasets at once:
  python validations/temporal_coverage/temporal_validator.py --all
"""

import json
import os
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime

_VAULT_ROOT = os.environ.get("VAULT_ROOT", "").strip() or "lekwankwa-historical-vault"


# ── Core mathematics ───────────────────────────────────────────────────────

def get_vault_years(product: str, country: str, source: str) -> set:
    """Scan Hive partition directories and return the set of years present."""
    base = f"{_VAULT_ROOT.rstrip('/')}/product={product}/country={country}/source={source}"
    years = set()

    if _VAULT_ROOT.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(base):
            return years
        for p in fs.find(base):
            m = re.search(r"year=(\d{4})", p)
            if m:
                years.add(int(m.group(1)))
    else:
        base_path = Path(base)
        if not base_path.exists():
            return years
        for p in base_path.iterdir():
            if p.is_dir() and p.name.startswith("year="):
                try:
                    years.add(int(p.name.split("=")[1]))
                except ValueError:
                    pass
    return years


def group_consecutive_runs(years: list) -> list:
    """Compress a sorted list of years into (start, end) range tuples."""
    if not years:
        return []
    runs, s, e = [], years[0], years[0]
    for y in years[1:]:
        if y == e + 1:
            e = y
        else:
            runs.append((s, e))
            s = e = y
    runs.append((s, e))
    return runs


def format_runs(runs: list) -> str:
    return ", ".join(str(s) if s == e else f"{s}–{e}" for s, e in runs)


def coverage_status(pct: float) -> str:
    if pct == 100.0:
        return "PASS"
    elif pct >= 80.0:
        return "WARN"
    else:
        return "FAIL"


def check_source_coverage(product: str, country: str, source: str,
                           year_start: int, year_end: int) -> dict:
    """Core mathematical check: expected years vs. present years for one source."""
    expected = set(range(year_start, year_end + 1))
    present  = get_vault_years(product, country, source)
    missing  = sorted(expected - present)
    extra    = sorted(present - expected)

    n_exp  = len(expected)
    n_pres = len(present)
    n_miss = len(missing)
    pct    = round(n_pres / n_exp * 100, 2) if n_exp else 0.0

    return {
        "product":        product,
        "country":        country,
        "source":         source,
        "expected_range": f"{year_start}–{year_end}",
        "n_expected":     n_exp,
        "n_present":      n_pres,
        "n_missing":      n_miss,
        "coverage_pct":   pct,
        "status":         coverage_status(pct),
        "missing_years":  missing,
        "extra_years":    extra,
        "missing_runs":   format_runs(group_consecutive_runs(missing)),
    }


# ── Config loader ──────────────────────────────────────────────────────────

def load_config(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"[ERROR] Config not found: {path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in {path}: {e}")
        sys.exit(1)


# ── Report printer ─────────────────────────────────────────────────────────

def print_report(config: dict, results: list) -> None:
    product_name = config["product_name"]
    print(f"\n{'='*70}")
    print(f"  TEMPORAL COVERAGE — {product_name.upper()}")
    print(f"  Vault product: {config['product']}")
    print(f"{'='*70}")

    for r in results:
        tag = {"PASS": "[OK  ]", "WARN": "[WARN]", "FAIL": "[FAIL]"}[r["status"]]
        print(f"\n  Source  : {r['source']}")
        print(f"  Expected: {r['expected_range']}  ({r['n_expected']} years)")
        print(f"  Present : {r['n_present']} years  ({r['coverage_pct']}%)")
        print(f"  Status  : {tag} {r['status']}")
        if r["missing_years"]:
            print(f"  Missing : {r['missing_runs']}  ({r['n_missing']} years)")
        else:
            print(f"  Missing : none")
        if r["extra_years"]:
            print(f"  Extra   : {r['extra_years'][:10]}")


# ── Gap report writer ──────────────────────────────────────────────────────

def write_gap_report(config: dict, results: list) -> Path:
    out = {
        "generated_at":  datetime.utcnow().isoformat() + "Z",
        "product":        config["product"],
        "product_name":   config["product_name"],
        "overall_status": "PASS" if all(r["status"] == "PASS" for r in results) else
                          "WARN" if all(r["status"] != "FAIL" for r in results) else "FAIL",
        "sources": {
            r["source"]: {
                "expected_range": r["expected_range"],
                "n_expected":     r["n_expected"],
                "n_present":      r["n_present"],
                "n_missing":      r["n_missing"],
                "coverage_pct":   r["coverage_pct"],
                "status":         r["status"],
                "missing_years":  r["missing_years"],
            }
            for r in results
        },
    }
    config_dir = Path(__file__).parent
    out_path = config_dir / f"gap_report_{config['product']}.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\n  Gap report written to: {out_path}")
    return out_path


# ── Runner ─────────────────────────────────────────────────────────────────

def run_config(config_path: Path) -> list:
    config  = load_config(config_path)
    product = config["product"]
    results = []

    # Multi-country support: "countries" list takes priority over single "country"
    countries = config.get("countries")
    if countries:
        # Aggregate: union of years across all countries (at least one must have each year)
        for source_cfg in config["sources"]:
            source     = source_cfg["source"]
            year_start = source_cfg["year_start"]
            year_end   = source_cfg["year_end"]
            expected   = set(range(year_start, year_end + 1))

            # Collect years present across ALL countries for this source
            years_per_country = {}
            for iso in countries:
                yrs = get_vault_years(product, iso, source)
                if yrs:
                    years_per_country[iso] = yrs

            # Union of all years found
            all_present = set()
            for yrs in years_per_country.values():
                all_present |= yrs

            missing  = sorted(expected - all_present)
            extra    = sorted(all_present - expected)
            n_exp    = len(expected)
            n_pres   = len(all_present)
            pct      = round(n_pres / n_exp * 100, 2) if n_exp else 0.0

            # Per-country summary
            countries_with_data = len(years_per_country)
            countries_missing   = [iso for iso in countries if iso not in years_per_country]

            result = {
                "product":          product,
                "country":          f"EU27 ({countries_with_data}/{len(countries)} countries)",
                "source":           source,
                "expected_range":   f"{year_start}–{year_end}",
                "n_expected":       n_exp,
                "n_present":        n_pres,
                "n_missing":        len(missing),
                "coverage_pct":     pct,
                "status":           coverage_status(pct),
                "missing_years":    missing,
                "extra_years":      extra,
                "missing_runs":     format_runs(group_consecutive_runs(missing)),
                "countries_missing_all_data": countries_missing,
            }
            results.append(result)

            if countries_missing:
                print(f"  [WARN] Countries with NO data for source={source}: {countries_missing}")
    else:
        country = config.get("country", "USA")
        for source_cfg in config["sources"]:
            result = check_source_coverage(
                product    = product,
                country    = country,
                source     = source_cfg["source"],
                year_start = source_cfg["year_start"],
                year_end   = source_cfg["year_end"],
            )
            results.append(result)

    print_report(config, results)
    write_gap_report(config, results)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Lekwankwa Temporal Coverage Validator"
    )
    parser.add_argument(
        "config", nargs="?",
        help="Path to a single dataset config JSON"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run all config files in the same directory"
    )
    args = parser.parse_args()

    config_dir = Path(__file__).parent

    if args.all:
        configs = sorted(config_dir.glob("config_*.json"))
        if not configs:
            print("[ERROR] No config_*.json files found.")
            sys.exit(1)
        all_results = []
        for cfg in configs:
            all_results.extend(run_config(cfg))

        # Overall summary
        total_miss = sum(r["n_missing"] for r in all_results)
        print(f"\n{'='*70}")
        print(f"  ALL PRODUCTS SUMMARY — total missing year-slots: {total_miss}")
        print(f"{'='*70}")
        for r in all_results:
            tag = {"PASS": "OK  ", "WARN": "WARN", "FAIL": "FAIL"}[r["status"]]
            print(f"  [{tag}] {r['product']}/{r['source']}  "
                  f"{r['n_present']}/{r['n_expected']} ({r['coverage_pct']}%)")

    elif args.config:
        run_config(Path(args.config))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
