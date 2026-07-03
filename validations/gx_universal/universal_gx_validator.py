"""
Universal GX-Style Validator (Parameterized)

Validates product data sources against a universal schema defined in a config file.
Uses direct pandas validation to avoid Great Expectations API compatibility issues.

This validator works with ANY product line by accepting a config file path as an argument.
Supports both single-country (USA) and multi-country (EU27) vault layouts.

Usage:
  python validations/gx_universal/universal_gx_validator.py configs/gx_config_food_micropricing_vault.json
  python validations/gx_universal/universal_gx_validator.py configs/gx_config_wages_eu27_vault.json

Config options for EU27 / multi-country vaults:
  vault_path     — product-level path (omit country=XXX)
  source_filter  — e.g. "eurostat_sdmx"; generates explicit Hive glob pattern
  excluded_countries — list of ISO3 codes to skip (e.g. ["USA"])

Author: Lekwankwa Corporation
Date: 2026-06-07 | Updated 2026-06-17 (EU27 multi-country support)
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import sys
import argparse
import fnmatch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import vault_glob_since as vault_glob, vault_read_parquet  # noqa: E402


def load_config(config_path: str) -> dict:
    """Load validation configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[FAIL] Error: Config file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[FAIL] Error: Invalid JSON in config file: {e}")
        sys.exit(1)


def run_validation(config_path: str) -> bool:
    """
    Run universal validation checks on product data.
    
    Returns:
        bool: True if all checks passed, False otherwise
    """
    # Load configuration
    config = load_config(config_path)
    product_code = config.get('product_code', 'unknown')
    product_name = config.get('product_name', 'Unknown Product')
    
    print("="*80)
    print(f"{product_name.upper()} - UNIVERSAL GX VALIDATION")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Config: {product_name} v{config.get('schema_version', 'N/A')}")
    print(f"Sources: {', '.join(config.get('sources', []))}")
    print()
    
    # Get vault path from config
    vault_path = config.get('vault_path')
    if not vault_path:
        print("[FAIL] Error: 'vault_path' not defined in config")
        return False
    
    # Find all parquet files — three routing strategies (checked in order):
    #   1. source_filter  — EU27/multi-country: explicit Hive glob per source
    #   2. source_file_map — single-country: per-source filename routing
    #   3. file_patterns  — fallback glob patterns
    # Always exclude outliers/changelog files.
    parquet_files = []
    source_filter    = config.get('source_filter')       # e.g. "eurostat_sdmx"
    source_file_map  = config.get('source_file_map', {})
    sources          = config.get('sources', [])
    excluded_countries = set(config.get('excluded_countries', []))

    # vault_glob recursively lists everything under vault_path matching the
    # filename suffix — fetch once and filter in Python, since it (unlike
    # stdlib glob) also works against gs:// paths via gcsfs.
    all_files = vault_glob(vault_path, "*.parquet")

    if source_filter:
        # Multi-country Hive layout: vault_path/country=*/source=X/year=*/month=*/*.parquet
        matches = [f for f in all_files if f"source={source_filter}" in f]
        for f in matches:
            if excluded_countries:
                # Extract country from path segment country=XXX
                parts = f.split("/")
                skip = False
                for part in parts:
                    if part.startswith("country="):
                        iso = part.split("=", 1)[1]
                        if iso in excluded_countries:
                            skip = True
                            break
                if skip:
                    continue
            parquet_files.append(f)
    elif source_file_map and sources:
        # Per-source explicit filename routing (single-country USA layout)
        for src in sources:
            fname = source_file_map.get(src)
            if not fname:
                continue
            matches = [f for f in all_files if f"source={src}" in f and f.endswith(fname)]
            parquet_files.extend(matches)
        if not parquet_files:
            fname = list(source_file_map.values())[0]
            parquet_files = [f for f in all_files if f.endswith(fname)]
    else:
        file_patterns = config.get('file_patterns', [
            "*base_data.parquet",
            "**/*_data.parquet",
            "**/*_pit_*.parquet",
        ])
        for pattern in file_patterns:
            glob_pattern = f"*{pattern.replace('**/', '')}"
            matches = [f for f in all_files if fnmatch.fnmatch(f, glob_pattern)]
            parquet_files.extend([
                f for f in matches
                if "outliers" not in f and "changelog" not in f
            ])

    # Deduplicate and exclude outliers/changelog
    parquet_files = list(set(
        f for f in parquet_files
        if "outliers" not in f and "changelog" not in f
    ))
    
    if not parquet_files:
        print(f"[FAIL] Error: No data files found in {vault_path}")
        return False
    
    print(f"Found {len(parquet_files)} data files")
    
    # Combine all data for validation (use_nullable_dtypes for cross-product column variance)
    frames = []
    for f in parquet_files:
        try:
            frames.append(vault_read_parquet(f))
        except Exception as e:
            print(f"  [WARN] Could not read {f}: {e}")
    if not frames:
        print(f"[FAIL] Error: No readable data files in {vault_path}")
        return False
    all_data = pd.concat(frames, ignore_index=True)
    print(f"Total records to validate: {len(all_data):,}")
    
    # Show source breakdown (if source column exists)
    if 'source' in all_data.columns:
        source_counts = all_data['source'].value_counts()
        print("\nRecords by source:")
        for source, count in source_counts.items():
            print(f"  {source.upper()}: {count:,} records")
    
    # Show dataset breakdown (if dataset column exists)
    if 'dataset' in all_data.columns:
        dataset_counts = all_data['dataset'].value_counts()
        print("\nRecords by dataset:")
        for dataset, count in dataset_counts.items():
            print(f"  {dataset}: {count:,} records")
    
    print()
    print("Running universal validation checks...")
    print()
    
    # Track results
    checks_passed = 0
    checks_failed = 0
    check_num = 1
    
    # Check 1: Row count
    try:
        min_rows = config['critical_checks']['row_count']['min']
        max_rows = config['critical_checks']['row_count']['max']
        if min_rows <= len(all_data) <= max_rows:
            print(f"[PASS] CHECK {check_num}: Row count within expected range ({len(all_data):,})")
            checks_passed += 1
        else:
            print(f"[FAIL] CHECK {check_num}: Row count out of range: {len(all_data):,} (expected {min_rows:,}-{max_rows:,})")
            checks_failed += 1
        check_num += 1
    except Exception as e:
        print(f"[FAIL] CHECK {check_num} FAILED: {e}")
        checks_failed += 1
        check_num += 1
    
    # Check 2: Required columns present
    required_columns = config['critical_checks'].get('required_columns', [])
    try:
        missing = set(required_columns) - set(all_data.columns)
        if not missing:
            print(f"[PASS] CHECK {check_num}: All {len(required_columns)} required columns present")
            checks_passed += 1
        else:
            print(f"[FAIL] CHECK {check_num}: Missing columns: {missing}")
            checks_failed += 1
        check_num += 1
    except Exception as e:
        print(f"[FAIL] CHECK {check_num} FAILED: {e}")
        checks_failed += 1
        check_num += 1
    
    # Check 3: Non-null fields
    non_null_fields = config['critical_checks'].get('non_null_fields', [])
    null_violations = {}
    for field in non_null_fields:
        if field in all_data.columns:
            null_count = all_data[field].isna().sum()
            if null_count > 0:
                null_violations[field] = null_count
    
    if not null_violations:
        print(f"[PASS] CHECK {check_num}: No null values in {len(non_null_fields)} critical fields")
        checks_passed += 1
    else:
        print(f"[FAIL] CHECK {check_num}: Null violations: {null_violations}")
        checks_failed += 1
    check_num += 1
    
    # Check 4: Value constraints
    value_constraints = config['critical_checks'].get('value_constraints', {})
    for field, constraints in value_constraints.items():
        if field in all_data.columns:
            try:
                min_val = constraints.get('min')
                max_val = constraints.get('max')
                all_data[field] = pd.to_numeric(all_data[field], errors='coerce')
                invalid = ((all_data[field] < min_val) | (all_data[field] > max_val)).sum()
                if invalid == 0:
                    print(f"[PASS] CHECK {check_num}: All {field} values in valid range")
                    checks_passed += 1
                else:
                    print(f"[FAIL] CHECK {check_num}: {invalid} {field} values out of range")
                    checks_failed += 1
                check_num += 1
            except Exception as e:
                print(f"[FAIL] CHECK {check_num} FAILED for {field}: {e}")
                checks_failed += 1
                check_num += 1
    
    # Check 5: Categorical constraints
    categorical_constraints = config['critical_checks'].get('categorical_constraints', {})
    for field, valid_values in categorical_constraints.items():
        if field in all_data.columns:
            try:
                invalid = set(all_data[field].dropna().unique()) - set(valid_values)
                if not invalid:
                    print(f"[PASS] CHECK {check_num}: All {field} values are valid")
                    checks_passed += 1
                else:
                    print(f"[FAIL] CHECK {check_num}: Invalid {field} values: {invalid}")
                    checks_failed += 1
                check_num += 1
            except Exception as e:
                print(f"[FAIL] CHECK {check_num} FAILED for {field}: {e}")
                checks_failed += 1
                check_num += 1
    
    # Check 6: Timestamp validation
    timestamp_checks = config['critical_checks'].get('timestamp_checks', {})
    for field, constraints in timestamp_checks.items():
        if field in all_data.columns:
            try:
                dates = pd.to_datetime(all_data[field], errors='coerce')
                invalid_dates = dates.isna().sum()
                
                if invalid_dates == 0:
                    years = dates.dt.year
                    min_year = constraints.get('min_year')
                    max_year = constraints.get('max_year')
                    out_of_range = ((years < min_year) | (years > max_year)).sum()
                    
                    if out_of_range == 0:
                        print(f"[PASS] CHECK {check_num}: {field} timestamps valid ({years.min()}-{years.max()})")
                        checks_passed += 1
                    else:
                        print(f"[FAIL] CHECK {check_num}: {out_of_range} {field} timestamps out of range")
                        checks_failed += 1
                else:
                    print(f"[FAIL] CHECK {check_num}: {invalid_dates} invalid {field} timestamps")
                    checks_failed += 1
                check_num += 1
            except Exception as e:
                print(f"[FAIL] CHECK {check_num} FAILED for {field}: {e}")
                checks_failed += 1
                check_num += 1
    
    # PIT Validation Checks (if present)
    pit_checks = config['critical_checks'].get('pit_validation_checks', {})
    if pit_checks:
        print()
        print("Point-in-Time (PIT) Validation:")
        print()
        
        # Temporal consistency
        if 'check_11_temporal_consistency' in pit_checks:
            if 'published_date' in all_data.columns and 'data_timestamp' in all_data.columns:
                try:
                    pub_dates = pd.to_datetime(all_data['published_date'])
                    data_dates = pd.to_datetime(all_data['data_timestamp'])
                    violations = (pub_dates < data_dates).sum()
                    if violations == 0:
                        print(f"[PASS] CHECK {check_num}: Temporal consistency validated")
                        checks_passed += 1
                    else:
                        print(f"[FAIL] CHECK {check_num}: {violations} temporal consistency violations")
                        checks_failed += 1
                    check_num += 1
                except Exception as e:
                    print(f"[FAIL] CHECK {check_num} FAILED: {e}")
                    checks_failed += 1
                    check_num += 1
        
        # Unique record IDs
        if 'check_13_unique_record_id' in pit_checks:
            if 'record_id' in all_data.columns:
                try:
                    duplicates = all_data['record_id'].duplicated().sum()
                    if duplicates == 0:
                        print(f"[PASS] CHECK {check_num}: All record IDs unique")
                        checks_passed += 1
                    else:
                        print(f"[FAIL] CHECK {check_num}: {duplicates} duplicate record IDs")
                        checks_failed += 1
                    check_num += 1
                except Exception as e:
                    print(f"[FAIL] CHECK {check_num} FAILED: {e}")
                    checks_failed += 1
                    check_num += 1
    
    # Summary
    total_checks = checks_passed + checks_failed
    pass_rate = (checks_passed / total_checks * 100) if total_checks > 0 else 0
    
    print()
    print("="*80)
    print(f"VALIDATION COMPLETE: {'[PASS] PASS' if checks_failed == 0 else '[FAIL] FAIL'}")
    print(f"Checks Passed: {checks_passed}/{total_checks} ({pass_rate:.1f}%)")
    print("="*80)
    
    # Save report
    report = {
        "timestamp": datetime.now().isoformat(),
        "product": product_code,
        "product_name": product_name,
        "total_records": len(all_data),
        "total_files": len(parquet_files),
        "checks_run": total_checks,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "pass_rate": f"{pass_rate:.1f}%",
        "status": "PASS" if checks_failed == 0 else "FAIL"
    }
    
    report_path = Path(f"{product_code}_gx_validation_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Report saved: {report_path}")
    print()
    
    return checks_failed == 0


def main():
    parser = argparse.ArgumentParser(description="Universal GX-style validator")
    parser.add_argument('config', help='Path to config JSON file')
    args = parser.parse_args()
    
    success = run_validation(args.config)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

