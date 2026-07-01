"""
Master Validation Runner — EU27 Trade Flows (Eurostat SDMX).

  Stage 1   PIT Validation
  Stage 2   Schema Compliance
  Stage 3   Sanity Checks
  Stage 4   Temporal Consistency
  Stage 4b  Temporal Coverage Audit
  Stage 5   Referential Integrity
  Stage 6   Outlier Extraction
  Stage 7   Changelog Generation
  Stage 8   Lineage & Provenance
  Stage GX  Universal GX Validation
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline_runner import run_pipeline

PRODUCT = "trade_flows"
BASE_EU = "validations/eurostat"
BASE_US = "validations/trade_flows"

STAGES = [
    {"id": 1,    "name": "PIT Validation",          "script": f"{BASE_EU}/pit_validation_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": True},
    {"id": 2,    "name": "Schema Compliance",        "script": f"{BASE_EU}/schema_compliance_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": True},
    {"id": 3,    "name": "Sanity Checks",            "script": f"{BASE_EU}/sanity_check_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": True},
    {"id": 4,    "name": "Temporal Consistency",     "script": f"{BASE_EU}/temporal_consistency_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit",  "script": "validations/temporal_coverage/temporal_validator.py",
                                                     "args": ["validations/temporal_coverage/config_trade_eu27.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",    "script": f"{BASE_EU}/referential_integrity_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": True},
    {"id": 6,    "name": "Outlier Extraction",       "script": f"{BASE_EU}/outlier_extractor_eurostat.py",
                                                     "args": ["--product", PRODUCT], "required": False},
    {"id": 7,    "name": "Changelog Generation",     "script": f"{BASE_US}/changelog_generator_trade_flows.py",
                                                     "args": ["--eu27"], "required": False},
    {"id": 8,    "name": "Lineage & Provenance",     "script": f"{BASE_US}/lineage_trade_flows.py",
                                                     "args": ["--eu27"], "required": False},
    {"id": "GX", "name": "Universal GX Validation",  "script": "validations/gx_universal/universal_gx_validator.py",
                                                     "args": ["configs/gx_config_trade_eu27_vault.json"], "required": True},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope="EU27", timeout=900))
