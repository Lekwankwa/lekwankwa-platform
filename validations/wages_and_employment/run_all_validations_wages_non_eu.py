"""
Master Validation Runner — Non-EU Wages & Employment (GBR/CAN).

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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline_runner import run_pipeline

PRODUCT = "wages_and_employment"
BASE    = "validations/non_eu"

STAGES = [
    {"id": 1,    "name": "PIT Validation",         "script": f"{BASE}/pit_validation_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": True},
    {"id": 2,    "name": "Schema Compliance",       "script": f"{BASE}/schema_compliance_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": True},
    {"id": 3,    "name": "Sanity Checks",           "script": f"{BASE}/sanity_check_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": True},
    {"id": 4,    "name": "Temporal Consistency",    "script": f"{BASE}/temporal_consistency_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit", "script": "validations/temporal_coverage/temporal_validator.py",
                                                    "args": ["validations/temporal_coverage/config_wages_non_eu.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",   "script": f"{BASE}/referential_integrity_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": True},
    {"id": 6,    "name": "Outlier Extraction",      "script": f"{BASE}/outlier_extractor_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": False},
    {"id": 7,    "name": "Changelog Generation",    "script": f"{BASE}/changelog_generator_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": False},
    {"id": 8,    "name": "Lineage & Provenance",    "script": f"{BASE}/lineage_non_eu.py",
                                                    "args": ["--product", PRODUCT], "required": False},
    {"id": "GX", "name": "Universal GX Validation", "script": "validations/gx_universal/universal_gx_validator.py",
                                                    "args": ["configs/gx_config_wages_non_eu_vault.json"], "required": True},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope="non_eu_GBR_CAN", timeout=900))
