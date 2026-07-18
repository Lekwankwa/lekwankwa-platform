"""
Master Validation Runner — USA Wages & Employment (BLS CES + CPS).

  Stage 1   PIT Validation
  Stage 2   Sanity Checks
  Stage 3   Schema Compliance
  Stage 4   Temporal Consistency
  Stage 4b  Temporal Coverage Audit
  Stage 5   Referential Integrity
  Stage 6   Lineage
  Stage 7   Outlier Extraction
  Stage 8   Changelog Generation
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline_runner import run_pipeline

PRODUCT = "wages_and_employment"
BASE    = "validations/wages_and_employment"

STAGES = [
    {"id": 1,    "name": "PIT Validation",         "script": f"{BASE}/pit_validation_employment.py",       "args": [], "required": True},
    {"id": 2,    "name": "Sanity Checks",           "script": f"{BASE}/sanity_check_employment.py",         "args": [], "required": True},
    {"id": 3,    "name": "Schema Compliance",       "script": f"{BASE}/schema_compliance_employment.py",    "args": [], "required": True},
    {"id": 4,    "name": "Temporal Consistency",    "script": f"{BASE}/temporal_consistency_employment.py", "args": [], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit", "script": "validations/temporal_coverage/temporal_validator.py",
                                                     "args": ["validations/temporal_coverage/config_wages_and_employment.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",   "script": f"{BASE}/referential_integrity_employment.py", "args": [], "required": True},
    {"id": 6,    "name": "Lineage",                 "script": f"{BASE}/lineage_macro_employment.py",         "args": [], "required": True},
    {"id": 7,    "name": "Outlier Extraction",      "script": f"{BASE}/outlier_extractor_employment.py",    "args": [], "required": False},
    {"id": 8,    "name": "Changelog Generation",    "script": f"{BASE}/changelog_generator_employment.py",  "args": [], "required": False},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope=None, timeout=1500))
