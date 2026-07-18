"""
Master Validation Runner — USA Global Macro (IMF WEO).

  Stage 1   PIT Validation
  Stage 2   Sanity Checks
  Stage 3   Schema Compliance
  Stage 4   Temporal Consistency
  Stage 4b  Temporal Coverage Audit
  Stage 5   Referential Integrity
  Stage 6   Lineage
  Stage 7   GX Universal Validation
  Stage 8   Outlier Extraction
  Stage 9   Changelog Generation
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline_runner import run_pipeline

PRODUCT = "global_macro"
BASE    = "validations/imf_global_macro"

STAGES = [
    {"id": 1,    "name": "PIT Validation",          "script": f"{BASE}/pit_validation_imf.py",        "args": [], "required": True},
    {"id": 2,    "name": "Sanity Checks",            "script": f"{BASE}/sanity_check_imf.py",           "args": [], "required": True},
    {"id": 3,    "name": "Schema Compliance",        "script": f"{BASE}/schema_compliance_imf.py",      "args": [], "required": True},
    {"id": 4,    "name": "Temporal Consistency",     "script": f"{BASE}/temporal_consistency_imf.py",   "args": [], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit",  "script": "validations/temporal_coverage/temporal_validator.py",
                                                      "args": ["validations/temporal_coverage/config_global_macro.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",    "script": f"{BASE}/referential_integrity_imf.py",  "args": [], "required": True},
    {"id": 6,    "name": "Lineage",                  "script": f"{BASE}/lineage_imf.py",                "args": [], "required": True},
    {"id": 7,    "name": "GX Universal Validation",  "script": f"{BASE}/gx_validation_imf.py",          "args": [], "required": True},
    {"id": 8,    "name": "Outlier Extraction",       "script": f"{BASE}/outlier_extractor_imf.py",      "args": [], "required": False},
    {"id": 9,    "name": "Changelog Generation",     "script": f"{BASE}/changelog_generator_imf.py",   "args": [], "required": False},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope=None, timeout=1500,
                          on_required_fail="skip_required_run_optional"))
