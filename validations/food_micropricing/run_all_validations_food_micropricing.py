"""
Master Validation Runner — USA Food Micropricing (BLS CPI + USDA ERS).

  Stage 1   PIT Validation
  Stage 2   Sanity Checks
  Stage 3   Schema Compliance
  Stage 4   Temporal Consistency
  Stage 4b  Temporal Coverage Audit
  Stage 5   Referential Integrity
  Stage 6   Lineage & Provenance
  Stage 7   Outlier Extraction
  Stage 8   Changelog Generation
  Stage GX  Universal GX Validation
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline_runner import run_pipeline

PRODUCT = "food_micropricing"
BASE    = "validations/food_micropricing"

STAGES = [
    {"id": 1,    "name": "PIT Validation",         "script": f"{BASE}/pit_validation_food.py",              "args": [], "required": True},
    {"id": 2,    "name": "Sanity Checks",           "script": f"{BASE}/sanity_check_food.py",                "args": [], "required": True},
    {"id": 3,    "name": "Schema Compliance",       "script": f"{BASE}/schema_compliance_food.py",           "args": [], "required": True},
    {"id": 4,    "name": "Temporal Consistency",    "script": f"{BASE}/temporal_consistency_food_pricing.py", "args": [], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit", "script": "validations/temporal_coverage/temporal_validator.py",
                                                     "args": ["validations/temporal_coverage/config_food_micropricing.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",   "script": f"{BASE}/referential_integrity_food_pricing.py", "args": [], "required": True},
    {"id": 6,    "name": "Lineage & Provenance",    "script": f"{BASE}/lineage_food_pricing.py",              "args": [], "required": True},
    {"id": 7,    "name": "Outlier Extraction",      "script": f"{BASE}/outlier_extractor_food.py",            "args": [], "required": False},
    {"id": 8,    "name": "Changelog Generation",    "script": f"{BASE}/changelog_generator_food.py",          "args": [], "required": False},
    {"id": "GX", "name": "Universal GX Validation", "script": "validations/gx_universal/universal_gx_validator.py",
                                                     "args": ["configs/gx_config_food_micropricing_vault.json"], "required": True},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope=None, timeout=1500))
