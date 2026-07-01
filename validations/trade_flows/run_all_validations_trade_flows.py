"""
Master Validation Runner — USA Trade Flows (US Census FT-900).

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

PRODUCT = "trade_flows"
BASE    = "validations/trade_flows"

STAGES = [
    {"id": 1,    "name": "PIT Validation",          "script": f"{BASE}/pit_validation_trade_flows.py",       "args": [], "required": True},
    {"id": 2,    "name": "Sanity Checks",            "script": f"{BASE}/sanity_check_trade_flows.py",         "args": [], "required": True},
    {"id": 3,    "name": "Schema Compliance",        "script": f"{BASE}/schema_compliance_trade_flows.py",    "args": [], "required": True},
    {"id": 4,    "name": "Temporal Consistency",     "script": f"{BASE}/temporal_consistency_trade_flows.py", "args": [], "required": True},
    {"id": "4b", "name": "Temporal Coverage Audit",  "script": "validations/temporal_coverage/temporal_validator.py",
                                                      "args": ["validations/temporal_coverage/config_trade_flows.json"], "required": True},
    {"id": 5,    "name": "Referential Integrity",    "script": f"{BASE}/referential_integrity_trade_flows.py", "args": [], "required": True},
    {"id": 6,    "name": "Lineage",                  "script": f"{BASE}/lineage_trade_flows.py",               "args": [], "required": True},
    {"id": 7,    "name": "GX Universal Validation",  "script": "validations/gx_universal/universal_gx_validator.py",
                                                      "args": ["configs/gx_config_trade_flows_vault.json"],   "required": True},
    {"id": 8,    "name": "Outlier Extraction",       "script": f"{BASE}/outlier_extractor_trade_flows.py",   "args": [], "required": False},
    {"id": 9,    "name": "Changelog Generation",     "script": f"{BASE}/changelog_generator_trade_flows.py", "args": [], "required": False},
]

if __name__ == "__main__":
    sys.exit(run_pipeline(PRODUCT, STAGES, scope=None, timeout=600,
                          on_required_fail="continue"))
