"""Cloud Function entry point for pit_disclosure_generator."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from tools.pit_disclosure_generator import cloud_function_handler  # noqa: F401
