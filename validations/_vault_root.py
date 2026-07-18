"""
validations/_vault_root.py — shared VAULT_ROOT resolution for validation scripts.

Every validation stage script historically hardcoded a local relative path
(Path("lekwankwa-historical-vault")), which silently returns "not found" for
every check when VAULT_ROOT=gs://... is set (Cloud Run production). This
module gives every stage script the same env-aware, GCS-capable primitives.

Usage:
    from _vault_root import VAULT_ROOT, vault_exists, vault_glob

    src_path = f"{VAULT_ROOT}/product={PRODUCT}/country={COUNTRY}/source={source}"
    if not vault_exists(src_path):
        ...
    for f in vault_glob(src_path, "*.parquet"):
        df = pd.read_parquet(f)   # pandas + gcsfs handles gs:// paths directly
"""
from __future__ import annotations

import os
import re
from pathlib import Path

VAULT_ROOT = os.environ.get("VAULT_ROOT", "").strip().rstrip("/") or "lekwankwa-historical-vault"
IS_GCS = VAULT_ROOT.startswith("gs://")

_YEAR_RE = re.compile(r"year=(\d{4})")


def vault_glob_since(path_str: str, pattern: str) -> list[str]:
    """
    Same as vault_glob(), but scoped to the incremental window instead of
    the full vault history.

    The vault has already been validated in full once; re-validating all
    ~46 years of history on every incremental run is what was making
    validation take 45-60+ minutes. run.py sets VALIDATION_SINCE_YEAR to
    the same start year the scraper itself used for its incremental fetch
    (see scrapers/utilities/incremental.py compute_scrape_range) before
    invoking validation, so both stay in lockstep. If the env var isn't
    set (e.g. a product not yet wired up, or ad-hoc/local runs), falls
    back to scanning everything — never silently narrows scope.
    """
    all_files = vault_glob(path_str, pattern)
    since_year = os.environ.get("VALIDATION_SINCE_YEAR", "").strip()
    if not since_year:
        return all_files
    try:
        cutoff = int(since_year)
    except ValueError:
        return all_files
    kept = []
    for f in all_files:
        m = _YEAR_RE.search(f)
        if m is None or int(m.group(1)) >= cutoff:
            kept.append(f)
    return kept


def vault_exists(path_str: str) -> bool:
    if IS_GCS:
        import gcsfs
        return gcsfs.GCSFileSystem().exists(path_str)
    return Path(path_str).exists()


def vault_glob(path_str: str, pattern: str) -> list[str]:
    """Return matching file paths under path_str, recursively, sorted."""
    if IS_GCS:
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(path_str):
            return []
        suffix = pattern.lstrip("*")
        # gcsfs.find() returns bare "bucket/key" paths (no "gs://" scheme),
        # which pandas/pyarrow then treat as local paths and fail to open.
        # Re-add the scheme so callers can pass results straight into
        # pd.read_parquet() / pd.DataFrame.to_parquet().
        return sorted(
            f"gs://{p}" for p in fs.find(path_str) if p.endswith(suffix)
        )
    base = Path(path_str)
    if not base.exists():
        return []
    return sorted(str(p).replace("\\", "/") for p in base.rglob(pattern))


def vault_subdirs(path_str: str, prefix: str) -> list["VaultFilePath"]:
    """
    Return immediate Hive-partition subdirectories under path_str whose
    name starts with `prefix` (e.g. "year=", "month="), as VaultFilePath
    objects — for scripts that walk year=/month= folders one level at a
    time (e.g. changelog generators) rather than just globbing files.
    """
    if IS_GCS:
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if not fs.exists(path_str):
            return []
        entries = [f"gs://{p}" for p in fs.ls(path_str, detail=False)]
        return sorted(
            VaultFilePath(e) for e in entries
            if e.rstrip("/").rsplit("/", 1)[-1].startswith(prefix)
        )
    base = Path(path_str)
    if not base.exists():
        return []
    return sorted(
        VaultFilePath(str(d).replace("\\", "/")) for d in base.iterdir()
        if d.is_dir() and d.name.startswith(prefix)
    )


class VaultFilePath:
    """
    Path-like wrapper whose .parent/.name use simple POSIX splitting, but whose
    __fspath__/str() preserve the original string untouched.

    pathlib.PurePosixPath normalizes "gs://bucket/x" down to "gs:/bucket/x"
    (collapses the double slash), which breaks gcsfs/pandas reads. This class
    keeps the original "gs://..." string intact for I/O while still supporting
    the .parent.parent.name-style chains used throughout the validation scripts.
    """
    __slots__ = ("_s",)

    def __init__(self, s: str) -> None:
        self._s = s

    def __fspath__(self) -> str:
        return self._s

    def __str__(self) -> str:
        return self._s

    def __repr__(self) -> str:
        return f"VaultFilePath({self._s!r})"

    @property
    def name(self) -> str:
        return self._s.rstrip("/").rsplit("/", 1)[-1]

    @property
    def parent(self) -> "VaultFilePath":
        head = self._s.rstrip("/").rsplit("/", 1)
        return VaultFilePath(head[0] if len(head) > 1 else self._s)

    def __lt__(self, other: "VaultFilePath") -> bool:
        return self._s < str(other)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, VaultFilePath) and self._s == other._s

    def __hash__(self) -> int:
        return hash(self._s)


def vault_glob_paths(path_str: str, pattern: str) -> list["VaultFilePath"]:
    """Same as vault_glob(), but wraps results in VaultFilePath for .parent/.name access."""
    return [VaultFilePath(p) for p in vault_glob(path_str, pattern)]


def vault_glob_paths_since(path_str: str, pattern: str) -> list["VaultFilePath"]:
    """Same as vault_glob_since(), but wraps results in VaultFilePath."""
    return [VaultFilePath(p) for p in vault_glob_since(path_str, pattern)]


def vault_read_parquet(path_str: str, columns: list[str] | None = None):
    """
    Read a single parquet file robustly.

    pandas.read_parquet() on a gs:// path routes through pyarrow's dataset
    API, which — for paths containing Hive-style "key=value" segments
    (year=2026/month=03/...) — can auto-discover a partitioned dataset
    rooted higher up the tree and try to unify schemas across unrelated
    sibling files, failing with "Unable to merge: Field ... incompatible
    types" even though only ONE specific file was requested. Reading via
    an explicit open file handle instead of a path string prevents pyarrow
    from ever inferring a directory/partitioning structure.
    """
    import pandas as pd
    path_str = str(path_str)  # accept VaultFilePath or other path-like objects too
    try:
        return pd.read_parquet(path_str, columns=columns)
    except Exception as exc:
        if "Unable to merge" not in str(exc):
            raise
        import pyarrow.parquet as pq
        if path_str.startswith("gs://"):
            import gcsfs
            with gcsfs.GCSFileSystem().open(path_str, "rb") as fh:
                table = pq.read_table(fh, columns=columns)
        else:
            with open(path_str, "rb") as fh:
                table = pq.read_table(fh, columns=columns)
        return table.to_pandas()


def vault_file_size_kb(path_str: str) -> float:
    if IS_GCS:
        import gcsfs
        try:
            return gcsfs.GCSFileSystem().info(path_str)["size"] / 1024
        except Exception:
            return 0.0
    try:
        return Path(path_str).stat().st_size / 1024
    except Exception:
        return 0.0
