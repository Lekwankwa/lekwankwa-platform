"""
scrapers/utilities/vault_io.py — Lekwankwa Corporation
=======================================================
Transparent local / GCS vault path handling.

Set VAULT_ROOT env var to redirect all scraper writes from local disk to GCS:
  export VAULT_ROOT=gs://lekwankwa-vault        # Cloud Run Jobs
  (unset)                                        # local dev — writes to disk

VaultPath is a drop-in replacement for pathlib.Path:
  - Supports / operator for path joining
  - .mkdir() is a no-op for gs:// paths (GCS has no real directories)
  - .exists() checks GCS or local
  - Implements __fspath__ so pandas read_parquet / to_parquet accept it directly
    with gcsfs installed, all gs:// reads/writes are handled automatically
"""
from __future__ import annotations

import os
from pathlib import Path


def _env_root() -> str | None:
    return os.environ.get("VAULT_ROOT", "").strip() or None


class VaultPath:
    """
    Path-like object that works transparently for both local and GCS paths.

    pandas/pyarrow accept VaultPath directly because __fspath__ is implemented.
    With gcsfs installed, gs:// reads and writes happen automatically.
    """
    __slots__ = ("_path",)

    def __init__(self, path: str) -> None:
        self._path = str(path).replace("\\", "/")

    # ── Path joining (mirrors pathlib.Path / operator) ────────────────────────
    def __truediv__(self, other: str) -> "VaultPath":
        other_str = str(other).replace("\\", "/").lstrip("/")
        return VaultPath(f"{self._path.rstrip('/')}/{other_str}")

    # ── String / OS protocol ──────────────────────────────────────────────────
    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"VaultPath('{self._path}')"

    def __fspath__(self) -> str:
        """Makes pd.read_parquet(path) and df.to_parquet(path) work directly."""
        return self._path

    # ── Directory ops ─────────────────────────────────────────────────────────
    @property
    def parent(self) -> "VaultPath":
        parts = self._path.rstrip("/").rsplit("/", 1)
        return VaultPath(parts[0]) if len(parts) > 1 else VaultPath(self._path)

    @property
    def name(self) -> str:
        return self._path.rstrip("/").rsplit("/", 1)[-1]

    def mkdir(self, parents: bool = True, exist_ok: bool = True) -> None:
        """Create directory for local paths. No-op for GCS (no real dirs)."""
        if not self._path.startswith("gs://"):
            Path(self._path).mkdir(parents=parents, exist_ok=exist_ok)

    # ── File ops ──────────────────────────────────────────────────────────────
    def exists(self) -> bool:
        if self._path.startswith("gs://"):
            import gcsfs
            return gcsfs.GCSFileSystem().exists(self._path)
        return Path(self._path).exists()


def get_vault_root(local_suffix: str) -> VaultPath:
    """
    Return a VaultPath for the given vault suffix.

    Without VAULT_ROOT env var: returns local path (relative or absolute as given).
    With VAULT_ROOT=gs://lekwankwa-vault: strips the lekwankwa-historical-vault
    prefix from local_suffix and appends the product/country/source remainder to
    the GCS bucket root.

    Examples
    --------
    Local (no env var):
        get_vault_root("lekwankwa-historical-vault/product=food/country=USA/source=bls")
        → VaultPath("lekwankwa-historical-vault/product=food/country=USA/source=bls")

    GCS (VAULT_ROOT=gs://lekwankwa-vault):
        get_vault_root("lekwankwa-historical-vault/product=food/country=USA/source=bls")
        → VaultPath("gs://lekwankwa-vault/product=food/country=USA/source=bls")

        get_vault_root("/opt/lekwankwa/lekwankwa-historical-vault")
        → VaultPath("gs://lekwankwa-vault")
    """
    env = _env_root()
    if env:
        normalized = str(local_suffix).replace("\\", "/")
        for marker in ("lekwankwa-historical-vault/", "lekwankwa-historical-vault"):
            if marker in normalized:
                after = normalized.split(marker, 1)[1].lstrip("/")
                result = f"{env.rstrip('/')}/{after}" if after else env.rstrip("/")
                return VaultPath(result)
        # No vault marker found — append as-is under env root
        return VaultPath(f"{env.rstrip('/')}/{normalized.lstrip('/')}")
    return VaultPath(local_suffix)
