"""Sanity checks for repository README assets."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_readme_logo_asset_exists():
    """README should reference a tracked logo asset that exists in the repo."""
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    asset = "docs/assets/rektaslug-logo.svg"

    assert asset in readme
    assert (REPO_ROOT / asset).is_file()
