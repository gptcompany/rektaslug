"""Deployment/runtime config tests for the production core stack."""

from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_docker_compose_defines_core_services():
    """docker-compose.yml should define the API and sync services."""
    compose_path = REPO_ROOT / "docker-compose.yml"
    data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    services = data["services"]
    assert "rektslug-api" in services
    assert "rektslug-sync" in services

    api = services["rektslug-api"]
    sync = services["rektslug-sync"]

    assert api["restart"] == "unless-stopped"
    assert sync["restart"] == "unless-stopped"
    assert "healthcheck" in api
    assert api["ports"] == ["${HEATMAP_PORT:-8002}:8002"]
    assert sync["depends_on"]["rektslug-api"]["condition"] == "service_healthy"


def test_docker_compose_mounts_db_and_ccxt_catalog():
    """The core stack should mount the local DB dir and external ccxt catalog."""
    compose_path = REPO_ROOT / "docker-compose.yml"
    data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    volumes = data["services"]["rektslug-api"]["volumes"]

    assert any("REKTSLUG_DB_DIR" in entry for entry in volumes)
    assert any("HEATMAP_CCXT_CATALOG" in entry for entry in volumes)


def test_core_deploy_workflow_targets_core_code():
    """The deploy workflow should react to core code and container runtime changes."""
    workflow_path = REPO_ROOT / ".github" / "workflows" / "core-deploy.yml"
    text = workflow_path.read_text(encoding="utf-8")

    assert "src/liquidationheatmap/**" in text
    assert "docker-compose.yml" in text
    assert "docker/build-push-action" in text
    assert "appleboy/ssh-action" in text
